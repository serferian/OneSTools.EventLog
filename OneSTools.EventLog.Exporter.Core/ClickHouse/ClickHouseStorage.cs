using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace OneSTools.EventLog.Exporter.Core.ClickHouse
{
    public class ClickHouseStorage : IEventLogStorage
    {
        private const string TableName = "EventLogItems";
        private readonly ILogger<ClickHouseStorage> _logger;
        private ClickHouseConnection _connection;
        private string _connectionString;
        private string _databaseName;
        private bool _ConvertJsonToSeparateTables;
        private readonly Dictionary<string, HashSet<string>> _dynamicTableColumns = new Dictionary<string, HashSet<string>>();
        private readonly object _columnsLock = new object();

        public ClickHouseStorage(ILogger<ClickHouseStorage> logger, IConfiguration configuration)
        {
            _logger = logger;
            _connectionString = configuration.GetValue("ClickHouse:ConnectionString", "");
            _ConvertJsonToSeparateTables = configuration.GetValue("ClickHouse:ConvertJsonToSeparateTables", false);
            Init();
        }

        public void Dispose()
        {
            if (_connection != null)
            {
                _connection.Dispose();
                _connection = null;
            }
        }

        private void Init()
        {
            if (string.IsNullOrWhiteSpace(_connectionString))
                throw new Exception("Connection string is not specified");

            _databaseName = Regex.Match(_connectionString, "(?<=Database=).*?(?=(;|$))", RegexOptions.IgnoreCase).Value;
            _connectionString = Regex.Replace(_connectionString, "Database=.*?(;|$)", "");

            if (string.IsNullOrWhiteSpace(_databaseName))
                throw new Exception("Database name is not specified");
            else
                _databaseName = FixDatabaseName(_databaseName);
        }

        private static string FixDatabaseName(string name)
        {
            return Regex.Replace(name, @"(?:\W|-)", "_", RegexOptions.Compiled);
        }

        private async Task CreateConnectionAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_connection == null)
            {
                _connection = new ClickHouseConnection(_connectionString);
                await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                if (_logger != null)
                    _logger.LogDebug("Connection established");

                await CreateEventLogItemsDatabaseAsync(cancellationToken).ConfigureAwait(false);
                if (_logger != null)
                    _logger.LogDebug("Database {0} checked ", _databaseName);
            }
        }

        private async Task CreateEventLogItemsDatabaseAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            string commandDbText = string.Format(@"CREATE DATABASE IF NOT EXISTS {0}", _databaseName);
            using (var cmdDb = _connection.CreateCommand())
            {
                cmdDb.CommandText = commandDbText;
                await cmdDb.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await _connection.ChangeDatabaseAsync(_databaseName, cancellationToken).ConfigureAwait(false);

            await EnsureDefaultTableExistsAsync(cancellationToken);
        }

        private async Task EnsureDefaultTableExistsAsync(CancellationToken cancellationToken)
        {
            string commandText =
                @"CREATE TABLE IF NOT EXISTS EventLogItems
                (
                    FileName LowCardinality(String),
                    EndPosition Int64 Codec(DoubleDelta, LZ4),
                    LgfEndPosition Int64 Codec(DoubleDelta, LZ4),
                    Id Int64 Codec(DoubleDelta, LZ4),
                    DateTime DateTime('UTC') Codec(Delta, LZ4),
                    TransactionStatus LowCardinality(String),
                    TransactionDate DateTime('UTC') Codec(Delta, LZ4),
                    TransactionNumber Int64 Codec(DoubleDelta, LZ4),
                    UserUuid LowCardinality(String),
                    User LowCardinality(String),
                    Computer LowCardinality(String),
                    Application LowCardinality(String),
                    Connection Int64 Codec(DoubleDelta, LZ4),
                    Event LowCardinality(String),
                    Severity LowCardinality(String),
                    Comment String Codec(ZSTD),
                    MetadataUuid String Codec(ZSTD),
                    Metadata LowCardinality(String),
                    Data String Codec(ZSTD),
                    DataPresentation String Codec(ZSTD),
                    Server LowCardinality(String),
                    MainPort Int32 Codec(DoubleDelta, LZ4),
                    AddPort Int32 Codec(DoubleDelta, LZ4),
                    Session Int64 Codec(DoubleDelta, LZ4)
                )
                ENGINE = MergeTree()
                PARTITION BY (toYYYYMM(DateTime))
                ORDER BY (DateTime, EndPosition)
                SETTINGS index_granularity = 8192;";

            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = commandText;
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task<EventLogPosition> ReadEventLogPositionAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            await CreateConnectionAsync(cancellationToken).ConfigureAwait(false);

            var commandText = string.Format(
                "SELECT FileName, EndPosition, LgfEndPosition, Id FROM {0} ORDER BY DateTime DESC, EndPosition DESC LIMIT 1", TableName);

            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = commandText;
                using (var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        return new EventLogPosition(
                            reader.GetString(0),
                            reader.GetInt64(1),
                            reader.GetInt64(2),
                            reader.GetInt64(3)
                        );
                    }
                }
            }
            return null;
        }

        // ОСНОВНОЙ МЕТОД
        public async Task WriteEventLogDataAsync(List<EventLogItem> entities,
            CancellationToken cancellationToken = default)
        {
            await CreateConnectionAsync(cancellationToken);

            var defaultEvents = new List<EventLogItem>();
            var dynamicRows = new Dictionary<string, List<Dictionary<string, object>>>();

            foreach (var item in entities)
            {
                if (TryParseDynamicTable(item.Comment, out string rawTableName, out Dictionary<string, object> fields))
                {
                    // Добавим служебные поля
                    fields["_event_id"] = item.Id;
                    fields["_event_stamp"] = item.DateTime;

                    if (!dynamicRows.ContainsKey(rawTableName))
                        dynamicRows[rawTableName] = new List<Dictionary<string, object>>();
                    dynamicRows[rawTableName].Add(fields);
                }
                else
                {
                    defaultEvents.Add(item);
                }
            }

            // Bulk-insert: динамические таблицы
            foreach (var pair in dynamicRows)
            {
                await BulkInsertIntoDynamicTableAsync(pair.Key, pair.Value, cancellationToken);
            }

            // Bulk-insert: стандартные события
            if (defaultEvents.Count > 0)
            {
                using var copy = new ClickHouseBulkCopy(_connection)
                {
                    DestinationTableName = TableName,
                    BatchSize = defaultEvents.Count
                };
                var data = defaultEvents.Select(item => new object[]
                {
                    item.FileName ?? "",
                    item.EndPosition,
                    item.LgfEndPosition,
                    item.Id,
                    item.DateTime,
                    item.TransactionStatus ?? "",
                    item.TransactionDateTime == DateTime.MinValue ? new DateTime(1970, 1, 1) : item.TransactionDateTime,
                    item.TransactionNumber,
                    item.UserUuid ?? "",
                    item.User ?? "",
                    item.Computer ?? "",
                    item.Application ?? "",
                    item.Connection,
                    item.Event ?? "",
                    item.Severity ?? "",
                    item.Comment ?? "",
                    item.MetadataUuid ?? "",
                    item.Metadata ?? "",
                    item.Data ?? "",
                    item.DataPresentation ?? "",
                    item.Server ?? "",
                    item.MainPort,
                    item.AddPort,
                    item.Session
                });

                try
                {
                    await copy.WriteToServerAsync(data, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, $"Failed to write data to {_databaseName}");
                    throw;
                }

                _logger?.LogDebug($"{defaultEvents.Count} items were being written to {_databaseName}");
            }
        }

        // Метод bulk-insert для динамической таблицы
        private async Task BulkInsertIntoDynamicTableAsync(
            string rawTableName,
            List<Dictionary<string, object>> rows,
            CancellationToken cancellationToken = default)
        {
            var tableName = Transliterate(rawTableName);

            // Обеспечить что существует таблица и структура актуальна
            if (rows.Count == 0)
                return;
            var fieldsSample = rows.First();
            await EnsureDynamicTableExistsAsync(rawTableName, fieldsSample, cancellationToken);

            // Получить актуальный список колонок в нужном порядке
            var columnNames = await GetActualColumnsAsync(tableName, cancellationToken);

            // Привести каждую строку к массиву object[] по списку колонок
            var data = rows.Select(dict =>
                columnNames
                    .Select(col => dict.ContainsKey(col) ? dict[col] ?? DBNull.Value : DBNull.Value)
                    .ToArray()).ToList();

            using var copy = new ClickHouseBulkCopy(_connection)
            {
                DestinationTableName = tableName,
                BatchSize = data.Count
            };

            try
            {
                await copy.WriteToServerAsync(data, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, $"Failed to write bulk data to dynamic table `{tableName}`");
                throw;
            }

            _logger?.LogDebug($"{rows.Count} dynamic items were written to {tableName}: ");
        }

        // Получить актуальные имена колонок (в порядке физических столбцов)
        private async Task<string[]> GetActualColumnsAsync(string tableName, CancellationToken cancellationToken = default)
        {
            var columnNames = new List<string>();
            var sql = $"DESCRIBE TABLE `{tableName}`";
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                columnNames.Add(reader.GetString(0));
            }
            return columnNames.ToArray();
        }

        // ОБНОВЛЕНИЕ СТРУКТУРЫ ТАБЛИЦЫ
        private async Task EnsureDynamicTableExistsAsync(string rawTableName, Dictionary<string, object> fields, CancellationToken cancellationToken)
        {
            var tableName = Transliterate(rawTableName);

            // CREATE TABLE IF NOT EXISTS с начальными полями
            var columnsSql = new List<string>
            {
                "`_event_id` Int64",
                "`_event_stamp` DateTime"
            };
            foreach (var f in fields)
            {
                var col = Transliterate(f.Key);
                if (col == "_event_id" || col == "_event_stamp") continue;
                columnsSql.Add($"`{col}` {InferClickhouseType(f.Value)}");
            }

            var create = $@"CREATE TABLE IF NOT EXISTS `{tableName}` ({string.Join(", ", columnsSql)}) ENGINE = MergeTree() ORDER BY _event_stamp";
            try
            {
                await using var cmd = _connection.CreateCommand();
                cmd.CommandText = create;
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (Exception e)
            {
                _logger?.LogError(e, $"Failed to create dynamic table `{tableName}`");
                throw;
            }

            // ALTER TABLE для новых полей
            HashSet<string> actualColumns = new HashSet<string>(await GetActualColumnsAsync(tableName, cancellationToken), StringComparer.OrdinalIgnoreCase);

            foreach (var f in fields)
            {
                var col = Transliterate(f.Key);
                if (!actualColumns.Contains(col, StringComparer.OrdinalIgnoreCase) && col != "_event_id" && col != "_event_stamp")
                {
                    var alter = $"ALTER TABLE `{tableName}` ADD COLUMN IF NOT EXISTS `{col}` {InferClickhouseType(f.Value)}";
                    try
                    {
                        await using var cmd = _connection.CreateCommand();
                        cmd.CommandText = alter;
                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                    }
                    catch
                    {
                        _logger?.LogDebug($"Failed to ALTER TABLE `{tableName}` ADD COLUMN `{col}`");
                        throw;
                    }
                }
            }
        }

        private static string Transliterate(string input)
        {
            Dictionary<char, string> map = new Dictionary<char, string>
            {
                {'а', "a"}, {'б', "b"}, {'в', "v"}, {'г', "g"}, {'д', "d"},
                {'е', "e"}, {'ё', "yo"}, {'ж', "zh"}, {'з', "z"}, {'и', "i"},
                {'й', "y"}, {'к', "k"}, {'л', "l"}, {'м', "m"}, {'н', "n"},
                {'о', "o"}, {'п', "p"}, {'р', "r"}, {'с', "s"}, {'т', "t"},
                {'у', "u"}, {'ф', "f"}, {'х', "kh"}, {'ц', "ts"}, {'ч', "ch"},
                {'ш', "sh"}, {'щ', "sch"}, {'ъ', ""}, {'ы', "y"}, {'ь', ""},
                {'э', "e"}, {'ю', "yu"}, {'я', "ya"},
                {'А', "A"}, {'Б', "B"}, {'В', "V"}, {'Г', "G"}, {'Д', "D"},
                {'Е', "E"}, {'Ё', "Yo"}, {'Ж', "Zh"}, {'З', "Z"}, {'И', "I"},
                {'Й', "Y"}, {'К', "K"}, {'Л', "L"}, {'М', "M"}, {'Н', "N"},
                {'О', "O"}, {'П', "P"}, {'Р', "R"}, {'С', "S"}, {'Т', "T"},
                {'У', "U"}, {'Ф', "F"}, {'Х', "Kh"}, {'Ц', "Ts"}, {'Ч', "Ch"},
                {'Ш', "Sh"}, {'Щ', "Sch"}, {'Ъ', ""}, {'Ы', "Y"}, {'Ь', ""},
                {'Э', "E"}, {'Ю', "Yu"}, {'Я', "Ya"}
            };
            var result = new System.Text.StringBuilder(input.Length * 2);
            foreach (char c in input)
            {
                if (map.ContainsKey(c))
                    result.Append(map[c]);
                else if (char.IsLetterOrDigit(c) || c == '_')
                    result.Append(c);
                else
                    result.Append('_');
            }
            return result.ToString();
        }
        private bool TryParseDynamicTable(string comment, out string tableName, out Dictionary<string, object> fields)
        {
            tableName = null;
            fields = null;
            if (string.IsNullOrWhiteSpace(comment))
                return false;
            try
            {

                var obj = JObject.Parse(comment.Replace(@"""""", @""""));
                var props = obj.Properties().ToList();
                if (props.Count == 1)
                {
                    var key = Transliterate(props[0].Name);
                    tableName = FixDatabaseName(key);
                    var value = props[0].Value;
                    if (value.Type == JTokenType.Object)
                    {
                        var dic = ((JObject)value).Properties()
                            .ToDictionary(
                                pair => Transliterate(pair.Name),
                                pair =>
                                {
                                    var token = pair.Value;
                                    if (token.Type == JTokenType.Object || token.Type == JTokenType.Array)
                                        return (object)token.ToString(Newtonsoft.Json.Formatting.None);
                                    return (object)token.ToString();
                                }
                            );
                        fields = dic;
                        _logger?.LogDebug("{0} fields: {1}", fields.Count, fields);
                        return true;
                    }
                }
            }
            catch { }
            return false;
        }

        private string InferClickhouseType(object value)
        {
            if (value == null)
                return "String";
            if (value is long || value is int || value is short)
                return "Int64";
            if (value is double || value is float || value is decimal)
                return "Float64";
            if (value is bool)
                return "UInt8";
            if (value is DateTime)
                return "DateTime";
            return "String";
        }
    }
}