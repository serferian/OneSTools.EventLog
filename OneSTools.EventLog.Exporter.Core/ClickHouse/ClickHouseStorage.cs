using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Parameters;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
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

        // Кэшируем описания пользовательских таблиц
        private readonly Dictionary<string, HashSet<string>> _dynamicTableColumns = new Dictionary<string, HashSet<string>>();
        private readonly object _columnsLock = new object();

        public ClickHouseStorage(string connectionsString, ILogger<ClickHouseStorage> logger = null)
        {
            _logger = logger;
            _connectionString = connectionsString;
            Init();
        }

        public ClickHouseStorage(ILogger<ClickHouseStorage> logger, IConfiguration configuration)
        {
            _logger = logger;
            _connectionString = configuration.GetValue("ClickHouse:ConnectionString", "");
            Init();
        }

        private void Init()
        {
            if (_connectionString == string.Empty)
                throw new Exception("Connection string is not specified");

            _databaseName = Regex.Match(_connectionString, "(?<=Database=).*?(?=(;|$))", RegexOptions.IgnoreCase).Value;
            _connectionString = Regex.Replace(_connectionString, "Database=.*?(;|$)", "");

            if (string.IsNullOrWhiteSpace(_databaseName))
                throw new Exception("Database name is not specified");
            else
                _databaseName = FixDatabaseName(_databaseName);
        }

        private static string FixDatabaseName(string name)
            => Regex.Replace(name, @"(?:\W|-)", "_", RegexOptions.Compiled);

        private async Task CreateConnectionAsync(CancellationToken cancellationToken = default)
        {
            if (_connection is null)
            {
                _connection = new ClickHouseConnection(_connectionString);
                await _connection.OpenAsync(cancellationToken);
                await CreateEventLogItemsDatabaseAsync(cancellationToken);
            }
        }

        private async Task CreateEventLogItemsDatabaseAsync(CancellationToken cancellationToken = default)
        {
            var commandDbText = string.Format(@"CREATE DATABASE IF NOT EXISTS {0}", _databaseName);
            await using (var cmdDb = _connection.CreateCommand())
            {
                cmdDb.CommandText = commandDbText;
                await cmdDb.ExecuteNonQueryAsync(cancellationToken);
            }
            await _connection.ChangeDatabaseAsync(_databaseName, cancellationToken);
        }

        // -- ДОРАБОТКА: Главная точка выгрузки
        public async Task WriteEventLogDataAsync(List<EventLogItem> entities, CancellationToken cancellationToken = default)
        {
            await CreateConnectionAsync(cancellationToken);

            var defaultEvents = new List<EventLogItem>();

            foreach (var item in entities)
            {
                string tableName;
                Dictionary<string, object> fields;
                if (TryParseDynamicTable(item.Comment, out tableName, out fields))
                {
                    // Добавим служебные поля
                    fields["_event_id"] = item.Id;
                    fields["_event_stamp"] = item.DateTime;

                    await EnsureDynamicTableExistsAsync(tableName, fields, cancellationToken);
                    await InsertIntoDynamicTableAsync(tableName, fields, cancellationToken);
                }
                else
                {
                    defaultEvents.Add(item);
                }
            }

            // Старый пайплайн для "обычных" эвентов
            if (defaultEvents.Count > 0)
            {
                using (var copy = new ClickHouseBulkCopy(_connection)
                {
                    DestinationTableName = TableName,
                    BatchSize = defaultEvents.Count
                })
                {

                    var data = defaultEvents.Select(ev => new object[]
                    {
                        ev.FileName ?? "",
                        ev.EndPosition,
                        ev.LgfEndPosition,
                        ev.Id,
                        ev.DateTime,
                        ev.TransactionStatus ?? "",
                        ev.TransactionDateTime == DateTime.MinValue ? new DateTime(1970, 1, 1) : ev.TransactionDateTime,
                        ev.TransactionNumber,
                        ev.UserUuid ?? "",
                        ev.User ?? "",
                        ev.Computer ?? "",
                        ev.Application ?? "",
                        ev.Connection,
                        ev.Event ?? "",
                        ev.Severity ?? "",
                        ev.Comment ?? "",
                        ev.MetadataUuid ?? "",
                        ev.Metadata ?? "",
                        ev.Data ?? "",
                        ev.DataPresentation ?? "",
                        ev.Server ?? "",
                        ev.MainPort,
                        ev.AddPort,
                        ev.Session
                    }).AsEnumerable();

                    try
                    {
                        await copy.WriteToServerAsync(data, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        if (_logger != null)
                            _logger.LogError(ex, "Failed to write data to " + _databaseName);
                        throw;
                    }

                    if (_logger != null)
                        _logger.LogDebug(defaultEvents.Count + " items were being written to " + _databaseName);
                }
            }
        }

        // Пытаемся парсить comment как JSON-объект с одним ключом
        private bool TryParseDynamicTable(string comment, out string tableName, out Dictionary<string, object> fields)
        {
            tableName = null;
            fields = null;
            if (string.IsNullOrWhiteSpace(comment))
                return false;
            try
            {
                var obj = JObject.Parse(comment);
                var props = obj.Properties().ToList();
                if (props.Count == 1)
                {
                    var key = props[0].Name;
                    tableName = key; // snake-case оставить как есть
                    var value = props[0].Value;
                    if (value.Type != JTokenType.Object) return false;

                    fields = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                    foreach (var prop in ((JObject)value).Properties())
                    {
                        fields[prop.Name] = JTokenToDotNet(prop.Value);
                    }
                    return true;
                }
            }
            catch
            {
                // ignore, fallback to default table
            }
            return false;
        }

        // Вложенные/сложные поля превращаем в строку (json), простые — приводим к базовому типу
        private object JTokenToDotNet(JToken t)
        {
            switch (t.Type)
            {
                case JTokenType.Integer: return t.Value<long>();
                case JTokenType.Float: return t.Value<double>();
                case JTokenType.Boolean: return t.Value<bool>();
                case JTokenType.String: return t.Value<string>();
                case JTokenType.Date: return t.Value<DateTime>();
                case JTokenType.Null: return null;
                case JTokenType.Undefined: return null;
                default: return t.ToString(Formatting.None);
            }
        }

        // CREATE TABLE, если не существует, + ALTER TABLE для новых полей по мере появления
        private async Task EnsureDynamicTableExistsAsync(string tableName, Dictionary<string, object> fields, CancellationToken cancellationToken)
        {
            lock (_columnsLock)
            {
                HashSet<string> set;
                if (!_dynamicTableColumns.TryGetValue(tableName, out set))
                {
                    set = new HashSet<string>(fields.Keys, StringComparer.OrdinalIgnoreCase);
                    _dynamicTableColumns[tableName] = set;
                }
                else
                {
                    foreach (var f in fields.Keys)
                        set.Add(f);
                }
            }

            // CREATE TABLE IF NOT EXISTS
            var columnsSql = new List<string>
            {
                "`_event_id` Int64",
                "`_event_stamp` DateTime"
            };

            foreach (var f in fields)
            {
                if (f.Key == "_event_id" || f.Key == "_event_stamp") continue;
                columnsSql.Add("`" + f.Key + "` " + InferClickhouseType(f.Value));
            }

            var create = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (" + string.Join(", ", columnsSql) + ") ENGINE = MergeTree() ORDER BY _event_stamp";
            try
            {
                await using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = create;
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
            catch (Exception e)
            {
                if (_logger != null)
                    _logger.LogError(e, "Failed to create dynamic table `" + tableName + "`");
            }

            HashSet<string> actualColumns = await GetActualColumnsAsync(tableName, cancellationToken);

            foreach (var f in fields)
            {
                if (!actualColumns.Contains(f.Key))
                {
                    var alter = "ALTER TABLE `" + tableName + "` ADD COLUMN IF NOT EXISTS `" + f.Key + "` " + InferClickhouseType(f.Value);
                    try
                    {
                        await using (var cmd = _connection.CreateCommand())
                        {
                            cmd.CommandText = alter;
                            await cmd.ExecuteNonQueryAsync(cancellationToken);
                        }
                    }
                    catch (Exception ex)
                    {
                        if (_logger != null)
                            _logger.LogError(ex, "Failed to ALTER TABLE `" + tableName + "` ADD COLUMN `" + f.Key + "`");
                    }
                }
            }
        }

        // Получаем набор существующих колонок для таблицы
        private async Task<HashSet<string>> GetActualColumnsAsync(string tableName, CancellationToken cancellationToken)
        {
            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var sql = "SELECT name FROM system.columns WHERE database = '" + _databaseName + "' AND table = '" + tableName + "'";
            await using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sql;
                await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        result.Add(reader.GetString(0));
                    }
                }
            }
            return result;
        }

        // Определяем тип ClickHouse исходя из значения
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

        // Вставка данных в динамическую таблицу (одна строка)
        private async Task InsertIntoDynamicTableAsync(string tableName, Dictionary<string, object> fields, CancellationToken cancellationToken)
        {
            var columns = fields.Keys.ToArray();
            var parameters = columns.Select((c, i) => "@p" + i).ToArray();
            var sql = "INSERT INTO `" + tableName + "`(" + string.Join(",", columns.Select(c => "`" + c + "`")) + ") VALUES(" + string.Join(",", parameters) + ")";

            await using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sql;
                for (int i = 0; i < columns.Length; ++i)
                {
                    var val = fields[columns[i]] ?? DBNull.Value;
                    cmd.Parameters.Add(new ClickHouseDbParameter
                    {
                        ParameterName = "p" + i,
                        Value = val
                    });
                }
                try
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
                catch (Exception e)
                {
                    if (_logger != null)
                        _logger.LogError(e, "Failed to insert row into dynamic table `" + tableName + "`");
                }
            }
        }

        public async Task<EventLogPosition> ReadEventLogPositionAsync(CancellationToken cancellationToken = default)
        {
            await CreateConnectionAsync(cancellationToken);

            var commandText =
                "SELECT TOP 1 FileName, EndPosition, LgfEndPosition, Id FROM " + TableName + " ORDER BY DateTime DESC, EndPosition DESC";

            await using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = commandText;

                await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                {
                    if (await reader.ReadAsync(cancellationToken))
                        return new EventLogPosition(reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3));
                }
            }
            return null;
        }

        public void Dispose()
        {
            if (_connection != null)
                _connection.Dispose();
        }
    }
}