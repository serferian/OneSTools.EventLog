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

                await CreateEventLogItemsDatabaseAsync(cancellationToken).ConfigureAwait(false);
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
                    TransactionDateTime DateTime('UTC') Codec(Delta, LZ4),
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

        public async Task WriteEventLogDataAsync(List<EventLogItem> entities, CancellationToken cancellationToken = default(CancellationToken))
        {
            await CreateConnectionAsync(cancellationToken).ConfigureAwait(false);

            var defaultEvents = new List<EventLogItem>();

            foreach (var item in entities)
            {
                string tableName;
                Dictionary<string, object> fields;
                if (TryParseDynamicTable(item.Comment, out tableName, out fields))
                {
                    // добавим служебные поля
                    fields["_event_id"] = item.Id;
                    fields["_event_stamp"] = item.DateTime;
                    await EnsureDynamicTableExistsAsync(tableName, fields, cancellationToken).ConfigureAwait(false);
                    await InsertIntoDynamicTableAsync(tableName, fields, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    defaultEvents.Add(item);
                }
            }

            if (defaultEvents.Count > 0)
            {
                await InsertBatchIntoDefaultTableAsync(defaultEvents, cancellationToken).ConfigureAwait(false);
            }
        }

        // Для дефолтной таблицы, батчевый вставщик без ClickHouseBulkCopy
        private async Task InsertBatchIntoDefaultTableAsync(List<EventLogItem> items, CancellationToken cancellationToken)
        {
            int batchSize = 1000; // Можно менять

            var columns = new string[]
            {
                "FileName",
                "EndPosition",
                "LgfEndPosition",
                "Id",
                "DateTime",
                "TransactionStatus",
                "TransactionDateTime",
                "TransactionNumber",
                "UserUuid",
                "User",
                "Computer",
                "Application",
                "Connection",
                "Event",
                "Severity",
                "Comment",
                "MetadataUuid",
                "Metadata",
                "Data",
                "DataPresentation",
                "Server",
                "MainPort",
                "AddPort",
                "Session"
            };

            for (int batchStart = 0; batchStart < items.Count; batchStart += batchSize)
            {
                var batch = items.Skip(batchStart).Take(batchSize).ToList();
                var values = new List<string>();
                var parameters = new List<object>();
                int paramIndex = 0;

                foreach (var ev in batch)
                {
                    var rowParams = new List<string>();
                    object[] rowValues = new object[]
                    {
                        ev.FileName ?? "",
                        ev.EndPosition,
                        ev.LgfEndPosition,
                        ev.Id,
                        ev.DateTime,
                        ev.TransactionStatus ?? "",
                        ev.TransactionDateTime == DateTime.MinValue ? new DateTime(1970,1,1) : ev.TransactionDateTime,
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
                    };

                    for (int i = 0; i < rowValues.Length; ++i)
                    {
                        rowParams.Add("@p" + paramIndex.ToString());
                        parameters.Add(rowValues[i]);
                        paramIndex++;
                    }
                    values.Add("(" + string.Join(",", rowParams) + ")");
                }

                var sql = string.Format(
                    "INSERT INTO {0} ({1}) VALUES {2}",
                    TableName,
                    string.Join(", ", columns.Select(x => "`" + x + "`")),
                    string.Join(", ", values)
                );

                using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    for (int i = 0; i < parameters.Count; ++i)
                    {
                        var param = cmd.CreateParameter();
                        param.ParameterName = "p" + i;
                        param.Value = parameters[i];
                        cmd.Parameters.Add(param);
                    }
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            if (_logger != null)
                _logger.LogDebug("{0} items were written to {1}", items.Count, TableName);
        }

        // --- ДИНАМИЧЕСКИЕ ТАБЛИЦЫ ---

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
                    tableName = FixDatabaseName(key);
                    var value = props[0].Value;
                    if (value.Type == JTokenType.Object)
                    {
                        fields = value.ToObject<Dictionary<string, object>>();
                        if (_logger != null)
                            _logger.LogDebug("{0} fields: {1}", fields.Count, fields);
                        return true;
                    }
                }
            }
            catch { }
            return false;
        }

        private async Task EnsureDynamicTableExistsAsync(string tableName, Dictionary<string, object> fields, CancellationToken cancellationToken)
        {
            lock (_columnsLock)
            {
                if (!_dynamicTableColumns.ContainsKey(tableName))
                {
                    _dynamicTableColumns[tableName] = new HashSet<string>(fields.Keys, StringComparer.OrdinalIgnoreCase);
                }
                else
                {
                    foreach (var f in fields.Keys)
                        _dynamicTableColumns[tableName].Add(f);
                }
            }

            var columnsSql = new List<string>
            {
                "`_event_id` Int64",
                "`_event_stamp` DateTime"
            };

            foreach (var f in fields)
            {
                if (f.Key == "_event_id" || f.Key == "_event_stamp") continue;
                columnsSql.Add(string.Format("`{0}` {1}", f.Key, InferClickhouseType(f.Value)));
            }

            string createSql =
                string.Format("CREATE TABLE IF NOT EXISTS `{0}` ({1}) ENGINE = MergeTree() ORDER BY _event_stamp",
                    tableName, string.Join(", ", columnsSql));

            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = createSql;
                try
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (_logger != null)
                        _logger.LogError(e, "Failed to create dynamic table `{0}`", tableName);
                }
            }
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

        private async Task InsertIntoDynamicTableAsync(string tableName, Dictionary<string, object> fields, CancellationToken cancellationToken)
        {
            var columns = fields.Keys.ToArray();
            var parameters = columns.Select((c, i) => "@p" + i.ToString()).ToArray();
            var sql = string.Format("INSERT INTO `{0}`({1}) VALUES({2})",
                tableName,
                string.Join(",", columns.Select(c => "`" + c + "`")),
                string.Join(",", parameters));
            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sql;
                for (int i = 0; i < columns.Length; i++)
                {
                    var val = fields[columns[i]] ?? DBNull.Value;
                    var param = cmd.CreateParameter();
                    param.ParameterName = "p" + i.ToString();
                    param.Value = val;
                    cmd.Parameters.Add(param);
                }
                try
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (_logger != null)
                        _logger.LogError(e, "Failed to insert row into dynamic table `{0}`", tableName);
                }
            }
        }
    }
}