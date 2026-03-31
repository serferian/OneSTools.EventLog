using System;
using System.Globalization;
using System.IO;
using System.Text;
using Microsoft.Extensions.Logging;

namespace OneSTools.EventLog
{
    internal sealed class LgpStreamingReader : IDisposable
    {
        private readonly ILogger _logger;
        private readonly string _sourceName;
        private readonly StreamReader _stream;
        private bool _disposedValue;

        public LgpStreamingReader(Stream stream, string sourceName = null, ILogger logger = null)
        {
            _stream = new StreamReader(stream);
            _sourceName = sourceName ?? string.Empty;
            _logger = logger;
        }

        public long Position
        {
            get => _stream.GetPosition();
            set => _stream.SetPosition(value);
        }

        public bool EndOfStream => _stream.EndOfStream;

        public LgpEventData NextItem(int maxDataLength = 0)
        {
            var startPosition = Position;

            try
            {
                if (!MoveToNextNodeStart())
                    return null;

                return ReadItem(maxDataLength > 0 ? maxDataLength : int.MaxValue);
            }
            catch (IncompleteNodeException)
            {
                Position = startPosition;
                return null;
            }
            catch (FormatException ex)
            {
                var failedPosition = Position;
                var context = TryReadContext(startPosition);
                _logger?.LogWarning(ex,
                    "Failed to parse LGP block in {SourceName}. StartPosition={StartPosition}, FailedPosition={FailedPosition}, Context={Context}",
                    _sourceName, startPosition, failedPosition, context);
                Position = startPosition;
                return null;
            }
        }

        private bool MoveToNextNodeStart()
        {
            while (!EndOfStream)
            {
                var value = _stream.Read();
                if (value == -1)
                    return false;

                if ((char)value == '{')
                    return true;
            }

            return false;
        }

        private LgpEventData ReadItem(int maxCommentLength)
        {
            var item = new LgpEventData
            {
                DateTime = ReadScalarValue()
            };

            Consume(',');
            item.TransactionStatus = ReadScalarValue();
            Consume(',');
            (item.TransactionDateHex, item.TransactionNumberHex) = ReadTransactionData();
            Consume(',');
            item.User = ReadInt32Value();
            Consume(',');
            item.Computer = ReadInt32Value();
            Consume(',');
            item.Application = ReadInt32Value();
            Consume(',');
            item.Connection = ReadInt64Value();
            Consume(',');
            item.Event = ReadInt32Value();
            Consume(',');
            item.Severity = ReadScalarValue();
            Consume(',');
            item.Comment = ReadScalarValue(maxCommentLength);
            Consume(',');
            item.Metadata = ReadInt32Value();
            Consume(',');
            item.Data = ReadDataValue();
            Consume(',');
            item.DataPresentation = ReadScalarValue();
            Consume(',');
            item.Server = ReadInt32Value();
            Consume(',');
            item.MainPort = ReadInt32Value();
            Consume(',');
            item.AddPort = ReadInt32Value();
            Consume(',');
            item.Session = ReadInt64Value();
            SkipRemainingNodeValues();

            return item;
        }

        private (string Date, string Number) ReadTransactionData()
        {
            Consume('{');

            var date = ReadScalarValue();
            Consume(',');
            var number = ReadScalarValue();

            SkipRemainingNodeValues();

            return (date, number);
        }

        private string ReadDataValue()
        {
            SkipWhitespace();

            if (PeekRequired() != '{')
                return ReadScalarValue();

            Consume('{');

            var dataType = ReadScalarValue();
            if (TryConsume('}'))
                return string.Empty;

            Consume(',');

            switch (dataType)
            {
                case "R":
                    {
                        var value = ReadScalarValue();
                        SkipRemainingNodeValues();
                        return value;
                    }
                case "U":
                    SkipRemainingNodeValues();
                    return string.Empty;
                case "S":
                    {
                        var value = ReadScalarValue();
                        SkipRemainingNodeValues();
                        return value;
                    }
                case "B":
                    {
                        var value = ReadScalarValue(5);
                        SkipRemainingNodeValues();
                        return value == "0" ? "false" : "true";
                    }
                case "P":
                    SkipValue();
                    SkipRemainingNodeValues();
                    return string.Empty;
                default:
                    SkipRemainingNodeValues();
                    return string.Empty;
            }
        }

        private string ReadComplexData()
        {
            Consume('{');

            if (TryConsume('}'))
                return string.Empty;

            var builder = new StringBuilder();
            var index = 0;

            while (true)
            {
                if (index == 0)
                {
                    SkipValue();
                }
                else
                {
                    var value = ReadDataValue();
                    if (value != string.Empty)
                    {
                        builder.Append("Item ");
                        builder.Append(index.ToString(CultureInfo.InvariantCulture));
                        builder.Append(": ");
                        builder.Append(value);
                        builder.Append(Environment.NewLine);
                    }
                }

                index++;

                if (TryConsume('}'))
                    return builder.ToString();

                Consume(',');
            }
        }

        private void SkipRemainingNodeValues()
        {
            while (true)
            {
                if (TryConsume('}'))
                    return;

                Consume(',');
                SkipValue();
            }
        }

        private void SkipValue()
        {
            SkipWhitespace();

            var next = PeekRequired();
            switch ((char)next)
            {
                case '{':
                    ReadRequired();
                    SkipNode();
                    return;
                case '"':
                    ReadRequired();
                    SkipQuotedValue();
                    return;
                default:
                    SkipBareValue();
                    return;
            }
        }

        private void SkipNode()
        {
            if (TryConsume('}'))
                return;

            while (true)
            {
                SkipValue();

                if (TryConsume('}'))
                    return;

                Consume(',');
            }
        }

        private void SkipQuotedValue()
        {
            var quotes = 1;

            while (true)
            {
                var value = ReadRequired();
                if (value != '"')
                    continue;

                quotes++;

                var next = PeekRequired();
                if ((next == ',' || next == '}') && quotes % 2 == 0)
                    return;
            }
        }

        private void SkipBareValue()
        {
            while (true)
            {
                var next = PeekRequired();
                if (next == ',' || next == '}' || char.IsWhiteSpace((char)next))
                    return;

                ReadRequired();
            }
        }

        private int ReadInt32Value()
        {
            var value = ReadNumericTokenValue();
            return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result)
                ? result
                : 0;
        }

        private long ReadInt64Value()
        {
            var value = ReadNumericTokenValue();
            return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result)
                ? result
                : 0L;
        }

        private string ReadNumericTokenValue()
        {
            SkipWhitespace();

            var next = PeekRequired();
            if (next == '{')
                return ReadFirstScalarFromNode();

            return ReadScalarValue().Trim();
        }

        private string ReadFirstScalarFromNode()
        {
            return ReadFirstScalarFromNode(int.MaxValue);
        }

        private string ReadFirstScalarFromNode(int maxLength)
        {
            Consume('{');

            if (TryConsume('}'))
                return string.Empty;

            var value = ReadFirstScalarValue(maxLength);
            SkipRemainingNodeValues();

            return value.Trim();
        }

        private string ReadFirstScalarValue()
        {
            return ReadFirstScalarValue(int.MaxValue);
        }

        private string ReadFirstScalarValue(int maxLength)
        {
            SkipWhitespace();

            var next = PeekRequired();
            if (next == '{')
                return ReadFirstScalarFromNode(maxLength);

            return ReadScalarValue(maxLength);
        }

        private string ReadScalarValue()
        {
            return ReadScalarValue(int.MaxValue);
        }

        private string ReadScalarValue(int maxLength)
        {
            SkipWhitespace();

            var next = PeekRequired();
            if (next == '{')
                return ReadFirstScalarFromNode(maxLength);

            if (next == '"')
            {
                ReadRequired();
                return ReadQuotedValue(maxLength);
            }

            return ReadBareValue(maxLength);
        }

        private string ReadQuotedValue(int maxLength)
        {
            var builder = maxLength > 0 ? new StringBuilder(Math.Min(maxLength, 256)) : null;
            var quotes = 1;

            while (true)
            {
                var value = ReadRequired();
                if (value == '"')
                {
                    quotes++;

                    var next = PeekRequired();
                    if ((next == ',' || next == '}') && quotes % 2 == 0)
                        return builder?.ToString() ?? string.Empty;
                }

                if (builder != null && builder.Length < maxLength)
                    builder.Append(value);
            }
        }

        private string ReadBareValue(int maxLength)
        {
            var builder = maxLength > 0 ? new StringBuilder(Math.Min(maxLength, 64)) : null;

            while (true)
            {
                var next = PeekRequired();
                if (next == ',' || next == '}' || char.IsWhiteSpace((char)next))
                    return builder?.ToString() ?? string.Empty;

                var value = ReadRequired();
                if (builder != null && builder.Length < maxLength)
                    builder.Append(value);
            }
        }

        private bool TryConsume(char expected)
        {
            SkipWhitespace();

            var next = PeekRequired();
            if (next != expected)
                return false;

            ReadRequired();
            return true;
        }

        private void Consume(char expected)
        {
            SkipWhitespace();

            var value = ReadRequired();
            if (value != expected)
                throw new FormatException($"Unexpected character '{value}', expected '{expected}'");
        }

        private void SkipWhitespace()
        {
            while (!EndOfStream)
            {
                var next = _stream.Peek();
                if (next == -1 || !char.IsWhiteSpace((char)next))
                    return;

                _stream.Read();
            }
        }

        private string TryReadContext(long position, int charsBefore = 64, int charsAfter = 256)
        {
            try
            {
                var currentPosition = Position;
                var contextStart = Math.Max(position - charsBefore, 0);
                Position = contextStart;

                var buffer = new char[charsBefore + charsAfter];
                var length = _stream.ReadBlock(buffer, 0, buffer.Length);
                Position = currentPosition;

                return length == 0 ? string.Empty : new string(buffer, 0, length)
                    .Replace("\r", "\\r")
                    .Replace("\n", "\\n");
            }
            catch (Exception ex)
            {
                _logger?.LogDebug(ex,
                    "Failed to read LGP parse context in {SourceName} at position {Position}",
                    _sourceName, position);
                return string.Empty;
            }
        }

        private char ReadRequired()
        {
            var value = _stream.Read();
            if (value == -1)
                throw new IncompleteNodeException();

            return (char)value;
        }

        private int PeekRequired()
        {
            var value = _stream.Peek();
            if (value == -1)
                throw new IncompleteNodeException();

            return value;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (_disposedValue)
                return;

            if (disposing)
                _stream?.Dispose();

            _disposedValue = true;
        }

        ~LgpStreamingReader()
        {
            Dispose(false);
        }

        private sealed class IncompleteNodeException : Exception
        {
        }

        private sealed class LimitedStringBuilder
        {
            private readonly StringBuilder _builder;
            private readonly int _maxLength;

            public LimitedStringBuilder(int maxLength)
            {
                _maxLength = maxLength;
                _builder = maxLength > 0 ? new StringBuilder(Math.Min(maxLength, 256)) : new StringBuilder();
            }

            public int RemainingLength => _maxLength == int.MaxValue ? int.MaxValue : Math.Max(_maxLength - _builder.Length, 0);

            public void Append(string value)
            {
                if (string.IsNullOrEmpty(value))
                    return;

                if (_maxLength == int.MaxValue)
                {
                    _builder.Append(value);
                    return;
                }

                var remaining = RemainingLength;
                if (remaining == 0)
                    return;

                if (value.Length <= remaining)
                    _builder.Append(value);
                else
                    _builder.Append(value, 0, remaining);
            }

            public override string ToString()
            {
                return _builder.ToString();
            }
        }
    }

    internal sealed class LgpEventData
    {
        public string DateTime { get; set; } = "";
        public string TransactionStatus { get; set; } = "";
        public string TransactionDateHex { get; set; } = "";
        public string TransactionNumberHex { get; set; } = "";
        public int User { get; set; }
        public int Computer { get; set; }
        public int Application { get; set; }
        public long Connection { get; set; }
        public int Event { get; set; }
        public string Severity { get; set; } = "";
        public string Comment { get; set; } = "";
        public int Metadata { get; set; }
        public string Data { get; set; } = "";
        public string DataPresentation { get; set; } = "";
        public int Server { get; set; }
        public int MainPort { get; set; }
        public int AddPort { get; set; }
        public long Session { get; set; }
    }
}