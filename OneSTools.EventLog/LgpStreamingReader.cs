using System;
using System.Globalization;
using System.IO;
using Microsoft.Extensions.Logging;

namespace OneSTools.EventLog
{
    internal sealed class LgpStreamingReader : IDisposable
    {
        private const int DefaultMaxCommentLength = 1024 * 1024;

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

                return ReadItem(maxDataLength > 0 ? maxDataLength : DefaultMaxCommentLength);
            }
            catch (IncompleteNodeException)
            {
                Position = startPosition;
                return null;
            }
            catch (FormatException ex)
            {
                var failedPosition = Position;
                var context = TryReadContext(failedPosition);
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
                    SkipValue();
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
                    SkipValue();
                    SkipRemainingNodeValues();
                    return string.Empty;
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
            while (true)
            {
                var value = ReadRequired();
                if (value != '"')
                    continue;

                var next = PeekRequired();
                if (next == '"')
                {
                    ReadRequired();
                    continue;
                }

                if (next == ',' || next == '}')
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
            var buffer = new LimitedCharBuffer(maxLength, 256);

            while (true)
            {
                var value = ReadRequired();
                if (value != '"')
                {
                    buffer.Append(value);
                    continue;
                }

                var next = PeekRequired();
                if (next == '"')
                {
                    ReadRequired();
                    buffer.Append('"');
                    continue;
                }

                if (next == ',' || next == '}')
                    return buffer.ToString();

                buffer.Append('"');
            }
        }

        private string ReadBareValue(int maxLength)
        {
            var buffer = new LimitedCharBuffer(maxLength, 64);

            while (true)
            {
                var next = PeekRequired();
                if (next == ',' || next == '}' || char.IsWhiteSpace((char)next))
                    return buffer.ToString();

                var value = ReadRequired();
                buffer.Append(value);
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

        private sealed class LimitedCharBuffer
        {
            private char[] _buffer;
            private readonly int _maxLength;
            private int _length;

            public LimitedCharBuffer(int maxLength, int initialCapacity)
            {
                _maxLength = Math.Max(maxLength, 0);
                _buffer = _maxLength == 0 ? Array.Empty<char>() : new char[Math.Min(_maxLength, Math.Max(initialCapacity, 1))];
            }

            public void Append(char value)
            {
                if (_length >= _maxLength)
                    return;

                EnsureCapacity(_length + 1);
                _buffer[_length] = value;
                _length++;
            }

            public override string ToString()
            {
                return _length == 0 ? string.Empty : new string(_buffer, 0, _length);
            }

            private void EnsureCapacity(int requiredLength)
            {
                if (_buffer.Length >= requiredLength)
                    return;

                var newCapacity = _buffer.Length == 0 ? 1 : _buffer.Length * 2;
                if (newCapacity < requiredLength)
                    newCapacity = requiredLength;
                if (newCapacity > _maxLength)
                    newCapacity = _maxLength;

                Array.Resize(ref _buffer, newCapacity);
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