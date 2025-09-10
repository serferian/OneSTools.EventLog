﻿using System;

namespace OneSTools.EventLog
{
    public class EventLogItem
    {
        public long Id { get; set; } = 0;
        public virtual string FileName { get; set; } = "";
        public virtual string DatabaseName { get; set; } = "";
        public virtual string Separator { get; set; } = "";
        public virtual long EndPosition { get; set; } = 0;
        public virtual long LgfEndPosition { get; set; } = 0;
        public virtual DateTime DateTime { get; set; } = DateTime.MinValue;
        public virtual string TransactionStatus { get; set; } = "";
        public virtual DateTime TransactionDateTime { get; set; } = DateTime.UnixEpoch;
        public virtual long TransactionNumber { get; set; } = 0;
        public virtual string UserUuid { get; set; } = "";
        public virtual string User { get; set; } = "";
        public virtual string Computer { get; set; } = "";
        public virtual string Application { get; set; } = "";
        public virtual long Connection { get; set; } = 0;
        public virtual string Event { get; set; } = "";
        public virtual string Severity { get; set; } = "";
        public virtual string Comment { get; set; } = "";
        public virtual string MetadataUuid { get; set; } = "";
        public virtual string Metadata { get; set; } = "";
        public virtual string Data { get; set; } = "";
        public virtual string DataPresentation { get; set; } = "";
        public virtual string Server { get; set; } = "";
        public virtual int MainPort { get; set; } = 0;
        public virtual int AddPort { get; set; } = 0;
        public virtual long Session { get; set; } = 0;
    }
}