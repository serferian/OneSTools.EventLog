﻿using System;
using NodaTime;

namespace OneSTools.EventLog.Exporter.Core
{
    public class EventLogExporterSettings
    {
        public string LogFolder { get; set; } = "";
        public int Portion { get; set; } = 10000;
        public DateTimeZone TimeZone { get; set; } = DateTimeZoneProviders.Tzdb.GetSystemDefault();
        public int WritingMaxDop { get; set; } = 1;
        public int CollectedFactor { get; set; } = 2;
        public int ReadingTimeout { get; set; } = 1;
        public bool LoadArchive { get; set; } = false;
        public DateTime SkipEventsBeforeDate { get; set; }
        /// <summary>
        /// Количество дней, после которых прочитанные lgp и lgx-файлы считаются старыми и подлежат удалению.
        /// Значение 0 или меньше — не удалять файлы.
        /// </summary>
        public int LogFilesStoringDays { get; set; } = 0;
    }
}