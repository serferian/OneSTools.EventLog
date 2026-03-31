---
description: Repository Information Overview
alwaysApply: true
---

# OneSTools.EventLog Information

## Summary
A .NET solution for reading and exporting 1C Enterprise event logs (Журнал регистрации) to ClickHouse and ElasticSearch. Uses TPL Dataflow pipeline processing for high-throughput export with configurable CPU/RAM resource consumption. Can run as a Windows Service or Linux systemd service.

## Structure
- **`OneSTools.EventLog/`** — Core library for reading/parsing 1C event log files (LGF/LGP format). Published as NuGet package.
- **`OneSTools.EventLog.Exporter.Core/`** — Shared exporter library with ClickHouse and ElasticSearch storage implementations. Contains `ClickHouse/` and `ElasticSearch/` subdirectories.
- **`OneSTools.EventLog.Exporter/`** — Worker service executable (`EventLogExporter.exe`) for exporting a single IB's event log.
- **`OneSTools.EventLog.Exporter.Manager/`** — Worker service executable (`EventLogExportersManager.exe`) that watches 1C server cluster directories and auto-manages exporters for multiple IBs.
- **`Build/`** — Output directory for compiled binaries (Debug/Release).

## Language & Runtime
**Language**: C# 8.0  
**Runtime**: .NET 5.0 (executables), .NET Standard 2.1 (libraries)  
**Build System**: MSBuild / Visual Studio 2019+  
**Package Manager**: NuGet

## Projects & Dependencies

### OneSTools.EventLog (v1.2.6) — NuGet Library
**Target**: `netstandard2.1`

**Dependencies**:
- `NodaTime` 3.0.5
- `OneSTools.BracketsFile` 2.1.9
- `System.ComponentModel.Annotations` 5.0.0
- `Microsoft.Extensions.Logging.Abstractions` 5.0.0

### OneSTools.EventLog.Exporter.Core (v1.1.1) — Shared Library
**Target**: `netstandard2.1`

**Dependencies**:
- `ClickHouse.Client` 3.0.0.357
- `NEST` 7.10.1 (ElasticSearch client)
- `System.Threading.Tasks.Dataflow` 5.0.0
- `Microsoft.Extensions.Hosting` 5.0.0
- `Microsoft.Extensions.Logging.Abstractions` 5.0.0
- Project ref: `OneSTools.EventLog`

### OneSTools.EventLog.Exporter (v1.0.5) — Executable Service
**Target**: `net5.0` | **Assembly**: `EventLogExporter`

**Dependencies**:
- `Microsoft.Extensions.Hosting` 5.0.0
- `Microsoft.Extensions.Hosting.WindowsServices` 5.0.1
- `Microsoft.Extensions.Hosting.Systemd` 5.0.1
- Project ref: `OneSTools.EventLog.Exporter.Core`

### OneSTools.EventLog.Exporter.Manager (v0.0.5) — Executable Service
**Target**: `net5.0` | **Assembly**: `EventLogExportersManager`

**Dependencies**:
- `Microsoft.Extensions.Hosting` 5.0.0
- `Microsoft.Extensions.Hosting.WindowsServices` 5.0.1
- `Microsoft.Extensions.Hosting.Systemd` 5.0.1
- Project ref: `OneSTools.EventLog.Exporter.Core`

## Build & Installation

```bash
dotnet build OneSTools.EventLog.sln
dotnet build OneSTools.EventLog.sln -c Release
dotnet publish OneSTools.EventLog.Exporter -c Release
dotnet publish OneSTools.EventLog.Exporter.Manager -c Release
```

Build output goes to `Build/Debug/` or `Build/Release/`.

## Main Entry Points
- **Exporter**: `OneSTools.EventLog.Exporter/Program.cs` → `EventLogExporterService.cs`
- **Manager**: `OneSTools.EventLog.Exporter.Manager/Program.cs` → `ExportersManager.cs`
- **Core reader**: `OneSTools.EventLog/EventLogReader.cs`

## Configuration
Both executables use `appsettings.json` with sections:
- **`Exporter`**: `StorageType` (1=ClickHouse, 2=ElasticSearch), `LogFolder`, `Portion`, `TimeZone`, `WritingMaxDegreeOfParallelism`, `CollectedFactor`, `ReadingTimeout`, `LoadArchive`, `LogFilesStoringDays`
- **`ClickHouse`**: `ConnectionString`, `ConvertJsonToSeparateTables`
- **`ElasticSearch`**: `Nodes[]` (host, auth type), `Index`, `Separation` (H/D/M/all), `MaximumRetries`, `MaxRetryTimeout`
- **`Manager`**: `ClstFolders[]` with `Folder` path and `Templates[]` (regex `Mask` + `Template` with `[IBNAME]` variable)

## Running as a Service

**Windows**:
```
sc create EventLogExporter binPath= "C:\elexporter\EventLogExporter.exe"
sc start EventLogExporter
```

**Linux (systemd)**:
```
systemctl start eventlogexporter.service
```
