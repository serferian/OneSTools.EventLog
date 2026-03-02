chcp 65001

sc stop EventLogExporter

timeout /t 4

xcopy "%~dp0*.*" "C:\EventLogExporter\" /y

sc start EventLogExporter