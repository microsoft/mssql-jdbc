-- Idempotently seed the local "Azure Arc" properties the JDBC driver self-discovers on telemetry
-- boot. Modelled on the metadata an Arc-enabled SQL Server exposes: its Azure resource id, the local
-- OTLP collector endpoint, and the Azure region. Values are injected by sqlcmd -v from env vars
-- (see docker-compose.yml: sqlserver-init). Safe to re-run: CREATE is guarded, the row is replaced.
USE msdb;
GO

IF OBJECT_ID('dbo.SQLServerAzureArcProperties', 'U') IS NULL
    CREATE TABLE dbo.SQLServerAzureArcProperties (
        AzureResourceId       NVARCHAR(512) NULL,
        DemoLocalOtelEndpoint NVARCHAR(512) NULL,
        AzureRegion           NVARCHAR(128) NULL
    );
GO

DELETE FROM dbo.SQLServerAzureArcProperties;
INSERT INTO dbo.SQLServerAzureArcProperties (AzureResourceId, DemoLocalOtelEndpoint, AzureRegion)
VALUES (N'$(ArcId)', N'$(OtelEndpoint)', N'$(Region)');
GO

SELECT AzureResourceId, DemoLocalOtelEndpoint, AzureRegion FROM dbo.SQLServerAzureArcProperties;
GO
