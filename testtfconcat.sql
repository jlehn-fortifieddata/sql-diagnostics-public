SET NOCOUNT ON;

USE [FDDBA];

DECLARE @DefinedStartupTraceFlags nvarchar(3950);
DECLARE @RevisedStartupTraceFlags nvarchar(3950);
DECLARE @DebugOutput nvarchar(4000);

-- Set this value to 1 if you actually want the config values to be updated.
DECLARE @PerformUpdate bit = 1;

-- Create a temporary table to hold active trace flags on the instance.
CREATE TABLE
	#ActiveTraceFlags
	(
		TraceFlag int
		,[Status] int
		,[Global] int
		,[Session] int
	);

-- Insert active trace flags into the temporary table.
INSERT INTO
	#ActiveTraceFlags
EXEC ('DBCC TRACESTATUS (-1) WITH NO_INFOMSGS');

-- Remove any trace flags that are not global.
DELETE
	#ActiveTraceFlags
WHERE
	[Global] != 1;

-- Get the current FDDBA trace flag config value.
SELECT TOP (1)
	@DefinedStartupTraceFlags = [value]
FROM
	[FDDBA].[dbo].[config_fddba2] X
WHERE
	[name] = 'trace_flags'
ORDER BY
	[CreatedOn] DESC;

-- Split the trace flags from the FDDBA config into separate rows in the temp table.
;WITH FDDBATraceFlags AS
(
	SELECT
		LEFT(X.[val], CHARINDEX(N'=', X.[val], 1) - 1) AS TraceFlag
		,SUBSTRING(X.[val], CHARINDEX(N'=', X.[val], 1) + 1, 1) AS TraceFlagValue
	FROM
		[FDDBA].dbo.fSplitNString(@DefinedStartupTraceFlags, N',') X
	WHERE
		LEN(LTRIM(RTRIM(X.[val]))) > 0
)
INSERT INTO
	#ActiveTraceFlags
	(
		TraceFlag
		,[Status]
		,[Global]
		,[Session]
	)
SELECT
	X.TraceFlag
	,X.TraceFlagValue
	,NULL
	,NULL
FROM
	FDDBATraceFlags X
WHERE
	X.TraceFlag NOT IN
	(
		SELECT DISTINCT
			Y.TraceFlag
		FROM
			#ActiveTraceFlags Y
	);

-- Gather up all of the trace flags from the temp table and put them into a new config value.
SELECT
	@RevisedStartupTraceFlags = STUFF((
		SELECT
			N',' + CAST(X.[TraceFlag] AS nvarchar(10)) + N'=' + CAST(X.[Status] AS nvarchar(10))
		FROM
			#ActiveTraceFlags X
		ORDER BY
			X.TraceFlag
		FOR XML PATH('')), 1, 1, '') + N',';

SET @DebugOutput = N'Server name:  ' + @@SERVERNAME + CHAR(13)
	+ N'Current FDDBA trace flags:  ' + @DefinedStartupTraceFlags + CHAR(13)
	+ N'New FDDBA trace flags:  ' + @RevisedStartupTraceFlags

PRINT @DebugOutput;

-- If we are going to update the config table, only do so if the trace flags are not defined properly.
IF (@PerformUpdate = 1 AND @RevisedStartupTraceFlags != @DefinedStartupTraceFlags)
BEGIN
	-- Write out the current value to the history table, just in case.
	INSERT INTO
		[FDDBA].[dbo].[config_fddba2_history]
		(
			[name]
			,[domain]
			,[CreatedOn]
			,[UpdatedOn]
			,[value]
			,[valuebig]
			,[comment]
		)
	SELECT
		[name]
		,[domain]
		,[CreatedOn]
		,[UpdatedOn]
		,[value]
		,[valuebig]
		,[comment]
	FROM
		[FDDBA].[dbo].[config_fddba2]
	WHERE
		[name] = 'trace_flags';

	UPDATE
		[FDDBA].[dbo].[config_fddba2]
	SET
		UpdatedOn = GETDATE()
		,[value] = @RevisedStartupTraceFlags
	WHERE
		[name] = 'trace_flags';
END

DROP TABLE #ActiveTraceFlags;
