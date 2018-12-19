/*
SELECT TOP (1)
	*
FROM
	[FDDBA].[dbo].[config_fddba2_history] X
WHERE
	X.[name] = 'trace_flags_existing'
ORDER BY
	X.CreatedOn DESC;

SELECT
	*
FROM
	[FDDBA].[dbo].[config_fddba2];
*/

CREATE TABLE
	#ActiveTraceFlags
	(
		TraceFlag int
		,[Status] int
		,[Global] int
		,[Session] int
	);

INSERT INTO
	#ActiveTraceFlags
EXEC ('DBCC TRACESTATUS (-1) WITH NO_INFOMSGS');

SELECT
	*
FROM
	#ActiveTraceFlags;

DECLARE @DefinedStartupTraceFlags nvarchar(3950);
DECLARE @RevisedStartupTraceFlags nvarchar(3950);

SELECT TOP (1)
	@DefinedStartupTraceFlags = [value]
FROM
	[FDDBA].[dbo].[config_fddba2] X
WHERE
	[name] = 'trace_flags'
ORDER BY
	[CreatedOn] DESC;

SELECT
	@DefinedStartupTraceFlags;

;WITH FDDBATraceFlags AS
(
	SELECT
		LEFT(X.[val], CHARINDEX(N'=', X.[val], 1) - 1) AS TraceFlag
		,SUBSTRING(X.[val], CHARINDEX(N'=', X.[val], 1) + 1, 1) AS TraceFlagValue
	FROM
		dbo.fSplitNString(@DefinedStartupTraceFlags, N',') X
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

SELECT
	*
FROM
	#ActiveTraceFlags;

SELECT
	@RevisedStartupTraceFlags = STUFF((
		SELECT
			N',' + CAST(X.[TraceFlag] AS nvarchar(10)) + N'=' + CAST(X.[Status] AS nvarchar(10))
		FROM
			#ActiveTraceFlags X
		FOR XML PATH('')), 1, 1, '') + N',';

SELECT
	@DefinedStartupTraceFlags;

SELECT
	@RevisedStartupTraceFlags;

DROP TABLE #ActiveTraceFlags;
