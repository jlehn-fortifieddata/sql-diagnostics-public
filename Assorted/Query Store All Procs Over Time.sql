/*
SELECT
	D.[name]
	,D.[is_query_store_on]
FROM
	sys.databases D
ORDER BY
	D.[name];
*/

CREATE TABLE
	#QueryStats
	(
		[database_name] sysname
		,[schema_name] sysname
		,[object_name] sysname
		,[count_executions] bigint
		,[total_duration] bigint
		,[total_cpu_time] bigint
		,[total_logical_io_reads] bigint
		,[total_logical_io_writes] bigint
		,[total_physical_io_reads] bigint
	);

DECLARE DatabaseCursor CURSOR LOCAL FAST_FORWARD FOR
SELECT
	D.[name]
FROM
	[sys].[databases] D
WHERE
	D.[name] NOT IN (N'master', N'model', N'msdb', N'distribution', N'tempdb')
	AND D.[is_query_store_on] = 1
ORDER BY
	D.[name];

DECLARE @CurrentDatabase sysname;
DECLARE @ExecuteSQL nvarchar(max);

OPEN DatabaseCursor;

FETCH
	DatabaseCursor
INTO
	@CurrentDatabase;

WHILE (@@FETCH_STATUS = 0)
BEGIN
	SET @ExecuteSQL = N'
	USE [' + @CurrentDatabase + '];

	DECLARE @StartUTC datetimeoffset = CAST(''20190501 00:00:00 -05:00'' AS datetimeoffset) AT TIME ZONE ''UTC'';
	DECLARE @EndUTC datetimeoffset = CAST(''20190604 00:00:00 -05:00'' AS datetimeoffset) AT TIME ZONE ''UTC'';

	;WITH MatchingQueries AS
	(
		SELECT
			DB_NAME() AS [database_name]
			,OBJECT_SCHEMA_NAME(QSQ.[object_id]) AS [schema_name]
			,OBJECT_NAME(QSQ.[object_id]) AS [object_name]
			,QSQ.[query_id]
		FROM
			sys.query_store_query QSQ
		WHERE
			OBJECT_SCHEMA_NAME(QSQ.[object_id]) = ''SQLReport''
			OR
			(
				OBJECT_SCHEMA_NAME(QSQ.[object_id]) = ''Lyra''
				AND OBJECT_NAME(QSQ.[object_id]) LIKE ''rpt%''
			)
	)
	,MatchingQueryStats AS
	(
		SELECT
			MQ.[database_name]
			,MQ.[schema_name]
			,MQ.[object_name]
			,QSRS.[count_executions]
			,QSRS.[count_executions] * QSRS.[avg_duration] AS total_duration
			,QSRS.[count_executions] * QSRS.[avg_cpu_time] AS total_cpu_time
			,QSRS.[count_executions] * QSRS.[avg_logical_io_reads] AS total_logical_io_reads
			,QSRS.[count_executions] * QSRS.[avg_logical_io_writes] AS total_logical_io_writes
			,QSRS.[count_executions] * QSRS.[avg_physical_io_reads] AS total_physical_io_reads
		FROM
			MatchingQueries MQ
			JOIN [sys].[query_store_plan] QSP ON MQ.[query_id] = QSP.[query_id]
			JOIN sys.query_store_runtime_stats QSRS ON QSP.[plan_id] = QSRS.[plan_id]
			JOIN [sys].[query_store_runtime_stats_interval] QSRSI ON QSRS.[runtime_stats_interval_id] = QSRSI.[runtime_stats_interval_id]
		WHERE
			QSRSI.start_time >= @StartUTC
			AND QSRSI.end_time <= @EndUTC
	)
	INSERT INTO
		#QueryStats
		(
			[database_name]
			,[schema_name]
			,[object_name]
			,[count_executions]
			,[total_duration]
			,[total_cpu_time]
			,[total_logical_io_reads]
			,[total_logical_io_writes]
			,[total_physical_io_reads]
		)
	SELECT
		MQS.[database_name]
		,MQS.[schema_name]
		,MQS.[object_name]
		,SUM(MQS.[count_executions]) AS [count_executions]
		,SUM(MQS.[total_duration]) AS [total_duration]
		,SUM(MQS.[total_cpu_time]) AS [total_cpu_time]
		,SUM(MQS.[total_logical_io_reads]) AS [total_logical_io_reads]
		,SUM(MQS.[total_logical_io_writes]) AS [total_logical_io_writes]
		,SUM(MQS.[total_physical_io_reads]) AS [total_physical_io_reads]
	FROM
		MatchingQueryStats MQS
	GROUP BY
		MQS.[database_name]
		,MQS.[schema_name]
		,MQS.[object_name];';

	PRINT @CurrentDatabase;

	EXEC sp_executesql @ExecuteSQL;

	FETCH
		DatabaseCursor
	INTO
		@CurrentDatabase;
END

CLOSE DatabaseCursor;
DEALLOCATE DatabaseCursor;

SELECT
	@@SERVERNAME AS [server_name]
	,QS.*
FROM
	#QueryStats QS;

DROP TABLE #QueryStats;
