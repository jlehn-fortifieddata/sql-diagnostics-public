DECLARE @DatabaseName nvarchar(128) = N'TestDB';
DECLARE @SchemaName nvarchar(128) = N'dbo';
DECLARE @ObjectName nvarchar(128) = N'TestObject';
DECLARE @TopResults int = 50;

DECLARE @FullyQualifiedObjectName nvarchar(2000) =  N'[' + @DatabaseName + '].[' + @SchemaName + '].[' + @ObjectName + ']';
DECLARE @DatabaseId smallint = DB_ID(@DatabaseName);
DECLARE @ObjectId int = OBJECT_ID(@FullyQualifiedObjectName);

;WITH AggregatedData AS
(
SELECT
	DB_NAME(EPS.[database_id]) AS [database]
	,OBJECT_SCHEMA_NAME(EPS.[object_id], EPS.[database_id]) AS [schema]
	,OBJECT_NAME(EPS.[object_id], EPS.[database_id]) AS [object]
	,EPS.[type]
	,EPS.[type_desc]
	,EPS.[cached_time]
	,EPS.[last_execution_time]
	,EPS.[execution_count]
	,EPS.[execution_count] / (DATEDIFF(MILLISECOND, EPS.[cached_time], GETDATE()) / 1000.0 / 60.0) AS [avg_executions_per_min]
	,EPS.[execution_count] / (DATEDIFF(MILLISECOND, EPS.[cached_time], GETDATE()) / 1000.0) AS [avg_executions_per_sec]
	,EPS.[total_worker_time]
	,CAST(EPS.[total_worker_time] AS float) / CAST(EPS.[execution_count] AS float) / 1000.0 AS [avg_worker_time_ms]
	,EPS.[min_worker_time]
	,EPS.[max_worker_time]
	,EPS.[total_elapsed_time]
	,CAST(EPS.[total_elapsed_time] AS float) / CAST(EPS.[execution_count] AS float) / 1000.0 AS [avg_elapsed_time_ms]
	,EPS.[min_elapsed_time]
	,EPS.[max_elapsed_time]
	,EPS.[total_logical_reads]
	,CAST(EPS.[total_logical_reads] AS float) / CAST(EPS.[execution_count] AS float) AS [avg_logical_reads]
	,EPS.[min_logical_reads]
	,EPS.[max_logical_reads]
	,EPS.[total_physical_reads]
	,CAST(EPS.[total_physical_reads] AS float) / CAST(EPS.[execution_count] AS float) AS [avg_physical_reads]
	,EPS.[min_physical_reads]
	,EPS.[max_physical_reads]
	,EPS.[total_logical_writes]
	,CAST(EPS.[total_logical_writes] AS float) / CAST(EPS.[execution_count] AS float) AS [avg_logical_writes]
	,EPS.[min_logical_writes]
	,EPS.[max_logical_writes]
	,EPS.[plan_handle]
FROM
	sys.dm_exec_procedure_stats EPS
WHERE
	1 = 1
	--AND EPS.database_id = @DatabaseId
	--AND EPS.object_id = @ObjectId
)
,AggregatedDataFiltered AS
(
	SELECT TOP (@TopResults)
		*
	FROM
		AggregatedData AG
	ORDER BY
		AG.[avg_executions_per_min] DESC
)
SELECT
	AGF.*
	,TRY_CAST(EQP.[query_plan] AS XML) AS [query_plan]
FROM
	AggregatedDataFiltered AGF
	OUTER APPLY sys.dm_exec_query_plan(AGF.[plan_handle]) EQP
ORDER BY
	AGF.[avg_executions_per_min] DESC;

