DECLARE @QueryHash binary(8) = 0x8F8FE6AB22D509B7;

/*
SELECT
	QS.*
FROM
	sys.dm_exec_query_stats QS
WHERE
	QS.query_hash = @QueryHash;
*/

;WITH SummarizedPlanCache AS
(
	SELECT
		QS.[plan_handle]
		,QS.[sql_handle]
		,QS.statement_start_offset
		,QS.statement_end_offset
		,QS.query_hash
		,QS.query_plan_hash
		,QS.creation_time
		,QS.last_execution_time
		,QS.execution_count
		,CASE WHEN SUM(QS.execution_count) > 0 THEN CAST(QS.execution_count AS FLOAT) / CAST(SUM(QS.execution_count) OVER () AS FLOAT) * 100.0 ELSE 0.0 END AS percent_executions
		,QS.total_worker_time
		,QS.last_worker_time
		,QS.min_worker_time
		,QS.max_worker_time
		,CASE WHEN SUM(QS.total_worker_time) > 0 THEN CAST(QS.total_worker_time AS FLOAT) / CAST(SUM(QS.total_worker_time) OVER () AS FLOAT) * 100.0 ELSE 0.0 END AS percent_total_worker_time
		,QS.total_physical_reads
		,QS.last_physical_reads
		,QS.min_physical_reads
		,QS.max_physical_reads
		,CASE WHEN SUM(QS.total_physical_reads) > 0 THEN CAST(QS.total_physical_reads AS FLOAT) / CAST(SUM(QS.total_physical_reads) OVER () AS FLOAT) * 100.0 ELSE 0.0 END AS percent_total_physical_reads
		,QS.total_logical_writes
		,QS.last_logical_writes
		,QS.min_logical_writes
		,QS.max_logical_writes
		,CASE WHEN SUM(QS.total_logical_writes) > 0 THEN CAST(QS.total_logical_writes AS FLOAT) / CAST(SUM(QS.total_logical_writes) OVER () AS FLOAT) * 100.0 ELSE 0.0 END AS percent_total_logical_writes
		,QS.total_logical_reads
		,QS.last_logical_reads
		,QS.min_logical_reads
		,QS.max_logical_reads
		,CASE WHEN SUM(QS.total_logical_reads) > 0 THEN CAST(QS.total_logical_reads AS FLOAT) / CAST(SUM(QS.total_logical_reads) OVER () AS FLOAT) * 100.0 ELSE 0.0 END AS percent_total_logical_reads
		,QS.total_clr_time
		,QS.last_clr_time
		,QS.min_clr_time
		,QS.max_clr_time
		,QS.total_elapsed_time
		,QS.last_elapsed_time
		,QS.min_elapsed_time
		,QS.max_elapsed_time
		,CASE WHEN SUM(QS.total_elapsed_time) > 0 THEN CAST(QS.total_elapsed_time AS FLOAT) / CAST(SUM(QS.total_elapsed_time) OVER () AS FLOAT) * 100.0 ELSE 0.0 END AS percent_total_elapsed_time
		,QS.total_rows
		,QS.last_rows
		,QS.min_rows
		,QS.max_rows
		,CASE WHEN SUM(QS.total_rows) > 0 THEN CAST(QS.total_rows AS FLOAT) / CAST(SUM(QS.total_rows) OVER () AS FLOAT) * 100.0 ELSE 0.0 END AS percent_total_rows
		,RANK() OVER (ORDER BY QS.execution_count DESC) AS execution_count_rank
		,RANK() OVER (ORDER BY QS.total_worker_time DESC) AS total_worker_time_rank
		,RANK() OVER (ORDER BY QS.total_logical_reads DESC) AS total_logical_reads_rank
		,RANK() OVER (ORDER BY QS.total_logical_writes DESC) AS total_logical_writes_rank
		,RANK() OVER (ORDER BY QS.total_physical_reads DESC) AS total_physical_reads_rank
		,RANK() OVER (ORDER BY QS.total_elapsed_time DESC) AS total_elapsed_time_rank
	FROM
		sys.dm_exec_query_stats QS
	WHERE
		QS.query_hash = @QueryHash
	GROUP BY
		QS.[plan_handle]
		,QS.[sql_handle]
		,QS.statement_start_offset
		,QS.statement_end_offset
		,QS.query_hash
		,QS.query_plan_hash
		,QS.creation_time
		,QS.last_execution_time
		,QS.execution_count
		,QS.total_worker_time
		,QS.last_worker_time
		,QS.min_worker_time
		,QS.max_worker_time
		,QS.total_physical_reads
		,QS.last_physical_reads
		,QS.min_physical_reads
		,QS.max_physical_reads
		,QS.total_logical_writes
		,QS.last_logical_writes
		,QS.min_logical_writes
		,QS.max_logical_writes
		,QS.total_logical_reads
		,QS.last_logical_reads
		,QS.min_logical_reads
		,QS.max_logical_reads
		,QS.total_clr_time
		,QS.last_clr_time
		,QS.min_clr_time
		,QS.max_clr_time
		,QS.total_elapsed_time
		,QS.last_elapsed_time
		,QS.min_elapsed_time
		,QS.max_elapsed_time
		,QS.total_rows
		,QS.last_rows
		,QS.min_rows
		,QS.max_rows
)
SELECT
	@@SERVERNAME AS server_name
	,D.name AS database_name
	,OBJECT_SCHEMA_NAME(ST.objectid, ST.[dbid]) AS [schema_name]
	,OBJECT_NAME(ST.objectid, ST.[dbid]) AS [object_name]
	,REPLACE(REPLACE(REPLACE(SUBSTRING(ST.[text], FCS.statement_start_offset / 2 + 1, (CASE WHEN FCS.statement_end_offset = -1 THEN LEN(CONVERT(NVARCHAR(MAX), ST.[text])) * 2 ELSE FCS.statement_end_offset END - FCS.statement_start_offset) / 2 + 1), CHAR(9), ' '), CHAR(10), ' '), CHAR(13), ' ') AS query_text
	,FCS.creation_time
	,FCS.last_execution_time
	,FCS.execution_count
	,FCS.percent_executions
	,CAST(FCS.execution_count AS FLOAT) / (CAST(DATEDIFF(SECOND, FCS.creation_time, GETDATE()) AS FLOAT) / 60.0) AS executions_per_minute
	,FCS.total_worker_time
	,FCS.last_worker_time
	,FCS.min_worker_time
	,FCS.max_worker_time
	,(CAST(FCS.total_worker_time AS FLOAT) / 1000.0) / CAST(FCS.execution_count AS FLOAT) AS avg_worker_time_ms
	,FCS.percent_total_worker_time
	,FCS.total_physical_reads
	,FCS.last_physical_reads
	,FCS.min_physical_reads
	,FCS.max_physical_reads
	,CAST(FCS.total_physical_reads AS FLOAT) / CAST(FCS.execution_count AS FLOAT) AS avg_physical_reads
	,FCS.percent_total_physical_reads
	,FCS.total_logical_writes
	,FCS.last_logical_writes
	,FCS.min_logical_writes
	,FCS.max_logical_writes
	,CAST(FCS.total_logical_writes AS FLOAT) / CAST(FCS.execution_count AS FLOAT) AS avg_logical_writes
	,FCS.percent_total_logical_writes
	,FCS.total_logical_reads
	,FCS.last_logical_reads
	,FCS.min_logical_reads
	,FCS.max_logical_reads
	,CAST(FCS.total_logical_reads AS FLOAT) / CAST(FCS.execution_count AS FLOAT) AS avg_logical_reads
	,FCS.percent_total_logical_reads
	,FCS.total_elapsed_time
	,FCS.last_elapsed_time
	,FCS.min_elapsed_time
	,FCS.max_elapsed_time
	,(CAST(FCS.total_elapsed_time AS FLOAT) / 1000.0) / CAST(FCS.execution_count AS FLOAT) AS avg_elapsed_time_ms
	,FCS.percent_total_elapsed_time
	,FCS.total_rows
	,FCS.last_rows
	,FCS.min_rows
	,FCS.max_rows
	,CAST(FCS.total_rows AS FLOAT) / CAST(FCS.execution_count AS FLOAT) AS avg_rows
	,FCS.percent_total_rows
	,FCS.execution_count_rank
	,FCS.total_worker_time_rank
	,FCS.total_logical_reads_rank
	,FCS.total_logical_writes_rank
	,FCS.total_physical_reads_rank
	,FCS.total_elapsed_time_rank
	,FCS.query_hash
	,FCS.query_plan_hash
	,EQP.query_plan
FROM
	SummarizedPlanCache FCS
	CROSS APPLY sys.dm_exec_sql_text(FCS.[sql_handle]) ST
	LEFT JOIN sys.databases D ON ST.[dbid] = D.database_id
	CROSS APPLY sys.dm_exec_query_plan(FCS.[plan_handle]) EQP
ORDER BY
	FCS.total_worker_time DESC
OPTION (RECOMPILE);
