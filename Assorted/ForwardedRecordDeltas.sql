DECLARE @DeltaDelay varchar(20) = '00:10:00';

CREATE TABLE
	#HeapStatsInitial
	(
		DatabaseId int NULL
		,DatabaseName sysname NULL
		,SchemaId int NULL
		,SchemaName sysname NULL
		,TableId int NULL
		,TableName sysname NULL
		,UsedPageCount bigint NULL
		,ReservedPageCount bigint NULL
		,[RowCount] bigint NULL
		,ForwardedFetchCount bigint NULL
	);

CREATE TABLE
	#HeapStatsFinal
	(
		DatabaseId int NULL
		,DatabaseName sysname NULL
		,SchemaId int NULL
		,SchemaName sysname NULL
		,TableId int NULL
		,TableName sysname NULL
		,UsedPageCount bigint NULL
		,ReservedPageCount bigint NULL
		,[RowCount] bigint NULL
		,ForwardedFetchCount bigint NULL
	);

DECLARE @CurrentDatabaseId int;
DECLARE @CurrentDatabaseName sysname;
DECLARE @Sql nvarchar(4000);

DECLARE DatabaseCursor CURSOR LOCAL FAST_FORWARD FOR
SELECT
	D.database_id
	,D.[name]
FROM
	sys.databases D
WHERE
	D.[name] NOT IN (N'master', N'model', N'msdb', N'tempdb', N'distribution');

OPEN DatabaseCursor;

FETCH
	DatabaseCursor
INTO
	@CurrentDatabaseId
	,@CurrentDatabaseName;

WHILE (@@FETCH_STATUS = 0)
BEGIN
	PRINT CAST(@CurrentDatabaseId AS varchar(20));

	SET @Sql =  N'
		INSERT INTO
			#HeapStatsInitial
			(
				DatabaseId
				,DatabaseName
				,SchemaId
				,SchemaName
				,TableId
				,TableName
				,UsedPageCount
				,ReservedPageCount
				,[RowCount]
				,ForwardedFetchCount
			)
		SELECT
			' + CAST(@CurrentDatabaseId AS nvarchar(10)) + N' AS DatabaseId
			,N''' + @CurrentDatabaseName + N''' AS DatabaseName
			,S.[schema_id] AS SchemaId
			,S.[name] AS SchemaName
			,T.[object_id] AS TableId
			,T.[name] AS TableName
			,COALESCE(SUM(PS.used_page_count), 0) AS UsedPageCount
			,COALESCE(SUM(PS.reserved_page_count), 0) AS ReservedPageCount
			,COALESCE(SUM(PS.row_count), 0) AS [RowCount]
			,COALESCE(IOS.forwarded_fetch_count, 0) AS ForwardedFetchCount
		FROM
			[' + @CurrentDatabaseName + N'].sys.schemas S
			JOIN [' + @CurrentDatabaseName + N'].sys.tables T ON S.[schema_id] = T.[schema_id]
			JOIN [' + @CurrentDatabaseName + N'].sys.indexes I ON T.[object_id] = I.[object_id]
			JOIN [' + @CurrentDatabaseName + N'].sys.dm_db_partition_stats PS ON I.[object_id] = PS.[object_id] AND I.[index_id] = PS.[index_id]
			--OUTER APPLY sys.dm_db_index_operational_stats(' + CAST(@CurrentDatabaseId AS nvarchar(20)) + N', T.[object_id], I.[index_id], NULL) IOS
			LEFT JOIN sys.dm_db_index_operational_stats(' + CAST(@CurrentDatabaseId AS nvarchar(20)) + N', NULL, NULL, NULL) IOS ON T.[object_id] = IOS.[object_id] AND I.[index_id] = IOS.[index_id]
		WHERE
			I.index_id = 0
		GROUP BY
			S.[schema_id]
			,S.[name]
			,T.[object_id]
			,T.[name]
			,IOS.forwarded_fetch_count;'

		EXEC sp_executesql @Sql;

	FETCH
		DatabaseCursor
	INTO
		@CurrentDatabaseId
		,@CurrentDatabaseName;
END

CLOSE DatabaseCursor;
DEALLOCATE DatabaseCursor;

WAITFOR DELAY @DeltaDelay;

DECLARE DatabaseCursor CURSOR LOCAL FAST_FORWARD FOR
SELECT
	D.database_id
	,D.[name]
FROM
	sys.databases D
WHERE
	D.[name] NOT IN (N'master', N'model', N'msdb', N'tempdb', N'distribution');

OPEN DatabaseCursor;

FETCH
	DatabaseCursor
INTO
	@CurrentDatabaseId
	,@CurrentDatabaseName;

WHILE (@@FETCH_STATUS = 0)
BEGIN
	PRINT CAST(@CurrentDatabaseId AS varchar(20));

	SET @Sql =  N'
		INSERT INTO
			#HeapStatsFinal
			(
				DatabaseId
				,DatabaseName
				,SchemaId
				,SchemaName
				,TableId
				,TableName
				,UsedPageCount
				,ReservedPageCount
				,[RowCount]
				,ForwardedFetchCount
			)
		SELECT
			' + CAST(@CurrentDatabaseId AS nvarchar(10)) + N' AS DatabaseId
			,N''' + @CurrentDatabaseName + N''' AS DatabaseName
			,S.[schema_id] AS SchemaId
			,S.[name] AS SchemaName
			,T.[object_id] AS TableId
			,T.[name] AS TableName
			,COALESCE(SUM(PS.used_page_count), 0) AS UsedPageCount
			,COALESCE(SUM(PS.reserved_page_count), 0) AS ReservedPageCount
			,COALESCE(SUM(PS.row_count), 0) AS [RowCount]
			,COALESCE(IOS.forwarded_fetch_count, 0) AS ForwardedFetchCount
		FROM
			[' + @CurrentDatabaseName + N'].sys.schemas S
			JOIN [' + @CurrentDatabaseName + N'].sys.tables T ON S.[schema_id] = T.[schema_id]
			JOIN [' + @CurrentDatabaseName + N'].sys.indexes I ON T.[object_id] = I.[object_id]
			JOIN [' + @CurrentDatabaseName + N'].sys.dm_db_partition_stats PS ON I.[object_id] = PS.[object_id] AND I.[index_id] = PS.[index_id]
			--OUTER APPLY sys.dm_db_index_operational_stats(' + CAST(@CurrentDatabaseId AS nvarchar(20)) + N', T.[object_id], I.[index_id], NULL) IOS
			LEFT JOIN sys.dm_db_index_operational_stats(' + CAST(@CurrentDatabaseId AS nvarchar(20)) + N', NULL, NULL, NULL) IOS ON T.[object_id] = IOS.[object_id] AND I.[index_id] = IOS.[index_id]
		WHERE
			I.index_id = 0
		GROUP BY
			S.[schema_id]
			,S.[name]
			,T.[object_id]
			,T.[name]
			,IOS.forwarded_fetch_count;'

		EXEC sp_executesql @Sql;

	FETCH
		DatabaseCursor
	INTO
		@CurrentDatabaseId
		,@CurrentDatabaseName;
END

CLOSE DatabaseCursor;
DEALLOCATE DatabaseCursor;

SELECT
	F.DatabaseId
	,F.DatabaseName
	,F.SchemaId
	,F.SchemaName
	,F.TableId
	,F.TableName
	,F.UsedPageCount * 8192.0 / 1024.0 / 1024.0 AS UsedMB
	,F.ReservedPageCount * 8192.0 / 1024.0 / 1024.0 AS ReservedMB
	,F.[RowCount]
	,F.ForwardedFetchCount
	,F.ForwardedFetchCount - COALESCE(I.ForwardedFetchCount, 0) AS ForwardedFetchDelta
FROM
	#HeapStatsFinal F
	LEFT JOIN #HeapStatsInitial I ON F.DatabaseId = I.DatabaseId AND F.SchemaId = I.SchemaId AND F.TableId = I.TableId
ORDER BY
	F.ForwardedFetchCount - COALESCE(I.ForwardedFetchCount, 0) DESC
	,F.ForwardedFetchCount DESC;

DROP TABLE #HeapStatsInitial;
DROP TABLE #HeapStatsFinal;
