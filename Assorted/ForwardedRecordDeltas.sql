/*
SELECT
	D.[database_id]
	,D.[name]
FROM
	sys.databases D;
*/

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

DECLARE @CurrentDatabaseId int;
DECLARE @CurrentDatabaseName sysname;
DECLARE @Sql nvarchar(4000);

DECLARE DatabaseCursor CURSOR LOCAL FAST_FORWARD FOR
SELECT
	D.database_id
	,D.[name]
FROM
	sys.databases D;

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
			' + @CurrentDatabaseName + N'.sys.schemas S
			JOIN ' + @CurrentDatabaseName + N'.sys.tables T ON S.[schema_id] = T.[schema_id]
			JOIN ' + @CurrentDatabaseName + N'.sys.indexes I ON T.[object_id] = I.[object_id]
			JOIN ' + @CurrentDatabaseName + N'.sys.dm_db_partition_stats PS ON I.[object_id] = PS.[object_id] AND I.[index_id] = PS.[index_id]
			OUTER APPLY sys.dm_db_index_operational_stats(' + CAST(@CurrentDatabaseId AS nvarchar(20)) + N', T.[object_id], I.[index_id], NULL) IOS
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

/*
INSERT INTO
	#HeapStatsInitial
SELECT
	1 AS DatabaseId
	,N'DK_DB_REFERENCE' AS DatabaseName
	,S.[schema_id] AS SchemaId
	,S.[name] AS SchemaName
	,T.[object_id] AS TableId
	,T.[name] AS TableName
	,COALESCE(SUM(PS.used_page_count), 0) AS UsedPageCount
	,COALESCE(SUM(PS.reserved_page_count), 0) AS ReservedPageCount
	,COALESCE(SUM(PS.row_count), 0) AS [RowCount]
	,COALESCE(IOS.forwarded_fetch_count, 0) AS ForwardedFetchCount
FROM
	[DK_DB_REFERENCE].sys.schemas S
	JOIN [DK_DB_REFERENCE].sys.tables T ON S.[schema_id] = T.[schema_id]
	JOIN [DK_DB_REFERENCE].sys.indexes I ON T.[object_id] = I.[object_id]
	JOIN [DK_DB_REFERENCE].sys.dm_db_partition_stats PS ON I.[object_id] = PS.[object_id] AND I.[index_id] = PS.[index_id]
	OUTER APPLY sys.dm_db_index_operational_stats(DB_ID('DK_DB_REFERENCE'), T.[object_id], I.[index_id], NULL) IOS
WHERE
	I.index_id = 0
GROUP BY
	S.[schema_id]
	,S.[name]
	,T.[object_id]
	,T.[name]
	,IOS.forwarded_fetch_count;
*/

SELECT * FROM #HeapStatsInitial;

DROP TABLE #HeapStatsInitial;
