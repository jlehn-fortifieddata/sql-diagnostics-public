DECLARE @SchemaName sysname = N'dbo';  -- Sales, Sales, Sales
DECLARE @ObjectName sysname = N'BankTransactions';  -- Invoices, SpecialDeals_HEAP, Orders

-- Variables to hold lookup information for later.
DECLARE @schema_id int;
DECLARE @object_id int;

-- Check to see if the table/view we want to inspect exists.
SELECT
	@schema_id = S.[schema_id]
	,@object_id = T.[object_id]
FROM
	[sys].[schemas] S
	JOIN [sys].[tables] T ON S.[schema_id] = T.[schema_id]
WHERE
	S.[name] = @SchemaName
	AND T.[name] = @ObjectName;

IF (@object_id IS NULL)
BEGIN
	PRINT 'No table named [' + @SchemaName + '].[' + @ObjectName + '] exists in the current database (' + DB_NAME() + ').';
	GOTO EndExecution;
END

-- Get the overall information for the table.
SELECT
	@SchemaName AS [schema_name]
	,T.[name] AS [table_name]
	,T.[create_date] AS [created]
	,T.[is_ms_shipped]
	,T.[is_published]
	,T.[is_schema_published]
	,T.[lock_on_bulk_load]
	,T.[is_replicated]
	,T.[has_replication_filter]
	,T.[is_merge_published]
	,T.[is_tracked_by_cdc]
	,T.[lock_escalation]
	,T.[lock_escalation_desc]
	,T.[durability]
	,T.[durability_desc]
FROM
	[sys].[tables] T
WHERE
	T.[object_id] = @object_id;

SELECT
	C.[name]
	,T.[name]
	,C.[max_length]
	,C.[precision]
	,C.[scale]
	,C.[collation_name]
	,C.[is_nullable]
	,C.[is_identity]
	,C.[is_computed]
	,C.[is_replicated]
	,DC.[definition] AS [default_definition]
	,CC.[definition] AS [computed_definition]
FROM
	[sys].[columns] C
	JOIN [sys].[types] T ON C.[user_type_id] = T.[user_type_id]
	LEFT JOIN [sys].[default_constraints] DC ON C.[column_id] = DC.[parent_column_id] AND DC.[parent_object_id] = @object_id
	LEFT JOIN [sys].[computed_columns] CC ON C.[object_id] = CC.[object_id] AND C.[column_id] = CC.[column_id]
WHERE
	C.[object_id] = @object_id
ORDER BY
	C.[column_id];

SELECT
	I.[index_id]
	,I.[name] AS [index_name]
	--,I.[type]
	,I.[type_desc]
	,DS.[name] AS [data_space_name]
	,I.[is_primary_key]
	,I.[is_unique]
	,I.[is_unique_constraint]
	,STUFF
		((
			SELECT
				--N', ' + CX.[name] + CASE WHEN ICX.is_descending_key = 0 THEN '' ELSE ' DESC' END
				N', ' + CX.[name] + ' (' + TX.[name] + ' [' + CAST(CX.[max_length] AS varchar) + '])' + CASE WHEN ICX.is_descending_key = 0 THEN '' ELSE ' DESC' END
			FROM
				[sys].[index_columns] ICX
				JOIN [sys].[columns] CX ON ICX.[object_id] = CX.[object_id] AND ICX.[column_id] = CX.[column_id]
				JOIN [sys].[types] TX ON CX.[user_type_id] = TX.[user_type_id]
			WHERE
				I.[object_id] = ICX.[object_id]
				AND I.[index_id] = ICX.[index_id]
				AND ICX.[is_included_column] = 0
			ORDER BY
				ICX.[object_id]
				,ICX.[index_id]
				,ICX.[is_included_column]
				,ICX.[key_ordinal]
				,CX.[name]
			FOR XML PATH, TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 2, N''
		) AS [key_columns]
	,STUFF
		((
			SELECT
				--N', ' + CX.[name]
				N', ' + CX.[name] + ' (' + TX.[name] + ' [' + CAST(CX.[max_length] AS varchar) + '])'
			FROM
				[sys].[index_columns] ICX
				JOIN [sys].[columns] CX ON ICX.[object_id] = CX.[object_id] AND ICX.[column_id] = CX.[column_id]
				JOIN [sys].[types] TX ON CX.[user_type_id] = TX.[user_type_id]
			WHERE
				I.[object_id] = ICX.[object_id]
				AND I.[index_id] = ICX.[index_id]
				AND ICX.[is_included_column] = 1
			ORDER BY
				ICX.[object_id]
				,ICX.[index_id]
				,ICX.[is_included_column]
				,ICX.[key_ordinal]
				,CX.[name]
			FOR XML PATH, TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 2, N''
		) AS [included_columns]
	--,I.[data_space_id]
	--,I.[has_filter]
	,I.[filter_definition]
	,PSA.[partition_count]
	,PSA.[reserved_page_count] * 8192.0 / 1024.0 / 1024.0 AS [reserved_mb]
	,PSA.[used_page_count] * 8192.0 / 1024.0 / 1024.0 AS [used_mb]
	,PSA.[reserved_page_count] * 8192.0 / 1024.0 / 1024.0 / 1024.0 AS [reserved_gb]
	,PSA.[used_page_count] * 8192.0 / 1024.0 / 1024.0 / 1024.0 AS [used_gb]
	,I.[fill_factor]
	,I.[is_padded]
	,I.[is_disabled]
	,I.[is_hypothetical]
	,I.[allow_row_locks]
	,I.[allow_page_locks]
	,IUS.[user_lookups]
	,IUS.[user_scans]
	,IUS.[user_seeks]
	,IUS.[user_updates]
	,IUS.[last_user_lookup]
	,IUS.[last_user_scan]
	,IUS.[last_user_seek]
	,IUS.[last_user_update]
	,IUS.[system_lookups]
	,IUS.[system_scans]
	,IUS.[system_seeks]
	,IUS.[system_updates]
	,IUS.[last_system_lookup]
	,IUS.[last_system_scan]
	,IUS.[last_system_seek]
	,IUS.[last_system_update]
	,IOS.[leaf_insert_count]
	,IOS.[leaf_delete_count]
	,IOS.[leaf_update_count]
	,IOS.[leaf_ghost_count]
	,IOS.[nonleaf_insert_count]
	,IOS.[nonleaf_delete_count]
	,IOS.[nonleaf_update_count]
	,IOS.[leaf_allocation_count]
	,IOS.[nonleaf_allocation_count]
	,IOS.[leaf_page_merge_count]
	,IOS.[nonleaf_page_merge_count]
	,IOS.[range_scan_count]
	,IOS.[singleton_lookup_count]
	,IOS.[forwarded_fetch_count]
	,IOS.[lob_fetch_in_bytes]
	,IOS.[lob_orphan_create_count]
	,IOS.[lob_orphan_insert_count]
	,IOS.[row_overflow_fetch_in_bytes]
	,IOS.[column_value_push_off_row_count]
	,IOS.[column_value_pull_in_row_count]
	,IOS.[row_lock_count]
	,IOS.[row_lock_wait_count]
	,IOS.[row_lock_wait_in_ms]
	,IOS.[page_lock_count]
	,IOS.[page_lock_wait_count]
	,IOS.[page_lock_wait_in_ms]
	,IOS.[index_lock_promotion_attempt_count]
	,IOS.[index_lock_promotion_count]
	,IOS.[page_latch_wait_count]
	,IOS.[page_latch_wait_in_ms]
	,IOS.[page_io_latch_wait_count]
	,IOS.[page_io_latch_wait_in_ms]
	,IOS.[tree_page_latch_wait_count]
	,IOS.[tree_page_latch_wait_in_ms]
	,IOS.[tree_page_io_latch_wait_count]
	,IOS.[tree_page_io_latch_wait_in_ms]
	,IOS.[page_compression_attempt_count]
	,IOS.[page_compression_success_count]
FROM
	[sys].[indexes] I
	JOIN [sys].[data_spaces] DS ON I.[data_space_id] = DS.[data_space_id]
	LEFT JOIN
	(
		SELECT
			PS.[object_id]
			,PS.[index_id]
			,COUNT(1) AS [partition_count]
			,SUM(PS.[reserved_page_count]) AS [reserved_page_count]
			,SUM(PS.[used_page_count]) AS [used_page_count]
		FROM
			sys.dm_db_partition_stats PS
		GROUP BY
			PS.[object_id]
			,PS.[index_id]
	) PSA ON I.[index_id] = PSA.[index_id] AND I.[object_id] = PSA.[object_id]
	LEFT JOIN sys.dm_db_index_usage_stats IUS ON I.[object_id] = IUS.[object_id] AND I.[index_id] = IUS.[index_id] AND IUS.[database_id] = DB_ID()
	OUTER APPLY sys.dm_db_index_operational_stats(DB_ID(), I.[object_id], I.[index_id], DEFAULT) IOS
WHERE
	I.[object_id] = @object_id
ORDER BY
	I.[index_id];

-- Get information about primary keys and unique constraints.
SELECT
	KC.[type_desc] AS [constraint_type]
	,KC.[name] AS [constraint_name]
	,I.[name] AS [constraint_index_name]
	,NULL AS [delete_action]
	,NULL AS [update_action]
	,CASE WHEN I.[is_disabled] = 0 THEN 1 ELSE 0 END AS [is_enabled]
	,NULL AS [is_for_replication]
	,NULL AS [is_trusted]
	,NULL AS [constraint_definition]
	,STUFF
		((
			SELECT
				N', ' + CX.[name] + CASE WHEN ICX.is_descending_key = 0 THEN '' ELSE ' DESC' END
				--N', ' + CX.[name] + ' (' + TX.[name] + ' [' + CAST(CX.[max_length] AS varchar) + '])' + CASE WHEN ICX.is_descending_key = 0 THEN '' ELSE ' DESC' END
			FROM
				[sys].[index_columns] ICX
				JOIN [sys].[columns] CX ON ICX.[object_id] = CX.[object_id] AND ICX.[column_id] = CX.[column_id]
				JOIN [sys].[types] TX ON CX.[user_type_id] = TX.[user_type_id]
			WHERE
				I.[object_id] = ICX.[object_id]
				AND I.[index_id] = ICX.[index_id]
				AND ICX.[is_included_column] = 0
			ORDER BY
				ICX.[object_id]
				,ICX.[index_id]
				,ICX.[is_included_column]
				,ICX.[key_ordinal]
				,CX.[name]
			FOR XML PATH, TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 2, N''
		) AS [key_columns]
	,STUFF
		((
			SELECT
				N', ' + CX.[name]
				--N', ' + CX.[name] + ' (' + TX.[name] + ' [' + CAST(CX.[max_length] AS varchar) + '])'
			FROM
				[sys].[index_columns] ICX
				JOIN [sys].[columns] CX ON ICX.[object_id] = CX.[object_id] AND ICX.[column_id] = CX.[column_id]
				JOIN [sys].[types] TX ON CX.[user_type_id] = TX.[user_type_id]
			WHERE
				I.[object_id] = ICX.[object_id]
				AND I.[index_id] = ICX.[index_id]
				AND ICX.[is_included_column] = 1
			ORDER BY
				ICX.[object_id]
				,ICX.[index_id]
				,ICX.[is_included_column]
				,ICX.[key_ordinal]
				,CX.[name]
			FOR XML PATH, TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 2, N''
		) AS [included_columns]
	,NULL AS [referenced_schema_name]
	,NULL AS [referenced_object_name]
	,NULL AS [referenced_key_columns]
	,KC.[create_date] AS [create_date]
	,KC.[modify_date] AS [modify_date]
FROM
	[sys].[key_constraints] KC
	JOIN [sys].[indexes] I ON KC.[parent_object_id] = I.[object_id] AND KC.[unique_index_id] = I.[index_id]
WHERE
	KC.[parent_object_id] = @object_id
UNION ALL
-- Get information about unique indexes.
SELECT
	'UNIQUE INDEX' AS [constraint_type]
	,NULL AS [constraint_name]
	,I.[name] AS [constraint_index_name]
	,NULL AS [delete_action]
	,NULL AS [update_action]
	,CASE WHEN I.[is_disabled] = 1 THEN 0 ELSE 1 END AS [is_enabled]
	,NULL AS [is_for_replication]
	,NULL AS [is_trusted]
	,NULL AS [constraint_definition]
	,STUFF
		((
			SELECT
				N', ' + CX.[name] + CASE WHEN ICX.is_descending_key = 0 THEN '' ELSE ' DESC' END
				--N', ' + CX.[name] + ' (' + TX.[name] + ' [' + CAST(CX.[max_length] AS varchar) + '])' + CASE WHEN ICX.is_descending_key = 0 THEN '' ELSE ' DESC' END
			FROM
				[sys].[index_columns] ICX
				JOIN [sys].[columns] CX ON ICX.[object_id] = CX.[object_id] AND ICX.[column_id] = CX.[column_id]
				JOIN [sys].[types] TX ON CX.[user_type_id] = TX.[user_type_id]
			WHERE
				I.[object_id] = ICX.[object_id]
				AND I.[index_id] = ICX.[index_id]
				AND ICX.[is_included_column] = 0
			ORDER BY
				ICX.[object_id]
				,ICX.[index_id]
				,ICX.[is_included_column]
				,ICX.[key_ordinal]
				,CX.[name]
			FOR XML PATH, TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 2, N''
		) AS [key_columns]
	,STUFF
		((
			SELECT
				N', ' + CX.[name]
				--N', ' + CX.[name] + ' (' + TX.[name] + ' [' + CAST(CX.[max_length] AS varchar) + '])'
			FROM
				[sys].[index_columns] ICX
				JOIN [sys].[columns] CX ON ICX.[object_id] = CX.[object_id] AND ICX.[column_id] = CX.[column_id]
				JOIN [sys].[types] TX ON CX.[user_type_id] = TX.[user_type_id]
			WHERE
				I.[object_id] = ICX.[object_id]
				AND I.[index_id] = ICX.[index_id]
				AND ICX.[is_included_column] = 1
			ORDER BY
				ICX.[object_id]
				,ICX.[index_id]
				,ICX.[is_included_column]
				,ICX.[key_ordinal]
				,CX.[name]
			FOR XML PATH, TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 2, N''
		) AS [included_columns]
	,NULL AS [referenced_schema_name]
	,NULL AS [referenced_object_name]
	,NULL AS [referenced_key_columns]
	,NULL AS [create_date]
	,NULL AS [modify_date]
FROM
	[sys].[indexes] I
WHERE
	I.[object_id] = @object_id
	AND I.[is_primary_key] = 0
	AND I.[is_unique] = 1
	AND I.[is_unique_constraint] = 0
UNION ALL
-- Get information about default constraints.
SELECT
	DC.[type_desc] AS [constraint_type]
	,DC.[name] AS [constraint_name]
	,NULL AS [constraint_index_name]
	,NULL AS [delete_action]
	,NULL AS [update_action]
	,NULL AS [is_enabled]
	,NULL AS [is_for_replication]
	,NULL AS [is_trusted]
	,C.[name] + ':  ' + DC.[definition] AS [constraint_definition]
	,NULL AS [key_columns]
	,NULL AS [included_columns]
	,NULL AS [referenced_schema_name]
	,NULL AS [referenced_object_name]
	,NULL AS [referenced_key_columns]
	,DC.[create_date] AS [create_date]
	,DC.[modify_date] AS [modify_date]
FROM
	[sys].[default_constraints] DC
	JOIN [sys].[columns] C ON DC.[parent_object_id] = C.[object_id] AND DC.[parent_column_id] = C.[column_id]
WHERE
	DC.[parent_object_id] = @object_id
UNION ALL
-- Get information about check constraints.
SELECT
	CC.[type_desc] AS [constraint_type]
	,CC.[name] AS [constraint_name]
	,NULL AS [constraint_index_name]
	,NULL AS [delete_action]
	,NULL AS [update_action]
	,CASE WHEN CC.[is_disabled] = 0 THEN 1 ELSE 0 END AS [is_enabled]
	,CASE WHEN CC.[is_not_for_replication] = 1 THEN 0 ELSE 1 END AS [is_for_replication]
	,CASE WHEN CC.[is_not_trusted] = 1 THEN 0 ELSE 1 END AS [is_trusted]
	,C.[name] + ':  ' + CC.[definition] AS [constraint_definition]
	,NULL AS [key_columns]
	,NULL AS [included_columns]
	,NULL AS [referenced_schema_name]
	,NULL AS [referenced_object_name]
	,NULL AS [referenced_key_columns]
	,CC.[create_date] AS [create_date]
	,CC.[modify_date] AS [modify_date]
FROM
	[sys].[check_constraints] CC
	JOIN [sys].[columns] C ON CC.[parent_object_id] = C.[object_id] AND CC.[parent_column_id] = C.[column_id]
WHERE
	CC.[parent_object_id] = @object_id
UNION ALL
-- Get information about foreign keys.
SELECT
	'FOREIGN KEY' AS [constraint_type]
	,FK.[name] AS [constraint_name]
	,NULL AS [constraint_index_name]
	,FK.[delete_referential_action_desc]
	,FK.[update_referential_action_desc]
	,CASE WHEN FK.[is_disabled] = 1 THEN 0 ELSE 1 END AS [is_enabled]
	,CASE WHEN FK.[is_not_for_replication] = 1 THEN 0 ELSE 1 END AS [is_for_replication]
	,CASE WHEN FK.[is_not_trusted] = 1 THEN 0 ELSE 1 END AS [is_trusted]
	,NULL AS [constraint_definition]
	,STUFF
	((
		SELECT
			N', ' + CX.[name]
			--N', ' + CX.[name] + ' (' + TX.[name] + ' [' + CAST(CX.[max_length] AS varchar) + '])' + CASE WHEN ICX.is_descending_key = 0 THEN '' ELSE ' DESC' END
		FROM
			[sys].[foreign_key_columns] FKCX
			JOIN [sys].[columns] CX ON FKCX.[parent_object_id] = CX.[object_id] AND FKCX.[parent_column_id] = CX.[column_id]
		WHERE
			FK.[object_id] = FKCX.[constraint_object_id]
		ORDER BY
			FKCX.[constraint_column_id]
		FOR XML PATH, TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 2, N''
	) AS [key_columns]
	,NULL AS [included_columns]
	,SR.[name] AS [referenced_schema_name]
	,TR.[name] AS [referenced_object_name]
	,STUFF
	((
		SELECT
			N', ' + CX.[name]
			--N', ' + CX.[name] + ' (' + TX.[name] + ' [' + CAST(CX.[max_length] AS varchar) + '])' + CASE WHEN ICX.is_descending_key = 0 THEN '' ELSE ' DESC' END
		FROM
			[sys].[foreign_key_columns] FKCX
			JOIN [sys].[columns] CX ON FKCX.[referenced_object_id] = CX.[object_id] AND FKCX.[referenced_column_id] = CX.[column_id]
		WHERE
			FK.[object_id] = FKCX.[constraint_object_id]
		ORDER BY
			FKCX.[constraint_column_id]
		FOR XML PATH, TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 2, N''
	) AS [referenced_key_columns]
	,FK.[create_date] AS [create_date]
	,FK.[modify_date] AS [modify_date]
FROM
	[sys].[foreign_keys] FK
	JOIN [sys].[tables] TR ON FK.[referenced_object_id] = TR.[object_id]
	JOIN [sys].[schemas] SR ON TR.[schema_id] = SR.[schema_id]
WHERE
	FK.[parent_object_id] = @object_id;

-- Missing index information.
SELECT
	CONVERT(decimal(18,2), MIGS.user_seeks * MIGS.avg_total_user_cost * (MIGS.avg_user_impact * 0.01)) AS [index_advantage]
	,MIGS.[unique_compiles]
	,MIGS.[user_seeks]
	,MIGS.[user_scans]
	,MIGS.[last_user_seek]
	,MIGS.[last_user_scan]
	,MIGS.[avg_total_user_cost]
	,MIGS.[avg_user_impact]
	,MIGS.[system_seeks]
	,MIGS.[system_scans]
	,MIGS.[last_system_seek]
	,MIGS.[last_system_scan]
	,MIGS.[avg_total_system_cost]
	,MIGS.[avg_system_impact]
	,MID.[equality_columns]
	,MID.[inequality_columns]
	,MID.[included_columns]
FROM
	[sys].[dm_db_missing_index_groups] MIG
	JOIN [sys].[dm_db_missing_index_group_stats] AS MIGS ON MIG.[index_group_handle] = MIGS.[group_handle]
	JOIN [sys].[dm_db_missing_index_details] AS MID ON MIG.[index_handle] = MID.[index_handle]
WHERE
	MID.[database_id] = DB_ID()
	AND MID.[object_id] = @object_id

EndExecution:
PRINT 'Execution completed.';
