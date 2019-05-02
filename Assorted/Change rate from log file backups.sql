WITH BackupSizes AS
(
	SELECT
		CAST(CAST(BS.[backup_finish_date] AS date) AS datetime) AS [backup_finish_date]
		,DATEPART(HOUR, BS.[backup_finish_date]) AS [backup_finish_hour]
		,BS.[database_name]
		,SUM(BS.[backup_size]) / 1024.0 / 1024.0 AS [backup_size_mb]
		,SUM(BS.[compressed_backup_size]) / 1024.0 / 1024.0 AS [compressed_backup_size_mb]
	FROM
		[msdb].[dbo].[backupset] BS
	WHERE
		BS.[type] = 'L'
		AND BS.[backup_finish_date] >= '20190414'
		AND BS.[backup_finish_date] < '20190430'
	GROUP BY
		CAST(BS.[backup_finish_date] AS date)
		,DATEPART(HOUR, BS.[backup_finish_date])
		,BS.[database_name]
)
SELECT
	DATEADD(HOUR, BS.[backup_finish_hour], BS.[backup_finish_date]) AS [backup_finish_hour]
	,BS.[database_name]
	,BS.[backup_size_mb]
	,BS.[compressed_backup_size_mb]
FROM
	BackupSizes BS;
