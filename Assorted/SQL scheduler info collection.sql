DECLARE @StopTime datetime = '20190206 19:00';

CREATE TABLE
	#SchedulerCounts
	(
		RecordTime datetime
		,CurrentTasksCount int
		,RunnableTasksCount int
		,CurrentWorkersCount int
		,ActiveWorkersCount int
	);

WHILE (@StopTime > GETDATE())
BEGIN
	INSERT INTO
		#SchedulerCounts
		(
			RecordTime
			,CurrentTasksCount
			,RunnableTasksCount
			,CurrentWorkersCount
			,ActiveWorkersCount
		)
	SELECT
		GETDATE()
		,SUM(current_tasks_count)
		,SUM(runnable_tasks_count)
		,SUM(current_workers_count)
		,SUM(active_workers_count)
	FROM
		sys.dm_os_schedulers;

	WAITFOR DELAY '00:00:02';
END

SELECT
	*
FROM
	#SchedulerCounts
ORDER BY
	RecordTime;

-- DROP TABLE #SchedulerCounts;
