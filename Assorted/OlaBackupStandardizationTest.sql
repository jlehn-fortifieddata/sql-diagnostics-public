/*
SELECT
 S.*
 ,B.*
FROM
 (
  SELECT
      @@SERVERNAME AS ServerName
 ) S
 CROSS JOIN
 (
  SELECT
      J.[name]
      ,J.[enabled]
      ,JS.step_name
      ,JS.subsystem
      ,JS.[command]
  FROM
      msdb.dbo.sysjobs J
      JOIN msdb.dbo.sysjobsteps JS ON J.job_id = JS.job_id
  WHERE
      J.[name] LIKE '%databasebackup%'
 ) B
ORDER BY
 B.[name];
 */

-- Create a table to hold a list of backup jobs.
CREATE TABLE
	#BackupJobs
	(
		StepUID uniqueidentifier
		,JobName sysname
		,IsJobEnabled bit
		,JobStepName sysname
		,Command nvarchar(max)
		,RevisedCommand nvarchar(max)
	);

-- Create a table to hold a list of job step parts.  We will use this to split out the actual
-- command that is executed to run the backups.
CREATE TABLE
	#JobStepParts
	(
		StepUID uniqueidentifier
		,Id int
		,JobStepPart nvarchar(max)
	);

-- Create a table to hold a list of command parts.  We will use this to split out the parameters
-- from the backup command.
CREATE TABLE
	#CommandParts
	(
		StepUID uniqueidentifier
		,Id int
		,CommandPart nvarchar(max)
		,Parameter nvarchar(max)
		,ParameterValue nvarchar(max)
	);

DECLARE @CurrentStepUID uniqueidentifier;
DECLARE @CurrentCommand nvarchar(max);

INSERT INTO
	#BackupJobs
	(
		StepUID
		,JobName
		,IsJobEnabled
		,JobStepName
		,Command
		,RevisedCommand
	)
SELECT
	JS.step_uid
	,J.[name]
	,J.[enabled]
	,JS.[step_name]
	,JS.command
	,NULL
FROM
	msdb.dbo.sysjobs J
	JOIN msdb.dbo.sysjobsteps JS ON J.job_id = JS.job_id
WHERE
	J.[name] LIKE N'%databasebackup%'
	AND JS.command LIKE '%execute%databasebackup%';

DECLARE JobStepCursor CURSOR LOCAL FAST_FORWARD
FOR
SELECT
	StepUID
	,Command	
FROM
	#BackupJobs;

OPEN JobStepCursor;

FETCH
	JobStepCursor
INTO
	@CurrentStepUID
	,@CurrentCommand;

WHILE (@@FETCH_STATUS = 0)
BEGIN
	INSERT INTO
		#JobStepParts
		(
			StepUID
			,Id
			,JobStepPart
		)
	SELECT
		@CurrentStepUID
		,X.id
		,X.val
	FROM
		dbo.fSplitNString(@CurrentCommand, N'"') X;

	INSERT INTO
		#CommandParts
		(
			StepUID
			,Id
			,CommandPart
		)
	SELECT
		@CurrentStepUID
		,X.id
		,X.val
	FROM
		dbo.fSplitNString((SELECT TOP (1) JobStepPart FROM #JobStepParts WHERE Id = 2), N'@') X;

	UPDATE
		CP
	SET
		CP.Parameter = LTRIM(RTRIM(P.val))
		,CP.ParameterValue = LTRIM(RTRIM(V.val))
	FROM
		#CommandParts CP
		CROSS APPLY
		(
			SELECT
				S.val
			FROM
				dbo.fSplitNString(CP.CommandPart, N'=') S
			WHERE
				S.Id = 1
		) P
		CROSS APPLY
		(
			SELECT
				S.val
			FROM
				dbo.fSplitNString(CP.CommandPart, N'=') S
			WHERE
				S.Id = 2
		) V;

	-- Remove trailing commas from parameter values.
	UPDATE
		#CommandParts
	SET
		ParameterValue = LEFT(ParameterValue, LEN(ParameterValue) - 1)
	WHERE
		ParameterValue IS NOT NULL
		AND RIGHT(ParameterValue, 1) = N',';

	FETCH
		JobStepCursor
	INTO
		@CurrentStepUID
		,@CurrentCommand;
END

CLOSE JobStepCursor;
DEALLOCATE JobStepCursor;

SELECT
	*
FROM
	#BackupJobs;

SELECT
	*
FROM
	#JobStepParts;

SELECT
	*
FROM
	#CommandParts;

/*
SELECT
	StepUID
	--,P.[Databases]
	--,P.[Directory]
	--,P.[BackupType]
	--,P.[BlockSize]
	--,P.[BufferCount]
	--,P.[MaxTransferSize]
	--,P.[Verify]
	--,P.[CleanupTime]
	--,P.[CheckSum]
	--,P.[LogToTable]
	,P.*
FROM
	#CommandParts
	PIVOT
	(
		MAX(ParameterValue)
		FOR Parameter IN
		(
			[Databases]
			,[Directory]
			,[BackupType]
			,[BlockSize]
			,[BufferCount]
			,[MaxTransferSize]
			,[Verify]
			,[CleanupTime]
			,[CheckSum]
			,[LogToTable]
		)
	) P;
*/


DROP TABLE #BackupJobs;
DROP TABLE #JobStepParts;
DROP TABLE #CommandParts;
