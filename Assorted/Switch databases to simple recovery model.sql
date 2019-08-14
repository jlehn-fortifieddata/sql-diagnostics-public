DECLARE @DatabaseName sysname;
DECLARE @SqlCommand nvarchar(1000);

DECLARE DatabaseCursor CURSOR LOCAL FAST_FORWARD FOR
SELECT
	D.[name]
FROM
	sys.databases D
WHERE
	D.recovery_model_desc = 'FULL'
ORDER BY
	D.[name];

OPEN DatabaseCursor;

FETCH
	DatabaseCursor
INTO
	@DatabaseName;

WHILE (@@FETCH_STATUS = 0)
BEGIN
	--PRINT @DatabaseName;

	SET @SqlCommand = N'ALTER DATABASE [' + @DatabaseName + '] SET RECOVERY SIMPLE;';

	PRINT @SqlCommand;

	EXEC sp_executesql @SqlCommand;

	FETCH
		DatabaseCursor
	INTO
		@DatabaseName;
END

CLOSE DatabaseCursor;
DEALLOCATE DatabaseCursor;
