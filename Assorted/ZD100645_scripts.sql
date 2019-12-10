-- Insert trace data into a table for review (run for each file, they are large and can't be done all at once)

--DROP TABLE FDDBA.dbo.ZD100645_Trace;

/*
File list:
--ZD100645_Trace_0_132201461013790000.xel
--ZD100645_Trace_0_132201839800080000.xel
--ZD100645_Trace_0_132202339600710000.xel
--ZD100645_Trace_0_132202764101220000.xel
--ZD100645_Trace_0_132203216540730000.xel
--ZD100645_Trace_0_132203664147410000.xel
--ZD100645_Trace_0_132203936860370000.xel
--ZD100645_Trace_0_132204286536130000.xel
--ZD100645_Trace_0_132204636178790000.xel
*/

SELECT
	CAST(F.[event_data] AS XML) AS [event_data]
INTO
	#EventData
FROM
	sys.fn_xe_file_target_read_file('C:\temp\ZD100645_Trace_0_132204636178790000.xel', NULL, NULL, NULL) F;

INSERT INTO
	FDDBA.dbo.ZD100645_Trace
SELECT
	X.[event_data]
	--,X.[event_data].value('data[@name="error_number"]/value)[1]', 'int')
	--,X.[event_data].value('(@timestamp)[1]', 'datetime2') AS [timestamp]
	,X.[event_data].value('(event/data[@name="error_number"]/value)[1]', 'int') AS [error_number]
	,X.[event_data].value('(event/data[@name="message"]/value)[1]', 'varchar(8000)') AS [message]
	,X.[event_data].value('(event/action[@name="collect_system_time"]/value)[1]', 'datetime2') AS [collect_system_time]
	,X.[event_data].value('(event/action[@name="process_id"]/value)[1]', 'int') AS [process_id]
	,X.[event_data].value('(event/action[@name="client_app_name"]/value)[1]', 'varchar(1000)') AS [client_app_name]
	,X.[event_data].value('(event/action[@name="client_hostname"]/value)[1]', 'varchar(100)') AS [client_hostname]
	,X.[event_data].value('(event/action[@name="client_pid"]/value)[1]', 'int') AS [client_pid]
	,X.[event_data].value('(event/action[@name="database_name"]/value)[1]', 'varchar(100)') AS [database_name]
	,X.[event_data].value('(event/action[@name="nt_username"]/value)[1]', 'varchar(100)') AS [nt_username]
	,X.[event_data].value('(event/action[@name="server_instance_name"]/value)[1]', 'varchar(100)') AS [server_instance_name]
	,X.[event_data].value('(event/action[@name="session_id"]/value)[1]', 'int') AS [session_id]
	,X.[event_data].value('(event/action[@name="session_nt_username"]/value)[1]', 'varchar(100)') AS [session_nt_username]
	,X.[event_data].value('(event/action[@name="sql_text"]/value)[1]', 'varchar(max)') AS [sql_text]
	,X.[event_data].value('(event/action[@name="username"]/value)[1]', 'varchar(100)') AS [username]
FROM
	#EventData X;

DROP TABLE #EventData;

-- Query trace data
SELECT
	X.collect_system_time
	,CAST(X.collect_system_time AS date) AS [error_date]
	,DATEPART(HOUR, X.collect_system_time) AS [error_hour]
	,X.[error_number]
	,X.[client_hostname]
	,X.[nt_username]
	,X.[session_nt_username]
	,X.[username]
	,X.*
FROM
	FDDBA.dbo.ZD100645_Trace X
WHERE
	--X.[error_number] = 18456
	X.[client_hostname] = 'VAAZGPYTWW001'
	AND X.[collect_system_time] >= '2019-12-10 16:00'
ORDER BY
	X.collect_system_time;
  
-- Aggregate trace data errors
SELECT
	CAST(X.collect_system_time AS date) AS [error_date]
	,DATEPART(HOUR, X.collect_system_time) AS [error_hour]
	,X.[error_number]
	,X.[client_hostname]
	,X.[nt_username]
	,X.[session_nt_username]
	,X.[username]
	,COUNT(1) AS [error_count]
FROM
	FDDBA.dbo.ZD100645_Trace X
WHERE
	X.[error_number] = 18456
	--AND X.[client_hostname] != 'VAAZGPYTWW001'
GROUP BY
	CAST(X.collect_system_time AS date)
	,DATEPART(HOUR, X.collect_system_time)
	,X.[error_number]
	,X.[client_hostname]
	,X.[nt_username]
	,X.[session_nt_username]
	,X.[username]
ORDER BY
	CAST(X.collect_system_time AS date)
	,DATEPART(HOUR, X.collect_system_time);
