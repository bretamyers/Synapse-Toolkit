

--KQL for Log Analytics
--SynapseBuiltinSqlPoolRequestsEnded 
--| where format_datetime(todatetime(Properties.startTime), 'yyyyMM') == 202104
--| order by todatetime(Properties.startTime) desc
--| project yearMonth=format_datetime(todatetime(Properties.startTime), 'yyyyMM')
--    ,startTime=Properties.startTime
--    ,queryText=Properties.queryText
--    ,dataProcessedMB=Properties.dataProcessedBytes/1000000
--    ,['costIn$']=(Properties.dataProcessedBytes/1000000)*5/100000.0


/*
Total cost per user for a given month
*/
SELECT		FORMAT(start_time, 'yyyMM') AS YearMonth
			,login_name
			,SUM(data_processed_mb) as [total_data_processed_MB]
			,SUM(CAST(data_processed_mb*5/100000.0 as decimal(19,3))) as cost_in_$
FROM		sys.dm_exec_requests_history AS erh
WHERE		FORMAT(start_time, 'yyyMM') = 202104
GROUP BY	FORMAT(start_time, 'yyyMM'), login_name


/*
Cost per query for a given month
*/
SELECT		FORMAT(start_time, 'yyyMM') AS YearMonth
			,login_name
			,query_text AS [command]
			,start_time AS [start_time]
			,end_time AS [end_time]
			,total_elapsed_time_ms as [duration_ms]
			,data_processed_mb as [data_processed_MB]
			,CAST(total_elapsed_time_ms/1000.0 AS DECIMAL(12,2)) as [duration_sec]
			,CAST(data_processed_mb*5/100000.0 AS DECIMAL(19,3)) AS cost_in_$
FROM		sys.dm_exec_requests_history
WHERE		FORMAT(start_time, 'yyyMM') = 202104
ORDER BY	start_time desc


----bytes to mb
--SELECT 2532000000/1048576 AS mb


