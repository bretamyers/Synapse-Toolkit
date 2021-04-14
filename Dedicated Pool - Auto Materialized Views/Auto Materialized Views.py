import xml.etree.ElementTree as ET
import re
import pyodbc

server = 'synapseworkspacebrmyers.sql.azuresynapse.net' # 'synapsedemoworkspacegbb.sql.azuresynapse.net'
database = 'TPCDSDW' 
username = 'LoadingUser' #'JMeterUser' #'sqladminuser'
password = 'MsftDemo202#'
driver= '{ODBC Driver 17 for SQL Server}'

def RunQuery(QueryString):
    with pyodbc.connect(f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}') as conn:
        with conn.cursor() as cursor:
            cursor.execute(QueryString)
            return cursor.fetchall() #return the full result set *list of tuples

def CommitQuery(QueryString):
    with pyodbc.connect(f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}') as conn:
        with conn.cursor() as cursor:
            cursor.execute(QueryString)
            conn.commit()


if __name__ == "__main__":

    SCHEMA = 'dbo'

    DMVQuery = """SELECT MIN(request_id) AS RequestId, SUM(total_elapsed_time) AS TotalElapsedTime, [command] AS SqlCommand
FROM
(
	SELECT r.total_elapsed_time, r.[command], r.request_id
	FROM sys.dm_pdw_exec_requests AS r
	JOIN sys.dm_pdw_exec_sessions AS s
	ON s.session_id = r.session_id
	WHERE r.status = 'Completed'
	AND r.session_id <> session_id()
	AND resource_class IS NOT NULL -- Only get records that users have submitted. Excludes system generated queries
	AND r.[command] NOT LIKE 'CREATE MATERIALIZED VIEW %'
) AS a
GROUP BY [command]
ORDER BY SUM(total_elapsed_time) DESC
;"""

    queriesToRun = RunQuery(QueryString=DMVQuery)
    for query in queriesToRun:
        queryRequestId = query[0]
        queryDefinition = query[2]
        # print(queryDefinition) #Get the query definition from the dmv results=
        try:
            xmlResult = RunQuery(QueryString=f"EXPLAIN WITH_RECOMMENDATIONS\n{queryDefinition}")
            # print(xmlResult)

            root = ET.fromstring(str(xmlResult[0][0])) # get first row first column
            if len(root.findall('materialized_view_candidates/materialized_view_candidates')) == 0:
                            print("No Recommendations")
            else:
                view = root.findall('materialized_view_candidates/materialized_view_candidates')[0] #Get the first recommended view
                viewDefinitionRename = view.text.replace("CREATE MATERIALIZED VIEW View", f"CREATE MATERIALIZED VIEW {SCHEMA}.mv_auto_{queryRequestId}")
                print(viewDefinitionRename)
                CommitQuery(QueryString=viewDefinitionRename)

        except:
            print('Could not create view')

