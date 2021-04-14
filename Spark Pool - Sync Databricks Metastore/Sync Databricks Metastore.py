from pyhive import hive
from thrift.transport import THttpClient
import base64
import ssl


## Included example of cluster JDBC connection from Databricks to show
## how to parse the string into the correct parameters
#jdbc:spark://adb-1487981951669006.6.azuredatabricks.net:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/1487981951669006/0407-160218-dory150;AuthMech=3;UID=token;PWD=<personal-access-token>

TOKEN = "dapi88bad1fae9d1351e35d388edeccf59a7"
WORKSPACE_URL = "adb-1487981951669006.6.azuredatabricks.net:443"
WORKSPACE_ID = "1487981951669006"
CLUSTER_ID = "0407-160218-dory150"
DATABASE_NAME = "testexternalmetastorenew2"

conn = 'https://%s/sql/protocolv1/o/%s/%s' % (WORKSPACE_URL, WORKSPACE_ID, CLUSTER_ID)

ssl._create_default_https_context = ssl._create_unverified_context

transport = THttpClient.THttpClient(conn)

auth = "token:%s" % TOKEN

auth = base64.standard_b64encode(auth.encode()).decode()

transport.setCustomHeaders({"Authorization": "Basic %s" % auth})

cursor = hive.connect(thrift_transport=transport).cursor()

cursor.execute(f'SHOW TABLES IN {DATABASE_NAME}',async_=True)

pending_states = (
        hive.ttypes.TOperationState.INITIALIZED_STATE,
        hive.ttypes.TOperationState.PENDING_STATE,
        hive.ttypes.TOperationState.RUNNING_STATE)

while cursor.poll().operationState in pending_states:
    print("Pending...")

print("Done. Results:")

for index, table in enumerate(cursor.fetchall()):
    print(table[1])
    cursor.execute(f"DESCRIBE FORMATTED {table[0]}.{table[1]}")
    for row in cursor.fetchall():
        if row[0] == 'Type' and (row[1] == 'EXTERNAL' or row[1] == 'VIEW'):

            cursor.execute(f"SHOW CREATE TABLE {table[0]}.{table[1]}")
            results = cursor.fetchall()

            if row[1] == 'EXTERNAL':
                spark.sql(f"DROP TABLE IF EXISTS {table[0]}.{table[1]}")
            else:
                spark.sql(f"DROP VIEW IF EXISTS {table[0]}.{table[1]}")
            
            spark.sql(results[0][0])

