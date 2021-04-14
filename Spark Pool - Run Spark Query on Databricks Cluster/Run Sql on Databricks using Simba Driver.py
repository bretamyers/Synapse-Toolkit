query = """
(
    SELECT * FROM testexternalmetastorenew2.externalTableD
) result
"""

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:spark://adb-1487981951669006.6.azuredatabricks.net:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/1487981951669006/0407-160218-dory150;AuthMech=3;") \
    .option("dbtable", query) \
    .option("user", "token") \
    .option("password", "dapi88bad1fae9d1351e35d388edeccf59a7") \
    .option("driver", "com.simba.spark.jdbc.Driver") \
    .load()

display(jdbcDF.limit(10))