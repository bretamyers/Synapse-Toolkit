


IF OBJECT_ID('tempdb..#tables') IS NOT NULL
	DROP TABLE #tables
;

CREATE TABLE #tables
(
	SchemaName NVARCHAR(100)
	,TableName NVARCHAR(100)
	,FolderPath NVARCHAR(1000)
	,RN INT NULL
)
;

--Enter the tables schema, name, and path to files for the views to be created over
INSERT INTO #tables (SchemaName, TableName, FolderPath) VALUES 
('stagingTPC', 'Supplier1', 'https://soeenterprisedatalake.dfs.core.windows.net/curated/TPC/Snowflake/TPCH_SF10/REGION/*.parquet')
,('stagingTPC', 'Supplier2', 'https://soeenterprisedatalake.dfs.core.windows.net/curated/TPC/Snowflake/TPCH_SF10/REGION/*.parquet')
,('stagingTPC', 'Supplier3', 'https://soeenterprisedatalake.dfs.core.windows.net/curated/TPC/Snowflake/TPCH_SF10/REGION/*.parquet')
;



UPDATE	a
SET		RN = b.RN
FROM	#tables AS a
JOIN	(SELECT SchemaName, TableName, ROW_NUMBER() OVER (ORDER BY SchemaName, TableName) AS RN FROM #tables) AS b
ON		b.SchemaName = a.SchemaName
AND		b.TableName = a.TableName
;

IF OBJECT_ID('tempdb..#ServerlessDDL') IS NOT NULL
	DROP TABLE #ServerlessDDL
;

CREATE TABLE #ServerlessDDL
(
	SchemaName NVARCHAR(100)
	,ViewName NVARCHAR(100)
	,CreateTableDDL NVARCHAR(MAX)
	,CreateTableStatsDDL NVARCHAR(MAX)
	,CreateViewDDL NVARCHAR(MAX)
	,CreateViewStatsDDL NVARCHAR(MAX)
)
;


DECLARE @cnt INT = 1
DECLARE @sqlCreateView NVARCHAR(MAX)
DECLARE @cntTablesToLoad INT = 0
DECLARE @SchemaName NVARCHAR(100)
DECLARE @TableName NVARCHAR(100)
DECLARE @FolderPath NVARCHAR(1000)

SELECT @cntTablesToLoad = COUNT(*) 
FROM #tables


WHILE (@cnt <= @cntTablesToLoad)
BEGIN

	SELECT	@SchemaName = SchemaName
			,@TableName = TableName
			,@FolderPath = FolderPath
			,@sqlCreateView = CONCAT('sp_describe_first_result_set @tsql=N''SELECT * FROM OPENROWSET(BULK ''''', FolderPath, ''''' , FORMAT=''''PARQUET'''') AS r''')
	FROM #tables WHERE RN = @cnt


	IF OBJECT_ID('tempdb..#InformationSchemaTempTable', 'U') IS NOT NULL
		DROP TABLE #InformationSchemaTempTable
	;

	CREATE TABLE #InformationSchemaTempTable 
	(
		is_hidden bit NOT NULL
		,column_ordinal int NOT NULL
		,name sysname NULL
		,is_nullable bit NOT NULL
		,system_type_id int NOT NULL
		,system_type_name nvarchar(256) NULL
		,max_length smallint NOT NULL
		,precision tinyint NOT NULL
		,scale tinyint NOT NULL
		,collation_name sysname NULL
		,user_type_id int NULL
		,user_type_database sysname NULL
		,user_type_schema sysname NULL
		,user_type_name sysname NULL
		,assembly_qualified_type_name nvarchar(4000)
		,xml_collection_id int NULL
		,xml_collection_database sysname NULL
		,xml_collection_schema sysname NULL
		,xml_collection_name sysname NULL
		,is_xml_document bit NOT NULL
		,is_case_sensitive bit NOT NULL
		,is_fixed_length_clr_type bit NOT NULL
		,source_server sysname NULL
		,source_database sysname NULL
		,source_schema sysname NULL
		,source_table sysname NULL
		,source_column sysname NULL
		,is_identity_column bit NULL
		,is_part_of_unique_key bit NULL
		,is_updateable bit NULL
		,is_computed_column bit NULL
		,is_sparse_column_set bit NULL
		,ordinal_in_order_by_list smallint NULL
		,order_by_list_length smallint NULL
		,order_by_is_descending smallint NULL
		,tds_type_id int NOT NULL
		,tds_length int NOT NULL
		,tds_collation_id int NULL
		,tds_collation_sort_id tinyint NULL
	);

	INSERT INTO #InformationSchemaTempTable
	EXEC (@sqlCreateView)

	DECLARE @GetMaxValueStatement NVARCHAR(MAX)
	DECLARE @GetColumnList NVARCHAR(MAX)

	SELECT	@GetMaxValueStatement = CONCAT('SELECT ', STRING_AGG(ColumnMaxLength, ','), ' FROM OPENROWSET(BULK ''', @FolderPath, ''' , FORMAT=''PARQUET'') AS r')
			,@GetColumnList = STRING_AGG(QUOTENAME([name]), ',')
	FROM
	(
		SELECT	CASE WHEN system_type_name LIKE ('%char%') OR system_type_name LIKE 'varbinary%' THEN CONCAT('COALESCE(NULLIF(MAX(LEN(', QUOTENAME([name]), ')), 0), 1) AS ', QUOTENAME([name])) ELSE CONCAT('SUM(0) AS ', QUOTENAME([name])) END AS ColumnMaxLength
				,[name]
		FROM #InformationSchemaTempTable
	) AS a
	;

	DECLARE @sqlUnpivot NVARCHAR(MAX)
	
	SET @sqlUnpivot = CONCAT('SELECT ''', @TableName, ''' AS TABLE_NAME, unpvt.col AS COLUMN_NAME, unpvt.datatype AS DATATYPE_MAX
	FROM 
	( ', @GetMaxValueStatement, ' ) AS a ', CHAR(13), ' UNPIVOT
	(
	datatype
	FOR col IN 
	( ', @GetColumnList, ')
	) AS unpvt')
	

	DROP TABLE IF EXISTS #tmpBus;
	CREATE TABLE #tmpBus
	(
		TABLE_CLEAN NVARCHAR(1000)
		,COLUMN_NAME NVARCHAR(1000)
		,DATATYPE_MAX NVARCHAR(1000)
	)
	;

	INSERT INTO #tmpBus EXEC (@sqlUnpivot)

	DROP TABLE IF EXISTS #tmpFinal;
	CREATE TABLE #tmpFinal
	(
		table_name NVARCHAR(1000)
		,column_name NVARCHAR(1000)
		,DataType NVARCHAR(1000)
		,ColumnFullDefinition NVARCHAR(1000)
	)
	;

	INSERT INTO #tmpFinal
	SELECT		@TableName AS table_name
				,c.[name] AS column_name
				,UPPER(TYPE_NAME(c.system_type_id)) AS DataType
				,CONCAT(c.[name], ' '
					,CASE WHEN TYPE_NAME(c.system_type_id) IN ('int', 'bigint', 'smallint', 'tinyint', 'bit', 'decimal', 'numeric', 'float', 'datetime2', 'date') 
						THEN UPPER(c.system_type_name)
						ELSE CONCAT(UPPER(TYPE_NAME(c.system_type_id)), '(', a.DATATYPE_MAX, ') COLLATE Latin1_General_100_BIN2_UTF8')
					END
					) AS ColumnFullDefinition
	FROM #InformationSchemaTempTable AS c
	JOIN #tmpBus AS a
	ON a.COLUMN_NAME = c.[name]
	ORDER BY column_ordinal
	OFFSET 0 ROWS
	;

	DECLARE @createTableDDL NVARCHAR(MAX)
	DECLARE @createTableStatsDDL NVARCHAR(MAX)
	DECLARE @createViewDDL NVARCHAR(MAX)
	DECLARE @createViewStatsDDL NVARCHAR(MAX)
	DECLARE @openrowsetValue NVARCHAR(MAX)
	DECLARE @DataSourceName NVARCHAR(MAX) = (SELECT CONCAT('ds_', SUBSTRING(FolderPath, CHARINDEX('//', FolderPath)+2, (CHARINDEX('.',FolderPath)-9))) FROM #tables WHERE RN = @cnt)
	DECLARE @DataSourceDefinition NVARCHAR(MAX) = (SELECT SUBSTRING(FolderPath, 0, CHARINDEX('/', REPLACE(FolderPath, '//', ''))+2) FROM #tables WHERE RN = @cnt)
	DECLARE @DataSourcePath NVARCHAR(MAX) = (SELECT SUBSTRING(FolderPath, CHARINDEX('/', REPLACE(FolderPath, '//', ''))+2, LEN(FolderPath)) FROM #tables WHERE RN = @cnt)
	DECLARE @DataSourceCreateDDL NVARCHAR(MAX) = (SELECT CONCAT('IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = ''', @DataSourceName, ''') CREATE EXTERNAL DATA SOURCE [', @DataSourceName, '] WITH (LOCATION   = ''', @DataSourceDefinition, ''')', ''))
	DECLARE @FileFormatCreateDDL NVARCHAR(MAX) = 'IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = ''SynapseParquetFormat'') CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] WITH ( FORMAT_TYPE = PARQUET)'
	DECLARE @CreateSchema NVARCHAR(MAX) = (SELECT CONCAT('IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE [name] = ''', @SchemaName, ''') EXEC(''CREATE SCHEMA ', @SchemaName, ''');'))

	SELECT	 @createTableDDL = CONCAT('CREATE EXTERNAL TABLE ', @SchemaName, '.', @TableName, ' (', STRING_AGG(ColumnFullDefinition, ','), ') WITH ( LOCATION = ''', @DataSourcePath, ''', DATA_SOURCE = [', @DataSourceName, '], FILE_FORMAT = [SynapseParquetFormat])')
			,@createTableStatsDDL = STRING_AGG(CONCAT('CREATE STATISTICS stat_', column_name, ' ON ', @Schemaname, '.', @TableName, ' (', column_name, ') WITH FULLSCAN, NORECOMPUTE'), ';')
			,@createViewDDL = CONCAT('CREATE VIEW ', @SchemaName, '.vw', @TableName, ' AS SELECT * FROM OPENROWSET(BULK ''', @FolderPath, ''' , FORMAT=''PARQUET'') WITH (', STRING_AGG(ColumnFullDefinition, ','), ') AS r')
			,@openrowsetValue = CONCAT('FROM OPENROWSET(BULK ''''', @FolderPath, ''''', FORMAT=''''PARQUET'''') WITH (', STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnFullDefinition), ','))
	FROM #tmpFinal
	;

	SELECT @createViewStatsDDL = STRING_AGG(CONCAT('EXEC sys.sp_create_openrowset_statistics N''SELECT ', column_name, ' ', @openrowsetValue, ') AS r'''), ';')
	FROM #tmpFinal
	;

	INSERT INTO #ServerlessDDL VALUES 
		(@SchemaName, @TableName
		,CONCAT(@FileFormatCreateDDL, ';', @DataSourceCreateDDL, ';', @CreateSchema, ' IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.tables WHERE TABLE_SCHEMA = ''', @SchemaName, ''' AND TABLE_NAME = ''', @TableName, ''') DROP EXTERNAL TABLE ', @SchemaName, '.', @TableName, '; ', @createTableDDL, ';')
		,@createTableStatsDDL
		,CONCAT(@CreateSchema, ' IF OBJECT_ID(''', @SchemaName, '.vw', @TableName, ''', ''V'') IS NOT NULL DROP VIEW ', @SchemaName, '.vw', @TableName, '; EXEC(''', REPLACE(@createViewDDL, '''', ''''''), ''');')
		,@createViewStatsDDL
		)

	SET @cnt = @cnt + 1
END

SELECT * FROM #ServerlessDDL
