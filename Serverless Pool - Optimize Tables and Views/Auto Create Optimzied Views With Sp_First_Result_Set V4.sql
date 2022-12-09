
/*
	A helper script that will dynamically create DDL statements for various artifacts within Synapse.
	This script only generates the DDL for parquet files!!

	ARTIFACTS GENERATED:
		1. Synapse serverless pool external table definition
		2. Synapse serverless pool external table stats (per column)
		3. Synapse serverless pool view definition
		4. Synapse serverless pool veiw stats (per column)
		5. Synapse dedicated pool external table definition
		6. Synapse dedicated pool external table stats (per column)
		7. Azure Data Explorer table definition

	HOW TO USE:
		1. Connect to the Synapse serverless endpoint and make sure the user executing has the appropriate RBAC or ACLs on the storage account(s)
		2. Update the the INSERT INTO #tables definition below to include the tables and the paths to the tables files. 
		3. Execute the script. The execution may take some time depending on how much data it has to scan.
		4. Copy the DDL statement and format using a t-sql formatter like poor sql (https://poorsql.com/). There's a notepad++ and VSCode extension for poorsql as well.
		5. Make any adjustments to scripts as necessary. 
			Examples: 
				The serverless views won't contain the hive partition paths (filepath()) and will need to be added.
				The stats columns will generate DDL to create stats for every column on a table whereas you may only want/need to create stats on a handful of the columns.
				String column data lengths if you ran the script on a subset of the data and not the full dataset.
		6. Run the updated DDL
*/


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

/*
	Enter the tables schema, name, and path to files for the views to be created over
	!!The files must be parquet files!!
*/
INSERT INTO #tables (SchemaName, TableName, FolderPath) VALUES 
('dbo', 'fact_data', 'abfss://tpc@adlsbrmyers.dfs.core.windows.net/tpcds/SourceFiles_001GB_parquet/call_center/*.parquet')
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
	,TableName NVARCHAR(100)
	,ServerlessCreateTableDDL NVARCHAR(MAX)
	,ServerlessCreateTableStatsDDL NVARCHAR(MAX)
	,ServerlessCreateViewDDL NVARCHAR(MAX)
	,ServerlessCreateViewStatsDDL NVARCHAR(MAX)
	,DedicatedCreateTableDDL NVARCHAR(MAX)
	,DedicatedCreateStatsDDL NVARCHAR(MAX)
	,DataExplorerCreateTableDDL NVARCHAR(MAX)
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

	SELECT	@SchemaName = QUOTENAME(SchemaName)
			,@TableName = QUOTENAME(TableName)
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

	SELECT	@GetMaxValueStatement = CONVERT(NVARCHAR(MAX), CONCAT('SELECT ', STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnMaxLength), ','), ' FROM OPENROWSET(BULK ''', @FolderPath, ''' , FORMAT=''PARQUET'') WITH (',STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnDatatypeWithMax), ','), ') AS r'))
			,@GetColumnList = STRING_AGG(CONVERT(NVARCHAR(MAX), QUOTENAME([name])), ',')
	FROM
	(
		SELECT	CASE WHEN system_type_name LIKE ('%char%') OR system_type_name LIKE 'varbinary%' THEN CONCAT('CONVERT(INT, COALESCE(NULLIF(MAX(DATALENGTH(', QUOTENAME([name]), ')), 0), 1)) AS ', QUOTENAME([name])) ELSE CONCAT('SUM(0) AS ', QUOTENAME([name])) END AS ColumnMaxLength
				,CASE 
					WHEN system_type_name LIKE ('%char%')
						THEN CONCAT (
								QUOTENAME([name])
								,' '
								,REPLACE(system_type_name, '8000', 'MAX')
								,' COLLATE Latin1_General_100_BIN2_UTF8'
								)
					WHEN system_type_name = 'varbinary(8000)'
						THEN CONCAT (
								QUOTENAME([name])
								,' '
								,REPLACE(system_type_name, '8000', 'MAX')
								)
					ELSE CONCAT (
							QUOTENAME([name])
							,' '
							,system_type_name
							)
					END AS ColumnDatatypeWithMax
				,[name]
		FROM #InformationSchemaTempTable
	) AS a
	;

	DECLARE @sqlUnpivot NVARCHAR(MAX)
	
	SET @sqlUnpivot = CONCAT('SELECT ''', @TableName, ''' AS TABLE_NAME, unpvt.col AS COLUMN_NAME, unpvt.__datatype AS DATATYPE_MAX
	FROM 
	( ', @GetMaxValueStatement, ' ) AS a ', CHAR(13), ' UNPIVOT
	(
	__datatype
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
		,ColumnFullDefinitionServerless NVARCHAR(1000)
		,ColumnFullDefinitionDedicated NVARCHAR(1000)
		,ColumnFullDefinitionADX NVARCHAR(1000)
	)
	;

	INSERT INTO #tmpFinal
	SELECT		@TableName AS table_name
				,c.[name] AS column_name
				,UPPER(TYPE_NAME(c.system_type_id)) AS DataType
				,CONCAT(QUOTENAME(c.[name]), ' '
					,CASE WHEN TYPE_NAME(c.system_type_id) IN ('int', 'bigint', 'smallint', 'tinyint', 'bit', 'decimal', 'numeric', 'float', 'datetime2', 'date', 'varbinary') 
						THEN UPPER(c.system_type_name)
						ELSE CONCAT(UPPER(TYPE_NAME(c.system_type_id)), '(', CASE WHEN CASE WHEN a.DATATYPE_MAX = 'MAX' THEN 8001 ELSE a.DATATYPE_MAX END > 8000 THEN 'MAX' ELSE a.DATATYPE_MAX END, ') COLLATE Latin1_General_100_BIN2_UTF8')
					END
					) AS ColumnFullDefinitionServerless
				,CONCAT(c.[name], ' '
					,CASE WHEN TYPE_NAME(c.system_type_id) IN ('int', 'bigint', 'smallint', 'tinyint', 'bit', 'decimal', 'numeric', 'float', 'datetime2', 'date', 'varbinary') 
						THEN UPPER(c.system_type_name)
						ELSE CONCAT(UPPER(TYPE_NAME(c.system_type_id)), '(', CASE WHEN CASE WHEN a.DATATYPE_MAX = 'MAX' THEN 8001 ELSE a.DATATYPE_MAX END > 8000 THEN 'MAX' ELSE a.DATATYPE_MAX END, ')')
					END
					) AS ColumnFullDefinitionDedicated
				,CONCAT(c.[name], ':'
					,CASE --WHEN TYPE_NAME(c.system_type_id) IN ('int', 'bigint', 'smallint', 'tinyint', 'bit', 'decimal', 'numeric', 'float', 'datetime2', 'date') THEN UPPER(c.system_type_name)
						WHEN TYPE_NAME(c.system_type_id) = 'bit' THEN 'bool'
						WHEN TYPE_NAME(c.system_type_id) IN ('datetime', 'date') THEN 'datetime'
						WHEN TYPE_NAME(c.system_type_id) IN ('int', 'smallint', 'tinyint') THEN 'int'
						WHEN TYPE_NAME(c.system_type_id) = 'bigint' THEN 'long'
						WHEN TYPE_NAME(c.system_type_id) IN ('float', 'real') THEN 'real'
						WHEN TYPE_NAME(c.system_type_id) IN ('decimal', 'numeric') THEN 'decimal'
						ELSE 'string'
						--WHEN TYPE_NAME(c.system_type_id) IN ('varchar', 'nvarchar') THEN 'string'
						--ELSE CONCAT(UPPER(TYPE_NAME(c.system_type_id)), '(', a.DATATYPE_MAX, ')')
					END
					) AS ColumnFullDefinitionADX
	FROM #InformationSchemaTempTable AS c
	JOIN #tmpBus AS a
	ON a.COLUMN_NAME = c.[name]
	ORDER BY column_ordinal
	OFFSET 0 ROWS
	;

	DECLARE @createServerlessTableDDL NVARCHAR(MAX)
	DECLARE @createServerlessTableStatsDDL NVARCHAR(MAX)
	DECLARE @createServerlessViewDDL NVARCHAR(MAX)
	DECLARE @createServerlessViewStatsDDL NVARCHAR(MAX)
	DECLARE @createDedicatedPoolTableDDL NVARCHAR(MAX)
	DECLARE @createDedicatedPoolStatsDDL NVARCHAR(MAX)
	DECLARE @createDataExplorerDDL NVARCHAR(MAX)
	DECLARE @openrowsetValue NVARCHAR(MAX)
	DECLARE @DataSourceName NVARCHAR(MAX) = (
			SELECT CASE WHEN LEFT(FolderPath, 5) = 'abfss' THEN CONCAT('ds_'
				,SUBSTRING(FolderPath, CHARINDEX('@', FolderPath)+1, (CHARINDEX('.', FolderPath)-CHARINDEX('@', FolderPath)-1))
				,'_'
				,SUBSTRING(FolderPath, CHARINDEX('//', FolderPath)+2, (CHARINDEX('@', FolderPath)-CHARINDEX('//', FolderPath)-2))
				)
				ELSE CONCAT('ds_'
				, SUBSTRING(FolderPath, CHARINDEX('//', FolderPath)+2, (CHARINDEX('.',FolderPath)-9))
				,'_'
				,SUBSTRING(SUBSTRING(FolderPath, CHARINDEX('/', REPLACE(FolderPath, '//', ''))+3, LEN(FolderPath)), 0, CHARINDEX('/', SUBSTRING(FolderPath, CHARINDEX('/', REPLACE(FolderPath, '//', ''))+3, LEN(FolderPath))))
				) 
				END
			FROM #tables WHERE RN = @cnt)
	DECLARE @DataSourceDefinition NVARCHAR(MAX) = (SELECT SUBSTRING(FolderPath, 0, CHARINDEX('/', REPLACE(FolderPath, '//', ''))+2) FROM #tables WHERE RN = @cnt)
	DECLARE @DataSourcePath NVARCHAR(MAX) = (SELECT SUBSTRING(FolderPath, CHARINDEX('/', REPLACE(FolderPath, '//', ''))+2, LEN(FolderPath)) FROM #tables WHERE RN = @cnt)
	DECLARE @DataSourceCreateDDL NVARCHAR(MAX) = (SELECT CONCAT('IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = ''', @DataSourceName, ''') CREATE EXTERNAL DATA SOURCE [', @DataSourceName, '] WITH (LOCATION   = ''', @DataSourceDefinition, ''')', ''))
	DECLARE @FileFormatCreateDDL NVARCHAR(MAX) = 'IF NOT EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = ''SynapseParquetFormat'') CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] WITH ( FORMAT_TYPE = PARQUET)'
	DECLARE @CreateSchema NVARCHAR(MAX) = (SELECT CONCAT('IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE [name] = ''', REPLACE(REPLACE(@SchemaName, ']', ''), '[', ''), ''') EXEC(''CREATE SCHEMA ', REPLACE(REPLACE(@SchemaName, ']', ''), '[', ''), ''');'))

	SELECT	 @createServerlessTableDDL = CONCAT('CREATE EXTERNAL TABLE ', @SchemaName, '.', @TableName, ' (', STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnFullDefinitionServerless), ','), ') WITH ( LOCATION = ''', @DataSourcePath, ''', DATA_SOURCE = [', @DataSourceName, '], FILE_FORMAT = [SynapseParquetFormat])')
			,@createServerlessTableStatsDDL = STRING_AGG(CONVERT(NVARCHAR(MAX), CONCAT('CREATE STATISTICS stat_', column_name, ' ON ', @Schemaname, '.', @TableName, ' (', column_name, ') WITH FULLSCAN, NORECOMPUTE')), ';')
			,@createServerlessViewDDL = CONCAT('CREATE VIEW ', @SchemaName, '.vw', REPLACE(@TableName, '[', ''), ' AS SELECT * FROM OPENROWSET(BULK ''', @FolderPath, ''' , FORMAT=''PARQUET'') WITH (', STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnFullDefinitionServerless), ','), ') AS r')
			,@openrowsetValue = CONCAT('FROM OPENROWSET(BULK ''''', @FolderPath, ''''', FORMAT=''''PARQUET'''') WITH (', STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnFullDefinitionServerless), ','))
			,@createDedicatedPoolTableDDL = CONCAT ('CREATE TABLE [' ,@SchemaName ,'].[' ,@TableName ,'] (' ,STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnFullDefinitionDedicated), ',') ,') WITH ( DISTRIBUTION = ROUND_ROBIN, HEAP)')
			,@createDedicatedPoolStatsDDL = STRING_AGG(CONVERT(NVARCHAR(MAX), CONCAT('CREATE STATISTICS stat_', column_name, ' ON ', @Schemaname, '.', @TableName, ' (', column_name, ') WITH FULLSCAN')), ';')
			,@createDataExplorerDDL = CONCAT('.create table ', @TableName, ' (', STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnFullDefinitionADX), ','), ')')
	FROM #tmpFinal
	;

	SELECT @createServerlessViewStatsDDL = STRING_AGG(CONVERT(NVARCHAR(MAX), CONCAT('EXEC sys.sp_create_openrowset_statistics N''SELECT ', column_name, ' ', @openrowsetValue, ') AS r''')), ';')
	FROM #tmpFinal
	;

	INSERT INTO #ServerlessDDL VALUES 
		(@SchemaName, @TableName
		,CONCAT(@FileFormatCreateDDL, ';', @DataSourceCreateDDL, ';', @CreateSchema, ' IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.tables WHERE TABLE_SCHEMA = ''', REPLACE(REPLACE(@SchemaName, ']', ''), '[', ''), ''' AND TABLE_NAME = ''', REPLACE(REPLACE(@TableName, ']', ''), '[', ''), ''') DROP EXTERNAL TABLE ', @SchemaName, '.', @TableName, '; ', @createServerlessTableDDL, ';')
		,@createServerlessTableStatsDDL
		,CONCAT(@CreateSchema, ' IF OBJECT_ID(''', @SchemaName, '.vw', @TableName, ''', ''V'') IS NOT NULL DROP VIEW ', @SchemaName, '.vw', @TableName, '; EXEC(''', REPLACE(@createServerlessViewDDL, '''', ''''''), ''');')
		,@createServerlessViewStatsDDL
		,CONCAT(@CreateSchema, ' IF OBJECT_ID(''', @SchemaName, '.', @TableName, ''', ''U'') IS NOT NULL DROP TABLE ', @SchemaName, '.', @TableName, ';', @createDedicatedPoolTableDDL)
		,@createDedicatedPoolStatsDDL
		,@createDataExplorerDDL
		)

	SET @cnt = @cnt + 1
END

SELECT * FROM #ServerlessDDL

