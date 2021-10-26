


IF OBJECT_ID('tempdb..#tables') IS NOT NULL
	DROP TABLE #tables
GO

CREATE TABLE #tables
(
	SchemaName NVARCHAR(100)
	,TableName NVARCHAR(100)
	,FolderPath NVARCHAR(1000)
)
;

DECLARE @Debug INT = 1

--Enter the tables schema, name, and path to files for the views to be created over
INSERT INTO #tables VALUES 
 ('dbo', 'vwTableA', 'https://adlsbrmyers.dfs.core.windows.net/landingzone/BAMStocksDB/dm/DimStockTicker/20210204/DimStockTicker_20210204_132033.parquet')
,('dbo', 'vwTableB', 'https://adlsbrmyers.dfs.core.windows.net/tpc/tpcds/SourceFiles_001GB_parquet/catalog_sales/part-00000-tid-8752900334898172838-ee1efabf-9b0b-4cd0-9140-a8a6fae3d8a1-777-1-c000.snappy.parquet')
,('dbo', 'vwTableC', 'https://adlsbrmyers.dfs.core.windows.net/tpc/tpcds/SourceFiles_001GB_parquet/catalog_sales/part-00000-tid-8752900334898172838-ee1efabf-9b0b-4cd0-9140-a8a6fae3d8a1-777-1-c000.snappy.parquet')


--Display the list of tables to be created
SELECT *
FROM #tables



IF OBJECT_ID('tempdb..#CreateViewsDDL') IS NOT NULL
	DROP TABLE #CreateViewsDDL
;

CREATE TABLE #CreateViewsDDL
(
	SchemaName NVARCHAR(100)
	,ViewName NVARCHAR(100)
	,ViewDDL NVARCHAR(MAX)
)
;

IF OBJECT_ID('tempdb..#CreateStatisticsDDL') IS NOT NULL
	DROP TABLE #CreateStatisticsDDL
;

CREATE TABLE #CreateStatisticsDDL
(
	SchemaName NVARCHAR(100)
	,TableName NVARCHAR(100)
	,StatisticsDDL NVARCHAR(MAX)
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
			--,@sqlCreateView = CONCAT('CREATE VIEW vwTemp AS SELECT * FROM OPENROWSET(BULK ''', FolderPath, ''' , FORMAT=''PARQUET'') AS r') 
			,@sqlCreateView = CONCAT('sp_describe_first_result_set @tsql=N''SELECT * FROM OPENROWSET(BULK ''''', FolderPath, ''''' , FORMAT=''''PARQUET'''') AS r''')
	FROM 
	(
		SELECT *, ROW_NUMBER() OVER (ORDER BY TableName) AS RN
		FROM #tables
	) AS a
	WHERE RN = @cnt


	IF OBJECT_ID('tempdb..#InformationSchemaTempTable', 'U') IS NOT NULL
		DROP TABLE #InformationSchemaTempTable
	;

	CREATE TABLE #InformationSchemaTempTable 
	(
		is_hidden bit NOT NULL
		, column_ordinal int NOT NULL
		, name sysname NULL
		, is_nullable bit NOT NULL
		, system_type_id int NOT NULL
		, system_type_name nvarchar(256) NULL
		, max_length smallint NOT NULL
		, precision tinyint NOT NULL
		, scale tinyint NOT NULL
		, collation_name sysname NULL
		, user_type_id int NULL
		, user_type_database sysname NULL
		, user_type_schema sysname NULL
		, user_type_name sysname NULL
		, assembly_qualified_type_name nvarchar(4000)
		, xml_collection_id int NULL
		, xml_collection_database sysname NULL
		, xml_collection_schema sysname NULL
		, xml_collection_name sysname NULL
		, is_xml_document bit NOT NULL
		, is_case_sensitive bit NOT NULL
		, is_fixed_length_clr_type bit NOT NULL
		, source_server sysname NULL
		, source_database sysname NULL
		, source_schema sysname NULL
		, source_table sysname NULL
		, source_column sysname NULL
		, is_identity_column bit NULL
		, is_part_of_unique_key bit NULL
		, is_updateable bit NULL
		, is_computed_column bit NULL
		, is_sparse_column_set bit NULL
		, ordinal_in_order_by_list smallint NULL
		, order_by_list_length smallint NULL
		, order_by_is_descending smallint NULL
		, tds_type_id int NOT NULL
		, tds_length int NOT NULL
		, tds_collation_id int NULL
		, tds_collation_sort_id tinyint NULL
	);

	INSERT INTO #InformationSchemaTempTable
	EXEC (@sqlCreateView)

	--SET @sqlCreateView = 'ALTER TABLE #InformationSchemaTempTable ADD table_name NVARCHAR(100) NOT NULL DEFAULT(''' + @TableName + ''')'
	EXEC (@sqlCreateView)

	SELECT * FROM #InformationSchemaTempTable

	DECLARE @GetMaxValueStatement NVARCHAR(MAX)
	DECLARE @GetColumnList NVARCHAR(MAX)

	SELECT	@GetMaxValueStatement = CONCAT('SELECT ', STRING_AGG(ColumnMaxLength, ','), ' FROM OPENROWSET(BULK ''', @FolderPath, ''' , FORMAT=''PARQUET'') AS r')
			,@GetColumnList = STRING_AGG(CONCAT('[', [name], ']'), ',')
	FROM
	(
		SELECT	CASE WHEN system_type_name LIKE ('%char%') THEN CONCAT('MAX(LEN([', [name], '])) AS [', [name], ']') ELSE CONCAT('SUM(0) AS ', [name]) END AS ColumnMaxLength
				,[name]
		FROM #InformationSchemaTempTable
	) AS a


	SELECT @GetMaxValueStatement
	SELECT @GetColumnList

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


	DECLARE @createFinalView NVARCHAR(MAX)
	DECLARE @openrowsetValue NVARCHAR(MAX)

	
	SELECT	@createFinalView = CONCAT('CREATE VIEW ', @SchemaName, '.', @TableName, ' AS SELECT * FROM OPENROWSET(BULK ''', @FolderPath, ''' , FORMAT=''PARQUET'') WITH (', STRING_AGG(ColumnFullDefinition, ','), ') AS r')
			,@openrowsetValue = CONCAT('FROM OPENROWSET(BULK ''''', @FolderPath, ''''', FORMAT=''''PARQUET'''') WITH (', STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnFullDefinition), ','))
	FROM
	(
		SELECT		@TableName AS table_name
					,c.[name]
					,UPPER(TYPE_NAME(c.system_type_id)) AS DataType
					,CASE TYPE_NAME(c.system_type_id)
						WHEN 'int' THEN ''
						WHEN 'bigint' THEN ''
						WHEN 'smallint' THEN ''
						WHEN 'tinyint' THEN ''
						WHEN 'bit' THEN ''
						WHEN 'decimal' THEN c.system_type_name
						WHEN 'numeric' THEN c.system_type_name
						WHEN 'datetime2' THEN c.system_type_name
						ELSE CONCAT('(', a.DATATYPE_MAX, ')')
					END AS ColumnPrecision
					,CONCAT(c.[name], ' '
						,CASE WHEN TYPE_NAME(c.system_type_id) IN ('int', 'bigint', 'smallint', 'tinyint', 'bit', 'decimal', 'numeric', 'datetime2') THEN UPPER(c.system_type_name)
							ELSE CONCAT(UPPER(TYPE_NAME(c.system_type_id)), '(', a.DATATYPE_MAX, ') COLLATE Latin1_General_100_BIN2_UTF8')
						END
						) AS ColumnFullDefinition
			FROM #InformationSchemaTempTable AS c
			JOIN #tmpBus AS a
			ON a.COLUMN_NAME = c.[name]
	) AS a

	--SELECT @createFinalView
	INSERT INTO #CreateViewsDDL
	SELECT @SchemaName, @TableName, @createFinalView
	;

	--IF @Debug = 0
	--BEGIN
	--	EXEC ('DROP VIEW IF EXISTS ' + @SchemaName + @TableName)
	--	EXEC (@createFinalView)
	--END

	--Stats for views
	INSERT INTO #CreateStatisticsDDL
	SELECT @SchemaName, @TableName, STRING_AGG(CONCAT('EXEC sys.sp_create_openrowset_statistics N''SELECT ', [name], ' ', @openrowsetValue, ') AS r'''), ';') AS StatisticsDDL
	FROM #InformationSchemaTempTable

	SET @cnt = @cnt + 1
END

SELECT *
FROM #CreateViewsDDL

--Create statistics statements that can be ran later to improve performance
--Note, this statements could take a long time to run if all are executed and the data size is large
SELECT *
FROM #CreateStatisticsDDL



--CREATE VIEW dbo.vwTableA AS SELECT * FROM OPENROWSET(BULK 'https://adlsbrmyers.dfs.core.windows.net/landingzone/BAMStocksDB/dm/DimStockTicker/20210204/DimStockTicker_20210204_132033.parquet' , FORMAT='PARQUET') WITH (StockTickerKey INT,StockTickerSymbol VARCHAR(6) COLLATE Latin1_General_100_BIN2_UTF8,IEXId VARCHAR(20) COLLATE Latin1_General_100_BIN2_UTF8,StockExchange VARCHAR(4) COLLATE Latin1_General_100_BIN2_UTF8,CompanyName VARCHAR(120) COLLATE Latin1_General_100_BIN2_UTF8,StockType VARCHAR(18) COLLATE Latin1_General_100_BIN2_UTF8,IEXLastGenerated DATETIME2(7),CurrencyCode VARCHAR(3) COLLATE Latin1_General_100_BIN2_UTF8,CountryCode VARCHAR(2) COLLATE Latin1_General_100_BIN2_UTF8,IEXTradingFlag INT,RowChecksum INT,RowInsertedETLLoadId VARCHAR(36) COLLATE Latin1_General_100_BIN2_UTF8,RowInsertedETLLoadDatetime DATETIME2(7)) AS r


	--	SELECT * FROM sys.types

	--	SELECT TYPE_NAME(system_type_id), *
	--	FROM #InformationSchemaTempTable


--SELECT MIN(CONVERT(VARCHAR(MAX), colA)), MAX(CONVERT(VARCHAR(MAX), colA))
--		,LEN(CONVERT(VARCHAR(MAX), colA))
--FROM
--(
--	SELECT 40.234123 as colA
--	UNION SELECT 31.234
--	UNION SELECT 1231231.452342
--	UNION SELECT 0.452342111
--	UNION SELECT 0.012
--) AS a



--SELECT  MAX(LEN(PARSENAME(colA, 2))) AS Precision, MAX(LEN(PARSENAME(colA, 1))) AS Scale
--FROM
--(
--	SELECT 40.234123 as colA
--	UNION SELECT 31.234
--	UNION SELECT 1332.452342
--	UNION SELECT 0.452342111
--	UNION SELECT 0.012
--) AS a


