


--CREATE EXTERNAL DATA SOURCE [adlsbrmyersDataSource] WITH (
--    LOCATION = 'https://adlsbrmyers.blob.core.windows.net', CREDENTIAL = [SasADLSBrmyersDatabaseScoped]
--);
--GO

--CREATE EXTERNAL FILE FORMAT [FF_Parquet] WITH (
--    FORMAT_TYPE = PARQUET,
--    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
--);
--GO



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
			,@sqlCreateView = CONCAT('CREATE VIEW vwTemp AS SELECT * FROM OPENROWSET(BULK ''', FolderPath, ''' , FORMAT=''PARQUET'') AS r') 
	FROM 
	(
		SELECT *, ROW_NUMBER() OVER (ORDER BY TableName) AS RN
		FROM #tables
	) AS a
	WHERE RN = @cnt
	
	EXEC ('DROP VIEW IF EXISTS vwTemp')
	EXEC (@sqlCreateView)

	IF OBJECT_ID('tempdb..#InformationSchemaTempTable') IS NOT NULL
		DROP TABLE #InformationSchemaTempTable
	;
	SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION
	INTO #InformationSchemaTempTable
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_NAME = 'vwTemp' 


	DECLARE @GetMaxValueStatement NVARCHAR(MAX)
	DECLARE @GetColumnList NVARCHAR(MAX)

	SELECT	@GetMaxValueStatement = CONCAT('SELECT ', STRING_AGG(ColumnMaxLength, ','), ' FROM OPENROWSET(BULK ''', @FolderPath, ''' , FORMAT=''PARQUET'') AS r')
			,@GetColumnList = STRING_AGG(CONCAT('[', COLUMN_NAME, ']'), ',')
	FROM
	(
		SELECT	CASE WHEN DATA_TYPE IN ('varchar', 'nvarchar', 'char') THEN CONCAT('MAX(LEN([', COLUMN_NAME, '])) AS [', COLUMN_NAME, ']') ELSE CONCAT('SUM(0) AS ', COLUMN_NAME) END AS ColumnMaxLength
				,COLUMN_NAME
		FROM #InformationSchemaTempTable
	) AS a


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

	EXEC ('DROP VIEW IF EXISTS ' + @SchemaName + '.vw' + @TableName)

	SELECT	@createFinalView = CONCAT('CREATE VIEW ', @SchemaName, '.vw', @TableName, ' AS SELECT * FROM OPENROWSET(BULK ''', @FolderPath, ''' , FORMAT=''PARQUET'') WITH (', STRING_AGG(ColumnFullDefinition, ','), ') AS r')
			,@openrowsetValue = CONCAT('FROM OPENROWSET(BULK ''''', @FolderPath, ''''', FORMAT=''''PARQUET'''') WITH (', STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnFullDefinition), ','))
	FROM
	(
		SELECT	c.TABLE_NAME
				,c.COLUMN_NAME
				,UPPER(DATA_TYPE) AS DataType
				,CASE DATA_TYPE
					WHEN 'int' THEN ''
					WHEN 'bigint' THEN ''
					WHEN 'smallint' THEN ''
					WHEN 'tinyint' THEN ''
					WHEN 'bit' THEN ''
					WHEN 'decimal' THEN '(' + CAST(numeric_precision as VARCHAR) + ', ' + CAST(numeric_scale as VARCHAR) + ')'
					WHEN 'numeric' THEN '(' + CAST(numeric_precision as VARCHAR) + ', ' + CAST(numeric_scale as VARCHAR) + ')'
					WHEN 'datetime2' THEN '(' + CAST(DATETIME_PRECISION as VARCHAR) + ')'
					ELSE CONCAT('(', a.DATATYPE_MAX, ')')
				END AS ColumnPrecision
				,CONCAT(c.COLUMN_NAME, ' ', UPPER(DATA_TYPE), ' ', CASE DATA_TYPE
					WHEN 'int' THEN ''
					WHEN 'bigint' THEN ''
					WHEN 'smallint' THEN ''
					WHEN 'tinyint' THEN ''
					WHEN 'bit' THEN ''
					WHEN 'decimal' THEN '(' + CAST(numeric_precision as VARCHAR) + ', ' + CAST(numeric_scale as VARCHAR) + ')'
					WHEN 'numeric' THEN '(' + CAST(numeric_precision as VARCHAR) + ', ' + CAST(numeric_scale as VARCHAR) + ')'
					WHEN 'datetime2' THEN '(' + CAST(DATETIME_PRECISION as VARCHAR) + ')'
					ELSE CONCAT('(', a.DATATYPE_MAX, ') COLLATE Latin1_General_100_BIN2_UTF8')
				END) AS ColumnFullDefinition
		FROM #InformationSchemaTempTable AS c
		JOIN #tmpBus AS a
		ON a.COLUMN_NAME = c.COLUMN_NAME
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
	SELECT @SchemaName, @TableName, STRING_AGG(CONCAT('EXEC sys.sp_create_openrowset_statistics N''SELECT ', COLUMN_NAME, ' ', @openrowsetValue, ') AS r'''), ';') AS StatisticsDDL
	FROM #InformationSchemaTempTable

	SET @cnt = @cnt + 1
END


SELECT *
FROM #CreateViewsDDL

--Create statistics statements that can be ran later to improve performance
--Note, this statements could take a long time to run if all are executed and the data size is large
SELECT *
FROM #CreateStatisticsDDL



