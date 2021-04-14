


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
	,DataSourceName NVARCHAR(100)
	,DataSourcePath NVARCHAR(1000)
	,FolderPath NVARCHAR(1000)
)

INSERT INTO #tables 
SELECT SCHEMA_NAME(schema_id) AS SchemaName, CONCAT(a.name, '_OPTIMIZED') AS TableName, ds.name AS DataSourceName, a.location AS DataSourcePath, CONCAT(ds.location, CASE WHEN RIGHT(ds.location, 1) = '/' THEN '' ELSE '/' END, a.location) AS FolderPath 
FROM sys.external_tables AS a
JOIN sys.external_data_sources AS ds
ON ds.data_source_id = a.data_source_id
WHERE a.name NOT LIKE '%_OPTIMIZED'


--Display the list of tables to be created
SELECT *
FROM #tables

IF OBJECT_ID('tempdb..#CreateStatisticsDDL') IS NOT NULL
	DROP TABLE #CreateStatisticsDDL
GO

CREATE TABLE #CreateStatisticsDDL
(
	SchemaName NVARCHAR(100)
	,TableName NVARCHAR(100)
	,StatisticsDDL NVARCHAR(MAX)
)
GO


DECLARE @cnt INT = 1
DECLARE @sqlCreateView NVARCHAR(MAX)
DECLARE @cntTablesToLoad INT = 0
DECLARE @SchemaName NVARCHAR(100)
DECLARE @TableName NVARCHAR(100)
DECLARE @DatasourceName NVARCHAR(100)
DECLARE @FolderPath NVARCHAR(1000)
DECLARE @DataSourcePath NVARCHAR(1000)

SELECT @cntTablesToLoad = COUNT(*) FROM #tables

WHILE (@cnt <= @cntTablesToLoad)
BEGIN
	SELECT	@SchemaName = SchemaName
			,@TableName = TableName
			,@DatasourceName = DataSourceName
			,@FolderPath = FolderPath
			,@DataSourcePath = DataSourcePath
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


	DECLARE @createFinalTable NVARCHAR(MAX)
	DECLARE @openrowsetValue NVARCHAR(MAX)

	EXEC ('IF EXISTS (SELECT 1 FROM SYS.EXTERNAL_TABLES WHERE CONCAT(SCHEMA_NAME(schema_id), ''.'', name) = ''' + @SchemaName + '.' + @TableName + ''') DROP EXTERNAL TABLE ' + @SchemaName + '.' + @TableName + ';')

	SELECT	@createFinalTable = CONCAT('CREATE EXTERNAL TABLE ', @SchemaName, '.', @TableName, ' (', STRING_AGG(ColumnFullDefinition, ','), ') WITH ( LOCATION = ''', @DataSourcePath, ''', DATA_SOURCE = [', @DataSourceName, '], FILE_FORMAT = [FF_Parquet])')
			,@openrowsetValue = CONCAT('FROM OPENROWSET(BULK ''''', @FolderPath, ''''', FORMAT=''''PARQUET'''') WITH (', STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnFullDefinition), ','))
	FROM
	(
		SELECT	c.TABLE_NAME
				,c.COLUMN_NAME
				,UPPER(DATA_TYPE) AS DataType
				,CASE DATA_TYPE
					WHEN 'int' THEN ''
					WHEN 'bigint' THEN ''
					WHEN 'decimal' THEN '(' + CAST(NUMERIC_PRECISION as VARCHAR) + ', ' + CAST(NUMERIC_SCALE as VARCHAR) + ')'
					WHEN 'numeric' THEN '(' + CAST(NUMERIC_PRECISION as VARCHAR) + ', ' + CAST(NUMERIC_SCALE as VARCHAR) + ')'
					WHEN 'datetime2' THEN '(' + CAST(DATETIME_PRECISION as VARCHAR) + ')'
					ELSE CONCAT('(', a.DATATYPE_MAX, ')')
				END AS ColumnPrecision
				,CONCAT(c.COLUMN_NAME, ' ', UPPER(DATA_TYPE), ' ', CASE DATA_TYPE
					WHEN 'int' THEN ''
					WHEN 'bigint' THEN ''
					WHEN 'decimal' THEN '(' + CAST(NUMERIC_PRECISION as VARCHAR) + ', ' + CAST(NUMERIC_SCALE as VARCHAR) + ')'
					WHEN 'numeric' THEN '(' + CAST(NUMERIC_PRECISION as VARCHAR) + ', ' + CAST(NUMERIC_SCALE as VARCHAR) + ')'
					WHEN 'datetime2' THEN '(' + CAST(DATETIME_PRECISION as VARCHAR) + ')'
					ELSE CONCAT('(', a.DATATYPE_MAX, ') COLLATE Latin1_General_100_BIN2_UTF8')
				END) AS ColumnFullDefinition
		FROM #InformationSchemaTempTable AS c
		JOIN #tmpBus AS a
		ON a.COLUMN_NAME = c.COLUMN_NAME
	) AS a

	--SELECT @createFinalTable
	EXEC (@createFinalTable)

	--Stats for tables
	INSERT INTO #CreateStatisticsDDL
	SELECT @SchemaName, @TableName, STRING_AGG(CONCAT('CREATE STATISTICS stat_', COLUMN_NAME, ' ON ', @Schemaname, '.', @TableName, ' (', COLUMN_NAME, ') WITH FULLSCAN, NORECOMPUTE'), ';') AS StatisticsDDL
	FROM #InformationSchemaTempTable

	SET @cnt = @cnt + 1
END


--Create statistics statements that can be ran later to improve performance
--Note, this statements could take a long time to run if all are executed and the data size is large
SELECT *
FROM #CreateStatisticsDDL


