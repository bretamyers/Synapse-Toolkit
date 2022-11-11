IF OBJECT_ID('tempdb..#tables') IS NOT NULL
	DROP TABLE #tables;

CREATE TABLE #tables (
	SchemaName NVARCHAR(100)
	,TableName NVARCHAR(100)
	,FolderPath NVARCHAR(1000)
	);

INSERT INTO #tables
VALUES (
	'stagingTPC'
	,'select'
	,'https://soeenterprisedatalake.dfs.core.windows.net/curated/TPC/Snowflake/TPCH_SF10/REGION/*.parquet'
	)

IF OBJECT_ID('tempdb..#CreateViewsDDL') IS NOT NULL
	DROP TABLE #CreateViewsDDL;

CREATE TABLE #CreateViewsDDL (
	SchemaName NVARCHAR(100)
	,ViewName NVARCHAR(100)
	,ViewDDL NVARCHAR(MAX)
	);

DECLARE @cnt INT = 1
DECLARE @sqlCreateView NVARCHAR(MAX)
DECLARE @SchemaName NVARCHAR(100)
DECLARE @TableName NVARCHAR(100)
DECLARE @FolderPath NVARCHAR(1000)

SELECT @SchemaName = SchemaName
	,@TableName = TableName
	,@FolderPath = FolderPath
	,@sqlCreateView = CONCAT (
		'sp_describe_first_result_set @tsql=N''SELECT * FROM OPENROWSET(BULK '''''
		,FolderPath
		,''''' , FORMAT=''''PARQUET'''') AS r'''
		)
FROM #tables;

IF OBJECT_ID('tempdb..#InformationSchemaTempTable', 'U') IS NOT NULL
	DROP TABLE #InformationSchemaTempTable;

CREATE TABLE #InformationSchemaTempTable (
	is_hidden BIT NOT NULL
	,column_ordinal INT NOT NULL
	,name SYSNAME NULL
	,is_nullable BIT NOT NULL
	,system_type_id INT NOT NULL
	,system_type_name NVARCHAR(256) NULL
	,max_length SMALLINT NOT NULL
	,precision TINYINT NOT NULL
	,scale TINYINT NOT NULL
	,collation_name SYSNAME NULL
	,user_type_id INT NULL
	,user_type_database SYSNAME NULL
	,user_type_schema SYSNAME NULL
	,user_type_name SYSNAME NULL
	,assembly_qualified_type_name NVARCHAR(4000)
	,xml_collection_id INT NULL
	,xml_collection_database SYSNAME NULL
	,xml_collection_schema SYSNAME NULL
	,xml_collection_name SYSNAME NULL
	,is_xml_document BIT NOT NULL
	,is_case_sensitive BIT NOT NULL
	,is_fixed_length_clr_type BIT NOT NULL
	,source_server SYSNAME NULL
	,source_database SYSNAME NULL
	,source_schema SYSNAME NULL
	,source_table SYSNAME NULL
	,source_column SYSNAME NULL
	,is_identity_column BIT NULL
	,is_part_of_unique_key BIT NULL
	,is_updateable BIT NULL
	,is_computed_column BIT NULL
	,is_sparse_column_set BIT NULL
	,ordinal_in_order_by_list SMALLINT NULL
	,order_by_list_length SMALLINT NULL
	,order_by_is_descending SMALLINT NULL
	,tds_type_id INT NOT NULL
	,tds_length INT NOT NULL
	,tds_collation_id INT NULL
	,tds_collation_sort_id TINYINT NULL
	);

INSERT INTO #InformationSchemaTempTable
EXEC (@sqlCreateView) /*SELECT * FROM #InformationSchemaTempTable*/

DECLARE @GetMaxValueStatement NVARCHAR(MAX)
DECLARE @GetColumnList NVARCHAR(MAX)

SELECT @GetMaxValueStatement = CONVERT(NVARCHAR(MAX), CONCAT (
			'SELECT '
			,STRING_AGG(ColumnMaxLength, ',')
			,' FROM OPENROWSET(BULK '''
			,@FolderPath
			,''' , FORMAT=''PARQUET'') WITH ('
			,STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnDatatypeWithMax), ',')
			,') AS r'
			))
	,@GetColumnList = STRING_AGG(QUOTENAME([name]), ',')
FROM (
	SELECT CASE 
			WHEN system_type_name LIKE ('%char%')
				OR system_type_name = 'varbinary(8000)'
				THEN CONCAT (
						'CONVERT(BIGINT, COALESCE(NULLIF(MAX(DATALENGTH('
						,QUOTENAME([name])
						,')), 0), 1)) AS '
						,QUOTENAME([name])
						)
			ELSE CONCAT (
					'COALESCE(CONVERT(BIGINT, SUM(0)), 0) AS '
					,QUOTENAME([name])
					)
			END AS ColumnMaxLength
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
	) AS a /*SELECT @GetMaxValueStatement*/ /*SELECT @GetColumnList*/

DECLARE @sqlUnpivot NVARCHAR(MAX)

SET @sqlUnpivot = CONCAT (
		'SELECT '''
		,@TableName
		,''' AS TABLE_NAME, unpvt.col AS COLUMN_NAME, CASE WHEN unpvt.datatype > 8000 THEN ''MAX'' ELSE CONVERT(NVARCHAR(100), unpvt.datatype) END AS DATATYPE_MAX FROM  ( '
		,@GetMaxValueStatement
		,' ) AS a '
		,CHAR(13)
		,' UNPIVOT ( datatype FOR col IN  ( '
		,@GetColumnList
		,') ) AS unpvt'
		)

DROP TABLE

IF EXISTS #tmpBus;
	CREATE TABLE #tmpBus (
		TABLE_CLEAN NVARCHAR(1000)
		,COLUMN_NAME NVARCHAR(1000)
		,DATATYPE_MAX NVARCHAR(1000)
		);

INSERT INTO #tmpBus
EXEC (@sqlUnpivot)

DECLARE @createFinalView NVARCHAR(MAX)
DECLARE @openrowsetValue NVARCHAR(MAX)

SELECT @createFinalView = CONCAT (
		'CREATE TABLE ['
		,@SchemaName
		,'].['
		,@TableName
		,'] ('
		,STRING_AGG(ColumnFullDefinition, ',')
		,') WITH ( DISTRIBUTION = ROUND_ROBIN, HEAP)'
		)
	,@openrowsetValue = CONCAT (
		'FROM OPENROWSET(BULK '''''
		,@FolderPath
		,''''', FORMAT=''''PARQUET'''') WITH ('
		,STRING_AGG(CONVERT(NVARCHAR(MAX), ColumnFullDefinition), ',')
		)
FROM (
	SELECT @TableName AS table_name
		,c.[name]
		,UPPER(TYPE_NAME(c.system_type_id)) AS DataType
		,CONCAT (
			QUOTENAME(c.[name])
			,' '
			,CASE 
				WHEN TYPE_NAME(c.system_type_id) IN (
						'int'
						,'bigint'
						,'smallint'
						,'tinyint'
						,'bit'
						,'decimal'
						,'numeric'
						,'float'
						,'real'
						,'datetime2'
						,'date'
						)
					THEN UPPER(c.system_type_name)
				ELSE CONCAT (
						UPPER(TYPE_NAME(c.system_type_id))
						,'('
						,a.DATATYPE_MAX
						,') '
						)
				END
			) AS ColumnFullDefinition
	FROM #InformationSchemaTempTable AS c
	JOIN #tmpBus AS a ON a.COLUMN_NAME = c.[name]
	ORDER BY column_ordinal OFFSET 0 ROWS
	) AS a /*SELECT @createFinalView*/ /*INSERT INTO #CreateViewsDDL*/

SELECT @SchemaName AS SchemaName
	,@TableName AS TableName
	,CONCAT (
		'IF OBJECT_ID('''
		,@SchemaName
		,'.'
		,@TableName
		,''', ''U'') IS NOT NULL DROP TABLE ['
		,@SchemaName
		,'].['
		,@TableName
		,']; '
		,@createFinalView
		,'; SELECT 1'
		) AS CreateTableDDL;
