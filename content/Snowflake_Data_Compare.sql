
/*
Jerry - please use SRC for 'Source' table and TGT for 'Target' table (instead of STG and EDW)
also reference 'NATURAL_KEY' instead of 'BUSINESS_KEY'
*/

CREATE OR REPLACE PROCEDURE SNOWFLAKE_DATA_COMPARE (STG_SCHEMA VARCHAR, 
														  STG_TABLE VARCHAR, 
														  EDW_SCHEMA VARCHAR, 
														  EDW_TABLE VARCHAR,  
														  BUSINESS_KEY_COLUMNS VARCHAR, 
														  IGNORE_COLUMNS VARCHAR, 
														  SOURCE_WHERE_CLAUSE VARCHAR, 
														  DESTINATION_WHERE_CLAUSE VARCHAR,
														  STG_DATABASE VARCHAR, 
														  EDW_DATABASE VARCHAR)
	RETURNS STRING 
	LANGUAGE JAVASCRIPT
	COMMENT = "Created by: Jerry Simpson"
AS $$

//try 
//{

stg_table_full = STG_DATABASE + '.' + STG_SCHEMA + '.' + STG_TABLE
edw_table_full = EDW_DATABASE + '.' + EDW_SCHEMA + '.' + EDW_TABLE


if ( BUSINESS_KEY_COLUMNS == '' )
	{
		return 'No Business key(s) found'
	}

if ( STG_DATABASE == '' )	
	{
		 stg_table_full = STG_SCHEMA + '.' + STG_TABLE
		 stg_info_schema = 'INFORMATION_SCHEMA.COLUMNS'
	}
 
if ( EDW_DATABASE == '' )	
	{
		 edw_table_full = EDW_SCHEMA + '.' + EDW_TABLE
	}

if ( IGNORE_COLUMNS != '' )
	{
		sql = `SELECT '''' || REPLACE('` + IGNORE_COLUMNS + `',',',''',''') || ''''`
		cmd_res = snowflake.execute({sqlText: sql});
		cmd_res.next();
		ignore_columns_list = cmd_res.getColumnValue(1);
	}
else 
	{
		ignore_columns_list = `''`
	}


//Populate temp table with staging table columns
sql = `CREATE OR REPLACE TEMPORARY TABLE source_table AS 
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, 'NULL' AS IS_NULLABLE, DATA_TYPE
			, CASE WHEN DATA_TYPE IN ('NUMBER','DECIMAL') THEN NUMERIC_PRECISION::VARCHAR || ',' || NUMERIC_SCALE::VARCHAR ELSE CHARACTER_MAXIMUM_LENGTH::VARCHAR END AS "COLUMN_LENGTH" 
			, COLLATION_NAME
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = '` + STG_SCHEMA + `'
			AND TABLE_NAME = '` + STG_TABLE + `'
			AND COLUMN_NAME NOT IN (` + ignore_columns_list + `)
			ORDER BY ORDINAL_POSITION; `
cmd_res = snowflake.execute({sqlText: sql});


//Populate temp table with destination table columns
sql = `CREATE OR REPLACE TEMPORARY TABLE dst_table AS 
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, 'NULL' AS IS_NULLABLE, DATA_TYPE
			, CASE WHEN DATA_TYPE IN ('NUMBER','DECIMAL') THEN NUMERIC_PRECISION::VARCHAR || ',' || NUMERIC_SCALE::VARCHAR ELSE CHARACTER_MAXIMUM_LENGTH::VARCHAR END AS "COLUMN_LENGTH" 
			, COLLATION_NAME
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = '` + EDW_SCHEMA + `'
			AND TABLE_NAME = '` + EDW_TABLE + `'
			AND COLUMN_NAME NOT IN (` + ignore_columns_list + `)
			ORDER BY ORDINAL_POSITION; `
cmd_res = snowflake.execute({sqlText: sql});


// Create business key table
sql = `CREATE OR REPLACE TEMPORARY TABLE BUSINESS_KEY_COLUMNS AS
		WITH CTE (KEY_COLUMN) AS (
		SELECT '` + BUSINESS_KEY_COLUMNS + `' AS KEY_COLUMN)
		SELECT TRIM(VALUE) AS COLUMN_NAME
		FROM CTE, table(SPLIT_TO_TABLE(KEY_COLUMN,','))` 
cmd_res = snowflake.execute({sqlText: sql});


//Staging Business Key Select/Join
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME 
			 FROM source_table c
			 JOIN BUSINESS_KEY_COLUMNS m
			   ON m.COLUMN_NAME = c.COLUMN_NAME
			ORDER BY c.ORDINAL_POSITION
		)
		  SELECT TRIM(LISTAGG(' SRC.' || COLUMN_NAME || ', '), ', ') || ' ' AS "SELECT"
				,REPLACE(REPLACE(LISTAGG(' DST.' || COLUMN_NAME || ' = SRC.' || COLUMN_NAME  || ' AND ~') || '~','AND ~~'), '~')  AS "BusKeyJoin"
				,REPLACE(REPLACE(LISTAGG('SRC.' || COLUMN_NAME || ' IS NULL AND ~') || '~','AND ~~'), '~') AS "SrcBusKeyWhere"
			FROM base`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
StagingBusKeySelect = cmd_res.getColumnValue(1);
StagingBusKeyJoin = cmd_res.getColumnValue(2);
StagingBusKeyWHERE = cmd_res.getColumnValue(3);


//Destination Business Key Select/Join
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME
			 FROM dst_table c
			 JOIN BUSINESS_KEY_COLUMNS m
			   ON m.COLUMN_NAME = c.COLUMN_NAME
			ORDER BY c.ORDINAL_POSITION
		)
		  SELECT TRIM(LISTAGG(' SRC.' || COLUMN_NAME || ', '), ', ') || ' ' AS "SELECT"
				,REPLACE(REPLACE(LISTAGG(' DST.' || COLUMN_NAME || ' = SRC.' || COLUMN_NAME  || ' AND ~') || '~','AND ~~'), '~')  AS "BusKeyJoin"
				,REPLACE(REPLACE(LISTAGG('DST.' || COLUMN_NAME || ' IS NULL AND ~') || '~','AND ~~'), '~') AS "DestBusKeyWhere"
			FROM base`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
DstBusKeySelect = cmd_res.getColumnValue(1);
DstBusKeyJoin = cmd_res.getColumnValue(2);
DstBusKeyWHERE = cmd_res.getColumnValue(3);


// Staging Columns
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME
			 FROM source_table c
			 LEFT JOIN BUSINESS_KEY_COLUMNS bk
				ON bk.COLUMN_NAME = c.COLUMN_NAME
			WHERE bk.COLUMN_NAME IS NULL
			ORDER BY c.ORDINAL_POSITION
		)
		SELECT TRIM(LISTAGG(' src.' || COLUMN_NAME || '' || ', '), ', ')
				, REPLACE(REPLACE(LISTAGG(' src.' || COLUMN_NAME || ' <> dst.' || COLUMN_NAME  || ' OR ~') || '~','OR ~~'), '~') 
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next(); 
StagingColumns = cmd_res.getColumnValue(1); 
StagingWhereClause = cmd_res.getColumnValue(2);


// Destination Columns
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME
			 FROM source_table c
			 LEFT JOIN BUSINESS_KEY_COLUMNS bk
				ON bk.COLUMN_NAME = c.COLUMN_NAME
			WHERE bk.COLUMN_NAME IS NULL
			ORDER BY c.ORDINAL_POSITION
		)
		SELECT TRIM(LISTAGG(' dst.' || COLUMN_NAME || '' || ', '), ', ')
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next(); 
DstColumns = cmd_res.getColumnValue(1); 


//Difference Statement

/*	to automate				*/

WITH Difference_Check AS (
	              SELECT    'Different Values' AS Type_Code
	              		  , DST.VEHICLE_TYPE_CODE_2 AS Natural_Key_Value
	                      , CASE WHEN dst.VEHICLE_TYPE_CODE_5 <> SRC.VEHICLE_TYPE_CODE_5 THEN 'VEHICLE_TYPE_CODE_5' END AS COLUMN_DIFFERENT
	              		  , SRC.VEHICLE_TYPE_CODE_5 AS Source_Value
	                      , dst.VEHICLE_TYPE_CODE_5 AS Dest_Value
	              FROM EDW.D_VEHICLE dst
	              JOIN STG.D_VEHICLE src
	                  ON  DST.VEHICLE_TYPE_CODE_2 = SRC.VEHICLE_TYPE_CODE_2
	              WHERE COLUMN_DIFFERENT IS NOT NULL
	              union
	              SELECT    'Different Values' AS Type_Code
	              		  , DST.VEHICLE_TYPE_CODE_2 AS Natural_Key_Value
	                      , CASE WHEN dst.VEHICLE_TYPE_CODE_4 <> SRC.VEHICLE_TYPE_CODE_4 THEN 'VEHICLE_TYPE_CODE_4' END AS COLUMN_DIFFERENT
	              		  , SRC.VEHICLE_TYPE_CODE_4 AS Source_Value
	                      , dst.VEHICLE_TYPE_CODE_4 AS Dest_Value
	              FROM EDW.D_VEHICLE dst
	              JOIN STG.D_VEHICLE src
	                  ON  DST.VEHICLE_TYPE_CODE_2 = SRC.VEHICLE_TYPE_CODE_2
	              WHERE COLUMN_DIFFERENT IS NOT NULL
	              union
	              SELECT     'Different Values' AS Type_Code
	              		  , DST.VEHICLE_TYPE_CODE_2 AS Natural_Key_Value
	                      , CASE WHEN dst.VEHICLE_TYPE_CODE_3 <> SRC.VEHICLE_TYPE_CODE_3 THEN 'VEHICLE_TYPE_CODE_3' END AS COLUMN_DIFFERENT              
	                      , SRC.VEHICLE_TYPE_CODE_3 AS Source_Value
	                      , dst.VEHICLE_TYPE_CODE_3 AS Dest_Value
	              FROM EDW.D_VEHICLE dst
	              JOIN STG.D_VEHICLE src
	                  ON  DST.VEHICLE_TYPE_CODE_2 = SRC.VEHICLE_TYPE_CODE_2
	              WHERE COLUMN_DIFFERENT IS NOT NULL
				)
, SRC_ONLY AS (SELECT 'Only In Source' AS Type_Code
						, src.VEHICLE_TYPE_CODE_2 AS Natural_Key_Value
						, NULL AS COLUMN_DIFFERENT
						, NULL AS Source_Value
						, NULL AS Dest_Value
                  FROM STG.D_VEHICLE src
                  LEFT JOIN EDW.D_VEHICLE dst
                    ON dst.VEHICLE_TYPE_CODE_2 = src.VEHICLE_TYPE_CODE_2
                  WHERE dst.VEHICLE_TYPE_CODE_2 IS NULL
                  )
, DST_ONLY AS (SELECT 'Only In Destination' AS Type_Code
						, dst.VEHICLE_TYPE_CODE_2 AS Natural_Key_Value
						, NULL AS COLUMN_DIFFERENT
						, NULL AS Source_Value
						, NULL AS Dest_Value
                  FROM EDW.D_VEHICLE dst
                  LEFT JOIN STG.D_VEHICLE src
                    ON dst.VEHICLE_TYPE_CODE_2 = src.VEHICLE_TYPE_CODE_2
                  WHERE src.VEHICLE_TYPE_CODE_2 IS NULL
                  )
SELECT * FROM SRC_ONLY
UNION 
SELECT * FROM DST_ONLY
UNION
SELECT * FROM Difference_Check
  


