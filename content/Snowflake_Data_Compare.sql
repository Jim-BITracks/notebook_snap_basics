CREATE OR REPLACE PROCEDURE STG.SNOWFLAKE_DATA_COMPARE (STG_SCHEMA VARCHAR, 
														  STG_TABLE VARCHAR, 
														  TGT_SCHEMA VARCHAR, 
														  TGT_TABLE VARCHAR,  
														  NATURAL_KEY_COLUMNS VARCHAR, 
														  IGNORE_COLUMNS VARCHAR, 
														  SOURCE_WHERE_CLAUSE VARCHAR, 
														  DESTINATION_WHERE_CLAUSE VARCHAR,
														  STG_DATABASE VARCHAR, 
														  TGT_DATABASE VARCHAR)
	RETURNS STRING 
	LANGUAGE JAVASCRIPT
	COMMENT = "Created by: Jerry Simpson"
AS $$

try 
{

stg_table_full = STG_DATABASE + '.' + STG_SCHEMA + '.' + STG_TABLE
tgt_table_full = TGT_DATABASE + '.' + TGT_SCHEMA + '.' + TGT_TABLE


if ( NATURAL_KEY_COLUMNS == '' )
	{
		return 'No Natural key(s) found'
	}

if ( STG_DATABASE == '' )	
	{
		 stg_table_full = STG_SCHEMA + '.' + STG_TABLE
		 stg_info_schema = 'INFORMATION_SCHEMA.COLUMNS'
	}
 
if ( TGT_DATABASE == '' )	
	{
		 tgt_table_full = TGT_SCHEMA + '.' + TGT_TABLE
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


if ( SOURCE_WHERE_CLAUSE != '' ) 
	{
	SOURCE_WHERE = SOURCE_WHERE_CLAUSE.split("'").join("''")
	}
else 
	{
	SOURCE_WHERE = ''
	}


if ( DESTINATION_WHERE_CLAUSE != '' ) 
	{
	TARGET_WHERE = DESTINATION_WHERE_CLAUSE.split("'").join("''")
	}
else 
	{
	TARGET_WHERE = ''
	}


//Populate temp table with staging table columns
sql = `CREATE OR REPLACE TEMPORARY TABLE src_table AS 
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
sql = `CREATE OR REPLACE TEMPORARY TABLE tgt_table AS 
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, 'NULL' AS IS_NULLABLE, DATA_TYPE
			, CASE WHEN DATA_TYPE IN ('NUMBER','DECIMAL') THEN NUMERIC_PRECISION::VARCHAR || ',' || NUMERIC_SCALE::VARCHAR ELSE CHARACTER_MAXIMUM_LENGTH::VARCHAR END AS "COLUMN_LENGTH" 
			, COLLATION_NAME
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = '` + TGT_SCHEMA + `'
			AND TABLE_NAME = '` + TGT_TABLE + `'
			AND COLUMN_NAME NOT IN (` + ignore_columns_list + `)
			ORDER BY ORDINAL_POSITION; `
cmd_res = snowflake.execute({sqlText: sql});


// Create natural key table
sql = `CREATE OR REPLACE TEMPORARY TABLE NATURAL_KEY_COLUMNS AS
		WITH CTE (KEY_COLUMN) AS (
		SELECT '` + NATURAL_KEY_COLUMNS + `' AS KEY_COLUMN)
		SELECT TRIM(VALUE) AS COLUMN_NAME
		FROM CTE, table(SPLIT_TO_TABLE(KEY_COLUMN,','));` 
cmd_res = snowflake.execute({sqlText: sql});


//Staging Natural Key Select/Join
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME 
			 FROM src_table c
			 JOIN NATURAL_KEY_COLUMNS m
			   ON m.COLUMN_NAME = c.COLUMN_NAME
			ORDER BY c.ORDINAL_POSITION
		)
		  SELECT TRIM(LISTAGG(' src.' || COLUMN_NAME || ', '), ', ') || ' ' AS "SELECT"
				,REPLACE(REPLACE(LISTAGG(' tgt.' || COLUMN_NAME || ' = src.' || COLUMN_NAME  || ' AND ~') || '~','AND ~~'), '~')  AS "NatKeyJoin"
				,REPLACE(REPLACE(LISTAGG('src.' || COLUMN_NAME || ' IS NULL AND ~') || '~','AND ~~'), '~') AS "SrcNatKeyWhere"
				,RTRIM(LISTAGG('src.' || COLUMN_NAME || ' || '', '' || '),' || '', '' || ') AS "StagingNatKeyValue"
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
StagingNatKeySelect = cmd_res.getColumnValue(1);
StagingNatKeyJoin = cmd_res.getColumnValue(2);
StagingNatKeyWHERE = cmd_res.getColumnValue(3);
StagingNatKeyValue = cmd_res.getColumnValue(4);


//Destination Natural Key Select/Join
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME
			 FROM tgt_table c
			 JOIN NATURAL_KEY_COLUMNS m
			   ON m.COLUMN_NAME = c.COLUMN_NAME
			ORDER BY c.ORDINAL_POSITION
		)
		  SELECT TRIM(LISTAGG(' tgt.' || COLUMN_NAME || ', '), ', ') || ' ' AS "SELECT"
				,REPLACE(REPLACE(LISTAGG(' tgt.' || COLUMN_NAME || ' = src.' || COLUMN_NAME  || ' AND ~') || '~','AND ~~'), '~')  AS "NatKeyJoin"
				,REPLACE(REPLACE(LISTAGG('tgt.' || COLUMN_NAME || ' IS NULL AND ~') || '~','AND ~~'), '~') AS "DestNatKeyWhere"
				,RTRIM(LISTAGG('tgt.' || COLUMN_NAME || ' || '''', '''' || '),' || '''', '''' || ') AS "DstNatKeyValue1"
				,RTRIM(LISTAGG('tgt.' || COLUMN_NAME || ' || '', '' || '),' || '', '' || ') AS "DstNatKeyValue2"
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
DstNatKeySelect = cmd_res.getColumnValue(1);
DstNatKeyJoin = cmd_res.getColumnValue(2);
DstNatKeyWHERE = cmd_res.getColumnValue(3);
DstNatKeyValue = cmd_res.getColumnValue(4);
DstNatKeyValue2 = cmd_res.getColumnValue(5);


// Staging Columns
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME
			 FROM src_table c
			 LEFT JOIN NATURAL_KEY_COLUMNS bk
				ON bk.COLUMN_NAME = c.COLUMN_NAME
			WHERE bk.COLUMN_NAME IS NULL
			ORDER BY c.ORDINAL_POSITION
		)
		SELECT TRIM(LISTAGG(' src.' || COLUMN_NAME || '' || ', '), ', ') AS "STAGING_COLUMNS"
				, REPLACE(REPLACE(LISTAGG(' src.' || COLUMN_NAME || ' <> tgt.' || COLUMN_NAME  || ' OR ~') || '~','OR ~~'), '~') AS "STAGING_WHERE_CLAUSE"
				, TRIM(LISTAGG('''' || COLUMN_NAME || '''' || ', '), ', ') AS "STAGING_COLUMNS_NO_ALIAS"
				, REPLACE(REPLACE(LISTAGG('\n SELECT ''Different Values'' AS Type_Code
				        , ` + DstNatKeyValue + ` AS Natural_Key_Value
				        , CASE WHEN tgt.' || COLUMN_NAME || ' <> src.' || COLUMN_NAME || ' THEN ''' || COLUMN_NAME || ''' END AS COLUMN_DIFFERENT              
				        , src.' || COLUMN_NAME || ' AS Source_Value
				        , tgt.' || COLUMN_NAME || ' AS Dest_Value
				FROM (SELECT * FROM ` + tgt_table_full + ' ' + TARGET_WHERE + `) tgt
				JOIN (SELECT * FROM ` + stg_table_full + ' ' + SOURCE_WHERE + `) src
				    ON ` + StagingNatKeyJoin + `
				WHERE COLUMN_DIFFERENT IS NOT NULL' || '\n UNION ~') || '~', 'UNION ~~'),'~') AS "DIFFERENCE_SELECT"
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next(); 
StagingColumns = cmd_res.getColumnValue(1); 
StagingWhereClause = cmd_res.getColumnValue(2);
StagingColumnsNoAlias = cmd_res.getColumnValue(3); 
Difference_SELECT = cmd_res.getColumnValue(4); 




// Destination Columns
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME
			 FROM src_table c
			 LEFT JOIN NATURAL_KEY_COLUMNS bk
				ON bk.COLUMN_NAME = c.COLUMN_NAME
			WHERE bk.COLUMN_NAME IS NULL
			ORDER BY c.ORDINAL_POSITION
		)
		SELECT TRIM(LISTAGG(' tgt.' || COLUMN_NAME || '' || ', '), ', ')
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next(); 
DstColumns = cmd_res.getColumnValue(1); 


//Difference Statement
sql = `CREATE OR REPLACE TABLE ` + STG_SCHEMA + `.Data_Compare_Table AS
		WITH Difference_Check AS ( ` + Difference_SELECT + ` 
						)
		, SRC_ONLY AS (SELECT 'Only In Source' AS Type_Code
								, ` + StagingNatKeyValue + ` AS Natural_Key_Value
								, NULL AS COLUMN_DIFFERENT
								, NULL AS Source_Value
								, NULL AS Dest_Value
		                  FROM ` + stg_table_full + ` src
		                  LEFT JOIN ` + tgt_table_full + ` tgt
		                    ON ` + StagingNatKeyJoin + `
		                  WHERE ` + DstNatKeyWHERE + `
		                  )
		, TGT_ONLY AS (SELECT 'Only In Destination' AS Type_Code
								, ` + DstNatKeyValue2 + ` AS Natural_Key_Value
								, NULL AS COLUMN_DIFFERENT
								, NULL AS Source_Value
								, NULL AS Dest_Value
		                  FROM ` + tgt_table_full + ` tgt
		                  LEFT JOIN ` + stg_table_full + ` src
		                    ON ` + StagingNatKeyJoin + `
		                  WHERE ` + StagingNatKeyWHERE + `
		                  )
		SELECT TYPE_CODE, Natural_Key_Value, COLUMN_DIFFERENT, Source_Value, Dest_Value 
		FROM src_ONLY
		UNION 
		SELECT TYPE_CODE, Natural_Key_Value, COLUMN_DIFFERENT, Source_Value, Dest_Value 
		FROM tgt_ONLY
		UNION
		SELECT TYPE_CODE, Natural_Key_Value, COLUMN_DIFFERENT, Source_Value, Dest_Value
		FROM Difference_Check
		`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
//return 'Table ' + STG_SCHEMA + '.DATA_COMPARE_TABLE successfully created.';
return sql
}
catch (err)
{
return err.message
}


$$;


