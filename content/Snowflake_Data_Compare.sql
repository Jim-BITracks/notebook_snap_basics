/*
{
"notebooksnap": {
        "cell_name": "Create Data Compare Procedure (snowflake)",
        "connection": "snowflake-demo",
        "pattern": "Execute SQL"
    }
}
*/
CREATE OR REPLACE PROCEDURE STG.SNOWFLAKE_DATA_COMPARE (SRC_DATABASE VARCHAR, 
														  SRC_SCHEMA VARCHAR, 
														  SRC_TABLE VARCHAR,
														  TGT_DATABASE VARCHAR,
														  TGT_SCHEMA VARCHAR, 
														  TGT_TABLE VARCHAR,  
														  NATURAL_KEY_COLUMNS VARCHAR, 
														  IGNORE_COLUMNS VARCHAR, 
														  SOURCE_WHERE_CLAUSE VARCHAR, 
														  DESTINATION_WHERE_CLAUSE VARCHAR,
														  OUTPUT_DATABASE VARCHAR,
														  OUTPUT_SCHEMA VARCHAR,
														  OUTPUT_TABLE VARCHAR,
														  CREATE_OUTPUT_TABLE VARCHAR)
	RETURNS STRING 
	LANGUAGE JAVASCRIPT
	COMMENT = 'Created by: Jerry Simpson (BI Tracks Consulting)
				Inputs:    
					SRC_SCHEMA                  (required)
					SRC_TABLE                   (required)
					TGT_SCHEMA                  (required)
					TGT_TABLE                   (required)
					NATURAL_KEY_COLUMNS  		(required)
					IGNORE_COLUMNS        		(required)
					SOURCE_WHERE_CLAUSE         (optional)
					DESTINATION_WHERE_CLAUSE    (optional)
					SRC_DATABASE                (optional)
					TGT_DATABASE                (optional)
					OUTPUT_DATABASE VARCHAR 	(optional)
					OUTPUT_SCHEMA VARCHAR 		(required)
					OUTPUT_TABLE VARCHAR 		(required)
					CREATE_OUTPUT_TABLE			(required)
					'
AS $$

try 
{

stg_table_full = SRC_DATABASE + '.' + SRC_SCHEMA + '.' + SRC_TABLE
tgt_table_full = TGT_DATABASE + '.' + TGT_SCHEMA + '.' + TGT_TABLE
output_table_full = OUTPUT_DATABASE + '.' + OUTPUT_SCHEMA + '.' + OUTPUT_TABLE
stg_info_schema = SRC_DATABASE + '.' + 'INFORMATION_SCHEMA.COLUMNS'
tgt_info_schema = TGT_DATABASE + '.' + 'INFORMATION_SCHEMA.COLUMNS'


if ( NATURAL_KEY_COLUMNS == '' )
	{
		return 'No Natural key(s) found'
	}

if ( SRC_DATABASE == '' )	
	{
		 stg_table_full = SRC_SCHEMA + '.' + SRC_TABLE
		 stg_info_schema = SRC_DATABASE + 'INFORMATION_SCHEMA.COLUMNS'
	}
 
if ( TGT_DATABASE == '' )	
	{
		 tgt_table_full = TGT_SCHEMA + '.' + TGT_TABLE
		 tgt_info_schema = TGT_DATABASE + 'INFORMATION_SCHEMA.COLUMNS'
	}

if ( OUTPUT_DATABASE == '' )	
	{
		 output_table_full = OUTPUT_SCHEMA + '.' + OUTPUT_TABLE
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


//Output table check 
sql = `SELECT IFF( EXISTS (SELECT 1 FROM ` + stg_info_schema + ` 
		WHERE TABLE_SCHEMA = '` + OUTPUT_SCHEMA + `' 
		AND TABLE_NAME = '` + OUTPUT_TABLE + `'), 'EXISTS', '');`
cmd_res = snowflake.execute({sqlText: sql}); 
cmd_res.next();

if ( cmd_res.getColumnValue(1) != 'EXISTS' && CREATE_OUTPUT_TABLE != 'Y' && CREATE_OUTPUT_TABLE != 'y' )
	{
	return "Table '" + output_table_full + "' does not exist"
	}


//Populate temp table with staging table columns
sql = `CREATE OR REPLACE TEMPORARY TABLE src_table AS 
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, 'NULL' AS IS_NULLABLE, DATA_TYPE
			, CASE WHEN DATA_TYPE IN ('NUMBER','DECIMAL') THEN NUMERIC_PRECISION::VARCHAR || ',' || NUMERIC_SCALE::VARCHAR ELSE CHARACTER_MAXIMUM_LENGTH::VARCHAR END AS "COLUMN_LENGTH" 
			, COLLATION_NAME
			FROM ` + stg_info_schema + `
			WHERE TABLE_SCHEMA = '` + SRC_SCHEMA + `'
			AND TABLE_NAME = '` + SRC_TABLE + `'
			AND COLUMN_NAME NOT IN (` + ignore_columns_list + `)
			ORDER BY ORDINAL_POSITION; `
cmd_res = snowflake.execute({sqlText: sql});


//Populate temp table with destination table columns
sql = `CREATE OR REPLACE TEMPORARY TABLE tgt_table AS 
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, 'NULL' AS IS_NULLABLE, DATA_TYPE
			, CASE WHEN DATA_TYPE IN ('NUMBER','DECIMAL') THEN NUMERIC_PRECISION::VARCHAR || ',' || NUMERIC_SCALE::VARCHAR ELSE CHARACTER_MAXIMUM_LENGTH::VARCHAR END AS "COLUMN_LENGTH" 
			, COLLATION_NAME
			FROM ` + tgt_info_schema + `
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
				,TRIM(LISTAGG( COLUMN_NAME || ', '), ', ') || ' ' AS "SELECT_NO_ALIAS"
				,REPLACE(REPLACE(LISTAGG(' tgt.' || COLUMN_NAME || ' = src.' || COLUMN_NAME  || ' AND ~') || '~','AND ~~'), '~')  AS "NatKeyJoin"
				,REPLACE(REPLACE(LISTAGG('src.' || COLUMN_NAME || ' IS NULL AND ~') || '~','AND ~~'), '~') AS "SrcNatKeyWhere"
				,RTRIM(LISTAGG('src.' || COLUMN_NAME || ' || '', '' || '),' || '', '' || ') AS "StagingNatKeyValue"
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
StagingNatKeySelect = cmd_res.getColumnValue(1);
StagingNatKeySelectNoAlias = cmd_res.getColumnValue(2);
StagingNatKeyJoin = cmd_res.getColumnValue(3);
StagingNatKeyWHERE = cmd_res.getColumnValue(4);
StagingNatKeyValue = cmd_res.getColumnValue(5);


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
				,TRIM(LISTAGG( COLUMN_NAME || ', '), ', ') || ' ' AS "SELECT_NO_ALIAS"
				,REPLACE(REPLACE(LISTAGG(' tgt.' || COLUMN_NAME || ' = src.' || COLUMN_NAME  || ' AND ~') || '~','AND ~~'), '~')  AS "NatKeyJoin"
				,REPLACE(REPLACE(LISTAGG('tgt.' || COLUMN_NAME || ' IS NULL AND ~') || '~','AND ~~'), '~') AS "DestNatKeyWhere"
				,RTRIM(LISTAGG('tgt.' || COLUMN_NAME || ' || '''', '''' || '),' || '''', '''' || ') AS "DstNatKeyValue1"
				,RTRIM(LISTAGG('tgt.' || COLUMN_NAME || ' || '', '' || '),' || '', '' || ') AS "DstNatKeyValue2"
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
DstNatKeySelect = cmd_res.getColumnValue(1);
DstNatKeySelectNoAlias = cmd_res.getColumnValue(2);
DstNatKeyJoin = cmd_res.getColumnValue(3);
DstNatKeyWHERE = cmd_res.getColumnValue(4);
DstNatKeyValue = cmd_res.getColumnValue(5);
DstNatKeyValue2 = cmd_res.getColumnValue(6);


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
				, TRIM(LISTAGG( COLUMN_NAME || ', '), ', ') AS "STAGING_COLUMNS_NO_ALIAS"
				, REPLACE(REPLACE(LISTAGG('\n SELECT ''Different Values'' AS Type_Code
						, TO_TIMESTAMP_LTZ(CURRENT_DATE) AS COMPARE_INSERT_DATE
						, ''` + tgt_table_full + `'' AS TABLE_NAME
				        , ` + DstNatKeyValue + ` AS Natural_Key_Value
				        , CASE WHEN tgt.' || COLUMN_NAME || ' <> src.' || COLUMN_NAME || ' THEN ''' || COLUMN_NAME || ''' END AS COLUMN_DIFFERENT              
				        , src.' || COLUMN_NAME || ' AS Source_Value
				        , tgt.' || COLUMN_NAME || ' AS Target_Value
				FROM (SELECT DST_COLUMNS_HERE FROM ` + tgt_table_full + ' ' + TARGET_WHERE + `) tgt
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


//Replace * with column list
Difference_SELECT_SRC = Difference_SELECT.split("*").join(StagingNatKeySelectNoAlias + ', ' + StagingColumnsNoAlias);


// Destination Columns
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME
			 FROM tgt_table c
			 LEFT JOIN NATURAL_KEY_COLUMNS bk
				ON bk.COLUMN_NAME = c.COLUMN_NAME
			WHERE bk.COLUMN_NAME IS NULL
			ORDER BY c.ORDINAL_POSITION
		)
		SELECT TRIM(LISTAGG(' tgt.' || COLUMN_NAME || '' || ', '), ', ')
			   ,TRIM(LISTAGG( COLUMN_NAME || ', '), ', ') AS "TARGET_COLUMNS_NO_ALIAS" 
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next(); 
DstColumns = cmd_res.getColumnValue(1); 
DstColumnsNoAlias = cmd_res.getColumnValue(2); 


//Switch destination column list
Difference_SELECT_final = Difference_SELECT_SRC.split("DST_COLUMNS_HERE").join(DstNatKeySelectNoAlias + ', ' + DstColumnsNoAlias);


//Difference Statement
sql = `WITH Difference_Check AS ( ` + Difference_SELECT_final + ` )
		, SRC_ONLY AS (SELECT 'Only In Source' AS Type_Code
								, TO_TIMESTAMP_LTZ(CURRENT_DATE) AS COMPARE_INSERT_DATE
								, '` + stg_table_full + `' AS TABLE_NAME
								, ` + StagingNatKeyValue + ` AS Natural_Key_Value
								, NULL AS COLUMN_DIFFERENT
								, NULL AS Source_Value
								, NULL AS Target_Value
		                  FROM ` + stg_table_full + ` src
		                  LEFT JOIN ` + tgt_table_full + ` tgt
		                    ON ` + StagingNatKeyJoin + `
		                  WHERE ` + DstNatKeyWHERE + `
		                  )
		, TGT_ONLY AS (SELECT 'Only In Destination' AS Type_Code
								, TO_TIMESTAMP_LTZ(CURRENT_DATE) AS COMPARE_INSERT_DATE
								, '` + tgt_table_full + `' AS TABLE_NAME
								, ` + DstNatKeyValue2 + ` AS Natural_Key_Value
								, NULL AS COLUMN_DIFFERENT
								, NULL AS Source_Value
								, NULL AS Target_Value
		                  FROM ` + tgt_table_full + ` tgt
		                  LEFT JOIN ` + stg_table_full + ` src
		                    ON ` + StagingNatKeyJoin + `
		                  WHERE ` + StagingNatKeyWHERE + `
		                  )
		SELECT COMPARE_INSERT_DATE, TYPE_CODE, TABLE_NAME, Natural_Key_Value, COLUMN_DIFFERENT, Source_Value, Target_Value 
		FROM src_ONLY
		UNION 
		SELECT COMPARE_INSERT_DATE, TYPE_CODE, TABLE_NAME, Natural_Key_Value, COLUMN_DIFFERENT, Source_Value, Target_Value 
		FROM tgt_ONLY
		UNION
		SELECT COMPARE_INSERT_DATE, TYPE_CODE, TABLE_NAME, Natural_Key_Value, COLUMN_DIFFERENT, Source_Value, Target_Value
		FROM Difference_Check
		`
DifferenceStatement = sql


//Final statement
if ( CREATE_OUTPUT_TABLE == 'Y' || CREATE_OUTPUT_TABLE == 'y' )
	{
	sql = `CREATE OR REPLACE TRANSIENT TABLE ` + output_table_full + ` AS \n` + DifferenceStatement
	cmd_res = snowflake.execute({sqlText: sql});
	return 'Table ' + SRC_SCHEMA + '.DATA_COMPARE_TABLE successfully created.';
	}
else 
	{
	sql = `INSERT INTO ` + output_table_full + ` \n` + DifferenceStatement
	cmd_res = snowflake.execute({sqlText: sql});
	cmd_res.next();
	return cmd_res.getColumnValue(1) + ' Rows Inserted.';
	}


}
catch (err)
{
return err.message
}


$$;


