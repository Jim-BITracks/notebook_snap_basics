/*
{
"notebooksnap": {
        "cell_name": "Create Dest Dimension Self-fix Procedure (snowflake)",
        "connection": "snowflake-demo",
        "pattern": "Execute SQL"
    }
}
*/
CREATE OR REPLACE PROCEDURE /*${snowflake_Schema}*/"STG"/**/.BUILD_AND_EXECUTE_MERGE_FOR_DIMENSION_DEST_REPAIR (STG_SCHEMA VARCHAR, 
																				  STG_TABLE VARCHAR, 
																				  EDW_SCHEMA VARCHAR, 
																				  EDW_TABLE VARCHAR, 
																				  ADDED_DIM_COLUMN_NAMES_TAG VARCHAR, 
																				  BUSINESS_KEY_COLUMNS VARCHAR, 
																				  TYPE_2_COLUMNS VARCHAR, 
																				  TYPE_0_COLUMNS VARCHAR, 
																				  STG_DATABASE VARCHAR, 
																				  EDW_DATABASE VARCHAR)
	RETURNS STRING 
	LANGUAGE JAVASCRIPT
	COMMENT = "Created by: Jerry Simpson"
AS $$

try 
{

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


// Get names for supplemental dimension columns
sql = `SELECT ROW_IS_CURRENT, ROW_EFFECTIVE_DATE, ROW_EXPIRATION_DATE, ROW_INSERT_DATE, ROW_UPDATE_DATE 
		FROM ` + STG_SCHEMA + `.ADDED_DIM_COLUMN_NAMES 
		WHERE ADDED_DIM_COLUMN_NAMES_TAG  = '` + ADDED_DIM_COLUMN_NAMES_TAG + `'`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
row_is_current = cmd_res.getColumnValue(1);
row_effective_date = cmd_res.getColumnValue(2);
row_expiration_date = cmd_res.getColumnValue(3);
row_insert_date = cmd_res.getColumnValue(4);
row_update_date = cmd_res.getColumnValue(5);


//Populate temp table with staging table columns
sql = `CREATE OR REPLACE TEMPORARY TABLE stg_table AS 
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, 'NULL' AS IS_NULLABLE, DATA_TYPE
			, CASE WHEN DATA_TYPE IN ('NUMBER','DECIMAL') THEN NUMERIC_PRECISION::VARCHAR || ',' || NUMERIC_SCALE::VARCHAR ELSE CHARACTER_MAXIMUM_LENGTH::VARCHAR END AS "COLUMN_LENGTH" 
			, COLLATION_NAME
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = '` + STG_SCHEMA + `'
			AND TABLE_NAME = '` + STG_TABLE + `'
			ORDER BY ORDINAL_POSITION; `
cmd_res = snowflake.execute({sqlText: sql});


//Get max ordinal position
sql = `SELECT MAX(ORDINAL_POSITION) AS "MAX_ORDINAL_POSITION" FROM stg_table`;
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
max_ordinal_position = cmd_res.getColumnValue(1);

PK_column = EDW_TABLE.replace('D_', '') + '_ID'


//Recreate destination table if needed
sql = `SELECT IFF( EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA || '.' || TABLE_NAME = '` + edw_table_full + `'), 'EXISTS', '')`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();


if ( cmd_res.getColumnValue(1) == '') 
	{
	PK_column_def = PK_column + ' NUMBER(38,0) autoincrement'

	sql = `CREATE OR REPLACE TEMPORARY TABLE table_list AS 
			SELECT COLUMN_NAME || ' ' || DATA_TYPE 
                   || COALESCE('(' || COLUMN_LENGTH || ')','')
                   || COALESCE('COLLATE ' || COLLATION_NAME,'')
                   || ' ' || IS_NULLABLE AS "COLUMNS"
				, ORDINAL_POSITION
			FROM stg_table
			ORDER BY ORDINAL_POSITION;`
	cmd_res = snowflake.execute({sqlText: sql});


	sql = `INSERT INTO table_list 
			SELECT 'ROW_EFFECTIVE_DATE DATE NULL', ` + max_ordinal_position + 1 + `
			UNION
			SELECT 'ROW_EXPIRATION_DATE DATE NULL', ` + max_ordinal_position + 2 + `
			UNION
			SELECT 'ROW_INSERT_DATE DATE NULL', ` + max_ordinal_position + 3 + `
			UNION
			SELECT 'ROW_IS_CURRENT VARCHAR(1) ', ` + max_ordinal_position + 4 + `
			UNION
			SELECT 'ROW_UPDATE_DATE DATE NULL', ` + max_ordinal_position + 5 + ``
	cmd_res = snowflake.execute({sqlText: sql});

	sql = `WITH cte AS (SELECT * FROM table_list ORDER BY ORDINAL_POSITION)
			SELECT LISTAGG(COLUMNS,', ') FROM cte`;
	cmd_res = snowflake.execute({sqlText: sql});
	cmd_res.next();
	column_list = cmd_res.getColumnValue(1);

	sql = `CREATE TABLE ` + edw_table_full + ` (` + PK_column_def + ` , ` + column_list + ` ) `
	cmd_res = snowflake.execute({sqlText: sql});
	}



//Populate temp table with destination table columns
sql = `CREATE OR REPLACE TEMPORARY TABLE dst_table AS 
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, 'NULL' AS IS_NULLABLE, DATA_TYPE
			, CASE WHEN DATA_TYPE IN ('NUMBER','DECIMAL') THEN NUMERIC_PRECISION::VARCHAR || ',' || NUMERIC_SCALE::VARCHAR ELSE CHARACTER_MAXIMUM_LENGTH::VARCHAR END AS "COLUMN_LENGTH" 
			, COLLATION_NAME
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = '` + EDW_SCHEMA + `'
			AND TABLE_NAME = '` + EDW_TABLE + `'
			ORDER BY ORDINAL_POSITION; `
cmd_res = snowflake.execute({sqlText: sql});


//Remove supplemental dimension columns from temp table 
sql = `DELETE FROM dst_table 
		WHERE COLUMN_NAME IN (SELECT ROW_IS_CURRENT FROM ` + STG_SCHEMA + `.ADDED_DIM_COLUMN_NAMES WHERE ADDED_DIM_COLUMN_NAMES_TAG = ADDED_DIM_COLUMN_NAMES_TAG)
		   OR COLUMN_NAME IN (SELECT ROW_EFFECTIVE_DATE FROM ` + STG_SCHEMA + `.ADDED_DIM_COLUMN_NAMES WHERE ADDED_DIM_COLUMN_NAMES_TAG = ADDED_DIM_COLUMN_NAMES_TAG)
		   OR COLUMN_NAME IN (SELECT ROW_EXPIRATION_DATE FROM ` + STG_SCHEMA + `.ADDED_DIM_COLUMN_NAMES WHERE ADDED_DIM_COLUMN_NAMES_TAG = ADDED_DIM_COLUMN_NAMES_TAG)
		   OR COLUMN_NAME IN (SELECT ROW_INSERT_DATE FROM ` + STG_SCHEMA + `.ADDED_DIM_COLUMN_NAMES WHERE ADDED_DIM_COLUMN_NAMES_TAG = ADDED_DIM_COLUMN_NAMES_TAG)
		   OR COLUMN_NAME IN (SELECT ROW_UPDATE_DATE FROM ` + STG_SCHEMA + `.ADDED_DIM_COLUMN_NAMES WHERE ADDED_DIM_COLUMN_NAMES_TAG = ADDED_DIM_COLUMN_NAMES_TAG)
		   OR COLUMN_NAME IN (SELECT COLUMN_NAME FROM dst_table WHERE COLUMN_NAME = '`+ PK_column +`');`
cmd_res = snowflake.execute({sqlText: sql});


sql = `SELECT IFF( EXISTS (SELECT COLUMN_NAME, DATA_TYPE, COLUMN_LENGTH, COLLATION_NAME FROM stg_table
		EXCEPT
		SELECT COLUMN_NAME, DATA_TYPE, COLUMN_LENGTH, COLLATION_NAME FROM dst_table), 'MISSING', '')`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
if ( cmd_res.getColumnValue(1) == 'MISSING' )
{
	
sql_stmt = `WITH CTE AS (
			SELECT COLUMN_NAME FROM stg_table
			EXCEPT
			SELECT COLUMN_NAME FROM dst_table)
			SELECT cte.COLUMN_NAME || ' ' || DATA_TYPE 
			               || COALESCE('(' || COLUMN_LENGTH || ')','')
			               || COALESCE('COLLATE ' || COLLATION_NAME,'')
			               || ' ' || IS_NULLABLE AS "COLUMNS"
			        FROM CTE cte
			        JOIN stg_table s
			          ON s.column_name = cte.column_name`;
	stmt = snowflake.createStatement({sqlText: sql_stmt});
	cmd_res = stmt.execute();
	//cmd_res = snowflake.execute({sqlText: sql});
	//cmd_res.next();
	//return_value = cmd_res.getColumnValue(1);
	while (cmd_res.next())
	{
	column_def = cmd_res.getColumnValue(1);

	sql = `ALTER TABLE ` + edw_table_full + ` ADD COLUMN ` + column_def 
	alter_stmt = snowflake.execute({sqlText: sql});
	
	}
}


CALL_ARGUMENTS = "'" + STG_SCHEMA + "','" + STG_TABLE + "','" + EDW_SCHEMA + "','" + EDW_TABLE + "','" + ADDED_DIM_COLUMN_NAMES_TAG + "','" + BUSINESS_KEY_COLUMNS + "','" + TYPE_2_COLUMNS + "','" + TYPE_0_COLUMNS + "','" + STG_DATABASE + "','" + EDW_DATABASE + "'"

sql = `CALL ` + STG_SCHEMA + `.BUILD_AND_EXECUTE_MERGE_FOR_DIMENSION(` + CALL_ARGUMENTS + `)`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
return cmd_res.getColumnValue(1);
}
catch (err)
{
return err.message
}

$$;





