CREATE OR REPLACE PROCEDURE UTIL_DB.STG.METADATA_REFRESH_SNOWFLAKE_COMPLETE
     ( SRC_SCHEMA VARCHAR, 
       TGT_SCHEMA VARCHAR, 
       ADDED_DIM_COLUMN_NAMES_TAG VARCHAR, 
       TYPE_2_COLUMNS VARCHAR, 
       TYPE_0_COLUMNS VARCHAR, 
       SRC_DATABASE VARCHAR, 
       TGT_DATABASE VARCHAR,
       DEST_FIX VARCHAR )
	RETURNS VARCHAR 
	LANGUAGE JAVASCRIPT
	COMMENT = "Created by: Jerry Simpson (BI Tracks Consulting)"
AS $$

try 
{

if ( SRC_DATABASE == '' )	
	{
		 SRC_DATABASE = 'UTIL_DB'
	}
 
if ( TGT_DATABASE == '' )	
	{
		 TGT_DATABASE = 'UTIL_DB'
	}

dim_column_table = SRC_DATABASE + '.' + SRC_SCHEMA + '.ADDED_DIM_COLUMN_NAMES'


//Capture database names
sql = `CREATE OR REPLACE TEMPORARY TABLE database_name AS 
       SELECT DATABASE_NAME, ROW_NUMBER() OVER(ORDER BY DATABASE_NAME) AS ROWNUMBER
        FROM INFORMATION_SCHEMA.DATABASES;`
cmd_res = snowflake.execute({sqlText: sql});


//Create DATABASE_V
sql = ` CREATE OR REPLACE VIEW DATABASE_V AS 
        SELECT * FROM INFORMATION_SCHEMA.DATABASES `            
cmd_res = snowflake.execute({sqlText: sql});


//Merge Database table
CALL_ARGUMENTS = "'" + SRC_SCHEMA + "','DATABASE_V','" + TGT_SCHEMA + "','DATABASE_D','" + ADDED_DIM_COLUMN_NAMES_TAG + "','DATABASE_NAME','DATABASE_NAME,LAST_ALTERED','" + TYPE_0_COLUMNS + "','" + SRC_DATABASE + "','" + TGT_DATABASE + "'"

sql = `CALL ` + SRC_SCHEMA + `.BUILD_AND_EXECUTE_MERGE_FOR_DIMENSION_DEST_REPAIR(` + CALL_ARGUMENTS + `)`
cmd_res = snowflake.execute({sqlText: sql});


//Capture view names
sql = ` CREATE OR REPLACE TEMPORARY TABLE view_name AS 
        SELECT TABLE_NAME, ROW_NUMBER() OVER(ORDER BY TABLE_NAME) AS ROWNUMBER
        FROM INFORMATION_SCHEMA.VIEWS ID
        WHERE TABLE_SCHEMA = 'INFORMATION_SCHEMA' 
        AND TABLE_NAME <> 'DATABASES'
        AND TABLE_NAME <> 'LOAD_HISTORY'
        AND TABLE_NAME <> 'OBJECT_PRIVILEGES'
        AND TABLE_NAME <> 'REPLICATION_DATABASES'
        AND TABLE_NAME <> 'TABLE_PRIVILEGES'
        AND TABLE_NAME <> 'USAGE_PRIVILEGES'  
        AND TABLE_NAME <> 'EXTERNAL_TABLES'
        AND TABLE_NAME <> 'INFORMATION_SCHEMA_CATALOG_NAME'
        `
cmd_res = snowflake.execute({sqlText: sql});


// Get max queue value
sql = `SELECT IFNULL(MAX(ROWNUMBER),0) FROM view_name`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
max_int = cmd_res.getColumnValue(1);


//loop through to create views
i = 1

while (i <= max_int) {
    
    //Build list of views to create
    sql = ` SELECT TABLE_NAME 
              , CASE WHEN TABLE_NAME = 'APPLICABLE_ROLES' THEN 'GRANTEE,ROLE_NAME'
                     WHEN TABLE_NAME = 'COLUMNS' THEN 'TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME'
                     WHEN TABLE_NAME = 'ENABLED_ROLES' THEN 'ROLE_NAME'
                     --WHEN TABLE_NAME = 'EXTERNAL_TABLES' THEN 'TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME'
                     WHEN TABLE_NAME = 'FILE_FORMATS' THEN 'FILE_FORMATS_CATALOG,FILE_FORMATS_SCHEMA,FILE_FORMATS_NAME'
                     WHEN TABLE_NAME = 'FUNCTIONS' THEN 'FUNCTIONS_CATALOG,FUNCTIONS_SCHEMA,FUNCTIONS_NAME,ARGUMENT_SIGNATURE'
                     --WHEN TABLE_NAME = 'INFORMATION_SCHEMA_CATALOG_NAME' THEN 'CATALOG_NAME'
                     WHEN TABLE_NAME = 'PIPES' THEN 'PIPE_CATALOG,PIPE_SCHEMA,PIPE_NAME'
                     WHEN TABLE_NAME = 'PROCEDURES' THEN 'PROCEDURE_CATALOG,PROCEDURE_SCHEMA,PROCEDURE_NAME,ARGUMENT_SIGNATURE'
                     WHEN TABLE_NAME = 'REFERENTIAL_CONSTRAINTS' THEN 'CONSTRAINTS_CATALOG,CONSTRAINTS_SCHEMA,CONSTRAINTS_NAME'
                     --WHEN TABLE_NAME = 'REPLICATION_DATABASES' THEN 'REGION_GROUP,SNOWFLAKE_REGION,ACCOUNT_NAME,DATABASE_NAME'
                     WHEN TABLE_NAME = 'SCHEMATA' THEN 'CATALOG_NAME,SCHEMA_NAME'
                     WHEN TABLE_NAME = 'SEQUENCES' THEN 'SEQUENCE_CATALOG,SEQUENCE_SCHEMA,SEQUENCE_NAME'
                     WHEN TABLE_NAME = 'STAGES' THEN 'STAGE_CATALOG,STAGE_SCHEMA,STAGE,NAME'
                     WHEN TABLE_NAME = 'TABLES' THEN 'TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME'
                     WHEN TABLE_NAME = 'TABLE_CONSTRAINTS' THEN 'CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME'
                     --WHEN TABLE_NAME = 'TABLE_PRIVILEGES' THEN 'GRANTOR,GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME'
                     WHEN TABLE_NAME = 'TABLE_STORAGE_METRICS' THEN 'TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME'
                     --WHEN TABLE_NAME = 'USAGE_PRIVILEGES' THEN 'GRANTOR,GRANTEE,OBJECT_CATALOG,OBJECT_SCHEMA,OBJECT_NAME'
                     WHEN TABLE_NAME = 'VIEWS' THEN 'TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME'
                 END AS NATURAL_KEY
            FROM view_name WHERE ROWNUMBER = ` + i + ` `
    cmd_res = snowflake.execute({sqlText: sql});
    cmd_res.next();
    view = cmd_res.getColumnValue(1);
    natural_key = cmd_res.getColumnValue(2);

    view_name = view + '_V'
    tgt_table = view + '_D'
    
    //Replace views if change detected
    sql = ` SELECT IFF(EXISTS(SELECT 1
            FROM ` + TGT_SCHEMA + `.DATABASE_D
            WHERE DATEDIFF(DAY,ROW_UPDATE_DATE,CURRENT_DATE) <= 1),'EXISTS','') `
    cmd_res = snowflake.execute({sqlText: sql});
    cmd_res.next();
    
    if ( cmd_res.getColumnValue(1) == 'EXISTS' ) {
    
          sql = ` SELECT 'CREATE OR REPLACE VIEW ~FULL_NAME_HERE~ AS \n' || LISTAGG('SELECT * FROM ' || DATABASE_NAME || '.INFORMATION_SCHEMA.~META_NAME_HERE~', '\n UNION \n')
                  FROM database_name; ` 
          cmd_res = snowflake.execute({sqlText: sql});
          cmd_res.next();
          union_statement = cmd_res.getColumnValue(1);

          view_stmt = union_statement.split("~META_NAME_HERE~").join(view)
          view_stmt_final = view_stmt.split("~FULL_NAME_HERE~").join(SRC_DATABASE + '.' + SRC_SCHEMA + '.' + view_name)

          sql = view_stmt_final

          cmd_res = snowflake.execute({sqlText: sql});
    
          }
    
    //Merge Database table
    CALL_ARGUMENTS = "'" + SRC_SCHEMA + "','" + view_name + "','" + TGT_SCHEMA + "','" + tgt_table + "','" + ADDED_DIM_COLUMN_NAMES_TAG + "','" + natural_key + "','" + TYPE_2_COLUMNS + "','" + TYPE_0_COLUMNS + "','" + SRC_DATABASE + "','" + TGT_DATABASE + "'"


    sql = `CALL ` + SRC_SCHEMA + `.BUILD_AND_EXECUTE_MERGE_FOR_DIMENSION(` + CALL_ARGUMENTS + `)`
    if ( DEST_FIX.toUpperCase() == 'Y' )	
        {
             sql = `CALL ` + SRC_SCHEMA + `.BUILD_AND_EXECUTE_MERGE_FOR_DIMENSION_DEST_REPAIR(` + CALL_ARGUMENTS + `)`
        }
    cmd_res = snowflake.execute({sqlText: sql});
    cmd_res.next()       
    return_string = cmd_res.getColumnValue(1);    
    if ( return_string != "success" )
        {
            return 'failed: ' + return_string;
        }
    
    i++
    
    }
}
catch (err)
{
return err.message
}

return 'success'

$$;