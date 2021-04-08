/*
{
"notebooksnap": {
        "cell_name": "src to stg - snowflake tables",
        "src_connection": "snowflake-demo",
        "pattern": "Execute SQL"        
    }
}
*/
EXEC ${snowflake-demo_Database}.${snowflake_Schema_Name}.BUILD_AND_EXECUTE_MERGE_FOR_DIMENSION(
'${STG_SCHEMA}', 
'${STG_TABLE}', 
'${EDW_SCHEMA}', 
'${EDW_TABLE}', 
'${ADDED_DIM_COLUMN_NAMES_TAG}', 
'${BUSINESS_KEY_COLUMNS}', 
'${TYPE_2_COLUMNS}', 
'${TYPE_0_COLUMN}', 
'${STG_DATABASE}', 
'${EDW_DATABASE}'
)
