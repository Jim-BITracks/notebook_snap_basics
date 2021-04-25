/*
{
"notebooksnap": {
        "cell_name": "Create Example DIM_COLUMN_VIEW as Source (snowflake)",
        "connection": "snowflake-demo",
        "pattern": "Execute SQL"
    }
}
*/
CREATE VIEW IF NOT EXISTS /*${snowflake_Schema}*/STG/**/.DIM_COLUMN_VIEW AS 
SELECT DISTINCT TOP 10 TABLE_CATALOG
	TABLE_SCHEMA,
	TABLE_NAME,
	COLUMN_NAME,
	ORDINAL_POSITION,
	IS_NULLABLE,
	DATA_TYPE,
	CHARACTER_MAXIMUM_LENGTH,
	'NULL' AS PRIMARY_KEY
FROM INFORMATION_SCHEMA.COLUMNS;