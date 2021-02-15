/*
{
"notebooksnap": {
        "cell_name": "src to stg - Snowflake columns",
        "src_connection": "snowflake-demo",
        "dst_connection": "eltsnap_v2",
        "dst_schema": "stg",
        "dst_table": "dim_column",
        "truncate_dst": "0",
        "identity_insert": "0",
        "batch_size": "0",
        "pattern": "Dataflow"
    }
}
*/

SELECT /*${snowflake_Metadata_Server_Name}*/'snowflake_dev'/**/::VARCHAR(128) AS server_name
, TABLE_CATALOG::VARCHAR(128) AS database_name
, TABLE_SCHEMA::VARCHAR(128) AS table_schema
, TABLE_NAME::VARCHAR(128) AS table_name
, COLUMN_NAME::VARCHAR(128) AS column_name 
, ORDINAL_POSITION AS ordinal_position 
, IS_NULLABLE::VARCHAR(3) AS is_nullable 
, DATA_TYPE::VARCHAR(128) AS data_type 
, CHARACTER_MAXIMUM_LENGTH AS char_max_length 
, 'N'::VARCHAR(1) AS primary_key
, 'snowflake'::VARCHAR(16) AS row_data_source
FROM ${snowflake-demo_Database}.INFORMATION_SCHEMA.COLUMNS 
 WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
AND IFNULL(COMMENT, '') != 'clustering shadow column'