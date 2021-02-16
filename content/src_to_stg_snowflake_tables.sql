/*
{
"notebooksnap": {
        "cell_name": "src to stg - snowflake tables",
        "src_connection": "snowflake-demo",
        "dst_connection": "eltsnap_v2",
        "dst_schema": "stg",
        "dst_table": "dim_table",
        "truncate_dst": "0",
        "identity_insert": "0",
        "batch_size": "0",
        "pattern": "Dataflow"
    }
}
*/
SELECT '${snowflake_Metadata_Server_Name}'::VARCHAR(128) AS server_name
, TABLE_CATALOG::VARCHAR(128) AS database_name
, TABLE_SCHEMA::VARCHAR(128) AS table_schema
, TABLE_NAME::VARCHAR(128) AS table_name
, TABLE_TYPE::VARCHAR(32) AS table_type
, 'N'::VARCHAR(1) AS has_identity
, 'N'::VARCHAR(1) AS has_primary_key
, 'SQL'::VARCHAR(16) AS row_data_source
 FROM ${snowflake-demo_Database}.INFORMATION_SCHEMA.TABLES 
 WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'