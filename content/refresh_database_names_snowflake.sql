/*
{
"notebooksnap": {
   "cell_name": "Refresh Database Names (snowflake)",
   "src_connection": "sql_server",
   "dst_connection": "eltsnap_v2",
   "dst_schema": "stg",
   "dst_table": "dim_database",
   "truncate_dst": "N",
   "identity_insert": "N",
   "batch_size": "0",
   "pattern": "Dataflow"
   }
}
*/
SELECT /*${snowflake_Metadata_Server_Name}*/'snowflake_dev'/**/::VARCHAR(128) AS server_name
     , DATABASE_NAME::VARCHAR(128) AS database_name
     , CREATED::date AS database_create_date
     , 'N'::VARCHAR(1) AS change_tracking_enabled
     , 'SQL'::VARCHAR(16) AS row_data_source
  FROM INFORMATION_SCHEMA.DATABASES;