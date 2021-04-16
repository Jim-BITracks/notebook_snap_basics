/*
{
"notebooksnap": {
        "cell_name": "Insert into Added Dim Column Names Table (snowflake)",
        "connection": "snowflake-demo",
        "pattern": "Execute SQL"
    }
}
*/
INSERT INTO ${snowflake_staging_schema}.ADDED_DIM_COLUMN_NAMES VALUES 
('Standard','row_is_current','row_effective_date','row_expiration_date','row_insert_date','row_update_date'),
('Standard_UC','ROW_IS_CURRENT','ROW_EFFECTIVE_DATE','ROW_EXPIRATION_DATE','ROW_INSERT_DATE','ROW_UPDATE_DATE'); 

