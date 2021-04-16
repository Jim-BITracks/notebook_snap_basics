/*
{
"notebooksnap": {
        "cell_name": "Create Added Dim Column Names Table (snowflake)",
        "connection": "snowflake-demo",
        "pattern": "Execute SQL"
    }
}
*/
CREATE TABLE IF NOT EXISTS ${snowflake_staging_schema}.ADDED_DIM_COLUMN_NAMES (
			ADDED_DIM_COLUMN_NAMES_TAG VARCHAR(255) NOT NULL,
			ROW_IS_CURRENT VARCHAR(255) NOT NULL,
			ROW_EFFECTIVE_DATE VARCHAR(255) NOT NULL,
			ROW_EXPIRATION_DATE VARCHAR(255) NOT NULL,
			ROW_INSERT_DATE VARCHAR(255) NOT NULL,
			ROW_UPDATE_DATE VARCHAR(1283) NOT NULL)

