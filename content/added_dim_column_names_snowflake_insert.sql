/*
{
"notebooksnap": {
        "cell_name": "Insert into Added Dim Column Names Table (snowflake)",
        "connection": "snowflake-demo",
        "pattern": "Execute SQL"
    }
}
*/
INSERT INTO /*${snowflake_Schema}*/"STG"/**/.ADDED_DIM_COLUMN_NAMES
WITH CTE (ADDED_DIM_COLUMN_NAMES_TAG,ROW_IS_CURRENT,ROW_EFFECTIVE_DATE,ROW_EXPIRATION_DATE,ROW_INSERT_DATE,ROW_UPDATE_DATE) AS (
SELECT 'Standard','row_is_current','row_effective_date','row_expiration_date','row_insert_date','row_update_date'
UNION ALL
SELECT 'Standard_UC','ROW_IS_CURRENT','ROW_EFFECTIVE_DATE','ROW_EXPIRATION_DATE','ROW_INSERT_DATE','ROW_UPDATE_DATE')
SELECT CTE.ADDED_DIM_COLUMN_NAMES_TAG,
	   CTE.ROW_IS_CURRENT,
	   CTE.ROW_EFFECTIVE_DATE,
	   CTE.ROW_EXPIRATION_DATE,
	   CTE.ROW_INSERT_DATE,
	   CTE.ROW_UPDATE_DATE
FROM CTE CTE 
LEFT JOIN /*${snowflake_Schema}*/"STG"/**/.ADDED_DIM_COLUMN_NAMES DIM
	ON DIM.ADDED_DIM_COLUMN_NAMES_TAG = CTE.ADDED_DIM_COLUMN_NAMES_TAG
WHERE DIM.ADDED_DIM_COLUMN_NAMES_TAG IS NULL;

