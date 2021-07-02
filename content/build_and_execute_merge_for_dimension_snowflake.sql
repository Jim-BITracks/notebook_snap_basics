/*
{
"notebooksnap": {
        "cell_name": "Create Dimension Merge Procedure (snowflake)",
        "connection": "snowflake-demo",
        "pattern": "Execute SQL"
    }
}
*/
CREATE OR REPLACE PROCEDURE /*${snowflake_Schema}*/STG/**/.BUILD_AND_EXECUTE_MERGE_FOR_DIMENSION(SRC_SCHEMA VARCHAR, 
																	  SRC_TABLE VARCHAR, 
																	  TGT_SCHEMA VARCHAR, 
																	  TGT_TABLE VARCHAR, 
																	  ADDED_DIM_COLUMN_NAMES_TAG VARCHAR, 
																	  BUSINESS_KEY_COLUMNS VARCHAR, 
																	  TYPE_2_COLUMNS VARCHAR, 
																	  TYPE_0_COLUMNS VARCHAR, 
																	  SRC_DATABASE VARCHAR, 
																	  TGT_DATABASE VARCHAR)
    RETURNS STRING
	LANGUAGE JAVASCRIPT	
	COMMENT = 'Created by: Jerry Simpson (BI Tracks Consulting)
				Inputs:    
				    SRC_SCHEMA                  (required)
				    SRC_TABLE                   (required)
				    TGT_SCHEMA                  (required)
				    TGT_TABLE                   (required)
				    ADDED_DIM_COLUMN_NAMES_TAG  (required)
				    BUSINESS_KEY_COLUMNS        (required)
				    TYPE_2_COLUMNS              (optional)
				    TYPE_0_COLUMNS              (optional)
				    SRC_DATABASE                (optional)
				    TGT_DATABASE                (optional)
					'
AS $$

try {

StgDatabaseParam = SRC_DATABASE
RptDatabaseParam = TGT_DATABASE
src_table_full = SRC_DATABASE + '.' + SRC_SCHEMA + '.' + SRC_TABLE
tgt_table_full = TGT_DATABASE + '.' + TGT_SCHEMA + '.' + TGT_TABLE
src_info_schema = SRC_DATABASE + '.INFORMATION_SCHEMA.COLUMNS'

if ( BUSINESS_KEY_COLUMNS == '' )
	{
		return 'No Business key(s) found'
	}

if ( SRC_DATABASE == '' )	
	{
		 src_table_full = SRC_SCHEMA + '.' + SRC_TABLE
		 src_info_schema = 'INFORMATION_SCHEMA.COLUMNS'
	}
 
if ( TGT_DATABASE == '' )	
	{
		 tgt_table_full = TGT_SCHEMA + '.' + TGT_TABLE
	}


// Get names for supplemental dimension columns
sql = `SELECT ROW_IS_CURRENT, ROW_EFFECTIVE_DATE, ROW_EXPIRATION_DATE, ROW_INSERT_DATE, ROW_UPDATE_DATE 
		FROM ` + SRC_SCHEMA + `.ADDED_DIM_COLUMN_NAMES 
		WHERE ADDED_DIM_COLUMN_NAMES_TAG  = '` + ADDED_DIM_COLUMN_NAMES_TAG + `'`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
row_is_current = cmd_res.getColumnValue(1);
row_effective_date = cmd_res.getColumnValue(2);
row_expiration_date = cmd_res.getColumnValue(3);
row_insert_date = cmd_res.getColumnValue(4);
row_update_date = cmd_res.getColumnValue(5);


// Create staging table 
sql = `CREATE OR REPLACE TEMPORARY TABLE STAGING_TABLE AS
		SELECT * FROM ` + src_info_schema + `
		WHERE TABLE_NAME = '` + SRC_TABLE + `'
		AND TABLE_SCHEMA = '` + SRC_SCHEMA + `'`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();


// Create business key table
sql = `CREATE OR REPLACE TEMPORARY TABLE BUSINESS_KEY_COLUMNS AS
WITH CTE (KEY_COLUMN) AS (
SELECT '` + BUSINESS_KEY_COLUMNS + `' AS KEY_COLUMN)
SELECT TRIM(VALUE) AS COLUMN_NAME
FROM CTE, table(SPLIT_TO_TABLE(KEY_COLUMN,','))` 
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();


// Business Key Select/Join
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME
			 FROM STAGING_TABLE c
			 JOIN BUSINESS_KEY_COLUMNS m
			   ON m.COLUMN_NAME = c.COLUMN_NAME
			ORDER BY c.ORDINAL_POSITION
		)
		  SELECT TRIM(LISTAGG(' s.' || COLUMN_NAME || ', '), ', ') || ' ' AS "SELECT"
				,REPLACE(REPLACE(LISTAGG(' r.' || COLUMN_NAME || ' = s.' || COLUMN_NAME  || ' AND ~') || '~','AND ~~'), '~')  AS "BusKeyJoin"
				,REPLACE(REPLACE(LISTAGG(' t1.' || COLUMN_NAME || ' = s.' || COLUMN_NAME  || ' AND ~') || '~','AND ~~'), '~')  AS "BusKeyJoin_t1"
				,REPLACE(REPLACE(LISTAGG(' t2.' || COLUMN_NAME || ' = s.' || COLUMN_NAME  || ' AND ~') || '~','AND ~~'), '~')  AS "BusKeyJoin_t2"
				,REPLACE(REPLACE(LISTAGG(' nc.' || COLUMN_NAME || ' = s.' || COLUMN_NAME  || ' AND ~') || '~','AND ~~'), '~')  AS "BusKeyJoin_nc"
				,REPLACE(REPLACE(LISTAGG(' src.' || COLUMN_NAME || ' = dst.' || COLUMN_NAME  || ' AND ~') || '~','AND ~~'), '~')  AS "BusKeyJoin_src"
			FROM base`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
BusKeySelect = cmd_res.getColumnValue(1);
BusKeyJoin = cmd_res.getColumnValue(2);
BusKeyJoin_t1 = cmd_res.getColumnValue(3);
BusKeyJoin_t2 = cmd_res.getColumnValue(4);
BusKeyJoin_nc = cmd_res.getColumnValue(5);
BusKeyJoin_src = cmd_res.getColumnValue(6);


// Type 0 Columns 
sql = `CREATE OR REPLACE TEMPORARY TABLE TYPE_0_COLUMNS AS
		WITH TYPE_0 (KEY_COLUMN) AS (
		SELECT '` + TYPE_0_COLUMNS + `')
		SELECT TRIM(VALUE) AS COLUMN_NAME
		FROM TYPE_0, table(SPLIT_TO_TABLE(KEY_COLUMN,','));`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();


// Type 2 Columns 
sql = `CREATE OR REPLACE TEMPORARY TABLE TYPE_2_COLUMNS AS
		WITH TYPE_2 (KEY_COLUMN) AS (
		SELECT '` + TYPE_2_COLUMNS + `')
		SELECT TRIM(VALUE) AS COLUMN_NAME
		FROM TYPE_2, table(SPLIT_TO_TABLE(KEY_COLUMN,','));`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();


// Type 1 Columns
sql = `CREATE OR REPLACE TEMPORARY TABLE TYPE_1_COLUMNS AS 
		SELECT c.COLUMN_NAME
		  FROM STAGING_TABLE c
		  LEFT JOIN TYPE_2_COLUMNS t2
			ON t2.COLUMN_NAME = c.COLUMN_NAME
		  LEFT JOIN BUSINESS_KEY_COLUMNS bk
			ON bk.COLUMN_NAME = c.COLUMN_NAME
		 WHERE t2.COLUMN_NAME IS NULL
		   AND bk.COLUMN_NAME IS NULL;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();


// Type 1 Change Check 
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME
			 FROM STAGING_TABLE c
			 JOIN TYPE_1_COLUMNS t1
			   ON t1.COLUMN_NAME = c.COLUMN_NAME
			ORDER BY c.ORDINAL_POSITION
		)
		SELECT TRIM(LISTAGG(' IFNULL(CAST(r.' || column_name || ' AS VARCHAR),''-99999'') != IFNULL(CAST(s.' || column_name || ' AS VARCHAR),''-99999'')' || ' OR '), 'OR ')
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
Type1ChangeCheck = cmd_res.getColumnValue(1);

if ( Type1ChangeCheck == '' ) {
Type1ChangeCheck = ' 1=2 ';
}


// Type 2 Change Check 
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME
			 FROM STAGING_TABLE c
			 JOIN TYPE_2_COLUMNS t1
			   ON t1.COLUMN_NAME = c.COLUMN_NAME
			ORDER BY c.ORDINAL_POSITION
		)
		SELECT TRIM(LISTAGG(' IFNULL(CAST(r.' || column_name || ' AS VARCHAR,''-99999'') != IFNULL(CAST(s.' || column_name || ' AS VARCHAR,''-99999'')' || ' OR '), 'OR ')
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
Type2ChangeCheck = cmd_res.getColumnValue(1);

if ( Type2ChangeCheck == '' ) {
Type2ChangeCheck = ' 1=2 ';
}


// Staging Columns
sql = `WITH base AS
		(
		   SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				, c.COLUMN_NAME
			 FROM STAGING_TABLE c
			ORDER BY c.ORDINAL_POSITION
		)
		SELECT TRIM(LISTAGG(' s.' || COLUMN_NAME || '' || ', '), ', ')
				, TRIM(LISTAGG(' ' || column_name || '' || ', '), ', ')
				, TRIM(LISTAGG(' SRC.' || column_name || '' || ', '), ', ')
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next(); 
StagingColumns = cmd_res.getColumnValue(1); 
MergeInsertList = cmd_res.getColumnValue(2);
MergeOutputList = cmd_res.getColumnValue(3);


// Merge Update Set
sql = `WITH base AS
		(
		    SELECT DISTINCT TOP 1000 c.ORDINAL_POSITION
				 , c.COLUMN_NAME
			  FROM STAGING_TABLE c
			  LEFT JOIN TYPE_0_COLUMNS t0
				ON t0.COLUMN_NAME = c.COLUMN_NAME
			  LEFT JOIN BUSINESS_KEY_COLUMNS bk
				ON bk.COLUMN_NAME = c.COLUMN_NAME
			 WHERE t0.COLUMN_NAME IS NULL
			   AND bk.COLUMN_NAME IS NULL
			 ORDER BY c.ORDINAL_POSITION
		)
		SELECT TRIM(LISTAGG(' ' || column_name || ' = CASE ChangeType WHEN ''Type 1'' THEN SRC.' || column_name || ' ELSE DST.' || column_name || ' END' || ' , '))
			FROM base;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next(); 
MergeUpdateSet = cmd_res.getColumnValue(1); 


// Merge changes table
sql = `CREATE OR REPLACE TEMPORARY TABLE MergeChanges AS
        SELECT CAST('' AS NVARCHAR(10)) AS merge_action_change_row_type
             , CAST('' AS NVARCHAR(10)) AS merge_action_change_dim_type
             , *
          FROM ` + src_table_full + `
         WHERE 1=2;`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();


// Merge statement 
sql = `// determine type 2 changes (intra-day changes are treated as Type 1)
	CREATE OR REPLACE TEMPORARY TABLE staging AS
    WITH type2 AS
            (
            SELECT ` + BusKeySelect  + `
                 , CASE WHEN CAST(r.` + row_update_date + ` AS DATE) = CAST(GETDATE() AS DATE)
                        THEN 'type1'
                        ELSE 'type2'
                   END AS Change
              FROM ` + src_table_full + ` s
              JOIN ` + tgt_table_full + ` r
                ON ` + BusKeyJoin + `
             WHERE r.` + row_is_current + ` = 'Y'
               AND (` + Type2ChangeCheck + `)
            )
// determine Type 1 changes
      , type1 AS
          (
          SELECT ` + BusKeySelect + `
               , 'type1' AS Change
            FROM ` + src_table_full + ` s
            JOIN ` + tgt_table_full + ` r
              ON ` + BusKeyJoin + `
           WHERE r.` + row_is_current + ` = 'Y'
             AND (` + Type1ChangeCheck + `)
          )
// determine matches with no changes (all rows not caught above will be treated as type 0 - ignored in merge statement below)
		, nochg AS
		(
		SELECT ` + BusKeySelect + `
   			 , 'nochg' AS Change
		  FROM ` + src_table_full + ` s
		  JOIN ` + tgt_table_full + ` r
		    ON ` + BusKeyJoin + `
		 WHERE r.` + row_is_current + ` = 'Y'
		)
// combine all the above CTEs and add no-matches from source
		SELECT ` + StagingColumns + `
			 , CASE WHEN t2.Change = 'type2' THEN 'Type 2'
					WHEN t2.Change = 'type1' THEN 'Type 1'
					WHEN t1.Change = 'type1' THEN 'Type 1'
					WHEN nc.Change = 'nochg' THEN 'No Change'
					ELSE 'Insert'
			   END AS ChangeType
		  FROM ` + src_table_full + ` s
		  LEFT JOIN ` + tgt_table_full + ` r
		    ON ` + BusKeyJoin + `
		  LEFT JOIN type1 t1
		    ON ` + BusKeyJoin_t1 + `
		  LEFT JOIN type2 t2
		    ON ` + BusKeyJoin_t2 + `
		  LEFT JOIN nochg nc
		    ON ` + BusKeyJoin_nc + `
	     WHERE IFNULL(r.` + row_is_current + `, 'Y') = 'Y';
`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();


// Merge statement
sql = `  MERGE INTO ` + tgt_table_full + ` AS DST
		 USING ( SELECT * FROM staging ) AS SRC
		    ON ` + BusKeyJoin_src + `
		   AND SRC.ChangeType != 'No Change'
		WHEN MATCHED AND DST.` + row_is_current + ` = 'Y' THEN
// Update dimension record, and flag as no longer active if type 2
		UPDATE SET ` + MergeUpdateSet + `
		           ` + row_is_current + ` = CASE ChangeType WHEN 'Type 1' THEN 'Y' ELSE 'N' END
				 , ` + row_expiration_date + ` = CASE ChangeType WHEN 'Type 1' THEN DST.` + row_expiration_date + ` ELSE CAST(DATEADD('day',-1,GETDATE()) AS DATE) END
		         , ` + row_update_date + ` = GETDATE()
// New records inserted (not used by type 2)
		  WHEN NOT MATCHED AND SRC.ChangeType != 'No Change' THEN 
			INSERT ( 
			` + MergeInsertList + `
				 , ` + row_is_current + `
				 , ` + row_effective_date + `
				 , ` + row_expiration_date + `
				 , ` + row_insert_date + `
				 , ` + row_update_date + `
				 )
			VALUES (
			` + MergeInsertList + `
				 , 'Y'
			     , CAST('01/01/1900' AS DATE) 
			     , CAST('12/31/9999' AS DATE) 
				 , CAST(GETDATE() AS DATE)
				 , CAST(GETDATE() AS DATE)
				 );
`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
MergeRowsInserted = cmd_res.getColumnValue(1);
MergeRowsUpdated = cmd_res.getColumnValue(2);
	

// outer merge insert (used only for type 2 new rows)
sql = `INSERT INTO ` + tgt_table_full + ` ( ` + MergeInsertList + `
				 , ` + row_is_current + `
				 , ` + row_effective_date + `
				 , ` + row_expiration_date + `
				 , ` + row_insert_date + `
			     , ` + row_update_date + `
			 )
		SELECT ` + MergeInsertList + `
			 , 'Y'
			 , CAST(GETDATE() AS date)
			 , CAST('12/31/9999' AS date)
			 , GETDATE()
			 , GETDATE()
		  FROM staging MRG
		 WHERE MRG.ChangeType = 'Type 2';
`
cmd_res = snowflake.execute({sqlText: sql});
cmd_res.next();
RowsInserted = cmd_res.getColumnValue(1); 

}
catch (err)
{
return err.message
}
return MergeRowsInserted + MergeRowsUpdated + RowsInserted

 $$;
 