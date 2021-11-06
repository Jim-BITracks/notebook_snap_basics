# %% [markdown]
# # Snowflake Metadata Refresh Setup

# %% [markdown]
# ## Import Python Libraries

# %%
import snowflake.connector
import os
import collections
import pandas as pd
import numpy as np
import pandas as pd
import pyarrow as pa
# pd.set_option('max_columns', 40)

# %% [markdown]
# ## Set Snowflake Variables

# %%
# snowflake connection variables
snowflake_user = 'JMILLER'
snowflake_password = os.environ['BISNOWPASS']
snowflake_account = 'eh69371.east-us-2.azure'
snowflake_role = 'SYSADMIN'
snowflake_warehouse = 'COMPUTE_WH'
snowflake_database = 'UTIL_DB'
snowflake_schema = 'METADATA'

print('Using Notebook Variables:')
print('snowflake_user: ' + snowflake_user)
print('snowflake_password: ' + '***************')
print('snowflake_account: ' + snowflake_account)
print('snowflake_role: ' + snowflake_role)
print('snowflake_warehouse: ' + snowflake_warehouse)
print('snowflake_database: ' + snowflake_database)
print('snowflake_schema: ' + snowflake_schema)

# %% [markdown]
# ## Set Notebook Variables

# %%
tgt_table_schema = 'METADATA'
tgt_table_name = 'D_DATABASES'
               
print('Using Notebook Variables:')
print('tgt_table_schema: ' + tgt_table_schema)
print('tgt_table_name: ' + tgt_table_name)

# %% [markdown]
# ## Establish Snowflake Connection

# %%
ctx = snowflake.connector.connect(
    user = snowflake_user,
    password = snowflake_password,
    account = snowflake_account,
    role = snowflake_role,
    warehouse = snowflake_warehouse,
    database = snowflake_database,
    schema = snowflake_schema
    )
cur = ctx.cursor()

# Return Client
cur.execute("select CURRENT_CLIENT()")
one_row = cur.fetchone()
print('Snowflake Connection Successful')
print(one_row[0])

# %% [markdown]
# ## Get Table Mappings

# %%
sql = """
select SRC_TABLE_SCHEMA, SRC_TABLE_NAME, NATURAL_KEY, TYPE_2_COLUMNS, TYPE_0_COLUMNS, MAP_ADDED_COLUMNS_TAG
  from METADATA.MAP_TABLE
 where TGT_TABLE_SCHEMA = '""" + tgt_table_schema + """'
   and TGT_TABLE_NAME = '""" + tgt_table_name + """'
"""
print(sql)
cur.execute(sql)
one_row = cur.fetchone()
src_table_schema = one_row[0]
src_table_name = one_row[1]
natural_key = one_row[2]
type_2_columns = one_row[3]
type_0_columns = one_row[4] or ""
map_added_columns_tag = one_row[5]

src_table_full = '"' + src_table_schema + '"."' + src_table_name + '"'
tgt_table_full = '"' + tgt_table_schema + '"."' + tgt_table_name + '"'

print('return values: ', src_table_schema, src_table_name, natural_key, type_2_columns, type_0_columns, map_added_columns_tag)
print('source table: ', src_table_full, '\ntarget table: ', tgt_table_full)

# %% [markdown]
# ## Get Type 1 Columns

# %%
sql = """
with exclude_columns as
(
      select VALUE as COLUMN_NAME from table(split_to_table('""" + natural_key + """', ','))  
union select VALUE as COLUMN_NAME from table(split_to_table('""" + type_2_columns + """', ',')) 
)
, all_columns as
(
select COLUMN_NAME
  from INFORMATION_SCHEMA.COLUMNS i
 where i.TABLE_SCHEMA = '""" + src_table_schema + """'
   and i.TABLE_NAME = '""" + src_table_name + """'
)  
, type_1_columns as
(
       select COLUMN_NAME from all_columns
except
       select COLUMN_NAME from exclude_columns
)
select listagg(COLUMN_NAME, ',')
  from type_1_columns;
"""
print(sql)
cur.execute(sql)
one_row = cur.fetchone()
type_1_columns = one_row[0]
print('type_1_columns: ' + type_1_columns)

# %% [markdown]
# ## Get Names for Supplemental Dimension Columns

# %%
sql = """
with base as
(
select "'row_is_current'" AS row_is_current
     , "'row_effective_date'" AS row_effective_date
     , "'row_expiration_date'" AS row_expiration_date
     , "'row_insert_date'" AS row_insert_date
     , "'row_update_date'" AS row_update_date
  from METADATA.MAP_ADDED_COLUMNS
  pivot(max(TGT_COLUMN_NAME) for LOGICAL_COLUMN_NAME in ('row_is_current', 'row_effective_date', 'row_expiration_date', 'row_insert_date', 'row_update_date'))
 where MAP_ADDED_COLUMNS_TAG = '""" + map_added_columns_tag + """'
 )
 select max(row_is_current) AS row_is_current
      , max(row_effective_date) AS row_effective_date
      , max(row_expiration_date) AS row_expiration_date
      , max(row_insert_date) AS row_insert_date
      , max(row_update_date) AS row_update_date
   from base;
"""

print(sql)
cur.execute(sql)
one_row = cur.fetchone()
row_is_current = one_row[0]
row_effective_date = one_row[1]
row_expiration_date = one_row[2]
row_insert_date = one_row[3]
row_update_date = one_row[4]

print('row_is_current: ', row_is_current, '\nrow_effective_date: ', row_effective_date, '\nrow_expiration_date: ', row_expiration_date, '\nrow_insert_date: ', row_insert_date, '\nrow_update_date: ', row_update_date)

# %% [markdown]
# ## Get Natural Key (Select)

# %%
sql = """
select listagg('s.' || i.COLUMN_NAME , ', ') within group (order by i.ORDINAL_POSITION) 
  from INFORMATION_SCHEMA.COLUMNS i
  join table(split_to_table('""" + natural_key + """', ',')) as t
    on t.VALUE = i.COLUMN_NAME
 where i.TABLE_SCHEMA = '""" + src_table_schema + """'
   and i.TABLE_NAME = '""" + src_table_name + """';
"""
print(sql)
cur.execute(sql)
one_row = cur.fetchone()
natural_key_select = one_row[0]
print('natural_key_select: ' + natural_key_select)

# %% [markdown]
# ## Get Natural Key (Join)

# %%
#natural_key = 'IS_TRANSIENT,RETENTION_TIME'
sql = """
select listagg('t."' || i.COLUMN_NAME || '" = s."' || i.COLUMN_NAME || '"', ' AND ') within group (order by i.ORDINAL_POSITION) 
  from INFORMATION_SCHEMA.COLUMNS i
  join table(split_to_table('""" + natural_key + """', ',')) as t
    on t.VALUE = i.COLUMN_NAME
 where i.TABLE_SCHEMA = '""" + src_table_schema + """'
   and i.TABLE_NAME = '""" + src_table_name + """';
"""
print(sql)
cur.execute(sql)
one_row = cur.fetchone()
natural_key_join = one_row[0]
print('natural_key_join: ' + natural_key_join)

# %% [markdown]
# ## Join Variations
# 

# %%
natural_key_join_t1 = natural_key_join.replace("t.", "t1.")
natural_key_join_t2 = natural_key_join.replace("t.", "t2.")
natural_key_join_nc = natural_key_join.replace("t.", "nc.")
natural_key_join_src = natural_key_join.replace("t.", "src.")
natural_key_join_src = natural_key_join_src.replace("s.", "tgt.")

print('natural_key_join_t1: ', natural_key_join_t1, '\nnatural_key_join_t2: ', natural_key_join_t2, '\nnatural_key_join_nc: ', natural_key_join_nc,  '\nnatural_key_join_src: ', natural_key_join_src)

# %% [markdown]
# ## Type 1 Change Check

# %%
#natural_key = 'IS_TRANSIENT,RETENTION_TIME'
sql = """
select listagg('NVL(t.' || column_name || '::varchar,''-99999'') != NVL(s.' || column_name || '::varchar,''-99999'')', ' OR ') within group (order by i.ORDINAL_POSITION) 
  from INFORMATION_SCHEMA.COLUMNS i
  join table(split_to_table('""" + type_1_columns + """', ',')) as t
    on t.VALUE = i.COLUMN_NAME
 where i.TABLE_SCHEMA = '""" + src_table_schema + """'
   and i.TABLE_NAME = '""" + src_table_name + """';
"""
print(sql)
cur.execute(sql)
one_row = cur.fetchone()
type1_change_check = one_row[0]
print('type1_change_check: ' + type1_change_check)

# %% [markdown]
# ## Type 2 Change Check

# %%
sql = """
select listagg('NVL(t.' || column_name || '::varchar,''-99999'') != NVL(s.' || column_name || '::varchar,''-99999'')', ' OR ') within group (order by i.ORDINAL_POSITION) 
  from INFORMATION_SCHEMA.COLUMNS i
  join table(split_to_table('""" + type_2_columns + """', ',')) as t
    on t.VALUE = i.COLUMN_NAME
 where i.TABLE_SCHEMA = '""" + src_table_schema + """'
   and i.TABLE_NAME = '""" + src_table_name + """';
"""
print(sql)
cur.execute(sql)
one_row = cur.fetchone()
type2_change_check = one_row[0]
print('type2_change_check: ' + type2_change_check)

# %% [markdown]
# ## Staging Columns

# %%
sql = """
with base as
(
select DISTINCT TOP 1000 c.ORDINAL_POSITION
     , c.COLUMN_NAME
from INFORMATION_SCHEMA.COLUMNS c
where c.TABLE_SCHEMA = '""" + src_table_schema + """'
and c.TABLE_NAME = '""" + src_table_name + """'
ORDER BY c.ORDINAL_POSITION
)
SELECT TRIM(LISTAGG(' s.' || COLUMN_NAME || '' || ', '), ', ')
     , TRIM(LISTAGG(' ' || column_name || '' || ', '), ', ')
     , TRIM(LISTAGG(' SRC.' || column_name || '' || ', '), ', ')
  FROM base;
"""
print(sql)
cur.execute(sql)
one_row = cur.fetchone()
staging_columns = one_row[0]
merge_insert_list = one_row[1]
merge_output_list = one_row[2]

print('staging_columns: ', staging_columns, '\nmerge_insert_list: ', merge_insert_list, '\nmerge_output_list: ', merge_output_list)


# %% [markdown]
# ## Merge Update Set...

# %%
sql = """
WITH base AS
(
select distinct top 1000 c.ORDINAL_POSITION
	   , c.COLUMN_NAME
  from INFORMATION_SCHEMA.COLUMNS c
 where c.TABLE_SCHEMA = '""" + src_table_schema + """'
   and c.TABLE_NAME = '""" + src_table_name + """'
   and c.COLUMN_NAME NOT IN ('""" + type_0_columns + """', ',')
   and c.COLUMN_NAME NOT IN ('""" + natural_key + """', ',')
 order by c.ORDINAL_POSITION
)
SELECT TRIM(LISTAGG(' ' || column_name || ' = CASE ChangeType WHEN ''Type 1'' THEN SRC.' || column_name || ' ELSE TGT.' || column_name || ' END' || ' , '))
FROM base;
"""
print(sql)
cur.execute(sql)
one_row = cur.fetchone()
merge_update_set = one_row[0]
print('merge_update_set: ' + merge_update_set)

# %% [markdown]
# ## Create Merge changes table

# %%
sql = """
CREATE OR REPLACE TEMPORARY TABLE MergeChanges AS
        SELECT CAST('' AS NVARCHAR(10)) AS merge_action_change_row_type
             , CAST('' AS NVARCHAR(10)) AS merge_action_change_dim_type
             , *
          FROM """ + src_table_full + """
         WHERE 1=2;
"""
print(sql)
cur.execute(sql)
one_row = cur.fetchone()
print(one_row[0])

# %% [markdown]
# ## Create Staging Table

# %%
sql = """
// determine type 2 changes (intra-day changes are treated as Type 1)
	CREATE OR REPLACE TEMPORARY TABLE staging AS
    WITH type2 AS
            (
            SELECT """ + natural_key_select + """
                 , CASE WHEN CAST(t.""" + row_update_date + """ AS DATE) = CAST(GETDATE() AS DATE)
                        THEN 'type1'
                        ELSE 'type2'
                   END AS Change
              FROM """ + src_table_full + """ s
              JOIN """ + tgt_table_full + """ t
                ON """ + natural_key_join + """
             WHERE t.""" + row_is_current + """ = 'Y'
               AND (""" + type2_change_check + """)
            )
// determine Type 1 changes
      , type1 AS
          (
          SELECT """ + natural_key_select + """
               , 'type1' AS Change
            FROM """ + src_table_full + """ s
            JOIN """ + tgt_table_full + """ t
              ON """ + natural_key_join + """
           WHERE t.""" + row_is_current + """ = 'Y'
             AND (""" + type1_change_check + """)
          )
// determine matches with no changes (all rows not caught above will be treated as type 0 - ignored in merge statement below)
		, nochg AS
		(
		SELECT """ + natural_key_select + """
   			 , 'nochg' AS Change
			FROM """ + src_table_full + """ s
            JOIN """ + tgt_table_full + """ t
              ON """ + natural_key_join + """
           WHERE t.""" + row_is_current + """ = 'Y'
		)
// combine all the above CTEs and add no-matches from source
		SELECT """ + staging_columns + """
			 , CASE WHEN t2.Change = 'type2' THEN 'Type 2'
					WHEN t2.Change = 'type1' THEN 'Type 1'
					WHEN t1.Change = 'type1' THEN 'Type 1'
					WHEN nc.Change = 'nochg' THEN 'No Change'
					ELSE 'Insert'
			   END AS ChangeType
		  FROM """ + src_table_full + """ s
		  LEFT JOIN """ + tgt_table_full + """ t
		    ON """ + natural_key_join + """
		  LEFT JOIN type1 t1
		    ON """ + natural_key_join_t1 + """
		  LEFT JOIN type2 t2
		    ON """ + natural_key_join_t2 + """
		  LEFT JOIN nochg nc
		    ON """ + natural_key_join_nc + """
	     WHERE NVL(t.""" + row_is_current + """, 'Y') = 'Y';
"""
print(sql)
cur.execute(sql)
one_row = cur.fetchone()
print(one_row[0])

# %% [markdown]
# ## Merge Statement

# %%
sql = """
MERGE INTO """ + tgt_table_full + """ AS TGT
USING ( SELECT * FROM staging ) AS SRC
   ON """ + natural_key_join_src + """
  AND SRC.ChangeType != 'No Change'
 WHEN MATCHED AND TGT.""" + row_is_current + """ = 'Y' THEN
// Update dimension record, and flag as no longer active if type 2
UPDATE SET """ + merge_update_set + """
		   """ + row_is_current + """ = CASE ChangeType WHEN 'Type 1' THEN 'Y' ELSE 'N' END
		 , """ + row_expiration_date + """ = CASE ChangeType WHEN 'Type 1' THEN TGT.""" + row_expiration_date + """ ELSE CAST(DATEADD('day',-1,GETDATE()) AS DATE) END
		 , """ + row_update_date + """ = GETDATE()
// New records inserted (not used by type 2)
WHEN NOT MATCHED AND SRC.ChangeType != 'No Change' THEN 
INSERT ( 
""" + merge_insert_list + """
		, """ + row_is_current + """
		, """ + row_effective_date + """
		, """ + row_expiration_date + """
		, """ + row_insert_date + """
		, """ + row_update_date + """
		)
VALUES (
""" + merge_insert_list + """
		, 'Y'
		, CAST('01/01/1900' AS DATE) 
		, CAST('12/31/9999' AS DATE) 
		, CAST(GETDATE() AS DATE)
		, CAST(GETDATE() AS DATE)
		);
"""
print(sql)
cur.execute(sql)
one_row = cur.fetchone()
print('rows inserted: ', one_row[0])
print('rows updated: ', one_row[1])


# %% [markdown]
# ## Debug (staging table)

# %%
#sql = "select * from staging"
#cur.execute(sql)
#df = cur.fetch_pandas_all()
#df

# %% [markdown]
# ## Outer Merge Insert (used only for type 2 new rows)

# %%
sql = """
INSERT INTO """ + tgt_table_full + """ ( """ + merge_insert_list + """
    , """ + row_is_current + """
    , """ + row_effective_date + """
    , """ + row_expiration_date + """
    , """ + row_insert_date + """
    , """ + row_update_date + """
)
SELECT """ + merge_insert_list + """
, 'Y'
, CAST(GETDATE() AS date)
, CAST('12/31/9999' AS date)
, GETDATE()
, GETDATE()
FROM staging MRG
WHERE MRG.ChangeType = 'Type 2';
"""
print(sql)
cur.execute(sql)
one_row = cur.fetchone()
print('rows inserted: ', one_row[0])


