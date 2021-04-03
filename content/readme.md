# notebookSnap Basics

These Notebooks contain Code and Instructions for _Creating Projects_ in the eltsnap_v2 database.

Key Concepts:

- All of the **Code** to be _transformed_ into eltSnap **Projects** are contained Notebook (.ipydb) and SQL (.sql) files
- Additional Notebooks are provided to assist in the _Setup_ as well as a potential _Refresh_ of these Projects
- Notebook (and .sql) files must first be _Ingested_ into the eltsnap_v2 database. One option for this is to use the Notebook: **Ingest ADS Notebook Files (Interactive)** contained in this folder
- Notebook Code cells contain _metadata_ (pre-populated for these Notebooks) which provide additional configurations needed by the eltSnap runtime
- If you have cloned this repository, you may see subsequent updates to these notebook files via GitHub. In that event, you may want to:
    - Save any custom changes you have made to separate folders
    - Sync your local folders with GitHub
    - **re**-ingest the Notebook based Projects into eltSnap

Notebook Types:

- ...(Project) - Contains the Notebook _Code_ which is deployed as runnable _Packages_ in eltSnap
- ...(Project Setup) - Contains setup instructions and code for _deploying_ notebook (and .sql) file based Projects to eltSnap
- ...(Interactive) - A self-contained solution which can be configured and run in Azure Data Studio, without any additional set-up or deployment 
- ...(Cmdlets) - One line powershell commands to providing re-usable finctionality (i.e., copying a File or Folder)

> The notebook **Ingest ADS Notebook Files** can be run _Interactively_ (directly in Azure Data Studio) and the project does **not** first need to be deployed to eltSnap. The related "Project" and "Project Setup" notebooks for _Ingest ADS Notebook Files_ are included below as an **eltSnap** deployment _option_

## Notebooks:

[Ingest ADS Notebook Files (Interactive)](ingest_ads_notebook_files_interactive.ipynb) - PowerShell Notebook to Ingest '.ipynb' Files into the **eltSnap** database **interactively**

[Ingest ADS Notebook Files (Project)](ingest_ads_notebook_files_project.ipynb) - PowerShell Notebook Project to Ingest '.ipynb' Files into the **eltSnap** database

[Ingest ADS Notebook Files (Project Setup)](ingest_ads_notebook_files_project_setup.ipynb) - Initial Setup scripts for **Ingest ADS Notebook Files (Project)**

[Database Log Clean-up (Project)](database_log_cleanup_project.ipynb) - SQL Notebook Project to Delete eltSnap related Log rows which are _older_ than thresholds set in configuration tables. **Note**: This is _pre_-installed into the eltsnap_v2 database, so **no setup or deployment** is needed to be able to run this project in eltSnap

[Database Log Clean-up (Project Setup)](database_log_cleanup_project_setup.ipynb) - Initial Setup scripts for **Database Log Clean-up (Project)**

[Metadata Refresh for SQL Server (Project)](metadata_refresh_for_sql_server_project.ipynb) - Refreshes Metadata collected from a SQL Server database and stored into an eltSnap database

[Metadata Refresh for SQL Server (Project Setup)](metadata_refresh_for_sql_server_project_setup.ipynb) - Initial Setup scripts for **Metadata Refresh for SQL Server (Project)**

[Metadata Refresh for Snowflake (Project)](metadata_refresh_for_snowflake_project.ipynb) - Refreshes Metadata collected from a Snowflake database and stored into an eltSnap database

[Metadata Refresh for Snowflake (Project Setup)](metadata_refresh_for_snowflake_project_setup.ipynb) - Initial Setup scripts for **Metadata Refresh for Snowflake (Project)**

[SQL Server Backup (Project)](sql_server_backup_project.ipynb) - Refreshes Metadata collected from a SQL Server database and stored into an eltSnap database

[SQL Server Backup (Project Setup)](sql_server_backup_project_setup.ipynb) - Initial Setup scripts for **Metadata Refresh for SQL Server (Project)**

[PowerShell Utilities (Project)](powershell_utilities.ipynb) - Contains miscellaneous Cmdlets (a one-line script) which can be incorporated into other notebookSnap project

## SQL Files:

[Refresh Database Names (snowflake)](refresh_database_names_snowflake.sql) - Used for Snowflake Metadata Refresh

[Refresh Table Names (snowflake)](src_to_stg_snowflake_tables.sql) - Used for Snowflake Metadata Refresh

[Refresh Column Names (snowflake)](src_to_stg_snowflake_columns.sql) - Used for Snowflake Metadata Refresh

