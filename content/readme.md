# notebookSnap Basics

The Notebooks included in this Solution contain Code and Instructions for _Creating Projects_ in the eltsnap_v2 database.

> Note: These projects require the PowerShell cmdlet for SqlServer to be installed: https://www.powershellgallery.com/packages/Sqlserver/21.1.18256 

Key Concepts:

- All of the **Code** which will be be _transformed_ into eltSnap **Projects** are contained in Notebook (.ipydb) and SQL (.sql) files
- Additional Notebooks are provided to assist in the _Setup_ as well as a later _Updates_ to these Projects
- Notebook (and .sql) files must first be _Ingested_ into the eltsnap_v2 database. One option for this is to use the Notebook: [Ingest ADS Notebook Files (Interactive)](ingest_ads_notebook_files_interactive.ipynb) contained in this folder
- Notebook Code cells contain _metadata_ (pre-populated in the following Notebooks) which provide additional configurations needed by the _eltSnap_ runtime
- If you have cloned this repository, you may see subsequent updates to these notebook files via GitHub. In that event, you may want to:
    - Save any custom changes you have made to separate folders
    - Sync your local folders with GitHub
    - **re**-ingest the Notebook based Projects into eltSnap (after re-applying your custom changes)

Notebook Types:

- ...(Project) - Contains the Notebook _Code_ which is deployed as runnable _Packages_ in eltSnap
- ...(Project Setup) - Contains setup instructions and code for _deploying_ notebook (and .sql) file based Projects to eltSnap
- ...(Interactive) - A self-contained solution which can be configured and run in Azure Data Studio, without any additional set-up or deployment 
- ...(Cmdlets) - One line powershell commands to providing re-usable functionality (i.e., copying a File or Folder)

> The notebook **Ingest ADS Notebook Files** can be run _Interactively_ (directly in Azure Data Studio) and the project does **not** first need to be deployed to eltSnap. The related "Project" and "Project Setup" notebooks for _Ingest ADS Notebook Files_ are included below as an **eltSnap** deployment _option_

## Notebooks:

[Ingest ADS Notebook Files (Interactive)](ingest_ads_notebook_files_interactive.ipynb) - PowerShell Notebook to Ingest '.ipynb' Files into the **eltSnap** database

[Database Log Clean-up (Project)](database_log_cleanup_project.ipynb) - SQL Notebook Project to Delete eltSnap related Log rows which are _older_ than thresholds set in configuration tables. **Note**: This is _pre_-installed into the eltsnap_v2 database, so **no setup or deployment** is needed to be able to run this project in eltSnap

[Database Log Clean-up (Project Setup)](database_log_cleanup_project_setup.ipynb) - Initial Setup scripts for **Database Log Clean-up (Project)**

[Metadata Refresh for SQL Server (Project)](metadata_refresh_for_sql_server_project.ipynb) - Refreshes Metadata collected from a SQL Server database and stored into an eltSnap database

[Metadata Refresh for SQL Server (Project Setup)](metadata_refresh_for_sql_server_project_setup.ipynb) - Initial Setup scripts for **Metadata Refresh for SQL Server (Project)**

[SQL Server Backup (Project)](sql_server_backup_project.ipynb) - Refreshes Metadata collected from a SQL Server database and stored into an eltSnap database

[SQL Server Backup (Project Setup)](sql_server_backup_project_setup.ipynb) - Initial Setup scripts for **Metadata Refresh for SQL Server (Project)**

[PowerShell Utilities (Project)](powershell_utilities.ipynb) - Contains miscellaneous Cmdlets (a one-line script) which can be incorporated into other notebookSnap project
