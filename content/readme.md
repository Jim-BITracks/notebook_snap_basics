# Notebook_Snap Basics

These Notebooks contain Code and Instructions for _Creating Projects_ eltSnap.

Key Concepts:

- All of the **Code** which will be _transformed_ into eltSnap **Projects** is contain in these Notebooks
- Additional Notebooks are provided to assist in the _Setup_ or _Refresh_ of these Projects
- Notebooks must first be _Ingested_ into the eltsnap_v2 database using the Notebook: **Ingest ADS Notebook Files (Interactive)**
- Notebook Code cells contain _metadata_ (pre-populated for these Notebooks) which provides additional configuration for the eltSnap runtime
- For subsequent updates, you will want to _maintain your code_ in these Notebooks, and then **re**-ingest into eltSnap


## Notebooks:

[Ingest ADS Notebook Files (Interactive)](ingest_ads_notebook_files_interactive.ipynb) - PowerShell Notebook to Ingest '.ipynb' Files into the **eltSnap** database **interactively**

[Ingest ADS Notebook Files (Project)](ingest_ads_notebook_files_project.ipynb) - PowerShell Notebook Project to Ingest '.ipynb' Files into the **eltSnap** database

[Ingest ADS Notebook Files (Project Setup)](ingest_ads_notebook_files_project_setup.ipynb) - Initial Setup scripts for **Ingest ADS Notebook Files (Project)**

[Database Log Clean-up (Project)](database_log_cleanup_project.ipynb) - SQL Notebook Project to Delete eltSnap related Log rows which are _older_ than thresholds set in configuration tables. **Note**: This is _pre_-installed into the eltsnap_v2 database, so **no setup or deployment** is needed to be able to run this project in eltSnap

[Metadata Refresh for SQL Server (Project)](metadata_refresh_for_sql_server_project.ipynb) - Refreshes Metadata collected from a SQL Server database and stored into an eltSnap database
