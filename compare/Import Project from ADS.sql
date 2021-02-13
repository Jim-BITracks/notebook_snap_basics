USE [eltsnap_v2]
GO
/****** Object:  StoredProcedure [ads].[Import Project from ADS Notebooks]    Script Date: 2/13/2021 1:39:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ========================================================================================================================================================================
-- Author:      Jim Miller - BI Tracks Consulting, LLC
-- Create date: 30 Dec 2020
-- Modify date: 07 Jan 2021 - No longer converting new syntax /*${Metadata_Server_Name}*/'dev'/**/ to the legacy syntax: '@[$Project::Azure_Metadata_Server_Name]'
-- Modify date: 17 Jan 2021 - Auto-creating Project if needed
-- Modify date: 19 Jan 2021 - Added helper text for creating a missing connection
-- Modify date: 22 Jan 2021 - Updated Execute Process Logic
-- Modify date: 24 Jan 2021 - Added Single Notebook Import without entries in: [ads].[project_cells]
-- Modify date: 06 Feb 2021 - Added 'config package' columns: [package_source_control]

-- Description:     Create or Update eltSnap project from ADS Notebook(s)
/*
EXEC [ads].[Import Project from ADS Notebooks] 'Database Log Clean-up', 'notebook_snap_basics\content\database_log_cleanup_project.ipynb';
EXEC [ads].[Import Project from ADS Notebooks] 'Ingest ADS Notebook Files', 'notebook_snap_basics\content\ingest_ads_notebook_files_project.ipynb';
EXEC [ads].[Import Project from ADS Notebooks] 'Metadata Refresh v2' 'notebook_snap_basics\content\metadata_refresh_for_sql_server_project.ipynb';
*/
-- ========================================================================================================================================================================
ALTER PROCEDURE [ads].[Import Project from ADS Notebooks]
      @project_name NVARCHAR(128)
    , @file_name NVARCHAR(128) = null
AS

SET NOCOUNT ON;

SET @file_name = TRIM(ISNULL(@file_name, ''))

DECLARE @project_id INT
	  , @template NVARCHAR(64)
	  , @node AS NVARCHAR(MAX)
	  , @key AS NVARCHAR(4000)
	  , @Counter INT
	  , @notebook_node NVARCHAR(64);

DECLARE @computer_name NVARCHAR(128)
	  , @full_name NVARCHAR(256)
	  , @cell_name NVARCHAR(64)
	  , @sequence_number INT
	  , @parallel_executions TINYINT
	  , @pattern NVARCHAR(64)
	  , @source_node NVARCHAR(64)
	  , @src_connection_name NVARCHAR(64)
	  , @dst_connection_name NVARCHAR(64)
	  , @dst_schema NVARCHAR(128)
	  , @dst_table NVARCHAR(128)
	  , @truncate_dst BIT
	  , @identity_insert BIT
	  , @batch_size INT
	  , @source NVARCHAR(MAX)
	  , @source_out NVARCHAR(MAX)
	  , @executable NVARCHAR(1024)
	  , @working_directory NVARCHAR(128)
	  , @working_directory_param_name NVARCHAR(128)
	  , @package_name NVARCHAR(128)
	  , @package_source_control NVARCHAR(MAX);

DROP TABLE IF EXISTS #project_config;
DROP TABLE IF EXISTS #json;
DROP TABLE IF EXISTS #json_parsed;

SELECT @project_id = [project_id]
  FROM [elt].[project]
 WHERE [project_name] = @project_name;

IF ISNULL(@project_id, 0) < 1
	BEGIN
		EXEC [elt].[Save Project] @project_name
		PRINT 'created project: ' + @project_name

		SELECT @project_id = [project_id]
		  FROM [elt].[project]
		 WHERE [project_name] = @project_name;

		IF ISNULL(@project_id, 0) < 1
			BEGIN
				PRINT 'ERROR: could not create project: ' + @project_name
				RETURN
			END
	END


-- clear existing project definition
DELETE [elt].[project_package] WHERE [project_id] = @project_id

DECLARE @project_cells TABLE(
	[full_name] [nvarchar](128) NOT NULL,
	[cell_name] [nvarchar](128) NOT NULL,
	[sequence_number] [int] NOT NULL,
	[parallel_executions] [tinyint] NULL);


/* populate @project_cells Table */
IF @file_name != ''
	BEGIN
		WITH [base] AS
		(
		SELECT [node]
			 , [value]
			 , REPLACE([node], '$.cells.','') AS [cell_number]
		  FROM [ads].[notebooks]
		 WHERE [node] LIKE '$.cells.%.metadata.notebooksnap'
		   AND [key] = 'cell_name'
		   AND [full_name] = @file_name
		   AND [type] = 1
		)
		INSERT @project_cells
		SELECT @file_name AS [full_name]
			 , [value] AS [cell_name]
			 , SUBSTRING ([cell_number], 1, CHARINDEX('.', [cell_number])-1 ) AS [sequence_number]
			 , 0 AS [parallel_executions]
		  FROM [base]
	END
ELSE
	BEGIN
		INSERT @project_cells
		SELECT [full_name]
			 , [cell_name]
			 , [sequence_number]
			 , [parallel_executions]
		  FROM [ads].[project_cells]
		 WHERE [project_name] = @project_name
		 ORDER BY [sequence_number]
	END


DECLARE cell_cursor CURSOR FOR
SELECT [full_name]
     , [cell_name]
     , [sequence_number]
     , [parallel_executions]
  FROM @project_cells
 ORDER BY [sequence_number]

OPEN cell_cursor
FETCH NEXT FROM cell_cursor INTO @full_name, @cell_name, @sequence_number, @parallel_executions

WHILE @@FETCH_STATUS = 0
BEGIN

 	SELECT @notebook_node = [node]
      FROM [ads].[notebooks]
	 WHERE [node] LIKE '$.cells.%.metadata.notebooksnap'
	   AND [key] = 'cell_name'
	   AND [value] = @cell_name
	   AND [full_name] = @full_name

 	SELECT @pattern = [value]
      FROM [ads].[notebooks]
	 WHERE [node] = @notebook_node
	   AND [key] = 'pattern'
	   AND [full_name] = @full_name

	SET @source_node = REPLACE(@notebook_node,'.metadata.notebooksnap','')

-----------------------------------------------------------------------------------------------------------------
-- clear package variables
-----------------------------------------------------------------------------------------------------------------

	SET @src_connection_name = null;
	SET @dst_connection_name = null;
	SET @dst_schema = null;
	SET @dst_table = null;
	SET @truncate_dst = null;
	SET @identity_insert = null;
	SET @batch_size = null;
	SET @source = null
	SET @source_out = null
	SET @executable = null
	SET @working_directory = null
	SET @working_directory_param_name = null

-----------------------------------------------------------------------------------------------------------------
-- logic for Execute SQL pattern
-----------------------------------------------------------------------------------------------------------------

	IF @pattern = 'Execute SQL'
	BEGIN
		SELECT @src_connection_name = [value]
		  FROM [ads].[notebooks]
		 WHERE [node] = @notebook_node
		   AND [key] = 'connection'
		   AND [full_name] = @full_name

		SELECT @source = [value]
		  FROM [ads].[notebooks]
		 WHERE [node] = @source_node
		   AND [key] = 'source'
		   AND [full_name] = @full_name

		SET @source = [ads].[Convert ADS Json Source](@source);
		
		EXEC [ads].[Manage ADS SQL Query Parameters] @project_id, @source --, @source_out = @source_out OUTPUT; SET @source = @source_out;

--print @source

		IF NOT EXISTS (SELECT 1 FROM [elt].[oledb_connection] WHERE [connection_name] = @src_connection_name)
		BEGIN
			PRINT 'Error: Connection name: ''' + @src_connection_name + ''' does not exist'
		    PRINT 'You can create a new connection with the following command...'
			PRINT ' '
			PRINT 'EXEC [elt].[Save Connection by Name] ''connection_name'', ''server_name'', ''database_name'', ''provider_name'''
			PRINT ' '
			PRINT 'Syntax helper - the ''connection_name'' and ''database_name'' are often the same value'
			PRINT '              - the ''provider_name'' for SQL Server is: ''SQL Server Native Client 11.0'''
			PRINT '              - you can override these initial values using ''parameters'' and ''environments'''
			CLOSE cell_cursor
			DEALLOCATE cell_cursor
			RETURN
		END

--print @project_id
--print @src_connection_name

		IF NOT EXISTS (SELECT 1 FROM [elt].[project_oledb_connection] WHERE [project_id] = @project_id AND [connection_name] = @src_connection_name)
			INSERT [elt].[project_oledb_connection] VALUES ( @project_id, @src_connection_name)

		DELETE [elt].[package_config_execute_sql] WHERE [package_qualifier] = @cell_name;

		SET @package_source_control = 
		(
			SELECT @full_name AS [full_name]
				 , @cell_name AS [cell_name]
			 FOR JSON PATH, WITHOUT_ARRAY_WRAPPER 
		 )

		INSERT [elt].[package_config_execute_sql]
		VALUES ( @src_connection_name
			   , @cell_name
			   , @source
			   , 0 -- is expression
			   , 1 -- return row count
			   , 'created from ADS notebook'
			   , @package_source_control )

		INSERT [elt].[project_package] VALUES (@project_id, @sequence_number, 'Execute SQL - ' + @cell_name, null)	
	END

-----------------------------------------------------------------------------------------------------------------
-- logic for Dataflow pattern
-----------------------------------------------------------------------------------------------------------------

	IF @pattern = 'Dataflow'
	BEGIN
		SELECT @src_connection_name = [value]
		  FROM [ads].[notebooks]
		 WHERE [node] = @notebook_node
		   AND [key] = 'src_connection'
		   AND [full_name] = @full_name

		SELECT @dst_connection_name = [value]
		  FROM [ads].[notebooks]
		 WHERE [node] = @notebook_node
		   AND [key] = 'dst_connection'
		   AND [full_name] = @full_name

		SELECT @dst_schema = [value]
		  FROM [ads].[notebooks]
		 WHERE [node] = @notebook_node
		   AND [key] = 'dst_schema'
		   AND [full_name] = @full_name

		SELECT @dst_table = [value]
		  FROM [ads].[notebooks]
		 WHERE [node] = @notebook_node
		   AND [key] = 'dst_table'
		   AND [full_name] = @full_name

		SELECT @truncate_dst = CASE WHEN [value] = 'N' THEN 0
									WHEN [value] = 'Y' THEN 1
									ELSE [value] END
		  FROM [ads].[notebooks]
		 WHERE [node] = @notebook_node
		   AND [key] = 'truncate_dst'
		   AND [full_name] = @full_name

		SELECT @identity_insert = CASE WHEN [value] = 'N' THEN 0
									   WHEN [value] = 'Y' THEN 1
									   ELSE [value] END
		  FROM [ads].[notebooks]
		 WHERE [node] = @notebook_node
		   AND [key] = 'identity_insert'
		   AND [full_name] = @full_name

		SELECT @batch_size = [value]
		  FROM [ads].[notebooks]
		 WHERE [node] = @notebook_node
		   AND [key] = 'batch_size'
		   AND [full_name] = @full_name

		SELECT @source = [value]
		  FROM [ads].[notebooks]
		 WHERE [node] = @source_node
		   AND [key] = 'source'
		   AND [full_name] = @full_name

		SET @source = [ads].[Convert ADS Json Source](@source);
		
		EXEC [ads].[Manage ADS SQL Query Parameters] @project_id, @source --, @source_out = @source_out OUTPUT; SET @source = @source_out;

--print @source

		IF NOT EXISTS (SELECT 1 FROM [elt].[oledb_connection] WHERE [connection_name] = @src_connection_name)
		BEGIN
			PRINT 'Error: Connection name: ''' + @src_connection_name + ''' does not exist'
			PRINT 'You can create a new connection with the following command...'
			PRINT ' '
			PRINT 'EXEC [elt].[Save Connection by Name] ''connection_name'', ''server_name'', ''database_name'', ''provider_name'''
			PRINT ' '
			PRINT 'Syntax helper - the ''connection_name'' and ''database_name'' are often the same value'
			PRINT '              - the ''provider_name'' for SQL Server is: ''SQL Server Native Client 11.0'''
			PRINT '              - you can override these initial values using ''parameters'' and ''environments'''
			CLOSE cell_cursor
			DEALLOCATE cell_cursor
			RETURN
		END

		IF NOT EXISTS (SELECT 1 FROM [elt].[project_oledb_connection] WHERE [project_id] = @project_id AND [connection_name] = @src_connection_name)
		BEGIN
			INSERT [elt].[project_oledb_connection] VALUES (@project_id, @src_connection_name)
			INSERT [elt].[project_parameter] VALUES (@project_id, @src_connection_name + '_ConnectString')
			INSERT [elt].[project_parameter] VALUES (@project_id, @src_connection_name + '_Server')
			INSERT [elt].[project_parameter] VALUES (@project_id, @src_connection_name + '_Database')
			INSERT [elt].[project_parameter] VALUES (@project_id, @src_connection_name + '_Provider')
		END

		IF NOT EXISTS (SELECT 1 FROM [elt].[oledb_connection] WHERE [connection_name] = @dst_connection_name)
		BEGIN
			PRINT 'Error: Connection name: ''' + @dst_connection_name + ''' does not exist'
			PRINT 'You can create a new connection with the following command...'
			PRINT ' '
			PRINT 'EXEC [elt].[Save Connection by Name] ''connection_name'', ''server_name'', ''database_name'', ''provider_name'''
			PRINT ' '
			PRINT 'Syntax helper - the ''connection_name'' and ''database_name'' are often the same value'
			PRINT '              - the ''provider_name'' for SQL Server is: ''SQL Server Native Client 11.0'''
			PRINT '              - you can override these initial values using ''parameters'' and ''environments'''
			CLOSE cell_cursor
			DEALLOCATE cell_cursor
			RETURN
		END

		IF NOT EXISTS (SELECT 1 FROM [elt].[project_oledb_connection] WHERE [project_id] = @project_id AND [connection_name] = @dst_connection_name)
		BEGIN
			INSERT [elt].[project_oledb_connection] VALUES (@project_id, @dst_connection_name)
			INSERT [elt].[project_parameter] VALUES (@project_id, @dst_connection_name + '_ConnectString')
			INSERT [elt].[project_parameter] VALUES (@project_id, @dst_connection_name + '_Server')
			INSERT [elt].[project_parameter] VALUES (@project_id, @dst_connection_name + '_Database')
			INSERT [elt].[project_parameter] VALUES (@project_id, @dst_connection_name + '_Provider')
		END

		DELETE [elt].[package_config_data_flow] WHERE [package_qualifier] = @cell_name

		SET @package_source_control = 
		(
			SELECT @full_name AS [full_name]
				 , @cell_name AS [cell_name]
			 FOR JSON PATH, WITHOUT_ARRAY_WRAPPER 
		 )

		INSERT [elt].[package_config_data_flow]
        VALUES ( @src_connection_name
			   , @source
			   , 1 -- is expression
			   , @cell_name
			   , @dst_connection_name
			   , @dst_schema
			   , @dst_table
			   , @truncate_dst
			   , @identity_insert
			   , 1 -- use_bulk_copy
		       , @batch_size
			   , 'created from ADS notebook' 
			   , @package_source_control )

		SELECT @package_name = [package_name]
		  FROM [elt].[package_config_data_flow] 
		 WHERE [package_qualifier] = @cell_name

		INSERT [elt].[project_package] VALUES (@project_id, @sequence_number, @package_name, null)	
	END


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------


	IF @pattern = 'Execute Process'
	BEGIN
		SELECT @executable = [value]
		  FROM [ads].[notebooks]
		 WHERE [node] = @notebook_node
		   AND [key] = 'executable'
		   AND [full_name] = @full_name

		SELECT @working_directory = [value]
		  FROM [ads].[notebooks]
		 WHERE [node] = @notebook_node
		   AND [key] = 'working_directory'
		   AND [full_name] = @full_name

		SET @working_directory_param_name = REPLACE(@working_directory, '${', '')
		SET @working_directory_param_name = REPLACE(@working_directory_param_name, '}', '')

		SET @working_directory = REPLACE(@working_directory, '${', '@[$Project::')
		SET @working_directory = REPLACE(@working_directory, '}', ']')

		IF NOT EXISTS (SELECT 1 FROM [elt].[parameter] WHERE [parameter_name] = @working_directory_param_name)
		BEGIN
			PRINT 'Error: Parameter name: ' + @working_directory_param_name + ' does not exist'
			RETURN
		END

		IF NOT EXISTS (SELECT 1 FROM [elt].[project_parameter] WHERE [project_id] = @project_id AND [parameter_name] = @working_directory_param_name)
		BEGIN
			INSERT [elt].[project_parameter] VALUES (@project_id, @working_directory_param_name)
		END
		   
		SELECT @source = [value]
		  FROM [ads].[notebooks]
		 WHERE [node] = @source_node
		   AND [key] = 'source'
		   AND [full_name] = @full_name

--print @source
		SET @source = [ads].[Convert ADS Json Source](@source);
--print @source
--print '----------------------------------------------------------------------------------------------------------'

		-- temp param syntax substitution

		EXEC [ads].[Manage ADS PWSH Query Parameters] @project_id, @source

		--SET @source = REPLACE(@source, '<#${eltsnap_v2_Server}#>''SRV6''<##>', '@[$Project::eltsnap_v2_Server]')
		--SET @source = REPLACE(@source, '<#${eltsnap_v2_Database}#>''eltsnap_v2''<##>', '@[$Project::eltsnap_v2_Database]')

		SET @source = '-c "& ' + @source + '"'

--print @source

		DELETE [elt].[package_config_execute_process] WHERE [package_qualifier] = @cell_name

		SET @package_source_control = 
		(
			SELECT @full_name AS [full_name]
				 , @cell_name AS [cell_name]
			 FOR JSON PATH, WITHOUT_ARRAY_WRAPPER 
		 )

		INSERT [elt].[package_config_execute_process]
		VALUES ( @cell_name
			   , @executable
			   , @source
			   , @working_directory
			   , 0 -- place values in ELT Framework
			   , 'created from ADS notebook'
			   , null
			   , @package_source_control )

		INSERT [elt].[project_package] VALUES (@project_id, @sequence_number, 'Execute Process - ' + @cell_name, null)	
	END

print 'Created/Updated: ' + @cell_name

SELECT 
       @computer_name [computer_name]
	 , @full_name [full_name]
	 , @cell_name [cell_name]
	 , @sequence_number [sequence_number]
	 , @source_node [source_node]
	 , @pattern [pattern]
	 , @src_connection_name [src_connection_name]
	 , @dst_connection_name [dst_connection_name]
	 , @dst_schema [dst_schema]
	 , @dst_table [dst_table]
	 , @truncate_dst [truncate_dst]
	 , @identity_insert [identity_insert]
	 , @batch_size [batch_size]
	 , @source [source]
	 , @executable [executable]
   , @working_directory [working_directory]
	

	FETCH NEXT FROM cell_cursor INTO @full_name, @cell_name, @sequence_number, @parallel_executions
END

CLOSE cell_cursor
DEALLOCATE cell_cursor

EXEC [elt].[Build Parameters for Connections] @project_id;
