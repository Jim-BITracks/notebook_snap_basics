/*
{
"notebooksnap": {
        "cell_name": "insert stg.d_vehicle",
        "working_directory": "python_script_path",
        "executable": "python",
        "arguments_expr": "execute_snowsql_v2.py -eltsnap_server @[$System::EltsnapServer] -eltsnap_database @[$System::EltsnapDatabase] -eltsnap_project @[$System::ProjectName] -eltsnap_package @[$System::PackageName] -prg @[$System::PackageRunGIUD] -qkey execute_sql -fsrv <#${ELT_Framework_Server}#>'localhost'<##> -fdb <#${ELT_Framework_Database}#>'elt_framework'<##> -snowflake_Account <#${snowflake_Account}#>'test'<##> -snowflake_Region <#${snowflake_Region}#>'test'<##> -snowflake_User <#${snowflake_User}#>'test'<##> -snowflake_Password <#${snowflake_Password}#>'test'<##> -use_snowflake_Warehouse <#${snowflake_Warehouse}#>'test'<##> -use_snowflake_Database <#${snowflake_Database}#>'test'<##> -use_snowflake_Schema <#${snowflake_Schema}#>'test'<##>",
        "pattern": "Execute Process"
    }
}
*/
INSERT INTO ${snowflake-demo_Database}.STG.D_VEHICLE 
VALUES ('TRUCK','1001' , '5' , 'Passenger' , 'commercial' , 'auto' )
