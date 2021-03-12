/*
{
"notebooksnap": {
        "cell_name": "insert stg.d_vehicle",
        "working_directory": "python_script_path",
        "executable": "python",
        "arguments_expr": "execute_snowsql_v2.py -eltsnap_server ${sys:EltsnapServer} -eltsnap_database ${sys:EltsnapDatabase} -eltsnap_project ${sys:ProjectName} -eltsnap_package ${sys:PackageName} -prg ${sys:PackageRunGIUD} -qkey execute_sql -fsrv <#${ELT_Framework_Server}#>'localhost'<##> -fdb <#${ELT_Framework_Database}#>'elt_framework'<##> -snowflake_Account <#${snowflake_Account}#>'jimm'<##> -snowflake_Region <#${snowflake_Region}#>'test'<##> -snowflake_User <#${snowflake_User}#>'jimm'<##> -snowflake_Password <#${snowflake_Password}#>'SnowBiz34%^'<##> -use_snowflake_Warehouse <#${snowflake_Warehouse}#>'DEV'<##> -use_snowflake_Database <#${snowflake_Database}#>'CAR_CRASH'<##> -use_snowflake_Schema <#${snowflake_Schema}#>'STG'<##>",
        "pattern": "Execute Process"
    }
}
*/
INSERT INTO ${snowflake-demo_Database}.STG.D_VEHICLE 
VALUES ('TRUCK','1001' , '5' , 'Passenger' , 'commercial' , 'auto' )
