import os
from dotenv import load_dotenv
from socket import socket
from mssql_python import connect
from pipeline_function.mssql_init.warehouse import create_dw
from pipeline_function.mssql_init.stage import create_stg

def check_db_ready(host="localhost", port=1433, timeout=3):
    s = socket()
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        s.close()

def check_create_database(cursor, db_name="SalesDataWarehouse"):
    # db_name = "SalesDataWarehouse"
    cursor.execute("""
        SELECT COUNT(*)
        FROM sys.databases
        WHERE name = ?
    """, (db_name,))

    exists = cursor.fetchone()[0]

    if exists == 0:
        cursor.execute(f"CREATE DATABASE {db_name}")
        print(f"Database '{db_name}' created successfully.")
    else:
        print(f"Database '{db_name}' already exists.")

# def is_table_exists(cursor, table_name, db_name="SalesDataWarehouse", schema="dbo"):
#     cursor.execute(f"""
#     SELECT COUNT(*)
#     FROM {db_name}.sys.tables t
#     JOIN {db_name}.sys.schemas s
#         ON t.schema_id = s.schema_id
#     WHERE t.name = '{table_name}'
#     AND s.name = '{schema}';
#     """)

#     return cursor.fetchone()[0]

def db_init():
    if not check_db_ready():
        raise RuntimeError("SQL Server is not ready. Please run: docker compose up -d")
    
        # connection_string = (
    #     "SERVER=localhost,1433;"
    #     "DATABASE=master;"
    #     "UID=sa;"
    #     "PWD=Str0ngP@ssw0rd!;"
    #     "Encrypt=no;"
    #     "TrustServerCertificate=yes;"
    # )

    load_dotenv()
    connection = connect(
        os.getenv("MSSQL_MASTER_CONNECTION_STR"),
        autocommit=True
    )

    cursor = connection.cursor()
    try:
        check_create_database(cursor)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    load_dotenv()
    connection = connect(
        os.getenv("MSSQL_DW_CONNECTION_STR"),
        autocommit=True
    )
    cursor = connection.cursor()
    try:
        # is_table_exists(cursor, "SalesOrder")
        create_dw(cursor)
        create_stg(cursor)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

            # is_table_exists(cursor, "SalesOrder")

