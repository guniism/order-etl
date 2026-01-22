from pipeline_function.mssql_init.warehouse import create_dw
from pipeline_function.mssql_init.stage import create_stg
from pipeline_function.mssql_init.connect import connect_mssql_dw, connect_mssql_master


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
    connection = connect_mssql_master()

    cursor = connection.cursor()
    try:
        check_create_database(cursor)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    connection = connect_mssql_dw()
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

