import os
from dotenv import load_dotenv
from socket import socket
from mssql_python import connect

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

def create_database():
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
    connection_string = os.getenv("MSSQL_CONNECTION_STR")
    # print(connection_string)

    connection = connect(
        connection_string,
        autocommit=True
    )

    cursor = connection.cursor()
    db_name = "SalesDataWarehouse"
    try:
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

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
