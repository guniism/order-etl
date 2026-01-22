import os
from dotenv import load_dotenv
from mssql_python import connect
from socket import socket

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

def connect_mssql_dw():
    if not check_db_ready():
        raise RuntimeError("SQL Server is not ready. Please run: docker compose up -d")
    load_dotenv()
    connection = connect(
        os.getenv("MSSQL_DW_CONNECTION_STR"),
        autocommit=True
    )
    return connection

def connect_mssql_master():
    if not check_db_ready():
        raise RuntimeError("SQL Server is not ready. Please run: docker compose up -d")
    load_dotenv()
    connection = connect(
        os.getenv("MSSQL_MASTER_CONNECTION_STR"),
        autocommit=True
    )
    return connection

    # connection_string = (
    #     "SERVER=localhost,1433;"
    #     "DATABASE=master;"
    #     "UID=sa;"
    #     "PWD=Str0ngP@ssw0rd!;"
    #     "Encrypt=no;"
    #     "TrustServerCertificate=yes;"
    # )