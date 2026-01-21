from pipeline_function.mssql_init.init import check_db_ready
from dotenv import load_dotenv
from mssql_python import connect
import os

def load_to_salesorders(cursor, df_order):
    cursor.execute("TRUNCATE TABLE stg.SalesOrders")

    sql = """
        INSERT INTO stg.SalesOrders
        (OrderID, OrderDate, CustomerID, ProductID, Quantity, HashDiff)
        VALUES (?, ?, ?, ?, ?, ?)
    """

    rows = [
        (
            r.OrderID,
            r.OrderDate,
            r.CustomerID,
            r.ProductID,
            int(r.Quantity),
            r.HashDiff,
        )
        for r in df_order.itertuples(index=False)
    ]

    cursor.executemany(sql, rows)

    cursor.execute("""
        UPDATE d
        SET d.IsCurrent = 0
        FROM dw.SalesOrders d
        JOIN stg.SalesOrders s
            ON d.OrderID = s.OrderID
        WHERE d.IsCurrent = 1
        AND d.HashDiff <> s.HashDiff;
                   
        INSERT INTO dw.SalesOrders (OrderID, OrderDate, CustomerID, ProductID, Quantity, LoadDate, IsCurrent, HashDiff)
        SELECT 
            s.OrderID, s.OrderDate, s.CustomerID, s.ProductID, s.Quantity, 
            CURRENT_TIMESTAMP, 1, s.HashDiff
        FROM stg.SalesOrders s
        LEFT JOIN dw.SalesOrders d 
            ON s.OrderID = d.OrderID AND d.IsCurrent = 1
        WHERE d.OrderID IS NULL;
    """)

def load_to_customers(cursor, df_customer):
    cursor.execute("TRUNCATE TABLE stg.Customers")

    sql = """
        INSERT INTO stg.Customers
        (CustomerID, CustomerName, Industry, Country, HashDiff)
        VALUES (?, ?, ?, ?, ?)
    """

    rows = [
        (
            r.CustomerID,
            r.CustomerName,
            r.Industry,
            r.Country,
            r.HashDiff,
        )
        for r in df_customer.itertuples(index=False)
    ]

    cursor.executemany(sql, rows)

    cursor.execute("""
        UPDATE d
        SET d.IsCurrent = 0
        FROM dw.Customers d
        JOIN stg.Customers s
            ON d.CustomerID = s.CustomerID
        WHERE d.IsCurrent = 1
        AND d.HashDiff <> s.HashDiff;
                   
        INSERT INTO dw.Customers (CustomerID, CustomerName, Industry, Country, LoadDate, IsCurrent, HashDiff)
        SELECT 
            s.CustomerID, s.CustomerName, s.Industry, s.Country,
            CURRENT_TIMESTAMP, 1, s.HashDiff
        FROM stg.Customers s
        LEFT JOIN dw.Customers d 
            ON s.CustomerID = d.CustomerID AND d.IsCurrent = 1
        WHERE d.CustomerID IS NULL;
    """)

def load_to_products(cursor, df_product):
    cursor.execute("TRUNCATE TABLE stg.Products")

    sql = """
        INSERT INTO stg.Products
        (ProductID, ProductName, Category, Price, HashDiff)
        VALUES (?, ?, ?, ?, ?)
    """

    rows = [
        (
            r.ProductID,
            r.ProductName,
            r.Category,
            r.Price,
            r.HashDiff,
        )
        for r in df_product.itertuples(index=False)
    ]

    cursor.executemany(sql, rows)

    cursor.execute("""
        UPDATE d
        SET d.IsCurrent = 0
        FROM dw.Products d
        JOIN stg.Products s
            ON d.ProductID = s.ProductID
        WHERE d.IsCurrent = 1
        AND d.HashDiff <> s.HashDiff;
                   
        INSERT INTO dw.Products (ProductID, ProductName, Category, Price, LoadDate, IsCurrent, HashDiff)
        SELECT 
            s.ProductID, s.ProductName, s.Category, s.Price,
            CURRENT_TIMESTAMP, 1, s.HashDiff
        FROM stg.Products s
        LEFT JOIN dw.Products d 
            ON s.ProductID = d.ProductID AND d.IsCurrent = 1
        WHERE d.ProductID IS NULL;
    """)
    

def load(df_order, df_customer,  df_product):
    if not check_db_ready():
        raise RuntimeError("SQL Server is not ready. Please run: docker compose up -d")
    load_dotenv()
    connection = connect(
        os.getenv("MSSQL_DW_CONNECTION_STR"),
        autocommit=True
    )
    cursor = connection.cursor()
    try:
        # print(df_order)
        load_to_salesorders(cursor, df_order)
        load_to_customers(cursor, df_customer)
        load_to_products(cursor, df_product)


    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()