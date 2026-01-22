import os
from dotenv import load_dotenv
from mssql_python import connect
from pipeline_function.mssql_init.connect import connect_mssql_dw

def merge_dim_customers(cursor):
    print("- Merging DimCustomers...")
    cursor.execute("""
        MERGE mart.DimCustomers AS target
        USING (
            SELECT CustomerID, CustomerName, Industry, Country 
            FROM dw.Customers 
            WHERE IsCurrent = 1
        ) AS source
        ON (target.CustomerID = source.CustomerID)
        WHEN MATCHED THEN
            UPDATE SET 
                target.CustomerName = source.CustomerName,
                target.Industry = source.Industry,
                target.LoadDate = SYSDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (CustomerID, CustomerName, Industry, Country, LoadDate)
            VALUES (source.CustomerID, source.CustomerName, source.Industry, source.Country, SYSDATETIME());
    """)

def merge_dim_products(cursor):
    print("- Merging DimProducts...")
    cursor.execute("""
        MERGE mart.DimProducts AS target
        USING (
            SELECT ProductID, ProductName, Category, Price
            FROM dw.Products 
            WHERE IsCurrent = 1
        ) AS source
        ON (target.ProductID = source.ProductID)
        WHEN MATCHED THEN
            UPDATE SET 
                target.ProductName = source.ProductName,
                target.Category = source.Category,
                target.Price = source.Price,
                target.LoadDate = SYSDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (ProductID, ProductName, Category, Price, LoadDate)
            VALUES (source.ProductID, source.ProductName, source.Category, source.Price, SYSDATETIME());
    """)

def merge_fact_sales(cursor):
    print("- Merging FactSalesOrders...")
    cursor.execute("""
        MERGE mart.FactSalesOrders AS target
        USING (
            SELECT 
                o.OrderID, o.OrderDate, 
                ISNULL(c.CustomerSK, -1) AS CustomerSK, 
                ISNULL(p.ProductSK, -1) AS ProductSK, 
                o.Quantity
            FROM dw.SalesOrders o
            LEFT JOIN mart.DimCustomers c ON o.CustomerID = c.CustomerID
            LEFT JOIN mart.DimProducts p ON o.ProductID = p.ProductID
            WHERE o.IsCurrent = 1
        ) AS source
        ON (target.OrderID = source.OrderID AND target.ProductSK = source.ProductSK)
        WHEN MATCHED THEN
            UPDATE SET 
                target.Quantity = source.Quantity,
                target.OrderDate = source.OrderDate,
                target.CustomerSK = source.CustomerSK,
                target.LoadDate = SYSDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (OrderID, OrderDate, CustomerSK, ProductSK, Quantity, LoadDate)
            VALUES (source.OrderID, source.OrderDate, source.CustomerSK, source.ProductSK, source.Quantity, SYSDATETIME());
    """)

def load2mart():
    connection = connect_mssql_dw()
    cursor = connection.cursor()
    try:
        merge_dim_customers(cursor)
        merge_dim_products(cursor)
        
        merge_fact_sales(cursor)

    except Exception as e:
        print(f"Error during Mart refresh: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()