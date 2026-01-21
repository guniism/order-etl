def create_schema_stg(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.schemas
            WHERE name = 'stg'
        )
        BEGIN
            EXEC('CREATE SCHEMA stg');
            SET @created = 1;
        END;

        SELECT @created AS schema_created;
    """)

    created = cursor.fetchone()[0]

    if created:
        print("- Schema 'stg' created successfully.")
    else:
        print("- Schema 'stg' already exists.")



def create_table_salesorder(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = 'SalesOrders'
            AND s.name = 'stg'
        )
        BEGIN
            CREATE TABLE stg.SalesOrders (
                OrderID VARCHAR(50) NOT NULL,
                OrderDate DATETIME2 NOT NULL,
                CustomerID VARCHAR(50) NOT NULL,
                ProductID VARCHAR(50) NOT NULL,
                Quantity INT NOT NULL,
                   
                HashDiff VARBINARY(32) NOT NULL,
            );
            SET @created = 1;
        END;
                   
        SELECT @created AS table_created;
     """)
    
    created = cursor.fetchone()[0]

    if created:
        print(" - Table 'stg.SalesOrders' created successfully.")
    else:
        print(" - Table 'stg.SalesOrders' already exists.")
    
def create_table_customer(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = 'Customers'
            AND s.name = 'stg'
        )
        BEGIN
           CREATE TABLE stg.Customers (
                CustomerID VARCHAR(50) NOT NULL,
                CustomerName VARCHAR(50) NOT NULL,
                Industry VARCHAR(50) NOT NULL,
                Country VARCHAR(50) NOT NULL,
                        
                HashDiff VARBINARY(32) NOT NULL,
            );
            SET @created = 1;
        END;
                   
        SELECT @created AS table_created;
     """)
    
    created = cursor.fetchone()[0]

    if created:
        print(" - Table 'stg.Customers' created successfully.")
    else:
        print(" - Table 'stg.Customers' already exists.")

def create_table_product(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = 'Products'
            AND s.name = 'stg'
        )
        BEGIN
            CREATE TABLE stg.Products (
                ProductID VARCHAR(50) NOT NULL,
                ProductName VARCHAR(50) NOT NULL,
                Category VARCHAR(50) NOT NULL,
                Price DECIMAL(18,2) NOT NULL,
                        
                HashDiff VARBINARY(32) NOT NULL,
            );
            SET @created = 1;
        END;
                   
        SELECT @created AS table_created;
     """)
    
    created = cursor.fetchone()[0]

    if created:
        print(" - Table 'stg.Products' created successfully.")
    else:
        print(" - Table 'stg.Products' already exists.")

def create_stg(cursor):
    create_schema_stg(cursor)

    create_table_salesorder(cursor)
    create_table_customer(cursor)
    create_table_product(cursor)