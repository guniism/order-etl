def create_schema_dw(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.schemas
            WHERE name = 'dw'
        )
        BEGIN
            EXEC('CREATE SCHEMA dw');
            SET @created = 1;
        END;

        SELECT @created AS schema_created;
    """)

    created = cursor.fetchone()[0]

    if created:
        print("- Schema 'dw' created successfully.")
    else:
        print("- Schema 'dw' already exists.")



def create_table_salesorder(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = 'SalesOrders'
            AND s.name = 'dw'
        )
        BEGIN
            CREATE TABLE dw.SalesOrders (
                OrderSK INT IDENTITY(1,1) PRIMARY KEY,

                OrderID VARCHAR(50) NOT NULL,
                OrderDate DATETIME2 NOT NULL,
                CustomerID VARCHAR(50) NOT NULL,
                ProductID VARCHAR(50) NOT NULL,
                Quantity INT NOT NULL,
                   
                HashDiff VARBINARY(32) NOT NULL,

                LoadDate DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
                IsCurrent BIT NOT NULL DEFAULT 1
            );
            SET @created = 1;
        END;
                   
        SELECT @created AS table_created;
     """)
    
    created = cursor.fetchone()[0]

    if created:
        print(" - Table 'dw.SalesOrders' created successfully.")
    else:
        print(" - Table 'dw.SalesOrders' already exists.")
    
def create_table_customer(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = 'Customers'
            AND s.name = 'dw'
        )
        BEGIN
           CREATE TABLE dw.Customers (
                CustomerSK INT IDENTITY(1,1) PRIMARY KEY,

                CustomerID VARCHAR(50) NOT NULL,
                CustomerName VARCHAR(50) NOT NULL,
                Industry VARCHAR(50) NOT NULL,
                Country VARCHAR(50) NOT NULL,
                        
                HashDiff VARBINARY(32) NOT NULL,

                LoadDate DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
                IsCurrent BIT NOT NULL DEFAULT 1
            );
            SET @created = 1;
        END;
                   
        SELECT @created AS table_created;
     """)
    
    created = cursor.fetchone()[0]

    if created:
        print(" - Table 'dw.Customers' created successfully.")
    else:
        print(" - Table 'dw.Customers' already exists.")

def create_table_product(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = 'Products'
            AND s.name = 'dw'
        )
        BEGIN
            CREATE TABLE dw.Products (
                ProductSK INT IDENTITY(1,1) PRIMARY KEY,

                ProductID VARCHAR(50) NOT NULL,
                ProductName VARCHAR(50) NOT NULL,
                Category VARCHAR(50) NOT NULL,
                Price DECIMAL(18,2) NOT NULL,
                        
                HashDiff VARBINARY(32) NOT NULL,

                LoadDate DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
                IsCurrent BIT NOT NULL DEFAULT 1
            );
            SET @created = 1;
        END;
                   
        SELECT @created AS table_created;
     """)
    
    created = cursor.fetchone()[0]

    if created:
        print(" - Table 'dw.Products' created successfully.")
    else:
        print(" - Table 'dw.Products' already exists.")

def create_dw(cursor):
    create_schema_dw(cursor)

    create_table_salesorder(cursor)
    create_table_customer(cursor)
    create_table_product(cursor)