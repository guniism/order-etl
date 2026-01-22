from pipeline_function.mssql_init.connect import connect_mssql_dw

def create_schema_mart(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.schemas
            WHERE name = 'mart'
        )
        BEGIN
            EXEC('CREATE SCHEMA mart');
            SET @created = 1;
        END;

        SELECT @created AS schema_created;
    """)

    created = cursor.fetchone()[0]

    if created:
        print("Schema 'mart' created successfully.")
    else:
        print("Schema 'mart' already exists.")



def create_mart_salesorder(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = 'FactSalesOrders'
            AND s.name = 'mart'
        )
        BEGIN
            CREATE TABLE mart.FactSalesOrders (
                SalesOrderSK INT IDENTITY(1,1) PRIMARY KEY,

                OrderID VARCHAR(50) NOT NULL,
                OrderDate DATETIME2 NOT NULL,

                CustomerSK INT NOT NULL,
                ProductSK INT NOT NULL,

                Quantity INT NOT NULL,

                LoadDate DATETIME2 NOT NULL DEFAULT SYSDATETIME(),

                CONSTRAINT FK_Fact_Customer
                    FOREIGN KEY (CustomerSK)
                    REFERENCES mart.DimCustomers(CustomerSK),

                CONSTRAINT FK_Fact_Product
                    FOREIGN KEY (ProductSK)
                    REFERENCES mart.DimProducts(ProductSK)
            );
            SET @created = 1;
        END;
                   
        SELECT @created AS table_created;
     """)
    
    created = cursor.fetchone()[0]

    if created:
        print("- Table 'mart.FactSalesOrders' created successfully.")
    else:
        print("- Table 'mart.FactSalesOrders' already exists.")
    
def create_mart_customer(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = 'DimCustomers'
            AND s.name = 'mart'
        )
        BEGIN
            CREATE TABLE mart.DimCustomers (
                CustomerSK INT IDENTITY(1,1) PRIMARY KEY,
                   
                CustomerID VARCHAR(50) NOT NULL,
                CustomerName VARCHAR(50) NOT NULL,
                Industry VARCHAR(50) NOT NULL,
                Country VARCHAR(50) NOT NULL,

                LoadDate DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
            );
            SET @created = 1;
        END;
                   
        SELECT @created AS table_created;
     """)
    
    created = cursor.fetchone()[0]

    if created:
        print("- Table 'mart.DimCustomers' created successfully.")
    else:
        print("- Table 'mart.DimCustomers' already exists.")

def create_mart_product(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = 'DimProducts'
            AND s.name = 'mart'
        )
        BEGIN
            CREATE TABLE mart.DimProducts (
                ProductSK INT IDENTITY(1,1) PRIMARY KEY,

                ProductID VARCHAR(50) NOT NULL,
                ProductName VARCHAR(50) NOT NULL,
                Category VARCHAR(50) NOT NULL,
                Price DECIMAL(18,2) NOT NULL,

                LoadDate DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
            );
            SET @created = 1;
        END;
                   
        SELECT @created AS table_created;
     """)
    
    created = cursor.fetchone()[0]

    if created:
        print("- Table 'mart.DimProducts' created successfully.")
    else:
        print("- Table 'mart.DimProducts' already exists.")

def create_vw_salesorders(cursor):
    cursor.execute("""
        DECLARE @created BIT = 0;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.views v
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE v.name = 'VWSalesOrders'
            AND s.name = 'mart'
        )
        BEGIN
            EXEC('
                CREATE VIEW mart.VWSalesOrders
                AS
                SELECT
                    f.SalesOrderSK,

                    f.OrderID,
                    f.OrderDate,

                    c.CustomerID,
                    c.CustomerName,
                    c.Industry,
                    c.Country,

                    p.ProductID,
                    p.ProductName,
                    p.Category,
                    p.Price,

                    f.Quantity,
                    (f.Quantity * p.Price) AS TotalAmount,

                    f.LoadDate
                FROM mart.FactSalesOrders f
                JOIN mart.DimCustomers c
                    ON f.CustomerSK = c.CustomerSK
                JOIN mart.DimProducts p
                    ON f.ProductSK = p.ProductSK
            ');
            SET @created = 1;
        END;

        SELECT @created AS view_created;
    """)

    created = cursor.fetchone()[0]

    if created:
        print("- View 'mart.VWSalesOrders' created successfully.")
    else:
        print("- View 'mart.VWSalesOrders' already exists.")

def create_mart(cursor):
    create_schema_mart(cursor)

    create_mart_customer(cursor)
    create_mart_product(cursor)
    create_mart_salesorder(cursor)

    create_vw_salesorders(cursor)


def mart_init():
    connection = connect_mssql_dw()
    cursor = connection.cursor()
    try:
        # print(df_order)
        create_mart(cursor)


    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()