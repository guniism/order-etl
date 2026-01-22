from pipeline_function.mssql_init.connect import connect_mssql_dw


def get_sales_orders_all():
    connection = connect_mssql_dw()
    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            SalesOrderSK,
            OrderID,
            OrderDate,

            CustomerID,
            CustomerName,
            Industry,
            Country,

            ProductID,
            ProductName,
            Category,
            Price,

            Quantity,
            TotalAmount,
            LoadDate
        FROM mart.VWSalesOrders
        ORDER BY OrderDate DESC
    """)

    rows = cursor.fetchall()

    result = []
    for r in rows:
        result.append({
            "SalesOrderSK": r[0],
            "OrderID": r[1],
            "OrderDate": r[2],

            "Customer": {
                "CustomerID": r[3],
                "CustomerName": r[4],
                "Industry": r[5],
                "Country": r[6],
            },

            "Product": {
                "ProductID": r[7],
                "ProductName": r[8],
                "Category": r[9],
                "Price": float(r[10]),
            },

            "Quantity": r[11],
            "TotalAmount": float(r[12]),
            "LoadDate": r[13],
        })

    cursor.close()
    connection.close()

    return result

def get_sales_order_by_id(order_id: str):
    connection = connect_mssql_dw()
    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            SalesOrderSK,
            OrderID,
            OrderDate,

            CustomerID,
            CustomerName,
            Industry,
            Country,

            ProductID,
            ProductName,
            Category,
            Price,

            Quantity,
            TotalAmount,
            LoadDate
        FROM mart.VWSalesOrders
        WHERE OrderID = ?
    """, (order_id,))

    row = cursor.fetchone()

    cursor.close()
    connection.close()

    if not row:
        return None

    return {
        "SalesOrderSK": row[0],
        "OrderID": row[1],
        "OrderDate": row[2],

        "Customer": {
            "CustomerID": row[3],
            "CustomerName": row[4],
            "Industry": row[5],
            "Country": row[6],
        },

        "Product": {
            "ProductID": row[7],
            "ProductName": row[8],
            "Category": row[9],
            "Price": float(row[10]),
        },

        "Quantity": row[11],
        "TotalAmount": float(row[12]),
        "LoadDate": row[13],
    }
