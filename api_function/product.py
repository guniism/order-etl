from pipeline_function.mssql_init.connect import connect_mssql_dw

def get_product_all():
    connection = connect_mssql_dw()
    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            ProductSK,
            ProductID,
            ProductName,
            Category,
            Price,
            LoadDate
        FROM mart.DimProducts
    """)

    rows = cursor.fetchall()

    result = []
    for row in rows:
        result.append({
            "ProductSK": row[0],
            "ProductID": row[1],
            "ProductName": row[2],
            "Category": row[3],
            "Price": float(row[4]),
            "LoadDate": row[5]
        })

    cursor.close()
    connection.close()

    return result

def get_product_by_id(ProductID: str):
    connection = connect_mssql_dw()
    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            ProductSK,
            ProductID,
            ProductName,
            Category,
            Price,
            LoadDate
        FROM mart.DimProducts
        WHERE ProductID = ?
    """, (ProductID,))

    row = cursor.fetchone()

    cursor.close()
    connection.close()

    if not row:
        return None

    return {
        "ProductSK": row[0],
        "ProductID": row[1],
        "ProductName": row[2],
        "Category": row[3],
        "Price": float(row[4]),
        "LoadDate": row[5]
    }
