from pipeline_function.mssql_init.connect import connect_mssql_dw


def get_customer_all():
    connection = connect_mssql_dw()
    cursor = connection.cursor()

    cursor.execute("""
        SELECT 
            CustomerSK,
            CustomerID,
            CustomerName,
            Industry,
            Country,
            LoadDate
        FROM mart.DimCustomers
    """)

    rows = cursor.fetchall()

    result = []
    for row in rows:
        result.append({
            "CustomerSK": row[0],
            "CustomerID": row[1],
            "CustomerName": row[2],
            "Industry": row[3],
            "Country": row[4],
            "LoadDate": row[5]
        })

    cursor.close()
    connection.close()

    return result


def get_customer_by_id(CustomerID: str):
    connection = connect_mssql_dw()
    cursor = connection.cursor()

    cursor.execute("""
        SELECT 
            CustomerSK,
            CustomerID,
            CustomerName,
            Industry,
            Country,
            LoadDate
        FROM mart.DimCustomers
        WHERE CustomerID = ?
    """, (CustomerID,))

    row = cursor.fetchone()

    cursor.close()
    connection.close()

    if not row:
        return None

    return {
        "CustomerSK": row[0],
        "CustomerID": row[1],
        "CustomerName": row[2],
        "Industry": row[3],
        "Country": row[4],
        "LoadDate": row[5]
    }

