import pandas as pd

def transform(df_customer, df_order, df_product):
    customer_col_map = {
        "cid": "CustomerID",
        "name": "CustomerName",
        "industry": "Industry",
        "country": "Country",
    }
    order_col_map = {
        "order_id": "OrderID",
        "order_date": "OrderDate",
        "customer_id": "CustomerID",
        "product_id": "ProductID",
        "quantity": "Quantity",
    }
    product_col_map = {
        "product_code": "ProductCode",
        "name": "ProductName",
        "category": "Category",
        "price": "Price",
    }
    df_customer = df_customer.rename(columns=customer_col_map)
    # print(df_customer)
    df_order = df_order.rename(columns=order_col_map)

    df_product = df_product.rename(columns=product_col_map)

    # print(df_customer.columns)
    # print(df_order.columns)
    # print(df_product.columns)

    return df_customer, df_order, df_product