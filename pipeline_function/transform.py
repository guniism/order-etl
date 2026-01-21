import pandas as pd
import hashlib


def build_hash(row, cols):
    raw = "|".join(str(row[c]) for c in cols)
    return hashlib.sha256(raw.encode("utf-8")).digest()

def transform(df_order, df_customer, df_product):
    order_col_map = {
        "order_id": "OrderID",
        "order_date": "OrderDate",
        "customer_id": "CustomerID",
        "product_id": "ProductID",
        "quantity": "Quantity",
    }
    customer_col_map = {
        "cid": "CustomerID",
        "name": "CustomerName",
        "industry": "Industry",
        "country": "Country",
    }
    product_col_map = {
        "product_code": "ProductID",
        "name": "ProductName",
        "category": "Category",
        "price": "Price",
    }
    
    df_order = df_order.rename(columns=order_col_map)
    df_order["HashDiff"] = df_order.apply(
        lambda r: build_hash(r, ["OrderDate", "CustomerID", "ProductID", "Quantity",]),
        axis=1
    )

    df_customer = df_customer.rename(columns=customer_col_map)
    df_customer["HashDiff"] = df_customer.apply(
        lambda r: build_hash(r, ["CustomerName", "Industry", "Country"]),
        axis=1
    )
    # print(df_customer)

    df_product = df_product.rename(columns=product_col_map)
    df_product["HashDiff"] = df_product.apply(
        lambda r: build_hash(r, ["ProductName", "Category", "Price"]),
        axis=1
    )

    # print(df_customer.columns)
    # print(df_order.columns)
    # print(df_product.columns)

    return df_order, df_customer, df_product