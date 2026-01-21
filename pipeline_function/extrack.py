import pandas as pd

def extract(order_path, customer_path, product_path):
    # df_customer = pd.read_csv('raw_data/customer_mock.csv')
    # df_order = pd.read_csv('raw_data/order_mock.csv')
    # df_product = pd.read_csv('raw_data/product_mock.csv')
    df_order = pd.read_csv(order_path)
    df_customer = pd.read_csv(customer_path)
    df_product = pd.read_csv(product_path)
    
    return df_order, df_customer, df_product