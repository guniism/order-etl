from pipeline_function.extrack import extract
from pipeline_function.transform import transform
from pipeline_function.mssql_init.init import db_init
from pipeline_function.load import load

def main():
    # Data Extraction process from mock csv files
    df_order_extracted, df_customer_extracted, df_product_extracted = extract(order_path='raw_data/order_mock.csv', customer_path='raw_data/customer_mock.csv', product_path='raw_data/product_mock.csv')
    print("Data Extraction Completed.")

    # Data Transformation process e.g. fixing headers
    df_order_transformed, df_customer_transformed, df_product_transformed = transform(df_order_extracted, df_customer_extracted, df_product_extracted)
    print("Data Transformation Completed.")

    # Docker initialization
    db_init()

    # print(df_order_transformed)
    load(df_order_transformed, df_customer_transformed, df_product_transformed)
    print("Data Load to Data Warehouse Completed.")

    # print(df_customer_transformed)

if __name__ == "__main__":
    main()

