from pipeline_function.extrack import extract
from pipeline_function.load2mart import load2mart
from pipeline_function.transform import transform
from pipeline_function.mssql_init.init import db_init
from pipeline_function.load import load
from pipeline_function.mssql_init.mart import mart_init

def main():
    # Data Extraction process from mock csv files
    df_order_extracted, df_customer_extracted, df_product_extracted = extract(order_path='raw_data/order_mock.csv', customer_path='raw_data/customer_mock.csv', product_path='raw_data/product_mock.csv')
    print("Data Extraction Completed.")

    # Data Transformation process e.g. fixing headers
    df_order_transformed, df_customer_transformed, df_product_transformed = transform(df_order_extracted, df_customer_extracted, df_product_extracted)
    print("Data Transformation Completed.")

    # Docker initialization
    db_init()

    # Load data to Data Warehouse
    load(df_order_transformed, df_customer_transformed, df_product_transformed)
    print("Data Load to Data Warehouse Completed.")

    # Dara Mart initialization
    mart_init()

    # Load data to Data Mart
    load2mart()
    print("Data Load to Data Mart Completed.")

if __name__ == "__main__":
    main()

