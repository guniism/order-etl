from pipeline_function.extrack import extract
from pipeline_function.transform import transform
from pipeline_function.mssql import create_database

def main():
    # Data Extraction process from mock csv files
    df_customer_extracted, df_order_extracted, df_product_extracted = extract(customer_path='raw_data/customer_mock.csv', order_path='raw_data/order_mock.csv', product_path='raw_data/product_mock.csv')
    print("Data Extraction Completed.")

    # Data Transformation process e.g. fixing headers
    df_customer_transformed, df_order_transformed, df_product_transformed = transform(df_customer_extracted, df_order_extracted, df_product_extracted)
    print("Data Transformation Completed.")

    # Docker initialization
    create_database()



if __name__ == "__main__":
    main()

