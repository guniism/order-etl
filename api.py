from fastapi import FastAPI, HTTPException
from api_function.customer import get_customer_all, get_customer_by_id
from api_function.product import get_product_all, get_product_by_id
from api_function.salesorder import get_sales_order_by_id, get_sales_orders_all

app = FastAPI(title="Data Mart API")

@app.get(
    "/customers",
    tags=["Customers"],
    summary="Get all customers",
    description="Return a list of all customers from the data mart"
)
def get_customers():
    customers = get_customer_all()
    return customers

@app.get(
    "/customer/{CustomerID}",
    tags=["Customers"],
    summary="Get customer by ID",
    description="Return a single customer by CustomerID"
)
def fetch_customer_by_id(CustomerID: str):
    customer = get_customer_by_id(CustomerID)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer

@app.get(
    "/products",
    tags=["Products"],
    summary="Get all products",
    description="Return a list of all products from the data mart"
)
def get_products():
    products = get_product_all()
    return products

@app.get(
    "/product/{ProductID}",
    tags=["Products"],
    summary="Get product by ID",
    description="Return a single product by ProductID"
)
def fetch_product_by_id(ProductID: str):
    product = get_product_by_id(ProductID)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product

@app.get(
    "/salesorders",
    tags=["Sales Orders"],
    summary="Get all sales orders",
    description="Return a list of all sales orders from the data mart"
)
def get_sales_orders():
    sales_orders = get_sales_orders_all()
    return sales_orders

@app.get(
    "/salesorder/{OrderID}",
    tags=["Sales Orders"],
    summary="Get sales order by ID",
    description="Return a single sales order by OrderID"
)
def fetch_sales_order_by_id(OrderID: str):
    sales_order = get_sales_order_by_id(OrderID)
    if not sales_order:
        raise HTTPException(status_code=404, detail="Sales Order not found")
    return sales_order