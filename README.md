## Project Setup

### 1. Clone the repository

```bash
git clone https://github.com/guniism/order-etl.git
cd order-etl
```

### 2. Create Python virtual environment

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux / macOS
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run Microsoft SQL Server with Docker
Make sure Docker is running, then execute:
```bash
docker compose up -d
```

### 5. Run ETL Pipeline

```bash
py main.py
```
This step will:

- Extract mock order data

- Transform the data

- Load data into the Data Warehouse 

- Perform an additional ETL process to populate the Data Mart using a Star Schema

### 6. Run the FastAPI Server
The API serves data from the Data Mart.
```bash
uvicorn api:app --reload
```

Open http://127.0.0.1:8000 with your browser to see the result.

## API Documentation (Swagger)

http://127.0.0.1:8000/docs