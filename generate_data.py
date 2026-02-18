from faker import Faker
import random
import psycopg2
from pymongo import MongoClient
import pandas as pd
from minio import Minio
from io import BytesIO

fake = Faker()

pg_conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="ecommerce",
    user="postgres",
    password="postgres"
)
pg_cursor = pg_conn.cursor()


mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["ecommerce"]
events_col = mongo_db["user_events"]

minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Création des tables si elles n'existent pas
pg_cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INT PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(150),
        country VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id INT PRIMARY KEY,
        name VARCHAR(100),
        category VARCHAR(50),
        price DECIMAL(10,2)
    );

    CREATE TABLE IF NOT EXISTS orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        status VARCHAR(50),
        total_amount DECIMAL(10,2)
    );
""")
pg_conn.commit() # Très important pour valider la création


customers = []
for i in range(1, 101):
    customers.append({
        "customer_id": i,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "country": fake.country(),
        "city": fake.city()
    })

products = []
for i in range(1, 51):
    products.append({
        "product_id": i,
        "name": fake.word(),
        "category": random.choice(["Tech", "Fashion", "Food"]),
        "price": round(random.uniform(10, 500), 2)
    })

orders = []
order_items = []

order_id = 1
for _ in range(300):
    customer = random.choice(customers)
    product = random.choice(products)
    quantity = random.randint(1, 5)

    orders.append({
        "order_id": order_id,
        "customer_id": customer["customer_id"],
        "status": random.choice(["paid", "shipped"]),
        "total_amount": product["price"] * quantity
    })

    order_items.append({
        "order_id": order_id,
        "product_id": product["product_id"],
        "quantity": quantity,
        "unit_price": product["price"]
    })

    order_id += 1

for c in customers:
    pg_cursor.execute(
        "INSERT INTO customers VALUES (%s,%s,%s,%s,%s)",
        (c["customer_id"], c["first_name"], c["last_name"], c["email"], c["country"])
    )

for p in products:
    pg_cursor.execute(
        "INSERT INTO products VALUES (%s,%s,%s,%s)",
        (p["product_id"], p["name"], p["category"], p["price"])
    )

for o in orders:
    pg_cursor.execute(
        "INSERT INTO orders VALUES (%s,%s,%s,%s)",
        (o["order_id"], o["customer_id"], o["status"], o["total_amount"])
    )

pg_conn.commit()


events = []
for _ in range(500):
    events.append({
        "user_id": random.choice(customers)["customer_id"],
        "product_id": random.choice(products)["product_id"],
        "event_type": random.choice(["view", "add_to_cart", "checkout"]),
        "device": random.choice(["mobile", "desktop"])
    })

events_col.insert_many(events)

df_sales = pd.DataFrame(orders)
csv_buffer = BytesIO()
df_sales.to_csv(csv_buffer, index=False)
csv_buffer.seek(0)

minio_client.put_object(
    "raw-data",
    "sales/sales_2024.csv",
    csv_buffer,
    length=len(csv_buffer.getvalue()),
    content_type="text/csv"
)

df_products_ext = pd.DataFrame(products)
csv_buffer = BytesIO()
df_products_ext.to_csv(csv_buffer, index=False)
csv_buffer.seek(0)

minio_client.put_object(
    "raw-data",
    "products/products_external.csv",
    csv_buffer,
    length=len(csv_buffer.getvalue()),
    content_type="text/csv"
)
