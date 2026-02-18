import marimo

__generated_with = "0.8.0"
app = marimo.App()

@app.cell
def __():
    from pyspark.sql import SparkSession
    import requests
    import pandas as pd
    
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .getOrCreate()
    
    return spark, requests, pd

@app.cell
def __(spark):
    # PostgreSQL - Orders
    df_orders = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/ecommerce") \
        .option("dbtable", "orders") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .load()
    
    df_orders.show(5)
    return df_orders

@app.cell
def __(df_orders):
    df_orders.write.mode("overwrite").parquet(
        "s3a://bronze/postgres/orders"
    )

@app.cell
def __(spark):
    # MongoDB - User Events
    df_events = spark.read \
        .format("mongodb") \
        .option("uri", "mongodb://mongodb:27017/ecommerce.user_events") \
        .load()
    
    df_events.show(5)
    return df_events

@app.cell
def __(spark):
    # MinIO Configuration
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Read CSV from MinIO
    df_sales = spark.read.csv(
        "s3a://raw-data/sales/sales_2024.csv",
        header=True,
        inferSchema=True
    )
    
    df_sales.show(5)
    return df_sales

@app.cell
def __(df_sales):
    df_sales.write.mode("overwrite").parquet(
        "s3a://bronze/files/sales"
    )

@app.cell
def __(spark, requests):
    # API - Exchange Rates
    response = requests.get("http://host.docker.internal:5000/exchange-rates")
    data = response.json()
    
    rates = [(k, v) for k, v in data["rates"].items()]
    df_rates = spark.createDataFrame(rates, ["currency", "rate"])
    
    df_rates.show()
    return df_rates

@app.cell
def __(df_rates):
    df_rates.write.mode("overwrite").parquet(
        "s3a://bronze/api/exchange_rates"
    )
