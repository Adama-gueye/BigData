import marimo

__generated_with = "0.19.11"
app = marimo.App()


@app.cell
def _():
    from pyspark.sql import SparkSession
    import requests
    from pyspark.sql.functions import col, sum as _sum

    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .config(
            "spark.jars.packages",
            ",".join([
                "org.postgresql:postgresql:42.7.3",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.540",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
            ])
        ) \
        .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/ecommerce.user_events") \
        .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/ecommerce") \
        .getOrCreate()

    # ===== MinIO / S3A config =====
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

    spark
    return col, requests, spark


@app.cell
def _(spark):
    # PostgreSQL - Orders
    df_orders = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/ecommerce") \
        .option("dbtable", "orders") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df_orders.show(5)
    return (df_orders,)


@app.cell
def _(df_orders):
    df_orders.write.mode("overwrite").parquet(
        "s3a://bronze/postgres/orders"
    )
    return


@app.cell
def _(spark):
    # MongoDB - User Events (robust version)

    df_events = None

    try:
        df_events = spark.read \
            .format("mongodb") \
            .option(
                "spark.mongodb.read.connection.uri",
                "mongodb://mongodb:27017/ecommerce.user_events"
            ) \
            .load()

        print("Données MongoDB chargées via le connecteur Spark")

    except Exception as e:
        print("Échec du connecteur Spark MongoDB, bascule vers pymongo")
        print(e)

        from pymongo import MongoClient

        client = MongoClient("mongodb", 27017)
        coll = client["ecommerce"]["user_events"]

        docs = list(coll.find())

        if not docs:
            print("Aucun document trouvé dans la collection MongoDB")
            df_events = None
        else:
            # Convert ObjectId → string for Spark compatibility
            for d in docs:
                if "_id" in d:
                    d["_id"] = str(d["_id"])

            df_events = spark.createDataFrame(docs)
            print("Données MongoDB chargées via pymongo (solution de secours)")

    # Display if available
    if df_events is not None:
        df_events.show(5)

    df_events
    return (df_events,)


@app.cell
def _(df_events):
    if df_events is not None:
        df_events.write.mode("overwrite").parquet(
            "s3a://bronze/mongodb/user_events"
        )
    return


@app.cell
def _(spark):
    # Read CSV from MinIO
    df_sales = spark.read.csv(
        "s3a://raw-data/files/online_retail.csv",
        header=True,
        inferSchema=True
    )

    df_sales.show(5)
    return (df_sales,)


@app.cell
def _(df_sales):
    df_sales.write.mode("overwrite").parquet(
        "s3a://bronze/files/online_retail"
    )
    return


@app.cell
def _(requests, spark):
    # API - Exchange Rates
    response = requests.get("http://host.docker.internal:5000/exchange-rates", timeout=5)
    response.raise_for_status()
    data = response.json()


    rates = [(k, v) for k, v in data["rates"].items()]
    df_rates = spark.createDataFrame(rates, ["currency", "rate"])

    df_rates.show()
    return (df_rates,)


@app.cell
def _(df_rates):
    df_rates.write.mode("overwrite").parquet(
        "s3a://bronze/api/exchange_rates"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Silver – Orders (PostgreSQL)
    """)
    return


@app.cell
def _(col, spark):
    orders_silver = spark.read.parquet(
        "s3a://bronze/postgres/orders"
    )

    orders_silver = orders_silver \
        .withColumn("order_id", col("order_id").cast("int")) \
        .withColumn("customer_id", col("customer_id").cast("int")) \
        .withColumn("total_amount", col("total_amount").cast("double")) \
        .withColumnRenamed("total_amount", "amount") \
        .filter(col("amount").isNotNull())

    orders_silver.write.mode("overwrite").parquet(
        "s3a://silver/orders"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Silver – Events (MongoDB)
    """)
    return


@app.cell
def _(col, spark):
    events_silver = spark.read.parquet(
        "s3a://bronze/mongodb/user_events"
    )

    events_silver = events_silver \
        .withColumn("user_id", col("user_id").cast("int")) \
        .withColumn("product_id", col("product_id").cast("int")) \
        .filter(col("event_type").isNotNull())

    events_silver.write.mode("overwrite").parquet(
        "s3a://silver/events"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Silver – Exchange Rates (API)
    """)
    return


@app.cell
def _(col, spark):
    rates_silver = spark.read.parquet(
        "s3a://bronze/api/exchange_rates"
    )

    rates_silver = rates_silver \
        .withColumn("rate", col("rate").cast("double"))

    rates_silver.write.mode("overwrite").parquet(
        "s3a://silver/exchange_rates"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### SILVER – FILES (CSV ventes)
    """)
    return


@app.cell
def _(col, spark):
    files_silver = spark.read.parquet(
        "s3a://bronze/files/online_retail"
    )

    files_silver = files_silver \
        .withColumnRenamed("InvoiceNo", "order_id") \
        .withColumnRenamed("CustomerID", "customer_id") \
        .withColumn("quantity", col("Quantity").cast("int")) \
        .withColumn("unit_price", col("UnitPrice").cast("double")) \
        .withColumn(
            "total_amount",
            col("quantity") * col("unit_price")
        ) \
        .filter(col("customer_id").isNotNull())

    files_silver.write.mode("overwrite").parquet(
        "s3a://silver/files_online_retails"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Gold – Chiffre d’affaires par statut
    """)
    return


@app.cell
def _(spark):
    from pyspark.sql.functions import sum as _sum
    orders_gold = spark.read.parquet(
        "s3a://silver/orders"
    )

    revenue_by_status = orders_gold \
        .groupBy("status") \
        .agg(_sum("amount").alias("total_%revenue"))

    revenue_by_status.write.mode("overwrite").parquet(
        "s3a://gold/revenue_by_status"
    )
    return (orders_gold,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Gold – Top clients
    """)
    return


@app.cell
def _(col, orders_gold):
    from pyspark.sql.functions import sum as _sum

    top_clients = orders_gold \
        .groupBy("customer_id") \
        .agg(_sum("amount").alias("total_spent")) \
        .orderBy(col("total_spent").desc())

    top_clients.write.mode("overwrite").parquet(
        "s3a://gold/top_clients"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Gold – Events par type
    """)
    return


@app.cell
def _(spark):
    events_gold = spark.read.parquet(
        "s3a://silver/events"
    )

    events_summary = events_gold \
        .groupBy("event_type") \
        .count()

    events_summary.write.mode("overwrite").parquet(
        "s3a://gold/events_summary"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Gold – Chiffre d’affaires issu des fichiers
    """)
    return


@app.cell
def _(spark):
    from pyspark.sql.functions import sum as _sum
    files_gold_revenue = spark.read.parquet(
        "s3a://silver/files_online_retails"
    )

    files_revenue = files_gold_revenue \
        .agg(_sum("total_amount").alias("total_revenue_files"))

    files_revenue.write.mode("overwrite").parquet(
        "s3a://gold/revenue_from_files"
    )
    return (files_gold_revenue,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Gold – Top clients (fichiers)
    """)
    return


@app.cell
def _(col, files_gold_revenue):
    from pyspark.sql.functions import sum as _sum
    files_top_clients = files_gold_revenue \
        .groupBy("customer_id") \
        .agg(_sum("total_amount").alias("total_spent")) \
        .orderBy(col("total_spent").desc())

    files_top_clients.write.mode("overwrite").parquet(
        "s3a://gold/top_clients_from_files"
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
