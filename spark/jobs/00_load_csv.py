"""
Задание 0: Загрузка CSV-файлов в PostgreSQL (таблица mock_data).

Запуск:
  docker exec spark-master spark-submit \
    --packages org.postgresql:postgresql:42.7.3 \
    /opt/spark/jobs/00_load_csv.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

PG_URL = "jdbc:postgresql://postgres:5432/bigdata"
PG_PROPS = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

DATA_PATH = "/opt/spark/data/*.csv"

spark = SparkSession.builder \
    .appName("00_load_csv_to_postgres") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .option("header", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .option("quote", '"') \
    .option("inferSchema", "false") \
    .csv(DATA_PATH)

df_typed = df \
    .withColumn("id",               col("id").cast(IntegerType())) \
    .withColumn("customer_age",     col("customer_age").cast(IntegerType())) \
    .withColumn("product_price",    col("product_price").cast(DoubleType())) \
    .withColumn("product_quantity", col("product_quantity").cast(IntegerType())) \
    .withColumn("sale_customer_id", col("sale_customer_id").cast(IntegerType())) \
    .withColumn("sale_seller_id",   col("sale_seller_id").cast(IntegerType())) \
    .withColumn("sale_product_id",  col("sale_product_id").cast(IntegerType())) \
    .withColumn("sale_quantity",    col("sale_quantity").cast(IntegerType())) \
    .withColumn("sale_total_price", col("sale_total_price").cast(DoubleType())) \
    .withColumn("product_weight",   col("product_weight").cast(DoubleType())) \
    .withColumn("product_rating",   col("product_rating").cast(DoubleType())) \
    .withColumn("product_reviews",  col("product_reviews").cast(IntegerType()))

df_typed.write \
    .jdbc(url=PG_URL, table="mock_data", mode="overwrite", properties=PG_PROPS)

print(f"Загружено строк: {df_typed.count()}")

spark.stop()
