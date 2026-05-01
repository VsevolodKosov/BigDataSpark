"""
Задание 1: Трансформация mock_data → схема «Звезда» в PostgreSQL.

Запуск:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/extra-jars/postgresql-42.7.3.jar \
    --driver-class-path /opt/spark/extra-jars/postgresql-42.7.3.jar \
    /opt/spark/jobs/01_star_schema.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, dayofmonth, month, year, quarter,
    dense_rank, lit
)
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/bigdata"
PG_PROPS = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("01_star_schema") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

src = spark.read.jdbc(url=PG_URL, table="mock_data", properties=PG_PROPS)

# Очищаем таблицы через JVM JDBC (после первого read драйвер уже загружен)
def exec_sql(sql):
    jvm = spark._jvm
    jvm.Class.forName("org.postgresql.Driver")
    conn = jvm.java.sql.DriverManager.getConnection(PG_URL, "admin", "admin")
    stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.close()
    conn.close()

exec_sql(
    "TRUNCATE TABLE fact_sales, dim_date, dim_customer, dim_product, "
    "dim_store, dim_supplier, dim_seller RESTART IDENTITY CASCADE"
)
print("Таблицы очищены")
src.cache()

# ── dim_date ────────────────────────────────────────────────────────────────
dates = src.select(
    to_date(col("sale_date"), "M/d/yyyy").alias("full_date")
).distinct().dropna()

w_date = Window.orderBy("full_date")
dim_date = dates \
    .withColumn("date_id",  dense_rank().over(w_date).cast("int")) \
    .withColumn("day",      dayofmonth("full_date")) \
    .withColumn("month",    month("full_date")) \
    .withColumn("year",     year("full_date")) \
    .withColumn("quarter",  quarter("full_date")) \
    .select("date_id", "full_date", "day", "month", "year", "quarter")

dim_date.write.jdbc(url=PG_URL, table="dim_date", mode="append", properties=PG_PROPS)
print(f"dim_date: {dim_date.count()} строк")

# ── dim_customer ─────────────────────────────────────────────────────────────
dim_customer = src.select(
    col("sale_customer_id").alias("customer_id"),
    col("customer_first_name").alias("first_name"),
    col("customer_last_name").alias("last_name"),
    col("customer_age").cast("int").alias("age"),
    col("customer_email").alias("email"),
    col("customer_country").alias("country"),
    col("customer_postal_code").alias("postal_code"),
    col("customer_pet_type").alias("pet_type"),
    col("customer_pet_name").alias("pet_name"),
    col("customer_pet_breed").alias("pet_breed")
).dropDuplicates(["customer_id"])

dim_customer.write.jdbc(url=PG_URL, table="dim_customer", mode="append", properties=PG_PROPS)
print(f"dim_customer: {dim_customer.count()} строк")

# ── dim_product ───────────────────────────────────────────────────────────────
dim_product = src.select(
    col("sale_product_id").alias("product_id"),
    col("product_name").alias("name"),
    col("product_category").alias("category"),
    col("product_price").cast("double").alias("price"),
    col("product_weight").cast("double").alias("weight"),
    col("product_color").alias("color"),
    col("product_size").alias("size"),
    col("product_brand").alias("brand"),
    col("product_material").alias("material"),
    col("product_description").alias("description"),
    col("product_rating").cast("double").alias("rating"),
    col("product_reviews").cast("int").alias("reviews_count"),
    to_date(col("product_release_date"), "M/d/yyyy").alias("release_date"),
    to_date(col("product_expiry_date"), "M/d/yyyy").alias("expiry_date"),
    col("pet_category")
).dropDuplicates(["product_id"])

dim_product.write.jdbc(url=PG_URL, table="dim_product", mode="append", properties=PG_PROPS)
print(f"dim_product: {dim_product.count()} строк")

# ── dim_seller ────────────────────────────────────────────────────────────────
dim_seller = src.select(
    col("sale_seller_id").alias("seller_id"),
    col("seller_first_name").alias("first_name"),
    col("seller_last_name").alias("last_name"),
    col("seller_email").alias("email"),
    col("seller_country").alias("country"),
    col("seller_postal_code").alias("postal_code")
).dropDuplicates(["seller_id"])

dim_seller.write.jdbc(url=PG_URL, table="dim_seller", mode="append", properties=PG_PROPS)
print(f"dim_seller: {dim_seller.count()} строк")

# ── dim_store ─────────────────────────────────────────────────────────────────
stores_raw = src.select(
    col("store_name"),
    col("store_location").alias("location"),
    col("store_city").alias("city"),
    col("store_state").alias("state"),
    col("store_country").alias("country"),
    col("store_phone").alias("phone"),
    col("store_email").alias("email")
).dropDuplicates(["store_name"])

w_store = Window.orderBy("store_name")
dim_store = stores_raw \
    .withColumn("store_id", dense_rank().over(w_store).cast("int")) \
    .withColumnRenamed("store_name", "name") \
    .select("store_id", "name", "location", "city", "state", "country", "phone", "email")

dim_store.write.jdbc(url=PG_URL, table="dim_store", mode="append", properties=PG_PROPS)
print(f"dim_store: {dim_store.count()} строк")

# ── dim_supplier ──────────────────────────────────────────────────────────────
suppliers_raw = src.select(
    col("supplier_name"),
    col("supplier_contact").alias("contact"),
    col("supplier_email").alias("email"),
    col("supplier_phone").alias("phone"),
    col("supplier_address").alias("address"),
    col("supplier_city").alias("city"),
    col("supplier_country").alias("country")
).dropDuplicates(["supplier_name"])

w_sup = Window.orderBy("supplier_name")
dim_supplier = suppliers_raw \
    .withColumn("supplier_id", dense_rank().over(w_sup).cast("int")) \
    .withColumnRenamed("supplier_name", "name") \
    .select("supplier_id", "name", "contact", "email", "phone", "address", "city", "country")

dim_supplier.write.jdbc(url=PG_URL, table="dim_supplier", mode="append", properties=PG_PROPS)
print(f"dim_supplier: {dim_supplier.count()} строк")

# ── fact_sales ────────────────────────────────────────────────────────────────
src_with_store = src.join(
    dim_store.select("store_id", col("name").alias("store_name")),
    on="store_name",
    how="left"
)

src_full = src_with_store.join(
    dim_supplier.select("supplier_id", col("name").alias("supplier_name")),
    on="supplier_name",
    how="left"
)

fact_sales = src_full.join(
    dim_date.select("date_id", "full_date"),
    on=(to_date(col("sale_date"), "M/d/yyyy") == col("full_date")),
    how="left"
).select(
    col("id").cast("int").alias("sale_id"),
    col("date_id").cast("int"),
    col("sale_customer_id").cast("int").alias("customer_id"),
    col("sale_product_id").cast("int").alias("product_id"),
    col("store_id").cast("int"),
    col("supplier_id").cast("int"),
    col("sale_seller_id").cast("int").alias("seller_id"),
    col("sale_quantity").cast("int").alias("quantity"),
    col("product_price").cast("double").alias("unit_price"),
    col("sale_total_price").cast("double").alias("total_price")
)

fact_sales.write.jdbc(url=PG_URL, table="fact_sales", mode="overwrite", properties=PG_PROPS)
print(f"fact_sales: {fact_sales.count()} строк")

src.unpersist()

# Восстанавливаем FK-ограничения (Spark пересоздаёт fact_sales без них)
exec_sql("""
    ALTER TABLE fact_sales ADD CONSTRAINT fk_date     FOREIGN KEY (date_id)     REFERENCES dim_date(date_id);
    ALTER TABLE fact_sales ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id);
    ALTER TABLE fact_sales ADD CONSTRAINT fk_product  FOREIGN KEY (product_id)  REFERENCES dim_product(product_id);
    ALTER TABLE fact_sales ADD CONSTRAINT fk_store    FOREIGN KEY (store_id)    REFERENCES dim_store(store_id);
    ALTER TABLE fact_sales ADD CONSTRAINT fk_supplier FOREIGN KEY (supplier_id) REFERENCES dim_supplier(supplier_id);
    ALTER TABLE fact_sales ADD CONSTRAINT fk_seller   FOREIGN KEY (seller_id)   REFERENCES dim_seller(seller_id)
""")
print("FK-ограничения восстановлены")

spark.stop()
