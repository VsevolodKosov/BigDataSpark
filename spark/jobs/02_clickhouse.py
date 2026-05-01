"""
Задание 2: Создание 6 отчётных витрин в ClickHouse на основе схемы «Звезда».
Запись в ClickHouse выполняется через HTTP API (порт 8123).

Запуск:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/extra-jars/postgresql-42.7.3.jar \
    --driver-class-path /opt/spark/extra-jars/postgresql-42.7.3.jar \
    /opt/spark/jobs/02_clickhouse.py
"""

import urllib.request
import urllib.parse
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, rank, round as _round,
    when, lit
)
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/bigdata"
PG_PROPS = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

CH_HOST = "http://clickhouse:8123"
CH_USER = "admin"
CH_PASS = "admin"
CH_DB   = "bigdata"


def ch_exec(sql):
    url = f"{CH_HOST}/?user={CH_USER}&password={CH_PASS}"
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode("utf-8")


def write_ch(df, table):
    rows = df.collect()
    if not rows:
        print(f"{table}: 0 строк")
        return

    ch_exec(f"TRUNCATE TABLE {CH_DB}.{table}")

    # Формируем VALUES батчами по 1000 строк
    batch_size = 1000
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        vals_list = []
        for row in batch:
            parts = []
            for v in row:
                if v is None:
                    parts.append("NULL")
                elif isinstance(v, str):
                    escaped = v.replace("\\", "\\\\").replace("'", "\\'")
                    parts.append(f"'{escaped}'")
                elif isinstance(v, bool):
                    parts.append("1" if v else "0")
                else:
                    parts.append(str(v))
            vals_list.append(f"({','.join(parts)})")
        sql = f"INSERT INTO {CH_DB}.{table} VALUES {','.join(vals_list)}"
        ch_exec(sql)
        total += len(batch)

    print(f"{table}: {total} строк записано")


spark = SparkSession.builder \
    .appName("02_clickhouse_reports") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def read_pg(table):
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)


fact         = read_pg("fact_sales")
dim_product  = read_pg("dim_product")
dim_customer = read_pg("dim_customer")
dim_date     = read_pg("dim_date")
dim_store    = read_pg("dim_store")
dim_supplier = read_pg("dim_supplier")

fact.cache()

# ── 1. Витрина продаж по продуктам ───────────────────────────────────────────
fp = fact.join(dim_product, "product_id")

product_agg = fp.groupBy("product_id", "name", "category", "rating", "reviews_count") \
    .agg(
        _round(_sum("total_price"), 2).alias("total_revenue"),
        _sum("quantity").alias("total_quantity")
    )

w_rank = Window.orderBy(col("total_revenue").desc())
report_product_sales = product_agg \
    .withColumn("avg_rating",   _round(col("rating").cast("double"), 2)) \
    .withColumn("total_reviews", col("reviews_count").cast("long")) \
    .withColumn("sales_rank",   rank().over(w_rank).cast("int")) \
    .select(
        col("product_id").cast("int"),
        col("name").alias("product_name"),
        col("category").alias("product_category"),
        col("total_revenue").cast("double"),
        col("total_quantity").cast("long"),
        col("avg_rating").cast("double"),
        col("total_reviews").cast("long"),
        col("sales_rank").cast("int")
    )

write_ch(report_product_sales, "report_product_sales")

# ── 2. Витрина продаж по клиентам ────────────────────────────────────────────
fc = fact.join(dim_customer, "customer_id")

customer_agg = fc.groupBy("customer_id", "first_name", "last_name", "country") \
    .agg(
        _round(_sum("total_price"), 2).alias("total_purchases"),
        count("sale_id").alias("orders_count")
    ) \
    .withColumn("avg_check", _round(col("total_purchases") / col("orders_count"), 2))

w_crank = Window.orderBy(col("total_purchases").desc())
report_customer_sales = customer_agg \
    .withColumn("full_name",  col("first_name") + lit(" ") + col("last_name")) \
    .withColumn("sales_rank", rank().over(w_crank).cast("int")) \
    .select(
        col("customer_id").cast("int"),
        col("full_name").cast("string"),
        col("country").cast("string"),
        col("total_purchases").cast("double"),
        col("orders_count").cast("long"),
        col("avg_check").cast("double"),
        col("sales_rank").cast("int")
    )

write_ch(report_customer_sales, "report_customer_sales")

# ── 3. Витрина продаж по времени ─────────────────────────────────────────────
ft = fact.join(dim_date, "date_id")

monthly = ft.groupBy("year", "month") \
    .agg(
        _round(_sum("total_price"), 2).alias("monthly_revenue"),
        count("sale_id").alias("monthly_orders"),
        _round(avg("total_price"), 2).alias("avg_order_size")
    )

yearly = ft.groupBy("year") \
    .agg(_round(_sum("total_price"), 2).alias("yearly_revenue"))

report_time_sales = monthly.join(yearly, "year") \
    .select(
        col("year").cast("int"),
        col("month").cast("int"),
        col("monthly_revenue").cast("double"),
        col("monthly_orders").cast("long"),
        col("avg_order_size").cast("double"),
        col("yearly_revenue").cast("double")
    ).orderBy("year", "month")

write_ch(report_time_sales, "report_time_sales")

# ── 4. Витрина продаж по магазинам ───────────────────────────────────────────
fs = fact.join(dim_store, "store_id")

store_agg = fs.groupBy("store_id", "name", "city", "country") \
    .agg(
        _round(_sum("total_price"), 2).alias("total_revenue"),
        count("sale_id").alias("total_orders")
    ) \
    .withColumn("avg_check", _round(col("total_revenue") / col("total_orders"), 2))

w_srank = Window.orderBy(col("total_revenue").desc())
report_store_sales = store_agg \
    .withColumn("sales_rank", rank().over(w_srank).cast("int")) \
    .select(
        col("store_id").cast("int"),
        col("name").alias("store_name"),
        col("city").cast("string"),
        col("country").cast("string"),
        col("total_revenue").cast("double"),
        col("total_orders").cast("long"),
        col("avg_check").cast("double"),
        col("sales_rank").cast("int")
    )

write_ch(report_store_sales, "report_store_sales")

# ── 5. Витрина продаж по поставщикам ─────────────────────────────────────────
fsp = fact.join(dim_supplier, "supplier_id") \
          .join(dim_product, "product_id")

supplier_agg = fsp.groupBy(
    "supplier_id",
    dim_supplier["name"].alias("sup_name"),
    dim_supplier["country"].alias("sup_country")
).agg(
    _round(_sum("total_price"), 2).alias("total_revenue"),
    _round(avg("unit_price"), 2).alias("avg_product_price"),
    count("sale_id").alias("orders_count")
)

w_sprank = Window.orderBy(col("total_revenue").desc())
report_supplier_sales = supplier_agg \
    .withColumn("sales_rank", rank().over(w_sprank).cast("int")) \
    .select(
        col("supplier_id").cast("int"),
        col("sup_name").alias("supplier_name"),
        col("sup_country").alias("supplier_country"),
        col("total_revenue").cast("double"),
        col("avg_product_price").cast("double"),
        col("orders_count").cast("long"),
        col("sales_rank").cast("int")
    )

write_ch(report_supplier_sales, "report_supplier_sales")

# ── 6. Витрина качества продукции ────────────────────────────────────────────
fq = fact.join(dim_product, "product_id")

quality_agg = fq.groupBy(
    "product_id", "name", "category", "rating", "reviews_count"
).agg(
    _sum("quantity").alias("total_sales_qty"),
    _round(_sum("total_price"), 2).alias("total_revenue")
)

global_avg = quality_agg.agg(avg("rating").alias("g_avg")).collect()[0]["g_avg"]

report_product_quality = quality_agg \
    .withColumn("avg_rating",      _round(col("rating").cast("double"), 2)) \
    .withColumn("total_reviews",   col("reviews_count").cast("long")) \
    .withColumn("rating_category", when(col("rating") >= global_avg, lit("high")).otherwise(lit("low"))) \
    .select(
        col("product_id").cast("int"),
        col("name").alias("product_name"),
        col("category").alias("product_category"),
        col("avg_rating").cast("double"),
        col("total_reviews").cast("long"),
        col("total_sales_qty").cast("long"),
        col("total_revenue").cast("double"),
        col("rating_category").cast("string")
    )

write_ch(report_product_quality, "report_product_quality")

fact.unpersist()
spark.stop()
