-- 1. Витрина продаж по продуктам
CREATE TABLE IF NOT EXISTS bigdata.report_product_sales (
    product_id       Int32,
    product_name     String,
    product_category String,
    total_revenue    Float64,
    total_quantity   Int64,
    avg_rating       Float64,
    total_reviews    Int64,
    sales_rank       Int32
) ENGINE = MergeTree()
ORDER BY product_id;

-- 2. Витрина продаж по клиентам
CREATE TABLE IF NOT EXISTS bigdata.report_customer_sales (
    customer_id      Int32,
    full_name        String,
    country          String,
    total_purchases  Float64,
    orders_count     Int64,
    avg_check        Float64,
    sales_rank       Int32
) ENGINE = MergeTree()
ORDER BY customer_id;

-- 3. Витрина продаж по времени
CREATE TABLE IF NOT EXISTS bigdata.report_time_sales (
    year              Int32,
    month             Int32,
    monthly_revenue   Float64,
    monthly_orders    Int64,
    avg_order_size    Float64,
    yearly_revenue    Float64
) ENGINE = MergeTree()
ORDER BY (year, month);

-- 4. Витрина продаж по магазинам
CREATE TABLE IF NOT EXISTS bigdata.report_store_sales (
    store_id       Int32,
    store_name     String,
    city           String,
    country        String,
    total_revenue  Float64,
    total_orders   Int64,
    avg_check      Float64,
    sales_rank     Int32
) ENGINE = MergeTree()
ORDER BY store_id;

-- 5. Витрина продаж по поставщикам
CREATE TABLE IF NOT EXISTS bigdata.report_supplier_sales (
    supplier_id       Int32,
    supplier_name     String,
    supplier_country  String,
    total_revenue     Float64,
    avg_product_price Float64,
    orders_count      Int64,
    sales_rank        Int32
) ENGINE = MergeTree()
ORDER BY supplier_id;

-- 6. Витрина качества продукции
CREATE TABLE IF NOT EXISTS bigdata.report_product_quality (
    product_id        Int32,
    product_name      String,
    product_category  String,
    avg_rating        Float64,
    total_reviews     Int64,
    total_sales_qty   Int64,
    total_revenue     Float64,
    rating_category   String
) ENGINE = MergeTree()
ORDER BY product_id;
