-- Исходная таблица (плоская денормализованная)
CREATE TABLE IF NOT EXISTS mock_data (
    id                    INTEGER,
    customer_first_name   VARCHAR(100),
    customer_last_name    VARCHAR(100),
    customer_age          INTEGER,
    customer_email        VARCHAR(200),
    customer_country      VARCHAR(100),
    customer_postal_code  VARCHAR(50),
    customer_pet_type     VARCHAR(50),
    customer_pet_name     VARCHAR(100),
    customer_pet_breed    VARCHAR(100),
    seller_first_name     VARCHAR(100),
    seller_last_name      VARCHAR(100),
    seller_email          VARCHAR(200),
    seller_country        VARCHAR(100),
    seller_postal_code    VARCHAR(50),
    product_name          VARCHAR(200),
    product_category      VARCHAR(100),
    product_price         NUMERIC(10,2),
    product_quantity      INTEGER,
    sale_date             VARCHAR(20),
    sale_customer_id      INTEGER,
    sale_seller_id        INTEGER,
    sale_product_id       INTEGER,
    sale_quantity         INTEGER,
    sale_total_price      NUMERIC(10,2),
    store_name            VARCHAR(200),
    store_location        VARCHAR(200),
    store_city            VARCHAR(100),
    store_state           VARCHAR(100),
    store_country         VARCHAR(100),
    store_phone           VARCHAR(50),
    store_email           VARCHAR(200),
    pet_category          VARCHAR(100),
    product_weight        NUMERIC(10,2),
    product_color         VARCHAR(100),
    product_size          VARCHAR(50),
    product_brand         VARCHAR(100),
    product_material      VARCHAR(100),
    product_description   TEXT,
    product_rating        NUMERIC(3,1),
    product_reviews       INTEGER,
    product_release_date  VARCHAR(20),
    product_expiry_date   VARCHAR(20),
    supplier_name         VARCHAR(200),
    supplier_contact      VARCHAR(200),
    supplier_email        VARCHAR(200),
    supplier_phone        VARCHAR(50),
    supplier_address      VARCHAR(200),
    supplier_city         VARCHAR(100),
    supplier_country      VARCHAR(100)
);

-- ============================================================
-- Схема «Звезда»
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_date (
    date_id   SERIAL PRIMARY KEY,
    full_date DATE,
    day       INTEGER,
    month     INTEGER,
    year      INTEGER,
    quarter   INTEGER
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id  INTEGER PRIMARY KEY,
    first_name   VARCHAR(100),
    last_name    VARCHAR(100),
    age          INTEGER,
    email        VARCHAR(200),
    country      VARCHAR(100),
    postal_code  VARCHAR(50),
    pet_type     VARCHAR(50),
    pet_name     VARCHAR(100),
    pet_breed    VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id    INTEGER PRIMARY KEY,
    name          VARCHAR(200),
    category      VARCHAR(100),
    price         NUMERIC(10,2),
    weight        NUMERIC(10,2),
    color         VARCHAR(100),
    size          VARCHAR(50),
    brand         VARCHAR(100),
    material      VARCHAR(100),
    description   TEXT,
    rating        NUMERIC(3,1),
    reviews_count INTEGER,
    release_date  DATE,
    expiry_date   DATE,
    pet_category  VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id    SERIAL PRIMARY KEY,
    name        VARCHAR(200),
    location    VARCHAR(200),
    city        VARCHAR(100),
    state       VARCHAR(100),
    country     VARCHAR(100),
    phone       VARCHAR(50),
    email       VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id  SERIAL PRIMARY KEY,
    name         VARCHAR(200),
    contact      VARCHAR(200),
    email        VARCHAR(200),
    phone        VARCHAR(50),
    address      VARCHAR(200),
    city         VARCHAR(100),
    country      VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id    INTEGER PRIMARY KEY,
    first_name   VARCHAR(100),
    last_name    VARCHAR(100),
    email        VARCHAR(200),
    country      VARCHAR(100),
    postal_code  VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id      INTEGER PRIMARY KEY,
    date_id      INTEGER REFERENCES dim_date(date_id),
    customer_id  INTEGER REFERENCES dim_customer(customer_id),
    product_id   INTEGER REFERENCES dim_product(product_id),
    store_id     INTEGER REFERENCES dim_store(store_id),
    supplier_id  INTEGER REFERENCES dim_supplier(supplier_id),
    seller_id    INTEGER REFERENCES dim_seller(seller_id),
    quantity     INTEGER,
    unit_price   NUMERIC(10,2),
    total_price  NUMERIC(10,2)
);
