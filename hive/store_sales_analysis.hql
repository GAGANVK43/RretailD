-- ====================================================================
-- Apache Hive Script: Retail Transaction Store-Wise Sales Analysis
-- ====================================================================

-- 1. Create and Use Database
CREATE DATABASE IF NOT EXISTS retail_analytics;
USE retail_analytics;

-- 2. Drop existing table if needed
DROP TABLE IF EXISTS retail_transactions;

-- 3. Create External/Managed Table for Raw Retail Data
CREATE TABLE IF NOT EXISTS retail_transactions (
    txn_date STRING,
    txn_time STRING,
    store_location STRING,
    product_category STRING,
    sales_amount DOUBLE,
    payment_method STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- 4. Load retail dataset into Hive Table
-- Replace path with your HDFS or local file path
LOAD DATA LOCAL INPATH '../data/retail_transactions.csv' OVERWRITE INTO TABLE retail_transactions;

-- 5. Primary Requirement Query: Calculate Store-Wise Sales Totals
-- MapReduce job is automatically spawned by Hive engine to execute the GROUP BY
SELECT 
    store_location,
    ROUND(SUM(sales_amount), 2) AS total_sales,
    COUNT(1) AS total_transactions,
    ROUND(AVG(sales_amount), 2) AS avg_transaction_value,
    ROUND(MIN(sales_amount), 2) AS min_single_sale,
    ROUND(MAX(sales_amount), 2) AS max_single_sale
FROM retail_transactions
GROUP BY store_location
ORDER BY total_sales DESC;

-- 6. Export / Store Results into a Summary ORC Table for Fast Retrieval
CREATE TABLE IF NOT EXISTS store_wise_sales_summary
STORED AS ORC
AS
SELECT 
    store_location,
    ROUND(SUM(sales_amount), 2) AS total_sales,
    COUNT(1) AS transaction_count,
    ROUND(AVG(sales_amount), 2) AS avg_sale
FROM retail_transactions
GROUP BY store_location;

-- 7. Advanced Analytical Query: Top Performing Category per Store
SELECT 
    store_location,
    product_category,
    ROUND(SUM(sales_amount), 2) AS category_sales
FROM retail_transactions
GROUP BY store_location, product_category
ORDER BY store_location, category_sales DESC;
