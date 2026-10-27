# Store-Wise Sales Totals from Retail Transaction Data
### Big Data Processing with Apache Hadoop MapReduce & Apache Hive

---

## 1. Project Title & Problem Statement
* **Project Title:** Develop a MapReduce program to calculate store-wise sales totals from retail transaction data.
* **Problem Statement:** Large retail chains generate millions of sales transactions daily across multiple branch stores. Processing, aggregating, and calculating sales totals per store on a traditional relational database or single-machine system leads to memory exhaustion and severe performance bottlenecks.
* **Objective:** 
  1. Build a robust **MapReduce** program (in Python Streaming and native Java) to ingest raw retail transactions, map each transaction to a store key, shuffle/sort, and aggregate total revenue per store in the reducer.
  2. Implement equivalent data warehousing and analytical querying using **Apache Hive (HiveQL)**.
  3. Provide an intuitive, lightweight **Web Application** to interactively run the MapReduce job, execute HiveQL queries, inspect intermediate Map emissions, and visualize store-wise sales charts.

---

## 2. Main Process & System Architecture

```
                                    +-----------------------------------------+
                                    | Raw Retail Transaction Data (HDFS/CSV)  |
                                    | (Date, Time, Store, Category, Cost, Pay)|
                                    +-----------------------------------------+
                                                         |
                     +-----------------------------------+-----------------------------------+
                     |                                                                       |
                     v                                                                       v
   [ 1. Apache Hadoop MapReduce Engine ]                                   [ 2. Apache Hive SQL Engine ]
                     |                                                                       |
  +-------------------------------------+                                 +-------------------------------------+
  | Input Splits: Reads records line by |                                 | Hive Metastore & Driver:            |
  | line via TextInputFormat            |                                 | Parses HiveQL into AST & creates    |
  +-------------------------------------+                                 | MapReduce DAG                       |
                     |                                                    +-------------------------------------+
                     v                                                                       |
  +-------------------------------------+                                                    v
  | Map Phase (mapper.py):              |                                 +-------------------------------------+
  | Emits Key-Value pairs:              |                                 | HiveQL Execution:                   |
  | Key: Store Location                 |                                 | SELECT store, SUM(sales_amount)     |
  | Value: Sales Amount                 |                                 | FROM retail_transactions            |
  +-------------------------------------+                                 | GROUP BY store;                     |
                     |                                                    +-------------------------------------+
                     v                                                                       |
  +-------------------------------------+                                                    |
  | Shuffle & Sort:                     |                                                    |
  | Hadoop groups all sales amounts     |                                                    |
  | having the same Store key together  |                                                    |
  +-------------------------------------+                                                    |
                     |                                                                       |
                     v                                                                       |
  +-------------------------------------+                                                    |
  | Reduce Phase (reducer.py):          |                                                    |
  | Sums sales per store:               |                                                    |
  | Store \t Total Sales                |                                                    |
  +-------------------------------------+                                                    |
                     |                                                                       v
                     +-----------------------------------+-----------------------------------+
                                                         |
                                                         v
                                    +-----------------------------------------+
                                    | Interactive Web Dashboard & Analytics   |
                                    | - Instant MapReduce Job Execution       |
                                    | - Hive Query Workbench & MR Logs        |
                                    | - Dynamic Bar & Donut Visualizations    |
                                    | - CSV Data Export & Schema Management   |
                                    +-----------------------------------------+
```

---

## 3. Project Directory Structure
```
retial/
├── app.py                         # Lightweight Flask Web Application & MapReduce/Hive backend
├── requirements.txt               # Python dependencies (Flask, pandas)
├── README.md                      # Complete Project documentation & Viva guide
├── data/
│   ├── retail_transactions.csv    # 1,500+ realistic retail transaction dataset
│   └── generate_dataset.py        # Synthetic dataset generator script
├── hadoop/
│   ├── mapper.py                  # Hadoop Streaming Mapper (Python)
│   ├── reducer.py                 # Hadoop Streaming Reducer (Python)
│   ├── StoreSalesMapper.java      # Apache Hadoop Java Mapper
│   ├── StoreSalesReducer.java     # Apache Hadoop Java Reducer
│   ├── StoreSalesDriver.java      # Apache Hadoop Java Driver/Job config
│   ├── run_hadoop_streaming.sh    # Linux/Mac cluster execution script
│   └── run_hadoop_streaming.bat   # Windows cluster execution script
├── hive/
│   ├── store_sales_analysis.hql   # HiveQL DDL & Analytical Aggregation queries
│   └── run_hive.sh                # Script to execute Hive queries via CLI/Beeline
└── templates/
    └── index.html                 # Interactive Web UI (Bootstrap 5, Chart.js)
```

---

## 4. How to Run the Web Application

### Prerequisites
- Python 3.8+ (Already installed on your system)
- Flask and Pandas

### Quick Start
1. Open PowerShell or Terminal in the project directory:
   ```powershell
   cd c:\Users\Admin\Downloads\retial
   ```
2. Install dependencies (if needed):
   ```powershell
   pip install -r requirements.txt
   ```
3. Run the Flask Web Application:
   ```powershell
   python app.py
   ```
4. Open your web browser and navigate to:
   ```
   http://127.0.0.1:5000
   ```

---

## 5. MapReduce Program Breakdown

### Mapper (`hadoop/mapper.py`)
Reads each line from standard input (`stdin`), parses the delimited fields, extracts the store name and the sales cost, and emits a tab-separated Key-Value pair:
```python
# Key: Store Location, Value: Sales Amount
print(f"{store}\t{sales_amount:.2f}")
```

### Reducer (`hadoop/reducer.py`)
Because Hadoop's Shuffle & Sort guarantees that identical keys arrive consecutively at the reducer, the reducer maintains a running sum of the sales for the current store. When the store key changes, it emits the aggregated total:
```python
if current_store == store:
    current_total_sales += sales_amount
else:
    if current_store is not None:
        print(f"{current_store}\t{current_total_sales:.2f}")
    current_store = store
    current_total_sales = sales_amount
```

### Local CLI Verification (Piped Simulation)
You can verify the exact MapReduce streaming logic in PowerShell without starting a Hadoop daemon:
```powershell
Get-Content data/retail_transactions.csv | python hadoop/mapper.py | Sort-Object | python hadoop/reducer.py
```

---

## 6. Apache Hive Analysis

### Hive DDL & Table Creation
```sql
CREATE DATABASE IF NOT EXISTS retail_analytics;
USE retail_analytics;

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
```

### Store-Wise Sales Totals Query
```sql
SELECT 
    store_location,
    ROUND(SUM(sales_amount), 2) AS total_sales,
    COUNT(1) AS total_transactions,
    ROUND(AVG(sales_amount), 2) AS avg_transaction_value
FROM retail_transactions
GROUP BY store_location
ORDER BY total_sales DESC;
```

---

## 7. Running on an Actual Apache Hadoop Cluster

If deploying onto an actual Hadoop cluster (e.g., Cloudera, Hortonworks, AWS EMR, or standalone single-node):

```bash
# 1. Start Hadoop daemons
start-dfs.sh
start-yarn.sh

# 2. Upload dataset to HDFS
hdfs dfs -mkdir -p /user/retail/input
hdfs dfs -put data/retail_transactions.csv /user/retail/input/

# 3. Execute Hadoop Streaming
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files hadoop/mapper.py,hadoop/reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /user/retail/input/retail_transactions.csv \
    -output /user/retail/output_store_sales

# 4. View output from HDFS
hdfs dfs -cat /user/retail/output_store_sales/part-*

# 5. Run Hive Script
hive -f hive/store_sales_analysis.hql
```
