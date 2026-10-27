#!/usr/bin/env python3
"""
Flask Web Application: Store-Wise Sales Retail Analytics using Hadoop & Hive
"""
import os
import sys
import time
import subprocess
import sqlite3
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_file, Response
from pathlib import Path

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max upload

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
HADOOP_DIR = BASE_DIR / "hadoop"
HIVE_DIR = BASE_DIR / "hive"
DEFAULT_DATASET = DATA_DIR / "retail_transactions.csv"

# Global state to store current dataset path
CURRENT_DATASET = DEFAULT_DATASET

def get_current_df():
    global CURRENT_DATASET
    if not CURRENT_DATASET.exists():
        # Fallback to default
        CURRENT_DATASET = DEFAULT_DATASET
    
    # Try reading csv
    df = pd.read_csv(CURRENT_DATASET)
    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]
    return df

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/summary", methods=["GET"])
def get_summary():
    try:
        df = get_current_df()
        total_records = len(df)
        
        # Identify columns
        store_col = next((c for c in df.columns if 'store' in c), df.columns[2] if len(df.columns) > 2 else df.columns[0])
        sales_col = next((c for c in df.columns if 'sale' in c or 'amount' in c or 'cost' in c), df.columns[4] if len(df.columns) > 4 else df.columns[1])
        category_col = next((c for c in df.columns if 'cat' in c), None)

        # Convert sales to numeric
        df[sales_col] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0.0)

        total_sales = float(df[sales_col].sum())
        total_stores = int(df[store_col].nunique())
        avg_sale = float(df[sales_col].mean()) if total_records > 0 else 0.0

        # Sample preview (first 10 records)
        preview_records = df.head(10).to_dict(orient="records")

        return jsonify({
            "success": True,
            "filename": CURRENT_DATASET.name,
            "total_records": total_records,
            "total_sales": round(total_sales, 2),
            "total_stores": total_stores,
            "avg_transaction": round(avg_sale, 2),
            "columns": list(df.columns),
            "preview": preview_records
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/upload", methods=["POST"])
def upload_dataset():
    global CURRENT_DATASET
    if 'file' not in request.files:
        return jsonify({"success": False, "error": "No file uploaded"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"success": False, "error": "Empty filename"}), 400

    upload_path = DATA_DIR / f"uploaded_{file.filename}"
    file.save(upload_path)
    CURRENT_DATASET = upload_path

    return jsonify({"success": True, "message": f"Successfully loaded {file.filename}"})

@app.route("/api/reset-dataset", methods=["POST"])
def reset_dataset():
    global CURRENT_DATASET
    CURRENT_DATASET = DEFAULT_DATASET
    return jsonify({"success": True, "message": "Reset to default sample dataset"})

@app.route("/api/generate-dataset", methods=["POST"])
def generate_dataset_endpoint():
    global CURRENT_DATASET
    data = request.get_json(silent=True) or {}
    count = int(data.get("count", 1000))
    count = max(100, min(count, 50000))

    gen_script = DATA_DIR / "generate_dataset.py"
    target_file = DATA_DIR / "retail_transactions.csv"
    
    subprocess.run([sys.executable, str(gen_script), str(count), str(target_file)], check=True)
    CURRENT_DATASET = target_file
    return jsonify({"success": True, "message": f"Generated {count} records successfully!"})

@app.route("/api/run-mapreduce", methods=["POST"])
def run_mapreduce():
    """
    Executes actual MapReduce streaming:
    1. Input Split -> Feed dataset lines to mapper.py
    2. Map Phase -> mapper.py outputs (Store, Sales_Amount)
    3. Shuffle & Sort Phase -> Sort by Store key
    4. Reduce Phase -> reducer.py outputs (Store, Total_Sales)
    """
    t_start = time.time()
    mapper_path = HADOOP_DIR / "mapper.py"
    reducer_path = HADOOP_DIR / "reducer.py"

    try:
        # Step 1 & 2: Mapper Phase
        with open(CURRENT_DATASET, "r", encoding="utf-8", errors="replace") as infile:
            p_map = subprocess.Popen(
                [sys.executable, str(mapper_path)],
                stdin=infile,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            map_stdout, map_stderr = p_map.communicate()

        if p_map.returncode != 0:
            return jsonify({"success": False, "error": f"Mapper failed: {map_stderr}"}), 500

        map_lines = [line.strip() for line in map_stdout.strip().split("\n") if line.strip()]
        total_map_emissions = len(map_lines)
        sample_map_output = map_lines[:15]  # first 15 key-value pairs

        # Step 3: Shuffle & Sort
        sorted_map_lines = sorted(map_lines, key=lambda x: x.split("\t")[0] if "\t" in x else x)
        sorted_input_text = "\n".join(sorted_map_lines)

        # Step 4: Reducer Phase
        p_reduce = subprocess.Popen(
            [sys.executable, str(reducer_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        reduce_stdout, reduce_stderr = p_reduce.communicate(input=sorted_input_text)

        if p_reduce.returncode != 0:
            return jsonify({"success": False, "error": f"Reducer failed: {reduce_stderr}"}), 500

        t_end = time.time()
        elapsed_sec = round(t_end - t_start, 4)

        # Parse Reducer output
        results = []
        for line in reduce_stdout.strip().split("\n"):
            line = line.strip()
            if not line or "\t" not in line:
                continue
            parts = line.split("\t")
            results.append({
                "store": parts[0],
                "total_sales": float(parts[1])
            })

        # Sort descending by total sales
        results.sort(key=lambda x: x["total_sales"], reverse=True)

        return jsonify({
            "success": True,
            "elapsed_seconds": elapsed_sec,
            "total_mapped_pairs": total_map_emissions,
            "sample_map_output": sample_map_output,
            "unique_stores": len(results),
            "results": results
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/run-hive", methods=["POST"])
def run_hive():
    """
    Simulates Apache Hive execution:
    - Parses HiveQL query
    - Translates query into relational execution / MapReduce stage simulation
    - Executes via in-memory SQLite table loaded from current dataset
    - Generates Hive execution log showing MapReduce Task 1, Map 100%, Reduce 100%
    """
    data = request.get_json(silent=True) or {}
    query_type = data.get("query_type", "store_sales")
    custom_query = data.get("custom_query", "").strip()

    t_start = time.time()
    
    # Predefined HiveQL queries
    queries = {
        "store_sales": (
            "-- HiveQL Query: Store-Wise Total Sales Aggregation\n"
            "SELECT \n"
            "    store AS store_location,\n"
            "    ROUND(SUM(sales_amount), 2) AS total_sales,\n"
            "    COUNT(1) AS transaction_count,\n"
            "    ROUND(AVG(sales_amount), 2) AS avg_sale,\n"
            "    ROUND(MIN(sales_amount), 2) AS min_sale,\n"
            "    ROUND(MAX(sales_amount), 2) AS max_sale\n"
            "FROM retail_transactions\n"
            "GROUP BY store\n"
            "ORDER BY total_sales DESC;"
        ),
        "top_stores": (
            "-- HiveQL Query: Top 5 Highest Performing Stores\n"
            "SELECT \n"
            "    store AS store_location,\n"
            "    ROUND(SUM(sales_amount), 2) AS total_sales\n"
            "FROM retail_transactions\n"
            "GROUP BY store\n"
            "ORDER BY total_sales DESC\n"
            "LIMIT 5;"
        ),
        "category_breakdown": (
            "-- HiveQL Query: Store & Category Cross Breakdown\n"
            "SELECT \n"
            "    store AS store_location,\n"
            "    category,\n"
            "    ROUND(SUM(sales_amount), 2) AS category_sales,\n"
            "    COUNT(1) AS item_count\n"
            "FROM retail_transactions\n"
            "GROUP BY store, category\n"
            "ORDER BY store, category_sales DESC\n"
            "LIMIT 25;"
        ),
        "payment_analysis": (
            "-- HiveQL Query: Payment Method Distribution\n"
            "SELECT \n"
            "    payment_method,\n"
            "    COUNT(1) AS transactions,\n"
            "    ROUND(SUM(sales_amount), 2) AS total_amount\n"
            "FROM retail_transactions\n"
            "GROUP BY payment_method\n"
            "ORDER BY total_amount DESC;"
        )
    }

    if custom_query:
        hql_query = custom_query
    else:
        hql_query = queries.get(query_type, queries["store_sales"])

    try:
        # Load data into SQLite for rapid relational execution
        df = get_current_df()
        
        # Standardize column names for Hive schema
        rename_map = {}
        for c in df.columns:
            if 'store' in c: rename_map[c] = 'store'
            elif 'sale' in c or 'amount' in c: rename_map[c] = 'sales_amount'
            elif 'date' in c: rename_map[c] = 'txn_date'
            elif 'time' in c: rename_map[c] = 'txn_time'
            elif 'cat' in c: rename_map[c] = 'category'
            elif 'pay' in c: rename_map[c] = 'payment_method'
        df_renamed = df.rename(columns=rename_map)

        conn = sqlite3.connect(":memory:")
        df_renamed.to_sql("retail_transactions", conn, if_exists="replace", index=False)

        # Convert HiveQL syntax to SQLite compatible syntax if needed
        # Strip comments
        sql_clean = "\n".join([line for line in hql_query.split("\n") if not line.strip().startswith("--")]).strip()
        
        cursor = conn.cursor()
        cursor.execute(sql_clean)
        col_names = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        conn.close()

        t_end = time.time()
        elapsed = round(t_end - t_start, 4)

        # Generate realistic Hive MapReduce Job Execution Log
        hive_log = [
            "Query ID = hive_retail_" + str(int(time.time())),
            "Total jobs = 1",
            "Launching Job 1 out of 1",
            "Number of reduce tasks not specified. Estimated: 1",
            "In order to change the average load for a reducer (in bytes): set hive.exec.reducers.bytes.per.reducer=<number>",
            "Starting Job = job_" + str(int(time.time())) + "_0001, Tracking URL = http://hadoop-master:8088/proxy/application/",
            "Kill Command = yarn application -kill application_" + str(int(time.time())) + "_0001",
            "Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1",
            "Stage-1 map = 0%,  reduce = 0%",
            "Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.25 sec",
            "Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 2.45 sec",
            f"MapReduce Total cumulative CPU time: {round(elapsed * 1.5, 2)} seconds",
            "Ended Job = job_" + str(int(time.time())) + "_0001",
            f"MapReduce Jobs Launched: Stage-1: Map: 1  Reduce: 1   Cumulative CPU: {round(elapsed * 1.5, 2)} sec   HDFS Read: {len(df) * 65} bytes  HDFS Write: {len(rows) * 45} bytes",
            f"SUCCESS: Total records returned: {len(rows)} in {elapsed} seconds"
        ]

        result_data = [dict(zip(col_names, row)) for row in rows]

        return jsonify({
            "success": True,
            "hiveql_query": hql_query,
            "columns": col_names,
            "rows": result_data,
            "row_count": len(rows),
            "elapsed_seconds": elapsed,
            "hive_execution_log": hive_log
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e), "hiveql_query": hql_query}), 500

@app.route("/api/source-code", methods=["GET"])
def get_source_code():
    """Returns source code of Hadoop & Hive files for in-browser inspection."""
    file_key = request.args.get("file", "mapper.py")
    file_paths = {
        "mapper.py": HADOOP_DIR / "mapper.py",
        "reducer.py": HADOOP_DIR / "reducer.py",
        "StoreSalesMapper.java": HADOOP_DIR / "StoreSalesMapper.java",
        "StoreSalesReducer.java": HADOOP_DIR / "StoreSalesReducer.java",
        "StoreSalesDriver.java": HADOOP_DIR / "StoreSalesDriver.java",
        "run_hadoop_streaming.sh": HADOOP_DIR / "run_hadoop_streaming.sh",
        "store_sales_analysis.hql": HIVE_DIR / "store_sales_analysis.hql"
    }

    target = file_paths.get(file_key)
    if not target or not target.exists():
        return jsonify({"success": False, "error": "File not found"}), 404

    with open(target, "r", encoding="utf-8") as f:
        code_content = f.read()

    return jsonify({
        "success": True,
        "filename": file_key,
        "code": code_content
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting Retail Store-Wise Sales Hadoop/Hive Analytics App on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)
