#!/bin/bash
# Shell script to run HiveQL script on Apache Hive
echo "=== Running Apache Hive Store-Wise Sales Analysis ==="
hive -f store_sales_analysis.hql
echo "=== Hive Analysis Execution Completed ==="
