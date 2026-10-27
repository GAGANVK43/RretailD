#!/bin/bash
# Shell script to run Store-Wise Sales MapReduce on Apache Hadoop Cluster
# Usage: ./run_hadoop_streaming.sh /path/to/input.csv /path/to/output

INPUT_HDFS=${1:-"/user/retail/input/retail_transactions.csv"}
OUTPUT_HDFS=${2:-"/user/retail/output_store_sales"}

echo "=== 1. Cleaning old output directory in HDFS if exists ==="
hdfs dfs -rm -r -f $OUTPUT_HDFS

echo "=== 2. Creating input directory in HDFS and uploading data ==="
hdfs dfs -mkdir -p /user/retail/input
hdfs dfs -put -f ../data/retail_transactions.csv /user/retail/input/

echo "=== 3. Executing Hadoop Streaming Job ==="
# Locate hadoop-streaming jar
STREAMING_JAR=$(find $HADOOP_HOME -name "hadoop-streaming*.jar" 2>/dev/null | head -n 1)

if [ -z "$STREAMING_JAR" ]; then
    STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar"
fi

hadoop jar $STREAMING_JAR \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input $INPUT_HDFS \
    -output $OUTPUT_HDFS

echo "=== 4. Displaying Results from HDFS ==="
hdfs dfs -cat $OUTPUT_HDFS/part-*
