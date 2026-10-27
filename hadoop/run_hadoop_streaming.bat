@echo off
REM Windows batch script to run Hadoop Streaming MapReduce for Retail Store-Wise Sales
set INPUT_HDFS=/user/retail/input/retail_transactions.csv
set OUTPUT_HDFS=/user/retail/output_store_sales

echo === 1. Cleaning old output directory ===
call hdfs dfs -rm -r -f %OUTPUT_HDFS%

echo === 2. Uploading input to HDFS ===
call hdfs dfs -mkdir -p /user/retail/input
call hdfs dfs -put -f ..\data\retail_transactions.csv /user/retail/input/

echo === 3. Running Hadoop Streaming MapReduce ===
call hadoop jar %HADOOP_HOME%\share\hadoop\tools\lib\hadoop-streaming-*.jar ^
    -files mapper.py,reducer.py ^
    -mapper "python mapper.py" ^
    -reducer "python reducer.py" ^
    -input %INPUT_HDFS% ^
    -output %OUTPUT_HDFS%

echo === 4. Results from HDFS ===
call hdfs dfs -cat %OUTPUT_HDFS%/part-*
