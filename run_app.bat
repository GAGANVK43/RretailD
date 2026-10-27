@echo off
title Store-Wise Sales Analytics - Hadoop & Hive Web App
echo ====================================================================
echo Starting Retail Store-Wise Sales Hadoop MapReduce and Hive App...
echo ====================================================================
python -m pip install -r requirements.txt
start http://127.0.0.1:5000
python app.py
pause
