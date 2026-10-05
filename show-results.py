#!/usr/bin/env python3
import os
import sys
import json

os.environ['SPARK_HOME'] = 'C:\\spark'
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['JAVA_HOME'] = 'C:\\PROGRA~1\\ECLIPS~1\\JDK-11~1.6-H'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round

def create_spark_session():
    return SparkSession.builder \
        .appName("BDA_ShowResults") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def safe_print_df(df, num_rows=5):
    try:
        rows = df.take(num_rows)
        cols = df.columns
        header_str = " | ".join(cols)
        print(header_str)
        print("-" * len(header_str))
        for row in rows:
            safe_str = str(row.asDict()).encode('ascii', 'ignore').decode('ascii')
            print(safe_str)
    except Exception as e:
        print(f"Printing summary row error: {e}")

def show_results(spark):
    print("\n" + "="*60)
    print("BDA_PROJECT - RESULTS SUMMARY")
    print("="*60)

    # Locate the active path tracking file
    manifest = "C:/BDA_Project/output/latest_run.json"
    if not os.path.exists(manifest):
        print("Error: No active processing metadata directory found.")
        return
        
    with open(manifest, "r") as f:
        out_base = json.load(f)["latest_path"]

    # ---- Cleaned Data ----
    print("\n CLEANED DATA SUMMARY")
    print("-"*40)
    try:
        df_clean = spark.read.parquet(f"{out_base}/cleaned_data")
        print(f"Total Records: {df_clean.count()}")
        print(f"Columns: {df_clean.columns}")
        print("\nSample Records:")
        safe_print_df(df_clean, 5)
    except Exception as e:
        print(f"Error reading cleaned data: {e}")

    # ---- Sentiment Results ----
    print("\n SENTIMENT ANALYSIS RESULTS")
    print("-"*40)
    try:
        df_sentiment = spark.read.parquet(f"{out_base}/sentiment_results")
        print(f"Total Records Analyzed: {df_sentiment.count()}")
        print("\nSentiment Distribution:")
        df_sentiment.groupBy("sentiment") \
            .agg(count("*").alias("Count")) \
            .orderBy("Count", ascending=False) \
            .show()
        print("\nAverage Rating by Sentiment:")
        df_sentiment.groupBy("sentiment") \
            .agg(spark_round(avg("Rating"), 2).alias("Avg_Rating")) \
            .orderBy("Avg_Rating", ascending=False) \
            .show()
    except Exception as e:
        print(f"Error reading sentiment data: {e}")

    # ---- Recommendations ----
    print("\n PRODUCT RECOMMENDATIONS")
    print("-"*40)
    try:
        df_recs = spark.read.parquet(f"{out_base}/recommendations")
        print(f"Total Users with Recommendations: {df_recs.count()}")
        print("\nSample Recommendations (Top 5 Users):")
        df_recs.show(5, truncate=False)
    except Exception as e:
        print(f"Error reading recommendations: {e}")

    print("\n" + "="*60)
    print("END OF RESULTS")
    print("="*60 + "\n")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        show_results(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
