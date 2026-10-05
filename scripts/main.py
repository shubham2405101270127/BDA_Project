#!/usr/bin/env python3
"""
BDA_project - Universal Orchestration Script with Hybrid MongoDB Ingestion
Dynamically structures any review dataset and handles NoSQL routing via fallbacks.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, trim, regexp_replace, regexp_extract, to_json
from pyspark.ml.recommendation import ALS
import sys
import os
import json

def create_spark_session():
    """Initialize Spark Session with a universal configuration"""
    return SparkSession.builder \
        .appName("BDA_Project") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def load_data(spark, path):
    """Load any CSV cleanly by automatically handling quotes and line breaks"""
    print(f"Loading data from {path}...")
    df = spark.read.csv(path, header=True, inferSchema=True, multiLine=True, escape='"')
    print(f"Loaded {df.count()} records")
    return df

def auto_detect_columns(columns):
    """Scans column headers for keywords and maps them to standard entities."""
    cols_lower = [c.lower().strip() for c in columns]
    
    review_keywords = ['review_text', 'text', 'body', 'review_body', 'comment', 'desc', 'review']
    review_col = columns[0]
    for kw in review_keywords:
        if kw in cols_lower:
            review_col = columns[cols_lower.index(kw)]
            break
            
    rating_keywords = ['review_rating', 'rating', 'stars', 'score', 'points']
    rating_col = columns[-1]
    for kw in rating_keywords:
        if kw in cols_lower:
            rating_col = columns[cols_lower.index(kw)]
            break

    product_keywords = ['product_name', 'product_title', 'product', 'item', 'asin', 'title', 'brand', 'product_id']
    product_col = columns[0]
    for kw in product_keywords:
        if kw in cols_lower and columns[cols_lower.index(kw)] != review_col:
            product_col = columns[cols_lower.index(kw)]
            break
            
    return review_col, rating_col, product_col

def clean_and_prepare_data(df):
    """Dynamically parses and structures columns for the pipeline"""
    print("Starting smart data cleaning...")
    df = df.dropDuplicates()
    rev_col, rat_col, prod_col = auto_detect_columns(df.columns)
    print(f"[METADATA DETECTED] Text: '{rev_col}' | Rating: '{rat_col}' | Target: '{prod_col}'")
    
    df = df.dropna(subset=[rev_col])
    df = df.withColumn(
        'review_clean',
        lower(trim(regexp_replace(col(rev_col).cast("string"), r'[^a-zA-Z0-9\s]', '')))
    )
    df = df.withColumn(
        "Rating_Cleaned", 
        regexp_extract(col(rat_col).cast("string"), r"(\d+\.?\d*)", 1).cast("float")
    )
    df = df.na.fill({"Rating_Cleaned": 3.0})
    
    df = df.withColumn('Product_name', col(prod_col).cast('string'))
    df = df.withColumn('Rating', col('Rating_Cleaned'))
    
    print(f"Cleaned data: {df.count()} records remain")
    return df, rev_col, prod_col

def sentiment_analysis(df):
    """Performs unified score categorization across datasets"""
    print("Starting sentiment analysis...")
    df = df.withColumn(
        'sentiment',
        when(col('Rating_Cleaned') >= 4, 'Positive')
        .when(col('Rating_Cleaned') <= 2, 'Negative')
        .otherwise('Neutral')
    )
    print("Sentiment analysis complete")
    return df

def product_recommendations(df, rev_col, prod_col):
    """Generates matrix recommendations handling implicit user tracking safely"""
    print("Starting collaborative product recommendations...")
    from pyspark.sql.functions import dense_rank
    from pyspark.sql.window import Window
    
    w1 = Window.orderBy(rev_col)
    w2 = Window.orderBy(prod_col)
    
    df = df.withColumn('user_id', dense_rank().over(w1).cast('int'))
    df = df.withColumn('product_id', dense_rank().over(w2).cast('int'))
    
    df_als = df.select('user_id', 'product_id', col('Rating_Cleaned').alias('rating_float')).dropna()
    
    user_distinct = df_als.select('user_id').distinct().count()
    item_distinct = df_als.select('product_id').distinct().count()
    
    if user_distinct < 2 or item_distinct < 2:
        print("⚠️ Dataset too small for collaborative filtering calculation.")
        return None

    als = ALS(
        maxIter=5,
        regParam=0.1,
        userCol='user_id',
        itemCol='product_id',
        ratingCol='rating_float',
        coldStartStrategy='drop'
    )
    model = als.fit(df_als)
    user_recs = model.recommendForAllUsers(5)
    print("Generated recommendations for users successfully")
    return user_recs

def fallback_python_mongo_write(df, collection_name):
    """Uses native Python environment to bypass Java classpath driver lockups"""
    try:
        import pymongo
        client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
        db = client["bda_database"]
        coll = db[collection_name]
        
        # Clear previous run data
        coll.delete_many({})
        
        # Convert Spark rows to local dictionary mappings safely
        local_data = [row.asDict() for row in df.limit(500).collect()]
        if local_data:
            coll.insert_many(local_data)
        print(f" Successfully synchronized {len(local_data)} records to MongoDB via Python Fallback Layer.")
    except Exception as e:
        print(f"⚠️ Python database sync exception: {e}")

def main():
    import time
    spark = create_spark_session()
    try:
        if len(sys.argv) > 1:
            input_path = sys.argv[1]
        else:
            input_path = "C:/BDA_Project/data/Amazon review dataset.csv"
            
        df = load_data(spark, input_path)
        df_cleaned, rev_col, prod_col = clean_and_prepare_data(df)
        
        run_id = str(int(time.time()))
        out_base = f"C:/BDA_Project/output/run_{run_id}"
        print(f"Saving processing package configuration down to: {out_base}")
        
        # A. Save Local Storage Parquet Packages
        df_cleaned.write.parquet(f"{out_base}/cleaned_data")
        
        df_sentiment = sentiment_analysis(df_cleaned)
        df_sentiment.write.parquet(f"{out_base}/sentiment_results")
        
        # B. Native Database Saving Loop
        print("Synchronizing Sentiment table results with local database layer...")
        try:
            df_sentiment.write.format("org.mongodb.spark.sql.DefaultSource") \
                .option("uri", "mongodb://127.0.0.1:27017/bda_database.sentiment_analysis") \
                .mode("overwrite").save()
        except Exception:
            print("[DRIVERS ALERT] Java Classpath Driver missing. Activating fallback driver sync...")
            fallback_python_mongo_write(df_sentiment, "sentiment_analysis")
        
        # C. Recommendations Table Saving Loop
        df_recommendations = product_recommendations(df_cleaned, rev_col, prod_col)
        if df_recommendations is not None:
            df_recommendations.write.parquet(f"{out_base}/recommendations")
            
            print("Synchronizing Collaborative Filtering Matrix with local database layer...")
            try:
                df_recs_json = df_recommendations.withColumn("recommendations", to_json(col("recommendations")))
                df_recs_json.write.format("org.mongodb.spark.sql.DefaultSource") \
                    .option("uri", "mongodb://127.0.0.1:27017/bda_database.recommendations") \
                    .mode("overwrite").save()
            except Exception:
                df_recs_json = df_recommendations.withColumn("recommendations", to_json(col("recommendations")))
                fallback_python_mongo_write(df_recs_json, "recommendations")
        
        with open("C:/BDA_Project/output/latest_run.json", "w") as f:
            json.dump({"latest_path": out_base}, f)
        
        print("\nBDA_project execution completed successfully!")
    except Exception as e:
        print(f"Universal engine execution halted due to error: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
