#!/usr/bin/env python3
"""
BDA_project - Data Cleaning Module
Handles data preprocessing and cleaning
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lower, trim, regexp_replace, 
    length, isnan, isnull, coalesce
)
from pyspark.sql.types import IntegerType, FloatType
import sys

def create_spark_session():
    """Initialize Spark Session"""
    return SparkSession.builder \
        .appName("BDA_DataCleaning") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def load_data(spark, path):
    """Load CSV data"""
    print(f"Loading data from {path}...")
    df = spark.read.csv(path, header=True, inferSchema=True)
    print(f"Initial records: {df.count()}")
    print(f"Columns: {df.columns}")
    return df

def remove_duplicates(df):
    """Remove duplicate records"""
    print("\nRemoving duplicates...")
    initial_count = df.count()
    df = df.dropDuplicates()
    final_count = df.count()
    print(f"Removed {initial_count - final_count} duplicate records")
    return df

def handle_missing_values(df):
    """Handle null and missing values"""
    print("\nHandling missing values...")
    
    # Show missing value statistics
    print("\nMissing values per column:")
    for col_name in df.columns:
        missing_count = df.filter(isnull(col(col_name))).count()
        if missing_count > 0:
            print(f"  {col_name}: {missing_count}")
    
    # Drop rows with null values in critical columns
    df = df.dropna(subset=['review', 'rating'])
    print(f"\nRecords after removing nulls: {df.count()}")
    return df

def clean_text(df):
    """Clean and normalize text data"""
    print("\nCleaning text data...")
    
    # Convert to lowercase
    df = df.withColumn('review_lower', lower(col('review')))
    
    # Remove special characters
    df = df.withColumn(
        'review_clean',
        regexp_replace(col('review_lower'), r'[^a-zA-Z0-9\s]', '')
    )
    
    # Remove extra whitespace
    df = df.withColumn('review_clean', trim(col('review_clean')))
    
    # Remove very short reviews (less than 5 characters)
    df = df.filter(length(col('review_clean')) >= 5)
    
    print(f"Records after text cleaning: {df.count()}")
    return df

def validate_ratings(df):
    """Validate and standardize ratings"""
    print("\nValidating ratings...")
    
    # Cast rating to integer
    df = df.withColumn('rating', col('rating').cast(IntegerType()))
    
    # Filter valid ratings (1-5)
    df = df.filter((col('rating') >= 1) & (col('rating') <= 5))
    
    print(f"Records with valid ratings: {df.count()}")
    
    # Show rating distribution
    print("\nRating distribution:")
    df.groupBy('rating').count().orderBy('rating').show()
    
    return df

def remove_outliers(df):
    """Remove outlier records"""
    print("\nRemoving outliers...")
    
    # Remove reviews that are too long (likely spam)
    max_length = 5000
    df = df.filter(length(col('review_clean')) <= max_length)
    
    print(f"Records after outlier removal: {df.count()}")
    return df

def select_final_columns(df):
    """Select and rename final columns"""
    print("\nSelecting final columns...")
    
    # Select relevant columns
    if 'user_id' in df.columns and 'product_id' in df.columns:
        df = df.select(
            col('user_id'),
            col('product_id'),
            col('rating'),
            col('review'),
            col('review_clean')
        )
    else:
        # If user_id/product_id don't exist, create them
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window = Window.orderBy('review')
        df = df.withColumn('user_id', row_number().over(window))
        df = df.withColumn('product_id', (col('user_id') % 1000).cast(IntegerType()))
        
        df = df.select(
            col('user_id'),
            col('product_id'),
            col('rating'),
            col('review'),
            col('review_clean')
        )
    
    return df

def save_cleaned_data(df, output_path):
    """Save cleaned data to HDFS"""
    print(f"\nSaving cleaned data to {output_path}...")
    df.write.mode('overwrite').parquet(output_path)
    print("Data saved successfully!")

def main():
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        # Load data
        input_path = "hdfs:///user/hadoop/bda_project/amazon_reviews.csv"
        df = load_data(spark, input_path)
        
        # Apply cleaning steps
        df = remove_duplicates(df)
        df = handle_missing_values(df)
        df = clean_text(df)
        df = validate_ratings(df)
        df = remove_outliers(df)
        df = select_final_columns(df)
        
        # Show final statistics
        print("\n" + "="*50)
        print("FINAL DATA STATISTICS")
        print("="*50)
        print(f"Total records: {df.count()}")
        print(f"Columns: {df.columns}")
        print("\nSchema:")
        df.printSchema()
        
        # Save cleaned data
        output_path = "hdfs:///user/hadoop/bda_project/output/cleaned_data"
        save_cleaned_data(df, output_path)
        
        print("\n✓ Data cleaning completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
