#!/usr/bin/env python3
"""
BDA_project - Sentiment Analysis Module
Performs NLP-based sentiment classification
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lower, trim, regexp_replace,
    split, size, array_contains
)
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
import sys

def create_spark_session():
    """Initialize Spark Session"""
    return SparkSession.builder \
        .appName("BDA_SentimentAnalysis") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def load_cleaned_data(spark, path):
    """Load cleaned data from HDFS"""
    print(f"Loading cleaned data from {path}...")
    df = spark.read.parquet(path)
    print(f"Loaded {df.count()} records")
    return df

def create_sentiment_labels(df):
    """Create sentiment labels based on ratings"""
    print("\nCreating sentiment labels...")
    
    df = df.withColumn(
        'sentiment',
        when(col('rating') >= 4, 'Positive')
        .when(col('rating') <= 2, 'Negative')
        .otherwise('Neutral')
    )
    
    # Show sentiment distribution
    print("\nSentiment Distribution:")
    df.groupBy('sentiment').count().show()
    
    return df

def extract_sentiment_features(df):
    """Extract features for sentiment analysis"""
    print("\nExtracting sentiment features...")
    
    # Positive words
    positive_words = [
        'good', 'great', 'excellent', 'amazing', 'awesome', 'love', 'perfect',
        'wonderful', 'fantastic', 'best', 'brilliant', 'outstanding', 'superb'
    ]
    
    # Negative words
    negative_words = [
        'bad', 'terrible', 'awful', 'horrible', 'hate', 'worst', 'poor',
        'disappointing', 'useless', 'waste', 'broken', 'defective', 'cheap'
    ]
    
    # Count positive words
    for word in positive_words:
        df = df.withColumn(
            f'has_{word}',
            when(col('review_clean').contains(word), 1).otherwise(0)
        )
    
    # Count negative words
    for word in negative_words:
        df = df.withColumn(
            f'has_{word}',
            when(col('review_clean').contains(word), 1).otherwise(0)
        )
    
    # Calculate sentiment score
    positive_cols = [f'has_{word}' for word in positive_words]
    negative_cols = [f'has_{word}' for word in negative_words]
    
    from pyspark.sql.functions import sum as spark_sum
    
    df = df.withColumn(
        'positive_score',
        spark_sum(*[col(c) for c in positive_cols])
    )
    
    df = df.withColumn(
        'negative_score',
        spark_sum(*[col(c) for c in negative_cols])
    )
    
    print("Features extracted successfully")
    return df

def analyze_sentiment_by_rating(df):
    """Analyze sentiment patterns by rating"""
    print("\nSentiment Analysis by Rating:")
    
    analysis = df.groupBy('rating', 'sentiment').count().orderBy('rating', 'sentiment')
    analysis.show()
    
    return df

def calculate_sentiment_confidence(df):
    """Calculate confidence scores for sentiment predictions"""
    print("\nCalculating sentiment confidence scores...")
    
    from pyspark.sql.functions import abs as spark_abs
    
    # Confidence based on rating extremity
    df = df.withColumn(
        'confidence',
        when(col('rating') == 5, 0.95)
        .when(col('rating') == 4, 0.75)
        .when(col('rating') == 3, 0.50)
        .when(col('rating') == 2, 0.75)
        .when(col('rating') == 1, 0.95)
        .otherwise(0.50)
    )
    
    return df

def select_sentiment_columns(df):
    """Select final sentiment analysis columns"""
    print("\nSelecting final sentiment columns...")
    
    df = df.select(
        col('user_id'),
        col('product_id'),
        col('rating'),
        col('review'),
        col('review_clean'),
        col('sentiment'),
        col('positive_score'),
        col('negative_score'),
        col('confidence')
    )
    
    return df

def save_sentiment_results(df, output_path):
    """Save sentiment analysis results"""
    print(f"\nSaving sentiment results to {output_path}...")
    df.write.mode('overwrite').parquet(output_path)
    print("Results saved successfully!")

def main():
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        # Load cleaned data
        input_path = "hdfs:///user/hadoop/bda_project/output/cleaned_data"
        df = load_cleaned_data(spark, input_path)
        
        # Perform sentiment analysis
        df = create_sentiment_labels(df)
        df = extract_sentiment_features(df)
        df = analyze_sentiment_by_rating(df)
        df = calculate_sentiment_confidence(df)
        df = select_sentiment_columns(df)
        
        # Show final statistics
        print("\n" + "="*50)
        print("SENTIMENT ANALYSIS RESULTS")
        print("="*50)
        print(f"Total records analyzed: {df.count()}")
        print("\nSentiment Distribution:")
        df.groupBy('sentiment').count().show()
        print("\nAverage Confidence by Sentiment:")
        df.groupBy('sentiment').agg({'confidence': 'avg'}).show()
        
        # Save results
        output_path = "hdfs:///user/hadoop/bda_project/output/sentiment_results"
        save_sentiment_results(df, output_path)
        
        print("\n✓ Sentiment analysis completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
