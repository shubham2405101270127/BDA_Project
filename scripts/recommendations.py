#!/usr/bin/env python3
"""
BDA_project - Product Recommendations Module
Generates recommendations using ALS algorithm
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rank
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import sys

def create_spark_session():
    """Initialize Spark Session"""
    return SparkSession.builder \
        .appName("BDA_Recommendations") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def load_cleaned_data(spark, path):
    """Load cleaned data from HDFS"""
    print(f"Loading cleaned data from {path}...")
    df = spark.read.parquet(path)
    print(f"Loaded {df.count()} records")
    return df

def prepare_als_data(df):
    """Prepare data for ALS algorithm"""
    print("\nPreparing data for ALS...")
    
    # Select and cast columns for ALS
    als_data = df.select(
        col('user_id').cast('int'),
        col('product_id').cast('int'),
        col('rating').cast('float')
    )
    
    # Remove any null values
    als_data = als_data.dropna()
    
    print(f"ALS data records: {als_data.count()}")
    print(f"Unique users: {als_data.select('user_id').distinct().count()}")
    print(f"Unique products: {als_data.select('product_id').distinct().count()}")
    
    return als_data

def split_train_test(df, train_ratio=0.8):
    """Split data into training and testing sets"""
    print(f"\nSplitting data ({train_ratio*100}% train, {(1-train_ratio)*100}% test)...")
    
    train_data, test_data = df.randomSplit([train_ratio, 1-train_ratio], seed=42)
    
    print(f"Training records: {train_data.count()}")
    print(f"Testing records: {test_data.count()}")
    
    return train_data, test_data

def train_als_model(train_data):
    """Train ALS recommendation model"""
    print("\nTraining ALS model...")
    
    als = ALS(
        maxIter=10,
        regParam=0.01,
        userCol='user_id',
        itemCol='product_id',
        ratingCol='rating',
        coldStartStrategy='drop',
        nonnegative=True,
        implicitPrefs=False
    )
    
    model = als.fit(train_data)
    print("Model training completed!")
    
    return model

def evaluate_model(model, test_data):
    """Evaluate model performance"""
    print("\nEvaluating model...")
    
    predictions = model.transform(test_data)
    
    # Remove NaN predictions
    predictions = predictions.dropna()
    
    evaluator = RegressionEvaluator(
        metricName='rmse',
        labelCol='rating',
        predictionCol='prediction'
    )
    
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE): {rmse:.4f}")
    
    return predictions

def generate_user_recommendations(model, num_recommendations=5):
    """Generate top N recommendations for each user"""
    print(f"\nGenerating top {num_recommendations} recommendations for each user...")
    
    user_recs = model.recommendForAllUsers(num_recommendations)
    
    # Explode recommendations
    from pyspark.sql.functions import explode
    
    user_recs_exploded = user_recs.select(
        col('user_id'),
        explode(col('recommendations')).alias('recommendation')
    ).select(
        col('user_id'),
        col('recommendation.product_id'),
        col('recommendation.rating').alias('predicted_rating')
    )
    
    print(f"Generated recommendations for {user_recs.count()} users")
    
    return user_recs_exploded

def generate_product_recommendations(model, num_recommendations=5):
    """Generate top N products similar to each product"""
    print(f"\nGenerating top {num_recommendations} similar products...")
    
    product_recs = model.recommendForAllItems(num_recommendations)
    
    # Explode recommendations
    from pyspark.sql.functions import explode
    
    product_recs_exploded = product_recs.select(
        col('product_id'),
        explode(col('recommendations')).alias('recommendation')
    ).select(
        col('product_id'),
        col('recommendation.user_id'),
        col('recommendation.rating').alias('predicted_rating')
    )
    
    print(f"Generated recommendations for {product_recs.count()} products")
    
    return product_recs_exploded

def analyze_recommendations(user_recs):
    """Analyze recommendation statistics"""
    print("\nRecommendation Statistics:")
    
    print("\nAverage predicted rating by user:")
    user_recs.groupBy('user_id').agg({'predicted_rating': 'avg'}).show(5)
    
    print("\nTop 10 most recommended products:")
    user_recs.groupBy('product_id').count().orderBy('count', ascending=False).show(10)
    
    return user_recs

def save_recommendations(user_recs, product_recs, output_path):
    """Save recommendations to HDFS"""
    print(f"\nSaving recommendations to {output_path}...")
    
    # Save user recommendations
    user_recs.write.mode('overwrite').parquet(f"{output_path}/user_recommendations")
    
    # Save product recommendations
    product_recs.write.mode('overwrite').parquet(f"{output_path}/product_recommendations")
    
    print("Recommendations saved successfully!")

def main():
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        # Load cleaned data
        input_path = "hdfs:///user/hadoop/bda_project/output/cleaned_data"
        df = load_cleaned_data(spark, input_path)
        
        # Prepare data for ALS
        als_data = prepare_als_data(df)
        
        # Split data
        train_data, test_data = split_train_test(als_data)
        
        # Train model
        model = train_als_model(train_data)
        
        # Evaluate model
        predictions = evaluate_model(model, test_data)
        
        # Generate recommendations
        user_recs = generate_user_recommendations(model, num_recommendations=5)
        product_recs = generate_product_recommendations(model, num_recommendations=5)
        
        # Analyze recommendations
        user_recs = analyze_recommendations(user_recs)
        
        # Show final statistics
        print("\n" + "="*50)
        print("RECOMMENDATION RESULTS")
        print("="*50)
        print(f"Total user recommendations: {user_recs.count()}")
        print(f"Total product recommendations: {product_recs.count()}")
        
        # Save results
        output_path = "hdfs:///user/hadoop/bda_project/output/recommendations"
        save_recommendations(user_recs, product_recs, output_path)
        
        print("\n✓ Recommendation generation completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
