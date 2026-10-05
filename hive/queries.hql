-- BDA_project Hive Queries
-- Run with: hive -f queries.hql

-- Create external table for cleaned data
CREATE EXTERNAL TABLE IF NOT EXISTS cleaned_reviews (
    user_id INT,
    product_id INT,
    rating INT,
    review STRING,
    review_clean STRING
)
STORED AS PARQUET
LOCATION '/user/hadoop/bda_project/output/cleaned_data';

-- Create table for sentiment analysis results
CREATE EXTERNAL TABLE IF NOT EXISTS sentiment_results (
    user_id INT,
    product_id INT,
    rating INT,
    review STRING,
    review_clean STRING,
    sentiment STRING
)
STORED AS PARQUET
LOCATION '/user/hadoop/bda_project/output/sentiment_results';

-- Query 1: Sentiment Distribution
SELECT 
    sentiment,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM sentiment_results
GROUP BY sentiment
ORDER BY count DESC;

-- Query 2: Average Rating by Sentiment
SELECT 
    sentiment,
    ROUND(AVG(rating), 2) as avg_rating,
    MIN(rating) as min_rating,
    MAX(rating) as max_rating,
    COUNT(*) as review_count
FROM sentiment_results
GROUP BY sentiment
ORDER BY avg_rating DESC;

-- Query 3: Top 10 Most Reviewed Products
SELECT 
    product_id,
    COUNT(*) as review_count,
    ROUND(AVG(rating), 2) as avg_rating
FROM cleaned_reviews
GROUP BY product_id
ORDER BY review_count DESC
LIMIT 10;

-- Query 4: Rating Distribution
SELECT 
    rating,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM cleaned_reviews
GROUP BY rating
ORDER BY rating DESC;

-- Query 5: Users with Most Reviews
SELECT 
    user_id,
    COUNT(*) as review_count,
    ROUND(AVG(rating), 2) as avg_rating
FROM cleaned_reviews
GROUP BY user_id
ORDER BY review_count DESC
LIMIT 10;

-- Query 6: Sentiment by Product (Top 5 Products)
SELECT 
    product_id,
    sentiment,
    COUNT(*) as count
FROM sentiment_results
WHERE product_id IN (
    SELECT product_id 
    FROM cleaned_reviews 
    GROUP BY product_id 
    ORDER BY COUNT(*) DESC 
    LIMIT 5
)
GROUP BY product_id, sentiment
ORDER BY product_id, count DESC;
