#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array, lit

def create_failing_job():
    """
    A Spark job that intentionally fails.
    This job:
    1. Attempts to read a non-existent CSV file
    2. Creates memory issues through large cross joins
    3. Attempts to collect all results
    """
    print("Starting intentionally failing Spark job...")
    
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("IntentionallyFailingJob") \
        .getOrCreate()
    
    print("SparkSession created successfully")
    
    # Get Spark version
    spark_version = spark.version
    print(f"Spark version: {spark_version}")
    
    try:
        # First failure: Read a non-existent file
        print("Attempting to read a non-existent file...")
        df = spark.read.csv("/nonexistent/path/data.csv")
        print("This should not print as the previous line should fail")
    except Exception as e:
        print(f"Expected error reading non-existent file: {str(e)}")
        
        # Create a small DataFrame for the memory issue
        print("Creating a small DataFrame for the memory issue...")
        data = [(i, f"value_{i}") for i in range(1000)]
        df = spark.createDataFrame(data, ["id", "value"])
        
        # Second failure: Create a large cross join that will cause memory issues
        print("Creating a large cross join that will cause memory issues...")
        
        # Create a large array and explode it to multiply the data
        df_exploded = df.withColumn("array_col", array([lit(i) for i in range(100)])) \
                        .withColumn("exploded", explode("array_col"))
        
        # Perform multiple cross joins to create an exponentially large result
        print("Performing cross joins...")
        df_large = df_exploded.crossJoin(df_exploded.limit(10)).crossJoin(df_exploded.limit(10))
        
        # Try to collect all results, which should cause memory issues
        print("Attempting to collect all results (this should fail)...")
        try:
            result = df_large.collect()
            print(f"Collected {len(result)} rows (this should not print)")
        except Exception as e:
            print(f"Expected error during collection: {str(e)}")
    
    print("Failing job execution completed")
    spark.stop()

if __name__ == "__main__":
    create_failing_job()
