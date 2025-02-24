#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import StringType, IntegerType
import time
import random

def create_failing_job():
    """Create a Spark job with various failure scenarios."""
    # Create SparkSession using the recommended pattern
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("IntentionallyFailingJob") \
        .config("spark.executor.memory", "512m") \
        .config("spark.driver.memory", "512m") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        print("Starting failing job scenarios...")
        
        # Scenario 1: Memory pressure through large dataset and joins
        try:
            print("\n1. Memory Pressure Test")
            print("Generating large datasets...")
            df1 = spark.range(0, 1000000).withColumn("random", col("id") % 100)
            df2 = spark.range(0, 1000000).withColumn("random", col("id") % 100)
            
            # Create memory pressure with multiple joins and aggregations
            print("Performing memory-intensive operations...")
            result = df1.crossJoin(df2) \
                .groupBy("random") \
                .count() \
                .crossJoin(df1.select("random").distinct())
            
            # Force computation with cache
            result.cache()
            print(f"Row count: {result.count()}")
            
        except Exception as e:
            print(f"Expected memory failure: {str(e)}")
            print("Stack trace:", e.__class__.__name__)
        
        # Scenario 2: Invalid data access and processing
        try:
            print("\n2. Data Access and Processing Test")
            
            # Try to read non-existent file
            print("Attempting to read non-existent file...")
            df_invalid = spark.read.csv("/nonexistent/path/data.csv")
            df_invalid.show()
            
        except Exception as e:
            print(f"Expected file access failure: {str(e)}")
            print("Stack trace:", e.__class__.__name__)
        
        # Scenario 3: Invalid transformations and UDF errors
        try:
            print("\n3. Transformation and UDF Test")
            
            # Create a DataFrame with potential type issues
            data = [(1, "a"), (2, "b"), (3, None), (4, "invalid")]
            df = spark.createDataFrame(data, ["id", "value"])
            
            # Define a UDF that will fail for certain inputs
            @udf(returnType=IntegerType())
            def problematic_udf(x):
                if x is None:
                    raise ValueError("Null value encountered")
                try:
                    return int(x) * 2
                except:
                    raise ValueError(f"Cannot convert {x} to integer")
            
            # Apply the UDF and force computation
            print("Applying problematic transformations...")
            df.select(
                "id",
                problematic_udf("value").alias("doubled_value")
            ).show()
            
        except Exception as e:
            print(f"Expected transformation failure: {str(e)}")
            print("Stack trace:", e.__class__.__name__)
        
        # Scenario 4: Resource exhaustion through infinite loop
        try:
            print("\n4. Resource Exhaustion Test")
            
            # Create a self-joining DataFrame that grows exponentially
            base_df = spark.range(0, 100)
            growing_df = base_df
            
            print("Starting resource exhaustion test...")
            import signal
            
            def timeout_handler(signum, frame):
                raise TimeoutError("Resource exhaustion test timeout")
            
            # Set 30 second timeout
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(30)
            
            try:
                for i in range(5):
                    growing_df = growing_df.crossJoin(base_df.select(
                        col("id").alias(f"id_{i}")
                    ))
                    print(f"Iteration {i}: forcing computation...")
                    growing_df.count()
            finally:
                signal.alarm(0)  # Disable alarm
            
        except Exception as e:
            print(f"Expected resource exhaustion: {str(e)}")
            print("Stack trace:", e.__class__.__name__)
        
    finally:
        print("\nCleaning up Spark session...")
        spark.stop()

if __name__ == "__main__":
    create_failing_job()
