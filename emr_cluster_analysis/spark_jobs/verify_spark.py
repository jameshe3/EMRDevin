#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def create_verification_job():
    """
    A simple Spark job to verify that Spark is working correctly on the EMR cluster.
    This job:
    1. Creates a SparkSession
    2. Creates a simple DataFrame
    3. Performs basic transformations
    4. Writes the results to a text file
    5. Reads the results back
    """
    print("Starting Spark verification job...")
    
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("SparkVerificationJob") \
        .getOrCreate()
    
    print("SparkSession created successfully")
    
    # Get Spark version
    spark_version = spark.version
    print(f"Spark version: {spark_version}")
    
    # Create a simple DataFrame
    data = [("Alice", 34), ("Bob", 45), ("Charlie", 29), ("Diana", 37)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)
    
    print("DataFrame created successfully")
    print("Sample data:")
    df.show()
    
    # Perform basic transformations
    df_transformed = df.withColumn("AgeNextYear", col("Age") + 1) \
                      .withColumn("UpperName", col("Name").cast("string").upper()) \
                      .withColumn("Greeting", lit("Hello, ").concat(col("Name")))
    
    print("Transformations applied successfully")
    print("Transformed data:")
    df_transformed.show()
    
    # Write the results to a text file
    output_path = "/tmp/spark_verification_output"
    df_transformed.write.mode("overwrite").csv(output_path)
    
    print(f"Data written to {output_path}")
    
    # Read the results back
    df_read = spark.read.csv(output_path)
    print("Data read back successfully")
    print("Read data:")
    df_read.show()
    
    # Get some cluster metrics
    print("\nCluster Metrics:")
    print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
    print(f"Available executors: {spark.sparkContext.getExecutorMemoryStatus().keys()}")
    
    # Stop the SparkSession
    spark.stop()
    print("SparkSession stopped")
    print("Spark verification job completed successfully")

if __name__ == "__main__":
    create_verification_job()
