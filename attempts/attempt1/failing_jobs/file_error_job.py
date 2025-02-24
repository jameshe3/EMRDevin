#!/usr/bin/env python3
from pyspark.sql import SparkSession

def create_file_error_job():
    """Create a job that will fail due to missing file."""
    spark = SparkSession.builder.appName("FileErrorJob").getOrCreate()
    
    # Try to read non-existent files
    df1 = spark.read.csv("/nonexistent/path/data1.csv")
    df2 = spark.read.parquet("/nonexistent/path/data2.parquet")
    
    # Try to write to invalid location
    df1.write.mode("overwrite").parquet("/root/invalid/path/output")
    
    spark.stop()

if __name__ == "__main__":
    create_file_error_job()
