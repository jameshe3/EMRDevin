#!/usr/bin/env python3
from pyspark.sql import SparkSession
import random

def create_memory_error_job():
    """Create a job that will fail due to memory exhaustion."""
    spark = SparkSession.builder.appName("MemoryErrorJob").getOrCreate()
    
    # Create large dataset that exceeds memory
    data = [(i, random.random()) for i in range(1000000)]
    df = spark.createDataFrame(data, ["id", "value"])
    
    # Force multiple shuffles and cache
    for _ in range(3):
        df = df.crossJoin(df.sample(0.1))
        df.cache()
    
    # Force evaluation
    df.count()
    
    spark.stop()

if __name__ == "__main__":
    create_memory_error_job()
