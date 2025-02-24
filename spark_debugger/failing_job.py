from pyspark.sql import SparkSession

def create_failing_job():
    # Initialize SparkSession with YARN cluster mode
    spark = SparkSession \
        .builder \
        .appName("IntentionallyFailingJob") \
        .master("yarn") \
        .config("spark.submit.deployMode", "client") \
        .config("spark.yarn.am.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.instances", "2") \
        .getOrCreate()

    # Read a non-existent file
    df = spark.read.csv("/nonexistent/path/data.csv")
    
    # Create a large cross join that will cause memory issues
    df2 = df.crossJoin(df).crossJoin(df)
    
    # Try to collect all results
    result = df2.collect()
    
    spark.stop()

if __name__ == "__main__":
    create_failing_job()
