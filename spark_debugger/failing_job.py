from pyspark.sql import SparkSession

def create_failing_job():
    builder = SparkSession.builder
    builder = builder.appName("IntentionallyFailingJob")
    builder = builder.config("spark.master", "yarn")
    spark = builder.getOrCreate()

    # Read a non-existent file
    df = spark.read.csv("/nonexistent/path/data.csv")
    
    # Create a large cross join that will cause memory issues
    df2 = df.crossJoin(df).crossJoin(df)
    
    # Try to collect all results
    result = df2.collect()
    
    spark.stop()

if __name__ == "__main__":
    create_failing_job()
