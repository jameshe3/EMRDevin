from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf
from pyspark.sql.types import StringType

def failing_udf(x):
    # Force an index error
    return x.split(',')[999]  # This will fail as there won't be 1000 elements

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("EMR Test - Failing Job") \
        .getOrCreate()
    
    try:
        # Create sample data
        data = [
            (1, "a,b,c"),
            (2, "x,y,z"),
            (3, "1,2,3")
        ]
        
        # Create DataFrame
        df = spark.createDataFrame(data, ["id", "csv_string"])
        
        # Register UDF that will fail
        fail_udf = udf(failing_udf, StringType())
        
        # Apply the failing UDF
        df_with_error = df.withColumn("will_fail", fail_udf(col("csv_string")))
        
        # Force eager evaluation
        result = df_with_error.collect()
        
    except Exception as e:
        print(f"Job failed as expected with error: {str(e)}")
        raise  # Re-raise to ensure the job fails
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
