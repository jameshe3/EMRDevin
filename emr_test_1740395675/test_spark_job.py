#!/usr/bin/env python3
from pyspark.sql import SparkSession

def create_test_job():
    # Create a Spark session with a meaningful app name
    spark = SparkSession.builder \
        .appName("EMRTestJob") \
        .getOrCreate()

    try:
        # Create a simple test DataFrame
        data = [
            (1, "alpha", 10.5),
            (2, "beta", 20.0),
            (3, "gamma", 30.7)
        ]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        
        # Perform some basic operations to verify Spark functionality
        df.createOrReplaceTempView("test_data")
        
        # Run a simple SQL query
        result = spark.sql("""
            SELECT 
                name,
                value,
                value * 2 as doubled_value
            FROM test_data
            WHERE value > 15
            ORDER BY value DESC
        """)
        
        # Show the results
        print("\nTest DataFrame:")
        df.show()
        
        print("\nSQL Query Results:")
        result.show()
        
        return True
    except Exception as e:
        print(f"Error in test job: {str(e)}")
        return False
    finally:
        # Always stop the Spark session
        spark.stop()

if __name__ == "__main__":
    success = create_test_job()
    exit(0 if success else 1)
