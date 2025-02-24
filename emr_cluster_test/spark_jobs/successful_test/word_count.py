from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("EMR Test - Word Count") \
        .getOrCreate()
    
    # Create sample data
    data = [
        "Hello Spark EMR Test",
        "Testing Spark Job",
        "EMR Cluster Test",
        "Spark Job Running"
    ]
    
    # Create RDD and perform word count
    rdd = spark.sparkContext.parallelize(data)
    word_counts = rdd.flatMap(lambda x: x.split()) \
                    .map(lambda x: (x, 1)) \
                    .reduceByKey(lambda x, y: x + y)
    
    # Collect and print results
    results = word_counts.collect()
    
    # Save results
    with open('/tmp/spark_results.txt', 'w') as f:
        f.write("Word Count Results:\n")
        for word, count in sorted(results):
            f.write(f"{word}: {count}\n")
    
    print("Job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
