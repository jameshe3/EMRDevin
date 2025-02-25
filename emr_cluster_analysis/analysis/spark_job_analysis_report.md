# Spark Job Analysis Report

## 1. Overview

This report provides a comprehensive analysis of a failing Spark job on an Alibaba Cloud EMR cluster. The job was intentionally designed to fail in specific ways to demonstrate common Spark job failures and how to diagnose them.

### Cluster Information
- **Cluster Type**: DATALAKE
- **EMR Version**: EMR-5.9.0
- **Spark Version**: 3.3.0
- **VPC ID**: vpc-bp167nedawwbwmt9ti0pv
- **Security Group ID**: sg-bp17gnp2vumd1o4okw4s

## 2. Spark Job Code

The failing job was designed to demonstrate two common failure scenarios:
1. Attempting to read a non-existent file
2. Creating memory issues through large cross joins

```python
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
```

## 3. Submission Parameters

The job was submitted to the EMR cluster using the following command:

```bash
spark-submit /root/failing_job.py
```

Default Spark configuration parameters used:
- **spark.driver.memory**: 2g
- **spark.executor.memory**: 2948m
- **spark.executor.cores**: 1
- **spark.executor.instances**: 2
- **spark.sql.shuffle.partitions**: 200
- **spark.memory.fraction**: 0.6
- **spark.memory.storageFraction**: 0.5

YARN resource allocation:
- **yarn.nodemanager.resource.memory-mb**: 13107
- **yarn.nodemanager.resource.cpu-vcores**: 8

## 4. Logs and Diagnostic Information

### 4.1 Error Logs

The job failed with the following errors:

1. **File Not Found Error**:
```
Path does not exist: hdfs://master-1-1.c-039890f9593be4c0.cn-hangzhou.emr.aliyuncs.com:9000/nonexistent/path/data.csv
```

2. **Out of Memory Error**:
```
java.lang.OutOfMemoryError: Java heap space
	at org.apache.spark.sql.execution.SparkPlan$$anon$1._next(SparkPlan.scala:393)
	at org.apache.spark.sql.execution.SparkPlan$$anon$1.getNext(SparkPlan.scala:404)
	at org.apache.spark.sql.execution.SparkPlan$$anon$1.getNext(SparkPlan.scala:390)
	at org.apache.spark.util.NextIterator.hasNext(NextIterator.scala:73)
	...
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:64)
```

### 4.2 Cluster Resources

**CPU Information**:
- **Count**: 4
- **Model**: Intel(R) Xeon(R) Platinum 8369B CPU @ 2.70GHz

**Disk Information**:
- **Filesystem**: /dev/vda1
  - Size: 118G
  - Used: 43G (38%)
  - Available: 71G
  - Mounted on: /

## 5. Root Cause Analysis

The job failed due to two intentional issues:

### 5.1 File Not Found Error
The first error occurred when the job attempted to read a non-existent CSV file at `/nonexistent/path/data.csv`. This is a common error in Spark jobs when:
- The file path is incorrect
- The file does not exist
- The user does not have permission to access the file
- The file system (HDFS in this case) is not properly configured

In this case, the error was intentional and caught in a try-except block, allowing the job to continue to the next failure scenario.

### 5.2 Out of Memory Error
The second and more severe error occurred during the execution of multiple cross joins. The job:
1. Created a DataFrame with 1,000 rows
2. Expanded it by exploding an array of 100 elements (resulting in 100,000 rows)
3. Performed two cross joins with 10 rows each, which would theoretically result in 100,000 × 10 × 10 = 10,000,000 rows
4. Attempted to collect all results to the driver

The `collect()` operation triggers the execution of the entire query plan and attempts to bring all data to the driver node. With the configured driver memory of 2GB, this operation exceeded the available memory, resulting in a `java.lang.OutOfMemoryError: Java heap space` error.

## 6. Optimization Recommendations

Based on the analysis of the failing job, here are recommendations to address the issues:

### 6.1 File Not Found Error
1. **Implement proper error handling**: Always wrap file operations in try-except blocks to handle potential errors gracefully.
2. **Validate file paths**: Before attempting to read files, validate that the paths exist.
3. **Use absolute paths**: When working with HDFS, use absolute paths starting with `hdfs://`.
4. **Check permissions**: Ensure the Spark user has appropriate permissions to access the files.

### 6.2 Memory Issues
1. **Increase driver memory**: For operations that collect large datasets to the driver, increase `spark.driver.memory` (e.g., to 4-8GB).
2. **Avoid using `collect()` on large datasets**: Instead, use:
   - `take(n)` to retrieve only a subset of records
   - `foreach()` to process records without collecting them
   - Write results to HDFS instead of collecting to the driver
3. **Optimize join operations**:
   - Avoid cross joins when possible, as they create a Cartesian product
   - Use more selective joins (inner, left, right) with join conditions
   - Consider broadcast joins for small tables: `broadcast(df1).join(df2)`
   - Filter data before joins to reduce the size of the datasets
4. **Partition data appropriately**:
   - Adjust `spark.sql.shuffle.partitions` based on data size (default is 200)
   - Use `repartition()` or `coalesce()` to optimize the number of partitions
5. **Memory tuning**:
   - Increase `spark.executor.memory` for executors processing large datasets
   - Adjust `spark.memory.fraction` (default 0.6) to allocate more memory for execution
   - Consider increasing the number of executors (`spark.executor.instances`)

### 6.3 General Performance Recommendations
1. **Monitor Spark UI**: Use the Spark UI (port 4040) to monitor job execution, identify bottlenecks, and track memory usage.
2. **Use caching strategically**: Cache frequently accessed DataFrames with `cache()` or `persist()`, but be mindful of memory usage.
3. **Implement incremental processing**: For very large datasets, consider processing data in smaller batches.
4. **Optimize serialization**: Use Kryo serialization for better performance with `spark.serializer`.
5. **Tune garbage collection**: Adjust JVM garbage collection parameters for better memory management.

## 7. Conclusion

The analyzed Spark job failed due to intentional design choices that demonstrate common failure scenarios in Spark applications. By implementing the recommended optimizations, similar issues in production Spark jobs can be avoided or mitigated.

The most critical recommendations are:
1. Implement proper error handling for file operations
2. Avoid collecting large datasets to the driver
3. Optimize join operations to prevent memory issues
4. Tune memory allocation based on workload requirements

These recommendations will help improve the reliability and performance of Spark jobs running on EMR clusters.
