# Spark Debug Information Collection

## Directory Structure
```
debug_info_{timestamp}/
├── configs/         # Configuration files (Spark, YARN, HDFS)
├── logs/           # Application logs and error traces
├── metrics/        # Cluster and application metrics
└── resources/      # System resource metrics
```

## Collected Information
1. Application logs and error traces
   - Spark driver logs
   - Spark executor logs
   - YARN application logs
   - Error stack traces

2. System resource metrics
   - CPU utilization
   - Memory usage
   - Disk I/O
   - Network statistics

3. Configuration files
   - Spark configuration
   - YARN configuration
   - HDFS configuration

4. Cluster health metrics
   - Node status
   - YARN metrics
   - HDFS metrics

5. Job execution data
   - Spark application metrics
   - Task statistics
   - Resource allocation

## Usage
The enhanced collector automatically organizes debug information in timestamped directories:
```python
collector = EnhancedSparkCollector(host='master-node', password='password')
debug_info_path = collector.collect_all()
```

## Security Note
All sensitive information (passwords, keys) is handled securely and not exposed in logs.
