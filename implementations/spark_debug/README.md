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

1. Configuration Files
   - Spark configuration (spark-defaults.conf)
   - YARN configuration (yarn-site.xml)
   - HDFS configuration (hdfs-site.xml)

2. Application Logs and Error Traces
   - Spark application logs
   - Error stack traces
   - System logs

3. System Resource Metrics
   - CPU: Utilization, load, stats
   - Memory: Usage, stats, details
   - Disk: Space, I/O, stats
   - Network: Stats, interfaces, throughput

4. Cluster Health Metrics
   - YARN metrics: nodes, applications, queues
   - HDFS metrics: report, health, space
   - Spark metrics: applications, executors
   - Node metrics: processes, system load

5. Job Execution Data
   - Application metrics
   - Container logs
   - Resource allocation
   - Task statistics

All metrics are collected securely with sensitive information redacted.

## Usage
The enhanced collector automatically organizes debug information in timestamped directories:
```python
collector = EnhancedSparkCollector(host='master-node', password='password')
debug_info_path = collector.collect_all()
```

## Security Note
All sensitive information (passwords, keys) is handled securely and not exposed in logs.
