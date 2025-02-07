# Spark Job Debugging Tool

A tool for collecting and analyzing Spark job information from EMR clusters.

## Directory Structure
- collectors/: Data collection modules
- utils/: Utility functions and helpers
- tests/: Test cases
- docs/: Documentation

## Usage
```python
from collectors.spark_info_collector import SparkInfoCollector

collector = SparkInfoCollector(host="your-emr-master-node", password="your-password")
output_dir = collector.collect_all()
print(f"Collected information saved to: {output_dir}")
```
