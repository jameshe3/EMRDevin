# Spark Job Debugging Tool

A comprehensive tool for collecting Spark job debugging information and formatting it for LLM analysis.

## Modules

### 1. Data Collection (`collectors/`)
- SparkInfoCollector: Gathers comprehensive debugging data
  - Application logs and error traces
  - System resource metrics (CPU, Memory, HDFS)
  - Configuration files (Spark, YARN, HDFS)
  - Cluster health metrics
  - Job execution data

### 2. Analysis Support (`analyzer/`)
- Log Analysis (`core/log_analyzer.py`)
  - Error parsing and categorization
  - Resource usage analysis
  - Configuration validation
- Report Generation (`core/report_generator.py`)
  - Structured analysis reports
  - Performance metrics summaries

### 3. LLM Analysis Support
- Debug Information Formatter
  - Structured markdown output
  - Comprehensive context inclusion
  - Guided analysis questions
- Example: `llm_analysis_input.md`

### 4. Example Components
- `failing_job.py`: Sample Spark job for testing
- `demo_run.py`: Example collection script
- `submit_and_debug.py`: Job submission utility

## Usage

### Basic Collection
```python
from collectors.spark_info_collector import SparkInfoCollector

collector = SparkInfoCollector(host="emr-master-node", password="root-password")
output_dir = collector.collect_all()
print(f"Debug information saved to: {output_dir}")
```

### Job Submission and Analysis
```bash
# Submit job and collect debug info
python submit_and_debug.py

# Format for LLM analysis
python analyze_debug_info.py
```

## Directory Structure
```
spark_debugger/
├── collectors/           # Data collection modules
├── analyzer/            # Analysis support
│   └── core/           # Core analysis modules
├── failing_job.py      # Example failing job
├── demo_run.py         # Demo collection script
├── submit_and_debug.py # Job submission utility
└── llm_analysis_input.md # LLM-formatted debug info
```

## Security
- Sensitive information via environment variables only
- Debug information stored locally
- No credentials in codebase
