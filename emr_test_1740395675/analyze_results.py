#!/usr/bin/env python3
import os
import sys
import json
from datetime import datetime

def read_file_content(filepath, section_name):
    """Safely read file content with error handling"""
    try:
        with open(filepath) as f:
            return f.read().strip()
    except Exception as e:
        return f"Error reading {section_name}: {str(e)}"

def analyze_spark_config(config_content):
    """Analyze Spark configuration settings"""
    lines = config_content.split('\n')
    important_settings = []
    
    for line in lines:
        line = line.strip()
        if line and not line.startswith('#'):
            important_settings.append(f"- {line}")
    
    return "\n".join(important_settings) if important_settings else "No configuration settings found"

def analyze_memory_usage(memory_content):
    """Analyze memory usage information"""
    if not memory_content:
        return "No memory information available"
    
    lines = memory_content.split('\n')
    analysis = []
    
    for line in lines:
        if 'Mem:' in line:
            analysis.append("Memory Usage:")
            analysis.append(f"```\n{line}\n```")
        elif 'Swap:' in line:
            analysis.append("\nSwap Usage:")
            analysis.append(f"```\n{line}\n```")
    
    return "\n".join(analysis) if analysis else "Memory information parsing failed"

def analyze_spark_logs(logs_content):
    """Analyze Spark job execution logs"""
    if not logs_content:
        return "No log content available"
    
    analysis = []
    error_count = logs_content.count('ERROR')
    warning_count = logs_content.count('WARN')
    
    analysis.append(f"Log Summary:")
    analysis.append(f"- Error count: {error_count}")
    analysis.append(f"- Warning count: {warning_count}")
    
    # Extract error messages
    if error_count > 0:
        analysis.append("\nError Messages:")
        for line in logs_content.split('\n'):
            if 'ERROR' in line:
                analysis.append(f"```\n{line}\n```")
    
    return "\n".join(analysis)

def analyze_debug_info(debug_dir):
    """Generate comprehensive analysis report from debug information"""
    report = []
    report.append("# EMR Cluster and Spark Job Analysis Report")
    report.append(f"\nGenerated at: {datetime.now().isoformat()}")
    
    # Analyze Spark configuration
    spark_conf = read_file_content(
        os.path.join(debug_dir, "configs/spark_defaults.conf"),
        "Spark configuration"
    )
    report.append("\n## Spark Configuration")
    report.append(analyze_spark_config(spark_conf))
    
    # Analyze resource usage
    memory_info = read_file_content(
        os.path.join(debug_dir, "resources/memory_info.txt"),
        "memory information"
    )
    report.append("\n## Resource Usage")
    report.append(analyze_memory_usage(memory_info))
    
    # Analyze job execution and failures
    spark_logs = read_file_content(
        os.path.join(debug_dir, "logs/spark_logs.txt"),
        "Spark logs"
    )
    report.append("\n## Job Analysis")
    report.append(analyze_spark_logs(spark_logs))
    
    # Add cluster metrics if available
    yarn_metrics = read_file_content(
        os.path.join(debug_dir, "metrics/yarn_metrics.txt"),
        "YARN metrics"
    )
    if yarn_metrics:
        report.append("\n## Cluster Metrics")
        report.append("### YARN Resources")
        report.append(f"```\n{yarn_metrics}\n```")
    
    # Write report
    report_path = os.path.join(os.path.dirname(debug_dir), "analysis_report.md")
    with open(report_path, "w") as f:
        f.write("\n".join(report))
    
    return report_path

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: analyze_results.py <debug_directory>")
        sys.exit(1)
    
    debug_dir = sys.argv[1]
    if not os.path.isdir(debug_dir):
        print(f"Error: Directory not found: {debug_dir}")
        sys.exit(1)
    
    try:
        report_path = analyze_debug_info(debug_dir)
        print(f"Analysis report generated: {report_path}")
    except Exception as e:
        print(f"Error generating analysis report: {str(e)}")
        sys.exit(1)
