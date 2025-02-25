#!/usr/bin/env python3
import os
import json
import re
import sys
from datetime import datetime

class SparkLogAnalyzer:
    def __init__(self, diagnostics_dir):
        self.diagnostics_dir = diagnostics_dir
        self.failing_job_dir = os.path.join(diagnostics_dir, "failing_job")
        self.configs_dir = os.path.join(diagnostics_dir, "configs")
        self.logs_dir = os.path.join(diagnostics_dir, "logs")
        self.metrics_dir = os.path.join(diagnostics_dir, "metrics")
        self.resources_dir = os.path.join(diagnostics_dir, "resources")
        
        # Initialize analysis results
        self.error_analysis = {}
        self.resource_analysis = {}
        self.config_analysis = {}
        self.recommendations = []
    
    def analyze_error_logs(self):
        """Analyze error logs to identify root causes of failures"""
        error_file = os.path.join(self.failing_job_dir, "failing_job_error.txt")
        output_file = os.path.join(self.failing_job_dir, "failing_job_output.txt")
        
        error_patterns = {
            "out_of_memory": r"java\.lang\.OutOfMemoryError",
            "file_not_found": r"Path does not exist",
            "executor_lost": r"Lost executor",
            "task_failed": r"Task \d+ failed",
            "serialization_error": r"java\.io\.NotSerializableException",
            "class_not_found": r"java\.lang\.ClassNotFoundException",
            "null_pointer": r"java\.lang\.NullPointerException"
        }
        
        error_messages = []
        error_types = {}
        
        # Analyze error file
        if os.path.exists(error_file):
            with open(error_file, 'r') as f:
                error_content = f.read()
                
                for error_type, pattern in error_patterns.items():
                    matches = re.findall(pattern, error_content)
                    if matches:
                        error_types[error_type] = len(matches)
                        
                # Extract stack traces
                stack_traces = re.findall(r'(at .*?)\n', error_content)
                if stack_traces:
                    error_messages.append("Stack traces found in error logs:")
                    error_messages.extend(stack_traces[:10])  # Limit to first 10 lines
        
        # Analyze output file for error messages
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                output_content = f.read()
                
                # Look for expected errors in the output
                expected_errors = re.findall(r'Expected error.*?: (.*)', output_content)
                if expected_errors:
                    error_messages.append("Expected errors found in output:")
                    error_messages.extend(expected_errors)
        
        self.error_analysis = {
            "error_types": error_types,
            "error_messages": error_messages
        }
        
        return self.error_analysis
    
    def analyze_resource_usage(self):
        """Analyze resource usage metrics"""
        memory_file = os.path.join(self.resources_dir, "memory_info.txt")
        cpu_file = os.path.join(self.resources_dir, "cpu_info.txt")
        disk_file = os.path.join(self.resources_dir, "disk_space.txt")
        
        resource_info = {}
        
        # Analyze memory usage
        if os.path.exists(memory_file):
            with open(memory_file, 'r') as f:
                memory_content = f.read()
                memory_total = re.search(r'Mem:\s+(\S+)\s+total', memory_content)
                memory_used = re.search(r'Mem:\s+\S+\s+(\S+)\s+used', memory_content)
                memory_free = re.search(r'Mem:\s+\S+\s+\S+\s+(\S+)\s+free', memory_content)
                
                if memory_total and memory_used and memory_free:
                    resource_info["memory"] = {
                        "total": memory_total.group(1),
                        "used": memory_used.group(1),
                        "free": memory_free.group(1)
                    }
        
        # Analyze CPU info
        if os.path.exists(cpu_file):
            with open(cpu_file, 'r') as f:
                cpu_content = f.read()
                cpu_count = len(re.findall(r'processor\s+:\s+\d+', cpu_content))
                cpu_model = re.search(r'model name\s+:\s+(.*)', cpu_content)
                
                if cpu_count > 0 and cpu_model:
                    resource_info["cpu"] = {
                        "count": cpu_count,
                        "model": cpu_model.group(1)
                    }
        
        # Analyze disk space
        if os.path.exists(disk_file):
            with open(disk_file, 'r') as f:
                disk_content = f.read()
                disk_usage = []
                
                for line in disk_content.split('\n'):
                    if line.startswith('/dev/'):
                        parts = line.split()
                        if len(parts) >= 6:
                            disk_usage.append({
                                "filesystem": parts[0],
                                "size": parts[1],
                                "used": parts[2],
                                "available": parts[3],
                                "use_percent": parts[4],
                                "mounted_on": parts[5]
                            })
                
                if disk_usage:
                    resource_info["disk"] = disk_usage
        
        self.resource_analysis = resource_info
        return self.resource_analysis
    
    def analyze_configurations(self):
        """Analyze Spark, YARN, and HDFS configurations"""
        spark_defaults_file = os.path.join(self.configs_dir, "spark_defaults.conf")
        yarn_site_file = os.path.join(self.configs_dir, "yarn-site.xml")
        
        config_info = {}
        
        # Analyze Spark configurations
        if os.path.exists(spark_defaults_file):
            spark_configs = {}
            with open(spark_defaults_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split(None, 1)
                        if len(parts) == 2:
                            key, value = parts
                            spark_configs[key] = value
            
            # Extract important Spark configurations
            important_configs = [
                "spark.driver.memory",
                "spark.executor.memory",
                "spark.executor.cores",
                "spark.executor.instances",
                "spark.default.parallelism",
                "spark.sql.shuffle.partitions",
                "spark.memory.fraction",
                "spark.memory.storageFraction"
            ]
            
            config_info["spark"] = {
                config: spark_configs.get(config, "Not set")
                for config in important_configs
                if config in spark_configs
            }
        
        # Analyze YARN configurations
        if os.path.exists(yarn_site_file):
            yarn_configs = {}
            with open(yarn_site_file, 'r') as f:
                content = f.read()
                
                # Extract important YARN configurations
                yarn_memory_pattern = r'<name>yarn\.nodemanager\.resource\.memory-mb</name>\s*<value>(\d+)</value>'
                yarn_cpu_pattern = r'<name>yarn\.nodemanager\.resource\.cpu-vcores</name>\s*<value>(\d+)</value>'
                
                yarn_memory = re.search(yarn_memory_pattern, content)
                yarn_cpu = re.search(yarn_cpu_pattern, content)
                
                if yarn_memory:
                    yarn_configs["yarn.nodemanager.resource.memory-mb"] = yarn_memory.group(1)
                if yarn_cpu:
                    yarn_configs["yarn.nodemanager.resource.cpu-vcores"] = yarn_cpu.group(1)
                
                config_info["yarn"] = yarn_configs
        
        self.config_analysis = config_info
        return self.config_analysis
    
    def generate_recommendations(self):
        """Generate recommendations based on the analysis"""
        recommendations = []
        
        # Check for out of memory errors
        if self.error_analysis.get("error_types", {}).get("out_of_memory", 0) > 0:
            # Check Spark memory configurations
            spark_configs = self.config_analysis.get("spark", {})
            
            if "spark.driver.memory" in spark_configs:
                driver_memory = spark_configs["spark.driver.memory"]
                recommendations.append(f"Current spark.driver.memory is {driver_memory}. Consider increasing it to handle larger datasets.")
            else:
                recommendations.append("spark.driver.memory is not explicitly set. Consider setting it based on your dataset size.")
            
            if "spark.executor.memory" in spark_configs:
                executor_memory = spark_configs["spark.executor.memory"]
                recommendations.append(f"Current spark.executor.memory is {executor_memory}. Consider increasing it to handle larger datasets.")
            else:
                recommendations.append("spark.executor.memory is not explicitly set. Consider setting it based on your dataset size.")
            
            # Recommend optimizing the join operation
            recommendations.append("The failing job performs multiple cross joins which can cause exponential data growth. Consider:")
            recommendations.append("- Using more selective joins (inner join, left join) instead of cross joins")
            recommendations.append("- Adding filters before joins to reduce the data size")
            recommendations.append("- Using broadcast joins for small tables with 'broadcast(df)' hint")
        
        # Check for file not found errors
        if self.error_analysis.get("error_types", {}).get("file_not_found", 0) > 0:
            recommendations.append("The job attempts to read a non-existent file. Ensure the file path is correct and the file exists.")
            recommendations.append("Consider implementing error handling for file operations.")
        
        # General recommendations
        recommendations.append("General recommendations:")
        recommendations.append("- Monitor memory usage with Spark UI during job execution")
        recommendations.append("- Use caching strategically for frequently accessed DataFrames")
        recommendations.append("- Consider repartitioning large DataFrames before expensive operations")
        recommendations.append("- Implement incremental processing for large datasets")
        
        self.recommendations = recommendations
        return self.recommendations
    
    def analyze_all(self):
        """Run all analysis methods and generate a comprehensive report"""
        self.analyze_error_logs()
        self.analyze_resource_usage()
        self.analyze_configurations()
        self.generate_recommendations()
        
        # Create a comprehensive analysis report
        report = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "diagnostics_directory": self.diagnostics_dir,
            "error_analysis": self.error_analysis,
            "resource_analysis": self.resource_analysis,
            "configuration_analysis": self.config_analysis,
            "recommendations": self.recommendations
        }
        
        # Save the report to a JSON file
        report_file = os.path.join(self.diagnostics_dir, "analysis_report.json")
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Generate a markdown report for better readability
        self.generate_markdown_report(report)
        
        return report
    
    def generate_markdown_report(self, report):
        """Generate a markdown report from the analysis results"""
        md_content = f"""# Spark Job Analysis Report

## Overview
- **Timestamp:** {report['timestamp']}
- **Diagnostics Directory:** {report['diagnostics_directory']}

## Error Analysis

### Error Types
"""
        
        error_types = report['error_analysis'].get('error_types', {})
        if error_types:
            for error_type, count in error_types.items():
                md_content += f"- **{error_type}:** {count} occurrences\n"
        else:
            md_content += "No specific error types identified.\n"
        
        md_content += "\n### Error Messages\n"
        
        error_messages = report['error_analysis'].get('error_messages', [])
        if error_messages:
            for message in error_messages:
                md_content += f"- {message}\n"
        else:
            md_content += "No specific error messages identified.\n"
        
        md_content += "\n## Resource Analysis\n"
        
        # Memory information
        memory_info = report['resource_analysis'].get('memory', {})
        if memory_info:
            md_content += "\n### Memory\n"
            md_content += f"- **Total:** {memory_info.get('total', 'N/A')}\n"
            md_content += f"- **Used:** {memory_info.get('used', 'N/A')}\n"
            md_content += f"- **Free:** {memory_info.get('free', 'N/A')}\n"
        
        # CPU information
        cpu_info = report['resource_analysis'].get('cpu', {})
        if cpu_info:
            md_content += "\n### CPU\n"
            md_content += f"- **Count:** {cpu_info.get('count', 'N/A')}\n"
            md_content += f"- **Model:** {cpu_info.get('model', 'N/A')}\n"
        
        # Disk information
        disk_info = report['resource_analysis'].get('disk', [])
        if disk_info:
            md_content += "\n### Disk\n"
            for disk in disk_info:
                md_content += f"- **Filesystem:** {disk.get('filesystem', 'N/A')}\n"
                md_content += f"  - Size: {disk.get('size', 'N/A')}\n"
                md_content += f"  - Used: {disk.get('used', 'N/A')} ({disk.get('use_percent', 'N/A')})\n"
                md_content += f"  - Available: {disk.get('available', 'N/A')}\n"
                md_content += f"  - Mounted on: {disk.get('mounted_on', 'N/A')}\n"
        
        md_content += "\n## Configuration Analysis\n"
        
        # Spark configurations
        spark_configs = report['configuration_analysis'].get('spark', {})
        if spark_configs:
            md_content += "\n### Spark Configurations\n"
            for config, value in spark_configs.items():
                md_content += f"- **{config}:** {value}\n"
        
        # YARN configurations
        yarn_configs = report['configuration_analysis'].get('yarn', {})
        if yarn_configs:
            md_content += "\n### YARN Configurations\n"
            for config, value in yarn_configs.items():
                md_content += f"- **{config}:** {value}\n"
        
        md_content += "\n## Recommendations\n"
        
        recommendations = report.get('recommendations', [])
        if recommendations:
            for recommendation in recommendations:
                md_content += f"- {recommendation}\n"
        else:
            md_content += "No specific recommendations available.\n"
        
        # Save the markdown report
        md_file = os.path.join(report['diagnostics_directory'], "analysis_report.md")
        with open(md_file, 'w') as f:
            f.write(md_content)

def main():
    # Find the latest diagnostics directory
    diagnostics_dirs = [d for d in os.listdir('.') if d.startswith('diagnostics_')]
    if not diagnostics_dirs:
        print("No diagnostics directory found")
        return False
    
    # Sort by creation time (newest first)
    diagnostics_dirs.sort(reverse=True)
    latest_dir = diagnostics_dirs[0]
    
    print(f"Analyzing diagnostics in {latest_dir}...")
    
    # Create an analyzer and run the analysis
    analyzer = SparkLogAnalyzer(latest_dir)
    report = analyzer.analyze_all()
    
    print(f"Analysis completed. Report saved to {os.path.join(latest_dir, 'analysis_report.md')}")
    return True

if __name__ == '__main__':
    success = main()
    if not success:
        sys.exit(1)
