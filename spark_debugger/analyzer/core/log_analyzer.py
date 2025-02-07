import json
import os
import re
from datetime import datetime

class SparkLogAnalyzer:
    def __init__(self, debug_info_dir):
        self.debug_info_dir = debug_info_dir
        self.findings = {
            'error_analysis': {},
            'resource_usage': {},
            'performance_metrics': {},
            'configuration_issues': {},
            'recommendations': []
        }
    
    def analyze_error(self):
        log_file = os.path.join(self.debug_info_dir, 'logs/spark_applications.txt')
        with open(log_file, 'r') as f:
            logs = f.read()
            
            # Extract all errors and exceptions
            error_pattern = r'Error:|Exception:|Caused by:|Traceback|pyspark\.sql\.utils\.'
            errors = re.findall(f'({error_pattern}.*?)(?={error_pattern}|\Z)', logs, re.DOTALL)
            
            # Also check spark logs for more detailed error information
            spark_log_file = os.path.join(self.debug_info_dir, 'logs/spark_logs.txt')
            with open(spark_log_file, 'r') as f:
                spark_logs = f.read()
                spark_errors = re.findall(f'({error_pattern}.*?)(?={error_pattern}|\Z)', spark_logs, re.DOTALL)
                errors.extend(spark_errors)
            
            # Categorize errors
            error_categories = {
                'data_access': r'Path does not exist|FileNotFoundException|AccessControl',
                'memory': r'OutOfMemoryError|SparkOutOfMemoryError|GC overhead',
                'execution': r'ExecutorLostFailure|TaskSetManager|Failed to execute task',
                'configuration': r'InvalidConfiguration|ConfigurationError|Properties.*not set',
                'resource': r'Resource.*exceeded|Cannot allocate memory|Container killed'
            }
            
            categorized_errors = {}
            for error in errors:
                for category, pattern in error_categories.items():
                    if re.search(pattern, error, re.IGNORECASE):
                        if category not in categorized_errors:
                            categorized_errors[category] = []
                        categorized_errors[category].append(error)
                        break
            
            self.findings['error_analysis'] = {
                'errors': errors,
                'categorized_errors': categorized_errors,
                'error_count': len(errors),
                'categories_found': list(categorized_errors.keys())
            }
    
    def analyze_resources(self):
        # Analyze memory usage
        memory_file = os.path.join(self.debug_info_dir, 'resources/memory_info.txt')
        with open(memory_file, 'r') as f:
            memory_info = f.read()
            # Parse memory values
            mem_pattern = r'Mem:\s+(\d+\w+)\s+(\d+\w+)\s+(\d+\w+)\s+(\d+\w+)\s+(\d+\w+)\s+(\d+\w+)'
            if match := re.search(mem_pattern, memory_info):
                self.findings['resource_usage']['memory'] = {
                    'total': match.group(1),
                    'used': match.group(2),
                    'free': match.group(3),
                    'shared': match.group(4),
                    'buff_cache': match.group(5),
                    'available': match.group(6)
                }
        
        # Analyze CPU usage
        cpu_file = os.path.join(self.debug_info_dir, 'resources/cpu_info.txt')
        with open(cpu_file, 'r') as f:
            cpu_info = f.read()
            cpu_count = len(re.findall(r'processor\s+:', cpu_info))
            model_name = re.search(r'model name\s+:\s+(.+)', cpu_info)
            self.findings['resource_usage']['cpu'] = {
                'count': cpu_count,
                'model': model_name.group(1) if model_name else 'Unknown',
                'raw_info': cpu_info
            }
    
    def analyze_configurations(self):
        config_files = {
            'spark': 'configs/spark_defaults.conf',
            'yarn': 'configs/yarn-site.xml',
            'hdfs': 'configs/hdfs-site.xml'
        }
        
        self.findings['configuration_issues'] = {}
        for config_type, filename in config_files.items():
            filepath = os.path.join(self.debug_info_dir, filename)
            try:
                with open(filepath, 'r') as f:
                    content = f.read()
                    if filename.endswith('.xml'):
                        # Extract properties from XML
                        properties = re.findall(r'<name>(.+?)</name>\s*<value>(.+?)</value>', content)
                        self.findings['configuration_issues'][config_type] = dict(properties)
                    else:
                        # Parse properties from conf file
                        properties = {}
                        for line in content.splitlines():
                            if line.strip() and not line.startswith('#'):
                                parts = line.split(None, 1)
                                if len(parts) == 2:
                                    properties[parts[0]] = parts[1]
                        self.findings['configuration_issues'][config_type] = properties
            except Exception as e:
                self.findings['configuration_issues'][config_type] = f"Error reading config: {str(e)}"
    
    def analyze_metrics(self):
        metrics_files = {
            'spark': 'metrics/spark_metrics.txt',
            'yarn': 'metrics/yarn_metrics.txt',
            'hdfs': 'metrics/hdfs_metrics.txt'
        }
        
        self.findings['performance_metrics'] = {}
        for metric_type, filename in metrics_files.items():
            filepath = os.path.join(self.debug_info_dir, filename)
            try:
                with open(filepath, 'r') as f:
                    content = f.read()
                    self.findings['performance_metrics'][metric_type] = {
                        'raw_data': content,
                        'parsed': self._parse_metrics(content, metric_type)
                    }
            except Exception as e:
                self.findings['performance_metrics'][metric_type] = f"Error reading metrics: {str(e)}"
    
    def _parse_metrics(self, content, metric_type):
        parsed = {}
        if metric_type == 'yarn':
            # Parse YARN node metrics
            nodes = re.findall(r'Node-Id.*?Total-Memory.*?(\d+)', content, re.DOTALL)
            if nodes:
                parsed['total_nodes'] = len(nodes)
                parsed['total_memory'] = sum(int(mem) for mem in nodes)
        elif metric_type == 'hdfs':
            # Parse HDFS metrics
            if dfs_used := re.search(r'DFS Used%:\s*(\d+\.?\d*)', content):
                parsed['dfs_used_percentage'] = float(dfs_used.group(1))
        return parsed
    
    def generate_recommendations(self):
        # Clear previous recommendations
        self.findings['recommendations'] = []
        
        # Error-based recommendations
        if error_analysis := self.findings.get('error_analysis', {}):
            categorized_errors = error_analysis.get('categorized_errors', {})
            
            if 'data_access' in categorized_errors:
                self.findings['recommendations'].append({
                    'category': 'Data Access',
                    'priority': 'High',
                    'issue': 'Data path access failure',
                    'suggestions': [
                        'Verify the input path exists in HDFS',
                        'Check HDFS permissions for the user running the job',
                        'Ensure proper HDFS scheme (hdfs://) is used in the path'
                    ]
                })
            
            if 'memory' in categorized_errors:
                self.findings['recommendations'].append({
                    'category': 'Memory Management',
                    'priority': 'High',
                    'issue': 'Memory-related failures',
                    'suggestions': [
                        'Increase executor memory (spark.executor.memory)',
                        'Adjust garbage collection settings',
                        'Consider reducing partition sizes or input data volume'
                    ]
                })
        
        # Resource-based recommendations
        if resource_usage := self.findings.get('resource_usage', {}):
            memory_info = resource_usage.get('memory', {})
            if memory_info and isinstance(memory_info, dict):
                available_mem = memory_info.get('available', '0')
                if 'G' in available_mem and float(available_mem.rstrip('G')) < 4:
                    self.findings['recommendations'].append({
                        'category': 'Resource Allocation',
                        'priority': 'Medium',
                        'issue': 'Low available memory',
                        'suggestions': [
                            'Consider adding more memory to the cluster',
                            'Optimize memory-intensive operations',
                            'Monitor memory usage patterns'
                        ]
                    })
    
    def analyze_all(self):
        self.analyze_error()
        self.analyze_resources()
        self.analyze_configurations()
        self.analyze_metrics()
        self.generate_recommendations()
        return self.findings

