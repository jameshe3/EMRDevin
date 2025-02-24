#!/usr/bin/env python3
import os
import subprocess
import json
from datetime import datetime
from typing import Dict, Any, List

class SparkDebugCollector:
    """Enhanced debug information collector for Spark jobs."""
    
    def __init__(self, host: str, password: str):
        self.host = host
        self.password = password
        self.ssh_prefix = f'sshpass -p "{self.password}" ssh -o StrictHostKeyChecking=no root@{self.host}'
    
    def _run_command(self, cmd: str, info_type: str, timeout: int = 30) -> str:
        """Run a command and capture its output."""
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=timeout
            )
            if result.returncode == 0:
                return result.stdout
            else:
                return f"Error collecting {info_type}: {result.stderr}"
        except subprocess.TimeoutExpired:
            return f"Timeout while collecting {info_type}"
        except Exception as e:
            return f"Exception collecting {info_type}: {str(e)}"
    
    def collect_configurations(self) -> Dict[str, str]:
        """Collect all relevant configuration files."""
        configs = {
            'spark_defaults': '/etc/ecm/spark-conf/spark-defaults.conf',
            'spark_env': '/etc/ecm/spark-conf/spark-env.sh',
            'yarn_site': '/etc/ecm/hadoop-conf/yarn-site.xml',
            'hdfs_site': '/etc/ecm/hadoop-conf/hdfs-site.xml',
            'core_site': '/etc/ecm/hadoop-conf/core-site.xml'
        }
        
        results = {}
        for config_name, config_path in configs.items():
            cmd = f'{self.ssh_prefix} "cat {config_path}"'
            results[config_name] = self._run_command(cmd, f"{config_name}_config")
        return results
    
    def collect_resource_metrics(self) -> Dict[str, str]:
        """Collect comprehensive resource usage metrics."""
        commands = {
            'cpu_info': 'cat /proc/cpuinfo | grep "processor\\|model name\\|cpu MHz"',
            'memory_info': 'free -h && vmstat 1 5',
            'disk_space': 'df -h && iostat -x 1 5',
            'network_stats': 'netstat -s && sar -n DEV 1 5',
            'process_list': 'ps aux --sort=-%cpu | head -20',
            'gpu_info': 'nvidia-smi || echo "No GPU found"'
        }
        
        results = {}
        for metric_type, cmd in commands.items():
            full_cmd = f'{self.ssh_prefix} "{cmd}"'
            results[metric_type] = self._run_command(full_cmd, metric_type)
        return results
    
    def collect_application_metrics(self) -> Dict[str, str]:
        """Collect application-specific metrics."""
        commands = {
            'yarn_metrics': '\
                yarn node -list -all && \
                yarn application -list -appStates ALL && \
                yarn queue -status default',
            'hdfs_metrics': 'hdfs dfsadmin -report && hdfs fsck /',
            'spark_metrics': '\
                curl -s http://localhost:18080/api/v1/applications && \
                curl -s http://localhost:18080/metrics/json/'
        }
        
        results = {}
        for metric_type, cmd in commands.items():
            full_cmd = f'{self.ssh_prefix} "{cmd}"'
            results[metric_type] = self._run_command(full_cmd, metric_type)
        return results
    
    def collect_logs(self, app_id: str = None) -> Dict[str, str]:
        """Collect relevant logs."""
        if app_id:
            yarn_logs_cmd = f'yarn logs -applicationId {app_id}'
        else:
            yarn_logs_cmd = 'yarn application -list -appStates ALL | grep FINISHED | head -1 | awk \'{print $1}\' | xargs -I% yarn logs -applicationId %'
        
        commands = {
            'yarn_logs': yarn_logs_cmd,
            'spark_logs': 'ls -l /var/log/spark/* && tail -n 1000 /var/log/spark/*',
            'yarn_rm_logs': 'tail -n 1000 /var/log/hadoop-yarn/yarn-yarn-resourcemanager-*.log',
            'hdfs_logs': 'tail -n 1000 /var/log/hadoop-hdfs/*.log'
        }
        
        results = {}
        for log_type, cmd in commands.items():
            full_cmd = f'{self.ssh_prefix} "{cmd}"'
            results[log_type] = self._run_command(full_cmd, log_type)
        return results
    
    def collect_all(self, app_id: str = None) -> str:
        """Collect all debug information."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = f"debug_info_{timestamp}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Create subdirectories
        subdirs = ['configs', 'resources', 'metrics', 'logs']
        for subdir in subdirs:
            os.makedirs(os.path.join(output_dir, subdir), exist_ok=True)
        
        # Collect all information
        collections = {
            'configs': self.collect_configurations(),
            'resources': self.collect_resource_metrics(),
            'metrics': self.collect_application_metrics(),
            'logs': self.collect_logs(app_id)
        }
        
        # Write collected information
        for category, data in collections.items():
            category_dir = os.path.join(output_dir, category)
            for name, content in data.items():
                filepath = os.path.join(category_dir, f"{name}.txt")
                with open(filepath, "w") as f:
                    f.write(content)
        
        # Create summary
        summary = {
            'timestamp': timestamp,
            'host': self.host,
            'app_id': app_id,
            'collected_files': {
                category: list(data.keys())
                for category, data in collections.items()
            }
        }
        
        summary_path = os.path.join(output_dir, "collection_summary.json")
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        
        return output_dir

if __name__ == "__main__":
    # Example usage
    collector = SparkDebugCollector("your-emr-master-ip", "your-password")
    output_dir = collector.collect_all()
    print(f"Debug information collected in: {output_dir}")
