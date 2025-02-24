#!/usr/bin/env python3
import os
import subprocess
import json
from datetime import datetime

class SparkInfoCollector:
    def __init__(self, host, password):
        self.host = host
        self.password = password
        self.ssh_prefix = f'sshpass -p "{self.password}" ssh -o StrictHostKeyChecking=no root@{self.host}'
    
    def collect_spark_defaults(self):
        cmd = f'{self.ssh_prefix} "cat /etc/ecm/spark-conf/spark-defaults.conf"'
        return self._run_command(cmd, "spark_defaults")
    
    def collect_yarn_config(self):
        cmd = f'{self.ssh_prefix} "cat /etc/ecm/hadoop-conf/yarn-site.xml"'
        return self._run_command(cmd, "yarn_config")
    
    def collect_hadoop_config(self):
        cmd = f'{self.ssh_prefix} "cat /etc/ecm/hadoop-conf/hdfs-site.xml"'
        return self._run_command(cmd, "hadoop_config")
    
    def collect_spark_apps(self):
        cmd = f'{self.ssh_prefix} "yarn application -list -appStates ALL"'
        return self._run_command(cmd, "spark_apps")
    
    def collect_spark_logs(self, app_id=None):
        if app_id:
            cmd = f'{self.ssh_prefix} "yarn logs -applicationId {app_id}"'
        else:
            # Get logs from the most recent application
            cmd = f'{self.ssh_prefix} "yarn application -list -appStates ALL | grep FINISHED | head -1 | awk \'{{print $1}}\' | xargs -I% yarn logs -applicationId %"'
        return self._run_command(cmd, "spark_logs")
    
    def collect_spark_ui_info(self):
        cmd = f'{self.ssh_prefix} "curl -s http://localhost:18080/api/v1/applications"'
        return self._run_command(cmd, "spark_ui")
    
    def collect_job_timeline(self, app_id=None):
        if app_id:
            cmd = f'{self.ssh_prefix} "yarn applicationattempt -list {app_id} && yarn logs -applicationId {app_id} | grep \'Submitted\|Launched\|Completed\|Failed\'"'
        else:
            cmd = f'{self.ssh_prefix} "yarn application -list -appStates ALL | grep FINISHED | head -1 | awk \'{{print $1}}\' | xargs -I% bash -c \'yarn applicationattempt -list % && yarn logs -applicationId % | grep \"Submitted\|Launched\|Completed\|Failed\"\'"'
        return self._run_command(cmd, "job_timeline")
    
    def _run_command(self, cmd, info_type, timeout=30):
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
            if result.returncode == 0:
                return result.stdout
            else:
                return f"Error collecting {info_type}: {result.stderr}"
        except subprocess.TimeoutExpired:
            return f"Timeout while collecting {info_type}"
        except Exception as e:
            return f"Exception collecting {info_type}: {str(e)}"
    
    def collect_system_resources(self):
        commands = {
            "cpu_info": "cat /proc/cpuinfo | grep 'processor\\|model name\\|cpu MHz'",
            "memory_info": "free -h",
            "disk_space": "df -h",
            "running_processes": "top -b -n 1"
        }
        results = {}
        for resource_type, cmd in commands.items():
            full_cmd = f'{self.ssh_prefix} "{cmd}"'
            results[resource_type] = self._run_command(full_cmd, resource_type)
        return results
    
    def collect_cluster_metrics(self):
        commands = {
            "yarn_metrics": "yarn node -list -all",
            "hdfs_metrics": "hdfs dfsadmin -report",
            "spark_metrics": "curl -s http://localhost:18080/metrics/json/"
        }
        results = {}
        for metric_type, cmd in commands.items():
            full_cmd = f'{self.ssh_prefix} "{cmd}"'
            results[metric_type] = self._run_command(full_cmd, metric_type)
        return results
    
    def collect_all(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = f"collected_info_{timestamp}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Create subdirectories for different types of information
        config_dir = os.path.join(output_dir, "configs")
        logs_dir = os.path.join(output_dir, "logs")
        metrics_dir = os.path.join(output_dir, "metrics")
        resources_dir = os.path.join(output_dir, "resources")
        
        for directory in [config_dir, logs_dir, metrics_dir, resources_dir]:
            os.makedirs(directory, exist_ok=True)
        
        # Collect configurations
        configs = {
            os.path.join(config_dir, "spark_defaults.conf"): self.collect_spark_defaults,
            os.path.join(config_dir, "yarn-site.xml"): self.collect_yarn_config,
            os.path.join(config_dir, "hdfs-site.xml"): self.collect_hadoop_config
        }
        
        # Collect application information
        logs = {
            os.path.join(logs_dir, "spark_applications.txt"): self.collect_spark_apps,
            os.path.join(logs_dir, "spark_logs.txt"): self.collect_spark_logs,
            os.path.join(logs_dir, "job_timeline.txt"): self.collect_job_timeline
        }
        
        # Collect metrics
        metrics = {
            os.path.join(metrics_dir, "spark_ui_info.json"): self.collect_spark_ui_info,
        }
        
        # Collect system resources
        resources = self.collect_system_resources()
        for resource_type, content in resources.items():
            filepath = os.path.join(resources_dir, f"{resource_type}.txt")
            with open(filepath, "w") as f:
                f.write(content)
        
        # Collect cluster metrics
        cluster_metrics = self.collect_cluster_metrics()
        for metric_type, content in cluster_metrics.items():
            filepath = os.path.join(metrics_dir, f"{metric_type}.txt")
            with open(filepath, "w") as f:
                f.write(content)
        
        # Write all collected information
        results = {}
        for filepath, collector in {**configs, **logs, **metrics}.items():
            content = collector()
            with open(filepath, "w") as f:
                f.write(content)
            results[os.path.basename(filepath)] = filepath
        
        # Create a summary file
        summary = {
            "timestamp": timestamp,
            "files_collected": list(results.keys()),
            "directories": {
                "configs": os.path.relpath(config_dir, output_dir),
                "logs": os.path.relpath(logs_dir, output_dir),
                "metrics": os.path.relpath(metrics_dir, output_dir),
                "resources": os.path.relpath(resources_dir, output_dir)
            }
        }
        
        summary_path = os.path.join(output_dir, "collection_summary.json")
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        
        return output_dir

if __name__ == "__main__":
    collector = SparkInfoCollector("112.124.29.1", "1qaz@WSX3edc")
    output_dir = collector.collect_all()
    print("Collected information saved to:")
    print(f"- {output_dir}")
