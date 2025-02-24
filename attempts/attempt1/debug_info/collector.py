#!/usr/bin/env python3
import os
import time
from datetime import datetime
from typing import Dict, List

from spark_debugger.collectors.spark_info_collector import SparkInfoCollector

class ExtendedSparkInfoCollector(SparkInfoCollector):
    def __init__(self, host: str, password: str):
        super().__init__(host, password)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def collect_hadoop_config(self, output_dir: str) -> None:
        """Collect Hadoop-related configuration files."""
        config_dir = os.path.join(output_dir, "configs")
        os.makedirs(config_dir, exist_ok=True)
        
        config_files = [
            "/etc/hadoop/conf/core-site.xml",
            "/etc/hadoop/conf/hdfs-site.xml",
            "/etc/hadoop/conf/mapred-site.xml",
            "/etc/hadoop/conf/yarn-site.xml",
            "/etc/hive/conf/hive-site.xml"
        ]
        
        for config_file in config_files:
            cmd = f'sshpass -p "{self.password}" scp root@{self.host}:{config_file} {config_dir}/'
            os.system(cmd)
    
    def collect_resource_metrics(self, output_dir: str) -> None:
        """Collect system resource metrics."""
        metrics_dir = os.path.join(output_dir, "resources")
        os.makedirs(metrics_dir, exist_ok=True)
        
        # CPU info
        self._run_remote_command(
            "top -b -n 1 > /tmp/cpu_info.txt && " +
            "vmstat 1 5 >> /tmp/cpu_info.txt && " +
            "cat /proc/cpuinfo >> /tmp/cpu_info.txt"
        )
        
        # Memory info
        self._run_remote_command(
            "free -h > /tmp/memory_info.txt && " +
            "cat /proc/meminfo >> /tmp/memory_info.txt"
        )
        
        # Disk space
        self._run_remote_command("df -h > /tmp/disk_info.txt")
        
        # HDFS space
        self._run_remote_command("hdfs dfs -df -h > /tmp/hdfs_info.txt")
        
        # Running processes
        self._run_remote_command("ps aux | grep -i 'spark\\|yarn\\|hadoop' > /tmp/running_processes.txt")
        
        # Copy all metrics files
        metrics_files = [
            "cpu_info.txt",
            "memory_info.txt",
            "disk_info.txt",
            "hdfs_info.txt",
            "running_processes.txt"
        ]
        for file in metrics_files:
            cmd = f'sshpass -p "{self.password}" scp root@{self.host}:/tmp/{file} {metrics_dir}/'
            os.system(cmd)
    
    def collect_yarn_logs(self, output_dir: str) -> None:
        """Collect YARN application logs."""
        logs_dir = os.path.join(output_dir, "yarn_logs")
        os.makedirs(logs_dir, exist_ok=True)
        
        # Get recent application IDs
        self._run_remote_command(
            "yarn application -list -appStates ALL | " +
            "grep -i 'spark' | awk '{print $1}' > /tmp/app_ids.txt"
        )
        
        # Copy application logs
        cmd = f'sshpass -p "{self.password}" scp root@{self.host}:/tmp/app_ids.txt {logs_dir}/'
        os.system(cmd)
        
        with open(os.path.join(logs_dir, "app_ids.txt")) as f:
            app_ids = f.read().splitlines()
        
        for app_id in app_ids:
            self._run_remote_command(
                f"yarn logs -applicationId {app_id} > /tmp/{app_id}_logs.txt"
            )
            cmd = f'sshpass -p "{self.password}" scp root@{self.host}:/tmp/{app_id}_logs.txt {logs_dir}/'
            os.system(cmd)
    
    def collect_all(self) -> str:
        """Collect all debug information."""
        output_dir = super().collect_all()
        
        try:
            self.collect_hadoop_config(output_dir)
            self.collect_resource_metrics(output_dir)
            self.collect_yarn_logs(output_dir)
        except Exception as e:
            print(f"Error collecting additional debug info: {str(e)}")
        
        return output_dir
    
    def _run_remote_command(self, command: str) -> None:
        """Run command on remote host."""
        ssh_cmd = f'sshpass -p "{self.password}" ssh -o StrictHostKeyChecking=no root@{self.host} "{command}"'
        os.system(ssh_cmd)
