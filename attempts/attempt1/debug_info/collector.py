#!/usr/bin/env python3
import os
import time
from datetime import datetime
from typing import Dict, List

import sys
sys.path.append("/home/ubuntu/repos/EMRDevin")
from spark_debugger.collectors.spark_info_collector import SparkInfoCollector

class ExtendedSparkInfoCollector(SparkInfoCollector):
    def __init__(self, host: str, password: str):
        super().__init__(host, password)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def collect_hadoop_config(self) -> str:
        """Collect Hadoop-related configuration files."""
        cmd = f'{self.ssh_prefix} "cat /etc/ecm/hadoop-conf/hdfs-site.xml"'
        return self._run_command(cmd, "hadoop_config")
    
    def collect_system_resources(self) -> dict:
        """Collect system resource metrics."""
        commands = {
            "cpu_info": "cat /proc/cpuinfo | grep 'processor\\|model name\\|cpu MHz'",
            "memory_info": "free -h && cat /proc/meminfo",
            "disk_space": "df -h",
            "hdfs_space": "hdfs dfs -df -h",
            "running_processes": "ps aux | grep -i 'spark\\|yarn\\|hadoop'"
        }
        results = {}
        for resource_type, cmd in commands.items():
            full_cmd = f'{self.ssh_prefix} "{cmd}"'
            results[resource_type] = self._run_command(full_cmd, resource_type)
        return results
    
    def collect_spark_logs(self, app_id=None) -> str:
        """Collect YARN application logs."""
        if app_id:
            cmd = f'{self.ssh_prefix} "yarn logs -applicationId {app_id}"'
        else:
            # Get logs from the most recent application
            cmd = f'{self.ssh_prefix} "yarn application -list -appStates ALL | grep FINISHED | head -1 | awk \'{{print $1}}\' | xargs -I% yarn logs -applicationId %"'
        return self._run_command(cmd, "spark_logs")
    
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
