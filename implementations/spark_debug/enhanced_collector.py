#!/usr/bin/env python3
import os
import subprocess
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

class EnhancedSparkCollector:
    """Enhanced debug information collector for Spark jobs."""
    
    def __init__(self, host: str, password: Optional[str] = None):
        self.host = host
        self.password = password or os.getenv('EMR_ROOT_PASSWORD', '')
        if not self.password:
            raise ValueError("Password must be provided either directly or via EMR_ROOT_PASSWORD environment variable")
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
    
    def collect_cluster_metrics(self) -> Dict[str, str]:
        """Collect comprehensive cluster metrics."""
        commands = {
            # Required metrics from completion criteria
            'yarn_metrics': 'yarn node -list -all',
            'hdfs_metrics': 'hdfs dfsadmin -report',
            'spark_metrics': 'curl -s http://localhost:18080/metrics/json/',
            'node_metrics': 'top -bn1 | head -n 20',
            'network_metrics': 'netstat -s',
            'disk_io': 'iostat -x 1 2',
            
            # Additional metrics for comprehensive monitoring
            'yarn_apps': 'yarn application -list -appStates ALL',
            'yarn_queues': 'yarn queue -status default',
            'hdfs_health': 'hdfs fsck /',
            'hdfs_space': 'hadoop fs -du -h /',
            'spark_apps': 'curl -s http://localhost:18080/api/v1/applications',
            'spark_executors': 'curl -s http://localhost:18080/api/v1/applications/[APP_ID]/executors'
        }
        
        results = {}
        for metric_type, cmd in commands.items():
            full_cmd = f'{self.ssh_prefix} "{cmd}"'
            results[metric_type] = self._run_command(full_cmd, metric_type)
        return results
    
    def collect_resource_metrics(self) -> Dict[str, str]:
        """Collect comprehensive resource usage metrics."""
        commands = {
            # CPU metrics
            'cpu_info': 'cat /proc/cpuinfo | grep "processor\\|model name\\|cpu MHz"',
            'cpu_load': 'uptime && mpstat -P ALL 1 5',
            'cpu_stats': 'sar -u 1 5',
            
            # Memory metrics
            'memory_info': 'free -h',
            'memory_stats': 'vmstat -s && vmstat 1 5',
            'memory_details': 'cat /proc/meminfo',
            
            # Disk metrics
            'disk_space': 'df -h',
            'disk_io': 'iostat -x 1 5',
            'disk_stats': 'cat /proc/diskstats',
            
            # Network metrics
            'network_stats': 'netstat -s',
            'network_interfaces': 'ip -s link',
            'network_connections': 'netstat -ant | grep ESTABLISHED',
            'network_throughput': 'sar -n DEV 1 5',
            
            # Process metrics
            'process_list': 'ps aux --sort=-%cpu | head -20',
            'process_tree': 'pstree -p',
            
            # System metrics
            'system_load': 'uptime && dmesg | tail',
            'system_limits': 'ulimit -a'
        }
        
        results = {}
        for metric_type, cmd in commands.items():
            full_cmd = f'{self.ssh_prefix} "{cmd}"'
            results[metric_type] = self._run_command(full_cmd, metric_type)
        return results
    
    def collect_logs(self, app_id: Optional[str] = None) -> Dict[str, str]:
        """Collect comprehensive logs."""
        if app_id:
            yarn_logs_cmd = f'yarn logs -applicationId {app_id}'
            container_logs_cmd = f'yarn logs -applicationId {app_id} -containerId ALL'
        else:
            yarn_logs_cmd = 'yarn application -list -appStates ALL | grep FINISHED | head -1 | awk \'{print $1}\' | xargs -I% yarn logs -applicationId %'
            container_logs_cmd = yarn_logs_cmd.replace('yarn logs', 'yarn logs -containerId ALL')
        
        commands = {
            # Application logs
            'yarn_app_logs': yarn_logs_cmd,
            'yarn_container_logs': container_logs_cmd,
            
            # Component logs
            'spark_logs': 'tail -n 1000 /var/log/spark/spark-*.log',
            'yarn_rm_logs': 'tail -n 1000 /var/log/hadoop-yarn/yarn-yarn-resourcemanager-*.log',
            'yarn_nm_logs': 'tail -n 1000 /var/log/hadoop-yarn/yarn-yarn-nodemanager-*.log',
            'hdfs_nn_logs': 'tail -n 1000 /var/log/hadoop-hdfs/hadoop-hdfs-namenode-*.log',
            'hdfs_dn_logs': 'tail -n 1000 /var/log/hadoop-hdfs/hadoop-hdfs-datanode-*.log',
            
            # System logs
            'system_messages': 'tail -n 1000 /var/log/messages || tail -n 1000 /var/log/syslog',
            'dmesg_logs': 'dmesg | tail -n 1000'
        }
        
        results = {}
        for log_type, cmd in commands.items():
            full_cmd = f'{self.ssh_prefix} "{cmd}"'
            results[log_type] = self._run_command(full_cmd, log_type)
        return results
    
    def collect_all(self, app_id: Optional[str] = None) -> str:
        """Collect all debug information."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = f"debug_info_{timestamp}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Create organized subdirectories based on requirements
        subdirs = {
            'configs': ['spark', 'yarn', 'hdfs'],
            'logs': ['application', 'error_traces', 'system'],
            'metrics': ['yarn', 'hdfs', 'spark', 'cluster_health', 'job_execution'],
            'resources': ['cpu', 'memory', 'disk', 'network']
        }
        
        for main_dir, sub_dirs in subdirs.items():
            for sub_dir in sub_dirs:
                os.makedirs(os.path.join(output_dir, main_dir, sub_dir), exist_ok=True)
        
        # Collect all information
        collections = {
            'configs': {
                'spark/defaults.conf': self._run_command(f'{self.ssh_prefix} "cat /etc/spark/conf/spark-defaults.conf"', 'spark config'),
                'yarn/site.xml': self._run_command(f'{self.ssh_prefix} "cat /etc/hadoop/conf/yarn-site.xml"', 'yarn config'),
                'hdfs/site.xml': self._run_command(f'{self.ssh_prefix} "cat /etc/hadoop/conf/hdfs-site.xml"', 'hdfs config')
            },
            'metrics': {
                'yarn/metrics': self._run_command(f'{self.ssh_prefix} "yarn node -list -all"', 'yarn metrics'),
                'hdfs/metrics': self._run_command(f'{self.ssh_prefix} "hdfs dfsadmin -report"', 'hdfs metrics'),
                'spark/metrics': self._run_command(f'{self.ssh_prefix} "curl -s http://localhost:18080/metrics/json/"', 'spark metrics'),
                'cluster_health/node_metrics': self._run_command(f'{self.ssh_prefix} "top -bn1 | head -n 20"', 'node metrics'),
                'cluster_health/network_metrics': self._run_command(f'{self.ssh_prefix} "netstat -s"', 'network metrics'),
                'cluster_health/disk_io': self._run_command(f'{self.ssh_prefix} "iostat -x 1 2"', 'disk io')
            },
            'resources': {
                'cpu/metrics': self._run_command(f'{self.ssh_prefix} "mpstat -P ALL 1 5"', 'cpu metrics'),
                'memory/metrics': self._run_command(f'{self.ssh_prefix} "free -h && vmstat -s"', 'memory metrics'),
                'disk/metrics': self._run_command(f'{self.ssh_prefix} "df -h && iostat -x"', 'disk metrics'),
                'network/metrics': self._run_command(f'{self.ssh_prefix} "netstat -i && sar -n DEV 1 5"', 'network metrics')
            },
            'resources': self.collect_resource_metrics(),
            'logs': self.collect_logs(app_id)
        }
        
        # Write collected information with proper directory structure
        for category, data in collections.items():
            for path, content in data.items():
                # Create full path including subdirectories
                full_path = os.path.join(output_dir, category, path)
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                
                # Write content to file
                with open(f"{full_path}.txt", "w") as f:
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
    collector = EnhancedSparkCollector("your-emr-master-ip", "your-password")
    output_dir = collector.collect_all()
    print(f"Debug information collected in: {output_dir}")
