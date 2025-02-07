#!/usr/bin/env python3
import os
from collectors.spark_info_collector import SparkInfoCollector

def print_directory_tree(startpath):
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f'{indent}{os.path.basename(root)}/')
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print(f'{subindent}{f}')

def main():
    print("Starting data collection...")
    collector = SparkInfoCollector('112.124.29.1', '1qaz@WSX3edc')
    
    print("\n1. Testing SSH connection...")
    test_cmd = collector._run_command(f'{collector.ssh_prefix} "echo SSH connection successful"', "connection_test", timeout=5)
    if not test_cmd.startswith("SSH connection successful"):
        print("Error: Could not establish SSH connection")
        return
    
    print("2. Collecting configuration files...")
    configs = collector.collect_spark_defaults()
    if configs:
        print("✓ Spark configuration collected")
    
    print("3. Checking for running Spark applications...")
    apps = collector.collect_spark_apps()
    if apps:
        print("✓ Application list collected")
    
    print("4. Collecting system resources...")
    resources = collector.collect_system_resources()
    if resources:
        print("✓ System resources collected")
    
    print("\n5. Starting full collection...")
    output_dir = collector.collect_all()
    print(f'\nAll information saved to: {output_dir}\n')
    print('Directory structure:')
    print_directory_tree(output_dir)

if __name__ == '__main__':
    main()
