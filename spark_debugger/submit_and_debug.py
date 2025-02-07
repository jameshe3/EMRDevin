#!/usr/bin/env python3
import os
import time
from collectors.spark_info_collector import SparkInfoCollector

def submit_job(host, password):
    # Copy the failing job to EMR cluster
    os.system(f'sshpass -p "{password}" scp failing_job.py root@{host}:/root/')
    
    # Submit the Spark job
    submit_cmd = f'sshpass -p "{password}" ssh -o StrictHostKeyChecking=no root@{host} "spark-submit /root/failing_job.py"'
    os.system(submit_cmd)
    
    # Wait a bit for logs to be generated
    time.sleep(5)

def main():
    host = '112.124.29.1'
    password = '1qaz@WSX3edc'
    
    print("1. Submitting failing Spark job...")
    submit_job(host, password)
    
    print("\n2. Collecting debug information...")
    collector = SparkInfoCollector(host, password)
    output_dir = collector.collect_all()
    
    print(f"\nDebug information collected in: {output_dir}")
    print("\nDirectory structure:")
    for root, dirs, files in os.walk(output_dir):
        level = root.replace(output_dir, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f'{indent}{os.path.basename(root)}/')
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print(f'{subindent}{f}')

if __name__ == '__main__':
    main()
