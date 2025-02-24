#!/usr/bin/env python3
import os
import sys
import time
from typing import Optional

from debug_info.collector import ExtendedSparkInfoCollector

def submit_and_debug(host: str, job_path: str, output_dir: Optional[str] = None) -> str:
    """Submit a Spark job and collect debug information."""
    # Verify environment variables
    root_password = os.getenv('EMR_ROOT_PASSWORD')
    if not root_password:
        raise ValueError("EMR_ROOT_PASSWORD environment variable must be set")
    
    # Initialize collector
    collector = ExtendedSparkInfoCollector(host, root_password)
    
    try:
        # Submit job
        print(f"Submitting job: {job_path}")
        submit_cmd = f'spark-submit --master yarn --deploy-mode cluster {job_path}'
        submit_status = os.system(f'sshpass -p "{root_password}" ssh -o StrictHostKeyChecking=no root@{host} "{submit_cmd}"')
        
        if submit_status != 0:
            print("Warning: Job submission returned non-zero status")
        
        # Wait for logs to be generated
        print("Waiting for logs to be generated...")
        time.sleep(10)
        
        # Collect debug information
        print("Collecting debug information...")
        debug_dir = collector.collect_all()
        
        print(f"\nDebug information collected in: {debug_dir}")
        print("\nDirectory structure:")
        for root, dirs, files in os.walk(debug_dir):
            level = root.replace(debug_dir, '').count(os.sep)
            indent = ' ' * 4 * level
            print(f'{indent}{os.path.basename(root)}/')
            subindent = ' ' * 4 * (level + 1)
            for f in files:
                print(f'{subindent}{f}')
        
        return debug_dir
        
    except Exception as e:
        print(f"Error during job submission or debugging: {str(e)}")
        raise

def main():
    if len(sys.argv) != 3:
        print("Usage: python run_debug.py <host> <job_path>")
        sys.exit(1)
    
    host = sys.argv[1]
    job_path = sys.argv[2]
    
    try:
        submit_and_debug(host, job_path)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
