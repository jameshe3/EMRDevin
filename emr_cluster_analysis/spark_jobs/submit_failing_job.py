#!/usr/bin/env python3
import os
import subprocess
import sys
import time

def submit_failing_job(host, password):
    """
    Submit the failing Spark job to the EMR cluster.
    
    Args:
        host (str): The IP address of the master node.
        password (str): The root password for SSH access.
        
    Returns:
        bool: True if the job was submitted, False otherwise.
    """
    print(f"Submitting failing Spark job to {host}...")
    
    # Copy the failing job to the master node
    scp_command = f'sshpass -p "{password}" scp failing_job.py root@{host}:/root/'
    try:
        result = subprocess.run(scp_command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Failed to copy failing job to master node: {result.stderr}")
            return False
        print("Failing job copied to master node")
    except Exception as e:
        print(f"Error copying failing job: {str(e)}")
        return False
    
    # Submit the Spark job
    submit_command = f'sshpass -p "{password}" ssh -o StrictHostKeyChecking=no root@{host} "spark-submit /root/failing_job.py"'
    try:
        print("Executing failing Spark job...")
        result = subprocess.run(submit_command, shell=True, capture_output=True, text=True)
        
        # Save the output to a file
        with open('failing_job_output.txt', 'w') as f:
            f.write(result.stdout)
        
        # Save the error output to a file
        with open('failing_job_error.txt', 'w') as f:
            f.write(result.stderr)
        
        print("Failing job executed")
        print("\nJob output:")
        print(result.stdout)
        
        print("\nJob errors:")
        print(result.stderr)
        
        # Check if the job failed as expected
        if "Expected error" in result.stdout or "Exception" in result.stdout:
            print("Job failed as expected")
            return True
        else:
            print("Job did not fail as expected")
            return False
    except Exception as e:
        print(f"Error submitting failing Spark job: {str(e)}")
        return False

def main():
    # Try to read master node IP from file
    try:
        with open('../verification/master_node_ip.txt', 'r') as f:
            master_ip = f.read().strip()
    except FileNotFoundError:
        print("ERROR: master_node_ip.txt not found. Please run verify_cluster.py first.")
        return False
    
    if not master_ip:
        print("ERROR: Empty master node IP")
        return False
    
    # Root password for SSH access
    root_password = '1qaz@WSX3edc'
    
    # Submit the failing job
    success = submit_failing_job(master_ip, root_password)
    
    # Save the submission result
    with open('failing_job_result.txt', 'w') as f:
        f.write(f"Failing job submission {'successful' if success else 'failed'}\n")
        f.write(f"Master node IP: {master_ip}\n")
        f.write(f"Submission completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    return success

if __name__ == '__main__':
    success = main()
    if not success:
        sys.exit(1)
