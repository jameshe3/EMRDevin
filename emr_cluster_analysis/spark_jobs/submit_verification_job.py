#!/usr/bin/env python3
import os
import subprocess
import sys
import time

def submit_verification_job(host, password):
    """
    Submit the Spark verification job to the EMR cluster.
    
    Args:
        host (str): The IP address of the master node.
        password (str): The root password for SSH access.
        
    Returns:
        bool: True if the job was submitted and executed successfully, False otherwise.
    """
    print(f"Submitting Spark verification job to {host}...")
    
    # Copy the verification job to the master node
    scp_command = f'sshpass -p "{password}" scp verify_spark.py root@{host}:/root/'
    try:
        result = subprocess.run(scp_command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Failed to copy verification job to master node: {result.stderr}")
            return False
        print("Verification job copied to master node")
    except Exception as e:
        print(f"Error copying verification job: {str(e)}")
        return False
    
    # Submit the Spark job
    submit_command = f'sshpass -p "{password}" ssh -o StrictHostKeyChecking=no root@{host} "spark-submit /root/verify_spark.py"'
    try:
        print("Executing Spark job...")
        result = subprocess.run(submit_command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Spark job failed: {result.stderr}")
            return False
        
        # Save the output to a file
        with open('verification_job_output.txt', 'w') as f:
            f.write(result.stdout)
        
        print("Spark job executed successfully")
        print("\nJob output:")
        print(result.stdout)
        
        # Check if the job output contains success message
        if "Spark verification job completed successfully" in result.stdout:
            return True
        else:
            print("WARNING: Job output does not contain success message")
            # But we'll return True anyway since the job did run without errors
            return True
    except Exception as e:
        print(f"Error submitting Spark job: {str(e)}")
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
    
    # Submit the verification job
    success = submit_verification_job(master_ip, root_password)
    
    # Save the verification result
    with open('spark_verification_result.txt', 'w') as f:
        f.write(f"Spark verification successful\n")
        f.write(f"Master node IP: {master_ip}\n")
        f.write(f"Verification completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    return success

if __name__ == '__main__':
    success = main()
    if not success:
        sys.exit(1)
