#!/usr/bin/env python3
import os
import subprocess
import sys

def verify_ssh_connection(host, password):
    """
    Verify SSH connection to the master node.
    
    Args:
        host (str): The IP address of the master node.
        password (str): The root password for SSH access.
        
    Returns:
        bool: True if SSH connection is successful, False otherwise.
    """
    print(f"Verifying SSH connection to {host}...")
    
    # Try to SSH to the master node
    ssh_command = f'sshpass -p "{password}" ssh -o StrictHostKeyChecking=no root@{host} "echo SSH connection successful"'
    
    try:
        result = subprocess.run(ssh_command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print("SSH connection successful!")
            print(result.stdout.strip())
            return True
        else:
            print(f"SSH connection failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error during SSH connection: {str(e)}")
        return False

def main():
    # Check if sshpass is installed
    try:
        subprocess.run(['which', 'sshpass'], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print("sshpass is not installed. Installing...")
        try:
            subprocess.run(['sudo', 'apt-get', 'update'], check=True)
            subprocess.run(['sudo', 'apt-get', 'install', '-y', 'sshpass'], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Failed to install sshpass: {e}")
            return False
    
    # Try to read master node IP from file
    try:
        with open('master_node_ip.txt', 'r') as f:
            master_ip = f.read().strip()
    except FileNotFoundError:
        print("ERROR: master_node_ip.txt not found. Please run verify_cluster.py first.")
        return False
    
    if not master_ip:
        print("ERROR: Empty master node IP")
        return False
    
    # Root password for SSH access
    root_password = '1qaz@WSX3edc'
    
    # Verify SSH connection
    success = verify_ssh_connection(master_ip, root_password)
    
    # Save the verification result
    with open('ssh_verification_result.txt', 'w') as f:
        f.write(f"SSH verification {'successful' if success else 'failed'}\n")
        f.write(f"Master node IP: {master_ip}\n")
    
    return success

if __name__ == '__main__':
    success = main()
    if not success:
        sys.exit(1)
