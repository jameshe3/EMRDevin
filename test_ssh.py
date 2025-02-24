#!/usr/bin/env python3
import os
import subprocess
from get_master_ip import get_master_ip

def test_ssh_connection(cluster_id: str):
    try:
        master_ip = get_master_ip(cluster_id)
        print(f"Master Node IP: {master_ip}")
        print("Testing SSH connection...")
        
        # Test SSH connection by running hostname and uptime commands
        ssh_cmd = f"ssh -o StrictHostKeyChecking=no root@{master_ip} 'hostname && uptime'"
        result = subprocess.run(ssh_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("SSH Connection successful!")
            print("Output:")
            print(result.stdout)
        else:
            print("SSH Connection failed!")
            print("Error:")
            print(result.stderr)
            
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: test_ssh.py <cluster_id>")
        sys.exit(1)
    
    test_ssh_connection(sys.argv[1])
