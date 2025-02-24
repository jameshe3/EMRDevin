#!/usr/bin/env python3
import os
import sys
from typing import Tuple

from alibabacloud_emr20210320.client import Client as Emr20210320Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_emr20210320 import models as emr_20210320_models
from alibabacloud_tea_console.client import Client as ConsoleClient

def create_client() -> Emr20210320Client:
    access_key_id = os.getenv('ACCESS_KEY_ID')
    access_key_secret = os.getenv('ACCESS_KEY_SECRET')
    if not access_key_id or not access_key_secret:
        raise ValueError("ACCESS_KEY_ID and ACCESS_KEY_SECRET environment variables must be set")
    
    config = open_api_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
        region_id='cn-hangzhou'
    )
    return Emr20210320Client(config)

def verify_cluster_status(cluster_id: str) -> Tuple[bool, str]:
    """Verify cluster status and get master node public IP."""
    client = create_client()
    
    # Get cluster info
    get_cluster_request = emr_20210320_models.GetClusterRequest(
        cluster_id=cluster_id,
        region_id='cn-hangzhou'
    )
    cluster_response = client.get_cluster(get_cluster_request)
    cluster = cluster_response.body.cluster
    
    # Get master node info
    list_nodes_request = emr_20210320_models.ListNodesRequest(
        cluster_id=cluster_id,
        region_id='cn-hangzhou'
    )
    nodes_response = client.list_nodes(list_nodes_request)
    nodes = nodes_response.body.nodes
    master_node = next(
        (node for node in nodes if node.node_group_type == 'MASTER'),
        None
    )
    
    if not master_node or not master_node.public_ip:
        return False, "Master node not found or no public IP"
    
    ConsoleClient.log(f"Cluster Status: {cluster.cluster_state}")
    ConsoleClient.log(f"Master Node Public IP: {master_node.public_ip}")
    
    return True, master_node.public_ip

def verify_ssh_access(public_ip: str) -> bool:
    """Test SSH access to master node."""
    ssh_cmd = f'ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 root@{public_ip} "echo SSH test successful"'
    return os.system(ssh_cmd) == 0

def main():
    if len(sys.argv) != 2:
        print("Usage: python verify_cluster.py <cluster_id>")
        sys.exit(1)
    
    cluster_id = sys.argv[1]
    ConsoleClient.log(f"Verifying cluster: {cluster_id}")
    
    try:
        status_ok, public_ip = verify_cluster_status(cluster_id)
        if not status_ok:
            ConsoleClient.log("Failed to verify cluster status")
            sys.exit(1)
        
        if verify_ssh_access(public_ip):
            ConsoleClient.log("SSH access verified successfully")
        else:
            ConsoleClient.log("Failed to verify SSH access")
            sys.exit(1)
            
    except Exception as e:
        ConsoleClient.log(f"Error verifying cluster: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
