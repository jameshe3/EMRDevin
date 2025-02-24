#!/usr/bin/env python3
import os
import subprocess
from typing import Dict, Any, Optional

from alibabacloud_emr20210320.client import Client as Emr20210320Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_emr20210320 import models as emr_20210320_models

def create_client(
    access_key_id: str,
    access_key_secret: str,
    region_id: str = 'cn-hangzhou'
) -> Emr20210320Client:
    """Create an EMR client with the provided credentials."""
    config = open_api_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
        region_id=region_id
    )
    return Emr20210320Client(config)

def verify_ssh_connection(host: str, password: str = '1qaz@WSX3edc') -> Dict[str, Any]:
    """Verify SSH connectivity to the master node."""
    cmd = f'sshpass -p "{password}" ssh -o StrictHostKeyChecking=no root@{host} "echo OK"'
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        success = result.returncode == 0 and 'OK' in result.stdout
        return {
            'success': success,
            'error': None if success else result.stderr
        }
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'error': 'SSH connection timed out'
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

def verify_cluster(
    cluster_id: str,
    ak: Optional[str] = None,
    sk: Optional[str] = None,
    region_id: str = 'cn-hangzhou'
) -> Dict[str, Any]:
    """Verify cluster status and SSH connectivity."""
    # Use environment variables if not provided
    if ak is None:
        ak = os.getenv('ACCESS_KEY_ID')
    if sk is None:
        sk = os.getenv('ACCESS_KEY_SECRET')
    
    if not ak or not sk:
        raise ValueError("ACCESS_KEY_ID and ACCESS_KEY_SECRET must be provided")
    
    client = create_client(ak, sk, region_id)
    
    try:
        # Get cluster info
        get_cluster_request = emr_20210320_models.GetClusterRequest(
            cluster_id=cluster_id,
            region_id=region_id
        )
        response = client.get_cluster(get_cluster_request)
        cluster = response.body.cluster
        
        # Get master node info
        list_nodes_request = emr_20210320_models.ListNodesRequest(
            cluster_id=cluster_id,
            region_id=region_id
        )
        nodes_response = client.list_nodes(list_nodes_request)
        # Add debug logging
        print(f"Node response body attributes: {dir(nodes_response.body)}")
        nodes = getattr(nodes_response.body, 'node_list', [])
        if not nodes:
            nodes = getattr(nodes_response.body, 'nodes', [])
        
        master_node = next(
            (node for node in nodes if getattr(node, 'node_group_type', '').upper() == 'MASTER'),
            None
        )
        
        if not master_node:
            raise Exception('Master node not found')
        
        if not master_node.public_ip:
            raise Exception('Master node does not have a public IP')
        
        # Verify SSH connectivity
        ssh_status = verify_ssh_connection(host=master_node.public_ip)
        
        return {
            'cluster_state': cluster.cluster_state,
            'master_public_ip': master_node.public_ip,
            'zone_id': getattr(cluster.node_attributes, 'zone_id', 'unknown'),
            'vpc_id': getattr(cluster.node_attributes, 'vpc_id', 'unknown'),
            'security_group_id': getattr(cluster.node_attributes, 'security_group_id', 'unknown'),
            'ssh_connectivity': ssh_status,
            'cluster_state': getattr(cluster, 'cluster_state', 'unknown'),
            'release_version': getattr(cluster, 'release_version', 'unknown'),
            'cluster_type': getattr(cluster, 'cluster_type', 'unknown')
        }
    except Exception as e:
        raise Exception(f'Failed to verify cluster: {str(e)}')

if __name__ == '__main__':
    # Get credentials from environment
    ak = os.getenv('ACCESS_KEY_ID')
    sk = os.getenv('ACCESS_KEY_SECRET')
    
    if not ak or not sk:
        raise ValueError("ACCESS_KEY_ID and ACCESS_KEY_SECRET environment variables must be set")
    
    try:
        # Example usage - replace with your cluster ID
        cluster_status = verify_cluster('c-your-cluster-id', ak, sk)
        print("\nCluster verification results:")
        for key, value in cluster_status.items():
            if key != 'ssh_connectivity':
                print(f"{key}: {value}")
            else:
                print(f"SSH connectivity: {'Success' if value['success'] else 'Failed - ' + str(value['error'])}")
    except Exception as e:
        print(f"\nError verifying cluster: {str(e)}")
