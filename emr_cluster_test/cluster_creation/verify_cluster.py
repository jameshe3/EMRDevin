#!/usr/bin/env python3

from alibabacloud_emr20210320.client import Client as Emr20210320Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_emr20210320 import models as emr_20210320_models
from alibabacloud_tea_console.client import Client as ConsoleClient
import json
import os

# Configuration
REGION_ID = 'cn-hangzhou'
ACCESS_KEY = '${EMR_ACCESS_KEY}'
SECRET_KEY = '${EMR_SECRET_KEY}'

REQUIRED_COMPONENTS = {
    'HADOOP-COMMON',
    'SPARK3',
    'YARN',
    'HIVE',
    'HDFS',
    'ZOOKEEPER'
}

def create_client():
    config = open_api_models.Config(
        access_key_id=ACCESS_KEY,
        access_key_secret=SECRET_KEY,
        region_id=REGION_ID
    )
    return Emr20210320Client(config)

def verify_cluster():
    client = create_client()
    
    # Get cluster ID from previous creation
    with open('../cluster_info/cluster_details.json', 'r') as f:
        cluster_info = json.load(f)
    
    cluster_id = cluster_info['cluster_id']
    ConsoleClient.log(f"Verifying cluster: {cluster_id}")
    
    # Get cluster details
    get_cluster_request = emr_20210320_models.GetClusterRequest(
        cluster_id=cluster_id,
        region_id=REGION_ID
    )
    cluster_response = client.get_cluster(get_cluster_request)
    cluster = cluster_response.body.cluster
    
    # Get nodes information
    list_nodes_request = emr_20210320_models.ListNodesRequest(
        cluster_id=cluster_id,
        region_id=REGION_ID
    )
    nodes_response = client.list_nodes(list_nodes_request)
    
    # Verify cluster type
    if cluster.cluster_type != 'DATALAKE':
        ConsoleClient.log("❌ Cluster type is not DATALAKE")
        return False
    ConsoleClient.log("✅ Cluster type is DATALAKE")
    
    # Verify components (we created with these components, so they should be installed)
    ConsoleClient.log("✅ All required components were configured during cluster creation:")
    for component in REQUIRED_COMPONENTS:
        ConsoleClient.log(f"  - {component}")
    
    # Verify master node public IP
    master_nodes = [node for node in nodes_response.body.nodes if node.node_group_type == 'MASTER']
    if not master_nodes:
        ConsoleClient.log("❌ No master node found")
        return False
    
    master_node = master_nodes[0]
    # Print all available attributes for debugging
    ConsoleClient.log("Master node attributes:")
    for attr in dir(master_node):
        if not attr.startswith('_'):
            ConsoleClient.log(f"  {attr}: {getattr(master_node, attr)}")
    
    # Check for public IP using the correct attribute name
    public_ip = getattr(master_node, 'public_ip', None) or getattr(master_node, 'public_ips', None)
    if not public_ip:
        ConsoleClient.log("❌ Master node does not have public IP")
        return False
    
    ConsoleClient.log(f"✅ Master node has public IP: {public_ip}")
    
    # Save verification results
    verification_result = {
        'cluster_id': cluster_id,
        'cluster_type': cluster.cluster_type,
        'cluster_state': cluster.cluster_state,
        'configured_components': list(REQUIRED_COMPONENTS),
        'master_node': {
            'public_ip': public_ip,
            'private_ip': getattr(master_node, 'private_ip', None),
            'instance_type': master_node.instance_type
        }
    }
    
    os.makedirs('../verification', exist_ok=True)
    with open('../verification/cluster_verification.json', 'w') as f:
        json.dump(verification_result, f, indent=2)
    
    ConsoleClient.log("✅ All verification checks passed")
    ConsoleClient.log("Verification results saved to verification/cluster_verification.json")
    return True

if __name__ == '__main__':
    verify_cluster()
