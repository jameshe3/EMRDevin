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

def create_client():
    config = open_api_models.Config(
        access_key_id=ACCESS_KEY,
        access_key_secret=SECRET_KEY,
        region_id=REGION_ID
    )
    return Emr20210320Client(config)

def main():
    client = create_client()
    
    # Get cluster ID from previous creation
    with open('../cluster_info/cluster_details.json', 'r') as f:
        cluster_info = json.load(f)
    
    cluster_id = cluster_info['cluster_id']
    
    # Get cluster details
    get_cluster_request = emr_20210320_models.GetClusterRequest(
        cluster_id=cluster_id,
        region_id=REGION_ID
    )
    cluster_response = client.get_cluster(get_cluster_request)
    
    # Get nodes information
    list_nodes_request = emr_20210320_models.ListNodesRequest(
        cluster_id=cluster_id,
        region_id=REGION_ID
    )
    nodes_response = client.list_nodes(list_nodes_request)
    
    # Update cluster information
    cluster_info.update({
        'status': cluster_response.body.cluster.cluster_state,
        'cluster_type': cluster_response.body.cluster.cluster_type,
        'components': [app.name for app in cluster_response.body.cluster.applications],
        'nodes': [
            {
                'node_id': node.node_id,
                'node_group_type': node.node_group_type,
                'public_ip_address': node.public_ip_address,
                'private_ip_address': node.private_ip_address,
                'instance_type': node.instance_type
            } for node in nodes_response.body.nodes
        ]
    })
    
    # Save updated cluster information
    with open('../cluster_info/cluster_details.json', 'w') as f:
        json.dump(cluster_info, f, indent=2)
    
    ConsoleClient.log('Updated cluster information saved to cluster_info/cluster_details.json')
    
    # Verify public IP is enabled for master node
    master_nodes = [node for node in cluster_info['nodes'] if node['node_group_type'] == 'MASTER']
    if master_nodes and master_nodes[0]['public_ip_address']:
        ConsoleClient.log(f"Master node public IP: {master_nodes[0]['public_ip_address']}")
    else:
        ConsoleClient.log("Warning: Master node does not have a public IP!")

if __name__ == '__main__':
    main()
