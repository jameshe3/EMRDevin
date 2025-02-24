#!/usr/bin/env python3
import os
from alibabacloud_emr20210320.client import Client as Emr20210320Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_emr20210320 import models as emr_20210320_models
from alibabacloud_tea_util.client import Client as UtilClient

def get_master_ip(cluster_id: str) -> str:
    # Verify environment variables
    access_key_id = os.getenv('ACCESS_KEY_ID')
    access_key_secret = os.getenv('ACCESS_KEY_SECRET')
    if not access_key_id or not access_key_secret:
        raise ValueError("ACCESS_KEY_ID and ACCESS_KEY_SECRET must be set")

    # Initialize client
    config = open_api_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
        region_id='cn-hangzhou'
    )
    client = Emr20210320Client(config)

    try:
        # List nodes request
        list_nodes_request = emr_20210320_models.ListNodesRequest(
            cluster_id=cluster_id,
            region_id='cn-hangzhou'
        )
        nodes_response = client.list_nodes(list_nodes_request)
        
        # Debug output
        print("Response:", UtilClient.to_jsonstring(nodes_response.body))
        
        # Find master node
        for node in nodes_response.body.node_list:
            if node.node_group_type == 'MASTER':
                return node.public_ip
        raise Exception("Master node not found")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: get_master_ip.py <cluster_id>")
        sys.exit(1)
    
    try:
        master_ip = get_master_ip(sys.argv[1])
        print(f"Master Node Public IP: {master_ip}")
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
