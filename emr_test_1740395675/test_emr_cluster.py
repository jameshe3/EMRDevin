#!/usr/bin/env python3
import os
import sys
import time
from pathlib import Path

def load_credentials():
    cred_file = Path(__file__).parent / 'credentials.env'
    with open(cred_file) as f:
        for line in f:
            key, value = line.strip().split('=', 1)
            os.environ[key] = value
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alibabacloud_emr20210320.client import Client as Emr20210320Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_emr20210320 import models as emr_20210320_models
from alibabacloud_tea_console.client import Client as ConsoleClient

from create_emr_cluster import create_client, main as create_cluster
from verify_cluster import verify_cluster
from spark_debugger.submit_and_debug import submit_job
from spark_debugger.collectors.spark_info_collector import SparkInfoCollector

def get_master_node_ip(client: Emr20210320Client, cluster_id: str) -> str:
    list_nodes_request = emr_20210320_models.ListNodesRequest(
        cluster_id=cluster_id,
        region_id='cn-hangzhou'
    )
    nodes_response = client.list_nodes(list_nodes_request)
    for node in nodes_response.body.node_list:
        if node.node_group_type == 'MASTER':
            return node.public_ip
    raise Exception("Master node not found")

def main():
    try:
        # Load credentials
        load_credentials()
        # 1. Create cluster
        ConsoleClient.log("Creating EMR cluster...")
        cluster_id = create_cluster()
        if not cluster_id:
            raise Exception("Failed to create cluster - no cluster ID returned")
        
        # 2. Verify cluster status
        ConsoleClient.log("\nVerifying cluster status...")
        verify_cluster(cluster_id)
        
        # 3. Get master node IP
        client = create_client(
            os.getenv('ACCESS_KEY_ID'),
            os.getenv('ACCESS_KEY_SECRET'),
            'cn-hangzhou'
        )
        master_ip = get_master_node_ip(client, cluster_id)
        ConsoleClient.log(f"\nMaster node IP: {master_ip}")
        
        # 4. Submit test jobs and collect info
        ConsoleClient.log("\nSubmitting test jobs...")
        submit_job(master_ip, os.getenv('EMR_ROOT_PASSWORD'))
        
        # 5. Collect and analyze debug info
        ConsoleClient.log("\nCollecting debug information...")
        collector = SparkInfoCollector(master_ip, os.getenv('EMR_ROOT_PASSWORD'))
        debug_dir = collector.collect_all()
        
        ConsoleClient.log(f"\nTest completed successfully!")
        ConsoleClient.log(f"Cluster ID: {cluster_id}")
        ConsoleClient.log(f"Debug information: {debug_dir}")
        
        return cluster_id, debug_dir
    except Exception as e:
        ConsoleClient.log(f"Error during test execution: {str(e)}")
        raise

if __name__ == '__main__':
    main()
