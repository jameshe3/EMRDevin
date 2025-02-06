#!/usr/bin/env python3
import os
from alibabacloud_emr20210320.client import Client as Emr20210320Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_emr20210320 import models as emr_20210320_models

def create_client():
    config = open_api_models.Config(
        access_key_id=os.getenv('ACCESS_KEY_ID'),
        access_key_secret=os.getenv('ACCESS_KEY_SECRET'),
        region_id='cn-hangzhou'
    )
    return Emr20210320Client(config)

def verify_cluster(cluster_id: str):
    client = create_client()
    get_cluster_request = emr_20210320_models.GetClusterRequest(
        cluster_id=cluster_id,
        region_id='cn-hangzhou'
    )
    response = client.get_cluster(get_cluster_request)
    cluster = response.body.cluster
    
    print(f"Cluster State: {cluster.cluster_state}")
    print(f"EMR Version: {cluster.release_version}")
    print(f"Cluster Type: {cluster.cluster_type}")
    print(f"Zone: {cluster.node_attributes.zone_id}")
    print(f"VPC ID: {cluster.node_attributes.vpc_id}")
    print(f"Security Group ID: {cluster.node_attributes.security_group_id}")
    print("\nInstalled Applications:")
    list_cluster_service_request = emr_20210320_models.ListClusterServiceRequest(
        cluster_id=cluster_id,
        region_id='cn-hangzhou'
    )
    services_response = client.list_cluster_service(list_cluster_service_request)
    for service in services_response.body.service_list:
        print(f"- {service.service_name} ({service.service_status})")

if __name__ == '__main__':
    verify_cluster('c-5b14c8044ba3fb30')
