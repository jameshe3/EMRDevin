#!/usr/bin/env python3
import os
from alibabacloud_emr20210320.client import Client as Emr20210320Client
from alibabacloud_tea_openapi import models as open_api_models

def delete_cluster(cluster_id: str):
    config = open_api_models.Config(
        access_key_id=os.getenv('ACCESS_KEY_ID'),
        access_key_secret=os.getenv('ACCESS_KEY_SECRET'),
        region_id='cn-hangzhou'
    )
    client = Emr20210320Client(config)
    client.delete_cluster(cluster_id)
    print(f"Cluster {cluster_id} deletion initiated")

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: cleanup_cluster.py <cluster_id>")
        sys.exit(1)
    delete_cluster(sys.argv[1])
