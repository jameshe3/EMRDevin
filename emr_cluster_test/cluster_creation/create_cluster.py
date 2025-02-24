#!/usr/bin/env python3

from alibabacloud_emr20210320.client import Client as Emr20210320Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_emr20210320 import models as emr_20210320_models
from alibabacloud_tea_console.client import Client as ConsoleClient
import json
import time
import os

# EMR Cluster Configuration
CLUSTER_NAME = 'devin-test-emr'
CLUSTER_TYPE = 'DATALAKE'
RELEASE_VERSION = 'EMR-5.9.0'
INSTANCE_TYPE = 'ecs.g7.xlarge'

# Network Configuration
REGION_ID = 'cn-hangzhou'
ZONE_ID = 'cn-hangzhou-i'
VPC_ID = 'vpc-bp167nedawwbwmt9ti0pv'
VSWITCH_ID = 'vsw-bp1bg5pnp84s73pms20cs'
SECURITY_GROUP_ID = 'sg-bp17gnp2vumd1o4okw4s'

# Authentication
ACCESS_KEY = '${EMR_ACCESS_KEY}'
SECRET_KEY = '${EMR_SECRET_KEY}'
ECS_PASSWORD = '${EMR_ECS_PASSWORD}'

# Components required
COMPONENTS = [
    'HADOOP-COMMON',
    'SPARK3',
    'YARN',
    'HIVE',
    'HDFS',
    'ZOOKEEPER'
]

def create_client():
    config = open_api_models.Config(
        access_key_id=ACCESS_KEY,
        access_key_secret=SECRET_KEY,
        region_id=REGION_ID
    )
    return Emr20210320Client(config)

def wait_for_cluster(client, cluster_id, operation_id):
    get_operation_request = emr_20210320_models.GetOperationRequest(
        operation_id=operation_id,
        cluster_id=cluster_id,
        region_id=REGION_ID
    )
    
    while True:
        operation_response = client.get_operation(get_operation_request)
        state = operation_response.body.operation.operation_state
        ConsoleClient.log(f"Current state: {state}")
        
        if state == 'COMPLETED':
            return True
        elif state in ['TERMINATED', 'FAILED']:
            ConsoleClient.log(f"Operation failed or terminated: {operation_response.body.operation.operation_state_message}")
            return False
            
        time.sleep(30)

def main():
    client = create_client()
    ConsoleClient.log('Creating EMR cluster...')
    
    # Create node groups
    master_group = emr_20210320_models.NodeGroupConfig(
        node_group_type='MASTER',
        node_count=1,
        instance_types=[INSTANCE_TYPE],
        v_switch_ids=[VSWITCH_ID],
        payment_type='PayAsYouGo',
        with_public_ip=True,  # Enable public IP for master node
        data_disks=[
            emr_20210320_models.DataDisk(
                category='cloud_essd',
                size=120,
                count=4  # Explicitly set disk count
            )
        ],
        system_disk=emr_20210320_models.SystemDisk(
            category='cloud_essd',
            size=120
        )
    )

    core_group = emr_20210320_models.NodeGroupConfig(
        node_group_type='CORE',
        node_count=2,
        instance_types=[INSTANCE_TYPE],
        v_switch_ids=[VSWITCH_ID],
        payment_type='PayAsYouGo',
        data_disks=[
            emr_20210320_models.DataDisk(
                category='cloud_essd',
                size=120,
                count=4  # Explicitly set disk count
            )
        ],
        system_disk=emr_20210320_models.SystemDisk(
            category='cloud_essd',
            size=120
        )
    )

    # Create cluster request
    request = emr_20210320_models.CreateClusterRequest(
        region_id=REGION_ID,
        cluster_name=CLUSTER_NAME,
        cluster_type=CLUSTER_TYPE,
        release_version=RELEASE_VERSION,
        security_mode='NORMAL',
        node_attributes=emr_20210320_models.NodeAttributes(
            zone_id=ZONE_ID,
            vpc_id=VPC_ID,
            security_group_id=SECURITY_GROUP_ID,
            master_root_password=ECS_PASSWORD,  # Set root password for SSH access
            ram_role='AliyunECSInstanceForEMRRole'
        ),
        applications=[
            emr_20210320_models.Application(
                application_name=app
            ) for app in COMPONENTS
        ],
        application_configs=[
            emr_20210320_models.ApplicationConfig(
                application_name='HIVE',
                config_file_name='hivemetastore-site.xml',
                config_item_key='hive.metastore.type',
                config_item_value='LOCAL'
            )
        ],
        node_groups=[master_group, core_group]
    )

    try:
        # Create cluster
        response = client.create_cluster(request)
        cluster_id = response.body.cluster_id
        operation_id = response.body.operation_id
        
        ConsoleClient.log(f'Cluster creation initiated. Cluster ID: {cluster_id}')
        ConsoleClient.log(f'Operation ID: {operation_id}')
        
        # Save initial cluster info
        cluster_info = {
            'cluster_id': cluster_id,
            'operation_id': operation_id,
            'creation_time': time.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'status': 'CREATING'
        }
        
        os.makedirs('../cluster_info', exist_ok=True)
        with open('../cluster_info/cluster_details.json', 'w') as f:
            json.dump(cluster_info, f, indent=2)
        
        # Wait for cluster creation
        if wait_for_cluster(client, cluster_id, operation_id):
            ConsoleClient.log('Cluster created successfully!')
            
            # Get final cluster details
            get_cluster_request = emr_20210320_models.GetClusterRequest(
                cluster_id=cluster_id,
                region_id=REGION_ID
            )
            cluster_response = client.get_cluster(get_cluster_request)
            
            # Update cluster info with final details
            cluster_info.update({
                'status': cluster_response.body.cluster.cluster_state,
                'node_groups': [
                    {
                        'type': group.node_group_type,
                        'nodes': [
                            {
                                'instance_id': node.instance_id,
                                'public_ip': node.public_ip,
                                'private_ip': node.private_ip
                            } for node in (group.nodes or [])
                        ]
                    } for group in cluster_response.body.cluster.node_groups
                ]
            })
            
            with open('../cluster_info/cluster_details.json', 'w') as f:
                json.dump(cluster_info, f, indent=2)
                
            ConsoleClient.log('Cluster information saved to cluster_info/cluster_details.json')
        else:
            ConsoleClient.log('Failed to create cluster')
            
    except Exception as e:
        ConsoleClient.log(f'Error creating cluster: {str(e)}')
        raise

if __name__ == '__main__':
    main()
