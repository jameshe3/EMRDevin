#!/usr/bin/env python3
import os
from typing import Dict, Any

from alibabacloud_emr20210320.client import Client as Emr20210320Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_emr20210320 import models as emr_20210320_models
from alibabacloud_tea_util.client import Client as UtilClient
from alibabacloud_tea_console.client import Client as ConsoleClient

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

def create_cluster(
    ak: str,
    sk: str,
    vpc_id: str = 'vpc-bp167nedawwbwmt9ti0pv',
    vswitch_id: str = 'vsw-bp1bg5pnp84s73pms20cs',
    security_group_id: str = 'sg-bp17gnp2vumd1o4okw4s',
    cluster_name: str = 'devin-test-cluster',
    instance_type: str = 'ecs.g7.xlarge',
    region_id: str = 'cn-hangzhou',
    zone_id: str = 'cn-hangzhou-i'
) -> Dict[str, Any]:
    """Create an EMR cluster with public IP enabled."""
    client = create_client(ak, sk, region_id)
    
    master_group = emr_20210320_models.NodeGroupConfig(
        node_group_type='MASTER',
        node_count=1,
        v_switch_ids=[vswitch_id],
        instance_types=[instance_type],
        payment_type='PayAsYouGo',
        with_public_ip=True,
        data_disks=[
            emr_20210320_models.DataDisk(
                category='cloud_essd',
                size=120,
                count=3
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
        v_switch_ids=[vswitch_id],
        instance_types=[instance_type],
        payment_type='PayAsYouGo',
        data_disks=[
            emr_20210320_models.DataDisk(
                category='cloud_essd',
                size=120,
                count=3
            )
        ],
        system_disk=emr_20210320_models.SystemDisk(
            category='cloud_essd',
            size=120
        )
    )

    request = emr_20210320_models.CreateClusterRequest(
        region_id=region_id,
        cluster_name=cluster_name,
        cluster_type='DATALAKE',
        deploy_mode='NORMAL',
        security_mode='NORMAL',
        release_version='EMR-5.9.0',
        node_attributes=emr_20210320_models.NodeAttributes(
            zone_id=zone_id,
            vpc_id=vpc_id,
            ram_role='AliyunECSInstanceForEMRRole',
            security_group_id=security_group_id,
            master_root_password='1qaz@WSX3edc'  # From requirements
        ),
        payment_type='PayAsYouGo',
        node_groups=[master_group, core_group],
        applications=[
            emr_20210320_models.Application(application_name='HDFS'),
            emr_20210320_models.Application(application_name='SPARK3'),
            emr_20210320_models.Application(application_name='ZOOKEEPER'),
            emr_20210320_models.Application(application_name='YARN'),
            emr_20210320_models.Application(application_name='HIVE'),
            emr_20210320_models.Application(application_name='HADOOP-COMMON')
        ],
        application_configs=[
            emr_20210320_models.ApplicationConfig(
                application_name='HIVE',
                config_file_name='hivemetastore-site.xml',
                config_item_key='hive.metastore.type',
                config_item_value='local'
            )
        ]
    )

    try:
        response = client.create_cluster(request)
        cluster_id = response.body.cluster_id
        operation_id = response.body.operation_id
        
        # Wait for cluster to be ready
        operation_state = await_cluster_status_to_running(
            client, region_id, cluster_id, operation_id
        )
        
        if operation_state in ['TERMINATED', 'FAILED']:
            raise Exception(f'Cluster creation failed with state: {operation_state}')
        
        return {
            'cluster_id': cluster_id,
            'operation_id': operation_id,
            'state': operation_state
        }
    except Exception as e:
        raise Exception(f'Failed to create EMR cluster: {str(e)}')

def await_cluster_status_to_running(
    client: Emr20210320Client,
    region_id: str,
    cluster_id: str,
    operation_id: str,
    max_attempts: int = 100,
    sleep_seconds: int = 5
) -> str:
    """Wait for cluster to reach running state."""
    get_operation_request = emr_20210320_models.GetOperationRequest(
        operation_id=operation_id,
        cluster_id=cluster_id,
        region_id=region_id
    )
    
    for attempt in range(max_attempts):
        operation_response = client.get_operation(get_operation_request)
        state = operation_response.body.operation.operation_state
        
        if state == 'COMPLETED':
            return state
        elif state in ['TERMINATED', 'FAILED']:
            return state
            
        ConsoleClient.log(f'Waiting for cluster (attempt {attempt + 1}/{max_attempts})')
        UtilClient.sleep(sleep_seconds * 1000)
    
    return 'TIMEOUT'

if __name__ == '__main__':
    # Example usage
    ak = os.getenv('ACCESS_KEY_ID')
    sk = os.getenv('ACCESS_KEY_SECRET')
    
    if not ak or not sk:
        raise ValueError("ACCESS_KEY_ID and ACCESS_KEY_SECRET environment variables must be set")
    
    cluster_info = create_cluster(
        ak=ak,
        sk=sk,
        vpc_id='vpc-bp167nedawwbwmt9ti0pv',
        vswitch_id='vsw-bp1bg5pnp84s73pms20cs',
        security_group_id='sg-bp17gnp2vumd1o4okw4s'
    )
    print(f'Cluster created: {cluster_info}')
