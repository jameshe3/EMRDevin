#!/usr/bin/env python3
import os
import sys
from typing import List
from Tea.core import TeaCore

from alibabacloud_emr20210320.client import Client as Emr20210320Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_emr20210320 import models as emr_20210320_models
from alibabacloud_tea_util.client import Client as UtilClient
from alibabacloud_tea_console.client import Client as ConsoleClient
from alibabacloud_darabonba_number.client import Client as NumberClient

def create_client(
    access_key_id: str,
    access_key_secret: str,
    region_id: str,
) -> Emr20210320Client:
    config = open_api_models.Config()
    config.access_key_id = access_key_id
    config.access_key_secret = access_key_secret
    config.region_id = region_id
    return Emr20210320Client(config)

def get_create_cluster_request(
    region_id: str,
    cluster_name: str,
    cluster_type: str,
    release_version: str,
    vpc_id: str,
    zone_id: str,
    security_group_id: str,
    v_switch_id: str,
    instance_type: str,
    root_password: str,
) -> emr_20210320_models.CreateClusterRequest:
    master_group = emr_20210320_models.NodeGroupConfig(
        node_group_type='MASTER',
        node_count=1,
        v_switch_ids=[v_switch_id],
        instance_types=[instance_type],
        payment_type='PayAsYouGo',
        with_public_ip=True,  # Ensure public IP is enabled
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
        v_switch_ids=[v_switch_id],
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
        cluster_type=cluster_type,
        deploy_mode='NORMAL',
        security_mode='NORMAL',
        release_version=release_version,
        node_attributes=emr_20210320_models.NodeAttributes(
            zone_id=zone_id,
            vpc_id=vpc_id,
            ram_role='AliyunECSInstanceForEMRRole',
            security_group_id=security_group_id,
            master_root_password=root_password  # Set root password for SSH access
        ),
        payment_type='PayAsYouGo',
        node_groups=[master_group, core_group],
        applications=[
            emr_20210320_models.Application(application_name='HADOOP-COMMON'),
            emr_20210320_models.Application(application_name='SPARK3'),
            emr_20210320_models.Application(application_name='YARN'),
            emr_20210320_models.Application(application_name='HIVE'),
            emr_20210320_models.Application(application_name='HDFS'),
            emr_20210320_models.Application(application_name='ZOOKEEPER')
        ],
        application_configs=get_application_config('LOCAL')
    )
    return request

def get_application_config(meta_store_type: str) -> List[emr_20210320_models.ApplicationConfig]:
    return [
        emr_20210320_models.ApplicationConfig(
            application_name='Hive',
            config_file_name='hivemetastore-site.xml',
            config_item_key='hive.metastore.type',
            config_item_value='LOCAL'
        )
    ]

def await_cluster_status_to_running(
    client: Emr20210320Client,
    region_id: str,
    cluster_id: str,
    operation_id: str,
) -> str:
    get_operation_request = emr_20210320_models.GetOperationRequest(
        operation_id=operation_id,
        cluster_id=cluster_id,
        region_id=region_id
    )
    operation_response = client.get_operation(get_operation_request)
    loop_times = 0
    while not UtilClient.equal_string(operation_response.body.operation.operation_state, 'COMPLETED'):
        UtilClient.sleep(5000)
        ConsoleClient.log(f'waiting for cluster to be ready, passed 5 seconds, loop times: {loop_times}')
        operation_response = client.get_operation(get_operation_request)
        if NumberClient.gt(loop_times, 100) or UtilClient.equal_string(operation_response.body.operation.operation_state, 'TERMINATED') or UtilClient.equal_string(operation_response.body.operation.operation_state, 'FAILED'):
            break
        loop_times = NumberClient.add(loop_times, 1)
    return operation_response.body.operation.operation_state

def main():
    # Read credentials from config file
    import configparser
    import os
    
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'credentials.ini')
    credentials = configparser.ConfigParser()
    credentials.read(config_path)
    
    access_key_id = credentials.get('DEFAULT', 'access_key_id')
    access_key_secret = credentials.get('DEFAULT', 'access_key_secret')
    region_id = 'cn-hangzhou'
    
    client = create_client(access_key_id, access_key_secret, region_id)
    ConsoleClient.log('create cluster begins')
    
    cluster_name = 'devin-emr-cluster'
    cluster_type = 'DATALAKE'  # As specified in requirements
    release_version = 'EMR-5.9.0'
    vpc_id = 'vpc-bp167nedawwbwmt9ti0pv'
    zone_id = 'cn-hangzhou-i'
    security_group_id = 'sg-bp17gnp2vumd1o4okw4s'
    v_switch_id = 'vsw-bp1bg5pnp84s73pms20cs'
    instance_type = 'ecs.g7.xlarge'
    root_password = '1qaz@WSX3edc'  # ECS login password
    
    create_cluster_req = get_create_cluster_request(
        region_id, cluster_name, cluster_type, release_version,
        vpc_id, zone_id, security_group_id, v_switch_id, instance_type, root_password
    )
    
    # Create the cluster
    create_cluster_response = client.create_cluster(create_cluster_req)
    body = create_cluster_response.body
    cluster_id = body.cluster_id
    operation_id = body.operation_id
    
    ConsoleClient.log(f'clusterId is {cluster_id}')
    ConsoleClient.log(f'clusterOperationId is {operation_id}')
    
    # Wait for the cluster to be ready
    operation_state = await_cluster_status_to_running(client, region_id, cluster_id, operation_id)
    if UtilClient.equal_string(operation_state, 'TERMINATED') or UtilClient.equal_string(operation_state, 'FAILED'):
        ConsoleClient.log(f'create cluster failed, please release cluster: {cluster_id} now.')
        return None
    
    # Get cluster details
    get_cluster_request = emr_20210320_models.GetClusterRequest(
        cluster_id=cluster_id,
        region_id=region_id
    )
    cluster_response = client.get_cluster(get_cluster_request)
    ConsoleClient.log(f'create cluster finished, cluster is ready, clusterId is {cluster_id}')
    
    # Save cluster details to a file
    with open('cluster_details.json', 'w') as f:
        f.write(UtilClient.to_jsonstring(TeaCore.to_map(cluster_response.body.cluster)))
    
    # Save cluster ID to a file for later use
    with open('cluster_id.txt', 'w') as f:
        f.write(cluster_id)
    
    return cluster_id

if __name__ == '__main__':
    main()
