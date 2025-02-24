#!/usr/bin/env python3
import sys
import os
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

# EMR configuration constants
EMR_CONFIG = {
    'REGION_ID': 'cn-hangzhou',
    'CLUSTER_TYPE': 'DATALAKE',
    'RELEASE_VERSION': 'EMR-5.9.0',
    'VPC_ID': 'vpc-bp167nedawwbwmt9ti0pv',
    'ZONE_ID': 'cn-hangzhou-i',
    'SECURITY_GROUP_ID': 'sg-bp17gnp2vumd1o4okw4s',
    'VSWITCH_ID': 'vsw-bp1bg5pnp84s73pms20cs',
    'INSTANCE_TYPE': 'ecs.g7.xlarge'
}

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
    emr_password: str,
    emr_root_password: str,
) -> emr_20210320_models.CreateClusterRequest:
    master_group = emr_20210320_models.NodeGroupConfig(
        node_group_type='MASTER',
        node_count=1,
        v_switch_ids=[v_switch_id],
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
        login_password=emr_password,
        node_attributes=emr_20210320_models.NodeAttributes(
            zone_id=zone_id,
            vpc_id=vpc_id,
            ram_role='AliyunECSInstanceForEMRRole',
            security_group_id=security_group_id,
            master_root_password=emr_root_password
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
    # Check required credentials
    access_key_id = os.getenv('ACCESS_KEY_ID')
    access_key_secret = os.getenv('ACCESS_KEY_SECRET')
    if not access_key_id or not access_key_secret:
        raise ValueError("ACCESS_KEY_ID and ACCESS_KEY_SECRET environment variables must be set")
    
    # Get EMR passwords with defaults
    emr_password = os.getenv('EMR_PASSWORD', '1qaz@WSX3edc')
    emr_root_password = os.getenv('EMR_ROOT_PASSWORD', '1qaz@WSX3edc')
    if os.getenv('EMR_PASSWORD') and os.getenv('EMR_ROOT_PASSWORD'):
        ConsoleClient.log("Using EMR passwords from environment variables")
    
    client = create_client(access_key_id, access_key_secret, 'cn-hangzhou')
    ConsoleClient.log('create cluster begins')
    
    region_id = 'cn-hangzhou'
    cluster_name = 'devin-test-cluster'
    cluster_type = 'DATALAKE'
    release_version = 'EMR-5.9.0'
    vpc_id = 'vpc-bp167nedawwbwmt9ti0pv'
    zone_id = 'cn-hangzhou-i'
    security_group_id = 'sg-bp17gnp2vumd1o4okw4s'
    v_switch_id = 'vsw-bp1bg5pnp84s73pms20cs'
    instance_type = 'ecs.g7.xlarge'
    
    create_cluster_req = get_create_cluster_request(
        region_id, cluster_name, cluster_type, release_version,
        vpc_id, zone_id, security_group_id, v_switch_id, instance_type,
        emr_password, emr_root_password
    )
    create_cluster_response = client.create_cluster(create_cluster_req)
    body = create_cluster_response.body
    ConsoleClient.log(f'clusterId is {body.cluster_id} .')
    ConsoleClient.log(f'clusterOperationId is {body.operation_id} .')
    
    operation_state = await_cluster_status_to_running(client, region_id, body.cluster_id, body.operation_id)
    if UtilClient.equal_string(operation_state, 'TERMINATED') or UtilClient.equal_string(operation_state, 'FAILED'):
        ConsoleClient.log(f'create cluster failed, please release cluster: {body.cluster_id} now.')
        return
    
    get_cluster_request = emr_20210320_models.GetClusterRequest(
        cluster_id=body.cluster_id,
        region_id=region_id
    )
    cluster_response = client.get_cluster(get_cluster_request)
    ConsoleClient.log(f'create cluster finished, cluster is ready, clusterId is {UtilClient.to_jsonstring(TeaCore.to_map(cluster_response.body.cluster))}.')

if __name__ == '__main__':
    main()
