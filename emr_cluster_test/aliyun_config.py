#!/usr/bin/env python3

# Alibaba Cloud Configuration
ACCESS_KEY = '${EMR_ACCESS_KEY}'  # Set via environment variable
SECRET_KEY = '${EMR_SECRET_KEY}'  # Set via environment variable
REGION_ID = 'cn-hangzhou'  # Default region
VPC_ID = 'vpc-bp167nedawwbwmt9ti0pv'
VSWITCH_ID = 'vsw-bp1bg5pnp84s73pms20cs'
SECURITY_GROUP_ID = 'sg-bp17gnp2vumd1o4okw4s'
ECS_PASSWORD = '${EMR_ECS_PASSWORD}'  # Set via environment variable

# EMR Cluster Configuration
CLUSTER_NAME = 'devin-test-emr-cluster'
CLUSTER_TYPE = 'DATALAKE'
COMPONENTS = [
    'HADOOP-COMMON',
    'SPARK3',
    'YARN',
    'HIVE',
    'HDFS',
    'ZOOKEEPER'
]
