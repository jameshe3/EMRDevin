#!/usr/bin/env python3

from aliyunsdkcore.client import AcsClient
from aliyunsdkemr.request.v20210320 import CreateClusterV2Request
from aliyunsdkcore.acs_exception.exceptions import ClientException, ServerException
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from aliyun_config import *

class EMRClient:
    def __init__(self):
        self.client = AcsClient(ACCESS_KEY, SECRET_KEY, REGION_ID)
    
    def create_cluster(self):
        request = CreateClusterV2Request.CreateClusterV2Request()
        request.set_accept_format('json')
        
        cluster_config = {
            "name": CLUSTER_NAME,
            "releaseVersion": "EMR-5.7.0",
            "clusterType": CLUSTER_TYPE,
            "components": COMPONENTS,
            "nodeGroups": [
                {
                    "nodeGroupType": "MASTER",
                    "nodeGroupName": "master_group",
                    "nodeCount": 1,
                    "nodeGroupConfig": {
                        "instanceType": "ecs.g6.xlarge",
                        "diskSize": 80,
                        "diskType": "cloud_efficiency",
                        "systemDiskSize": 120,
                        "systemDiskType": "cloud_efficiency",
                        "withPublicIp": True,  # Enable public IP
                        "vSwitchId": VSWITCH_ID,
                        "securityGroupId": SECURITY_GROUP_ID,
                        "nodeAttributes": {
                            "master_root_password": ECS_PASSWORD
                        }
                    }
                },
                {
                    "nodeGroupType": "CORE",
                    "nodeGroupName": "core_group",
                    "nodeCount": 2,
                    "nodeGroupConfig": {
                        "instanceType": "ecs.g6.xlarge",
                        "diskSize": 80,
                        "diskType": "cloud_efficiency",
                        "systemDiskSize": 120,
                        "systemDiskType": "cloud_efficiency",
                        "vSwitchId": VSWITCH_ID,
                        "securityGroupId": SECURITY_GROUP_ID
                    }
                }
            ],
            "vpcConfig": {
                "vpcId": VPC_ID,
                "vSwitchId": VSWITCH_ID,
                "securityGroupId": SECURITY_GROUP_ID
            }
        }
        
        request.set_content(json.dumps(cluster_config).encode('utf-8'))
        
        try:
            response = self.client.do_action_with_exception(request)
            return json.loads(response)
        except (ClientException, ServerException) as e:
            print(f"Error creating cluster: {str(e)}")
            raise
