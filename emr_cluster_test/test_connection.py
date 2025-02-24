#!/usr/bin/env python3

from alibabacloud_emr20210320.client import Client as Emr20210320Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_emr20210320 import models as emr_20210320_models
from alibabacloud_tea_console.client import Client as ConsoleClient
import os
from aliyun_config import *

def create_client() -> Emr20210320Client:
    config = open_api_models.Config()
    config.access_key_id = ACCESS_KEY
    config.access_key_secret = SECRET_KEY
    config.region_id = REGION_ID
    return Emr20210320Client(config)

def test_connection():
    try:
        client = create_client()
        request = emr_20210320_models.ListClustersRequest(
            region_id=REGION_ID
        )
        
        response = client.list_clusters(request)
        print("Successfully connected to Alibaba Cloud EMR API")
        print("Response:", response.body.to_map())
        return True
    except Exception as e:
        print(f"Error connecting to Alibaba Cloud: {str(e)}")
        return False

if __name__ == "__main__":
    test_connection()
