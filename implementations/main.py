#!/usr/bin/env python3
import os
import time
from typing import Dict, Any

from implementations.emr_cluster.create_cluster import create_cluster
from implementations.emr_cluster.verify_cluster import verify_cluster
from implementations.spark_debug.failing_job import create_failing_job
from implementations.spark_debug.enhanced_collector import EnhancedSparkCollector

def validate_environment() -> None:
    """Validate that all required environment variables are set."""
    required_vars = ['ACCESS_KEY_ID', 'ACCESS_KEY_SECRET']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

def main() -> Dict[str, Any]:
    """Main execution flow."""
    print("Starting EMR cluster creation and debugging process...")
    
    # Validate environment
    validate_environment()
    
    try:
        # Step 1: Create EMR cluster
        print("\n1. Creating EMR cluster...")
        cluster_info = create_cluster(
            ak=os.getenv('ACCESS_KEY_ID'),
            sk=os.getenv('ACCESS_KEY_SECRET'),
            vpc_id='vpc-bp167nedawwbwmt9ti0pv',
            vswitch_id='vsw-bp1bg5pnp84s73pms20cs',
            security_group_id='sg-bp17gnp2vumd1o4okw4s'
        )
        print(f"Cluster created with ID: {cluster_info['cluster_id']}")
        
        # Step 2: Verify cluster and SSH connectivity
        print("\n2. Verifying cluster status and connectivity...")
        cluster_status = verify_cluster(
            cluster_id=cluster_info['cluster_id'],
            ak=os.getenv('ACCESS_KEY_ID'),
            sk=os.getenv('ACCESS_KEY_SECRET')
        )
        
        if not cluster_status['ssh_connectivity']['success']:
            raise Exception(
                f"SSH connectivity failed: {cluster_status['ssh_connectivity']['error']}"
            )
        
        print("Cluster verification successful:")
        print(f"- State: {cluster_status['cluster_state']}")
        print(f"- Master IP: {cluster_status['master_public_ip']}")
        
        # Step 3: Initialize debug collector
        print("\n3. Initializing debug collector...")
        collector = EnhancedSparkCollector(
            host=cluster_status['master_public_ip'],
            password='1qaz@WSX3edc'  # From requirements
        )
        
        # Step 4: Run failing job
        print("\n4. Submitting failing job...")
        try:
            create_failing_job()
        except Exception as e:
            print(f"Job failed as expected: {str(e)}")
        
        # Step 5: Collect debug information
        print("\n5. Collecting debug information...")
        debug_info_path = collector.collect_all()
        print(f"Debug information collected at: {debug_info_path}")
        
        return {
            'cluster_id': cluster_info['cluster_id'],
            'master_ip': cluster_status['master_public_ip'],
            'cluster_state': cluster_status['cluster_state'],
            'debug_info_path': debug_info_path
        }
        
    except Exception as e:
        print(f"\nError in execution: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        result = main()
        print("\nExecution completed successfully:")
        for key, value in result.items():
            print(f"{key}: {value}")
    except Exception as e:
        print(f"\nExecution failed: {str(e)}")
        exit(1)
