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

def test_local() -> Dict[str, Any]:
    """Test failing job and debug collection locally."""
    print("Starting local debug testing process...")
    
    try:
        # Step 1: Run failing job
        print("\n1. Running failing job scenarios...")
        from implementations.spark_debug.failing_job import create_failing_job
        create_failing_job()
        
        # Step 2: Initialize debug collector
        print("\n2. Collecting debug information...")
        from implementations.spark_debug.enhanced_collector import EnhancedSparkCollector
        collector = EnhancedSparkCollector(
            host='localhost',
            password=None  # Local testing doesn't need password
        )
        
        # Step 3: Run debug collection
        debug_info_path = collector.collect_all()
        print(f"\nDebug information collected at: {debug_info_path}")
        
        return {
            'debug_info_path': debug_info_path,
            'test_scenarios': [
                'memory_pressure',
                'invalid_data_access',
                'transformation_errors',
                'resource_exhaustion'
            ]
        }
    except Exception as e:
        print(f"\nError in test execution: {str(e)}")
        raise
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
        result = test_local()
        print("\nLocal testing completed successfully:")
        for key, value in result.items():
            print(f"{key}: {value}")
    except Exception as e:
        print(f"\nLocal testing failed: {str(e)}")
        print("\nError details:")
        import traceback
        traceback.print_exc()
        exit(1)
