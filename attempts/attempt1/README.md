# EMR Cluster Creation and Spark Debugging - Attempt 1

## Overview
This attempt implements:
1. EMR cluster creation with public IP enabled
2. Cluster verification and SSH access
3. Spark debugging tool implementation
4. Failing job creation and debugging

## Configuration
- Region: cn-hangzhou
- VPC: vpc-bp167nedawwbwmt9ti0pv
- VSwitch: vsw-bp1bg5pnp84s73pms20cs
- Security Group: sg-bp17gnp2vumd1o4okw4s
- Instance Type: ecs.g7.xlarge
- EMR Version: EMR-5.9.0

## Cluster Details
- Cluster ID: c-875d732a1a98286d
- Master Node Public IP: 121.40.231.137
- Status: RUNNING
- SSH Access: Verified

## Directory Structure
```
attempt1/
├── cluster/          # EMR cluster creation and verification scripts
└── debug_info/       # Spark debugging tools and collected information
```

## Implementation Details
1. EMR Cluster Creation
   - Public IP enabled on master node
   - Proper credential handling via environment variables
   - Error handling and status verification

2. Spark Debugging Framework
   - Configuration file collection
   - Resource metrics gathering
   - Log analysis capabilities
   - LLM-based debugging support

3. Test Cases
   - Resource exhaustion test
   - File access error test
   - Configuration validation

## Usage
1. Set required environment variables:
   ```bash
   export ACCESS_KEY_ID=your_access_key
   export ACCESS_KEY_SECRET=your_secret_key
   export EMR_PASSWORD=your_emr_password      # Optional, defaults to 1qaz@WSX3edc
   export EMR_ROOT_PASSWORD=your_root_password # Optional, defaults to 1qaz@WSX3edc
   ```

2. Create EMR cluster:
   ```bash
   python create_emr_cluster.py
   ```

3. Verify cluster access:
   ```bash
   python verify_cluster.py <cluster_id>
   ```

4. Run debugging tests:
   ```bash
   cd debug_info
   python run_debug.py
   ```
