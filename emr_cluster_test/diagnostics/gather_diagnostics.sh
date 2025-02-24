#!/bin/bash

# Configuration files (already gathered successfully)
echo "Gathering configuration files..."
ssh -o StrictHostKeyChecking=no root@116.62.148.109 "
  cd /etc/taihao-apps && \
  tar czf /tmp/emr_configs.tar.gz \
    hadoop-conf/core-site.xml \
    hadoop-conf/hdfs-site.xml \
    hadoop-conf/yarn-site.xml \
    hadoop-conf/mapred-site.xml \
    spark-conf/spark-defaults.conf \
    spark-conf/spark-env.sh \
    hive-conf/hive-site.xml \
    zookeeper-conf/zoo.cfg
"

# Get the config archive
scp -o StrictHostKeyChecking=no root@116.62.148.109:/tmp/emr_configs.tar.gz diagnostics/configs/

# Extract configs
cd diagnostics/configs && tar xzf emr_configs.tar.gz && rm emr_configs.tar.gz && cd ../..

# Cluster resources information (already gathered successfully)
echo "Gathering cluster resource information..."
ssh -o StrictHostKeyChecking=no root@116.62.148.109 "
  echo '=== CPU Info ===' > /tmp/resources.txt && \
  lscpu >> /tmp/resources.txt && \
  echo -e '\n=== Memory Info ===' >> /tmp/resources.txt && \
  free -h >> /tmp/resources.txt && \
  echo -e '\n=== Disk Info ===' >> /tmp/resources.txt && \
  df -h >> /tmp/resources.txt && \
  echo -e '\n=== YARN Resource Manager Info ===' >> /tmp/resources.txt && \
  yarn node -list -all >> /tmp/resources.txt && \
  echo -e '\n=== YARN Applications ===' >> /tmp/resources.txt && \
  yarn application -list -appStates ALL >> /tmp/resources.txt
"

# Get resource information
scp -o StrictHostKeyChecking=no root@116.62.148.109:/tmp/resources.txt diagnostics/resources/

# Gather logs with corrected EMR paths
echo "Gathering logs..."
ssh -o StrictHostKeyChecking=no root@116.62.148.109 "
  # Create a temporary directory for logs
  mkdir -p /tmp/emr_logs && \
  
  # Get Spark application logs from YARN
  yarn logs -applicationId application_1740409812679_0004 > /tmp/emr_logs/spark_application.log 2>/dev/null
  
  # Get YARN logs
  cp /opt/apps/YARN/log/yarn-root-resourcemanager-*.log /tmp/emr_logs/ 2>/dev/null
  cp /opt/apps/YARN/log/yarn-root-nodemanager-*.log /tmp/emr_logs/ 2>/dev/null
  
  # Get HDFS logs
  cp /opt/apps/HDFS/log/hadoop-root-namenode-*.log /tmp/emr_logs/ 2>/dev/null
  
  # Archive all logs
  cd /tmp && tar czf emr_logs.tar.gz emr_logs/
"

# Get the logs archive
scp -o StrictHostKeyChecking=no root@116.62.148.109:/tmp/emr_logs.tar.gz diagnostics/logs/

# Extract logs
cd diagnostics/logs && tar xzf emr_logs.tar.gz && rm emr_logs.tar.gz
