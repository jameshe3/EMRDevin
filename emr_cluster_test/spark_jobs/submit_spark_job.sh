#!/bin/bash

# Copy the Spark job to the master node
scp -o StrictHostKeyChecking=no word_count.py root@116.62.148.109:/root/

# Submit the Spark job
ssh -o StrictHostKeyChecking=no root@116.62.148.109 "spark-submit /root/word_count.py"

# Get the results
scp -o StrictHostKeyChecking=no root@116.62.148.109:/tmp/spark_results.txt ./
