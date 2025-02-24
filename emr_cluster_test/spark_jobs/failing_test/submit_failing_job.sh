#!/bin/bash

# Copy the Spark job to the master node
scp -o StrictHostKeyChecking=no failing_job.py root@116.62.148.109:/root/

# Submit the Spark job
ssh -o StrictHostKeyChecking=no root@116.62.148.109 "spark-submit /root/failing_job.py" 2>&1 | tee failing_job_output.log
