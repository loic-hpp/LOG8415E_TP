#!/bin/bash

sudo yum update -y
sudo yum install python3 python3-pip -y

pip3 install fastapi uvicorn

cd /home/ec2-user/app

export INSTANCE_ID=$1
export CLUSTER_NAME=$2

sudo -E -u ec2-user INSTANCE_ID="$1" CLUSTER_NAME="$2" \
  /home/ec2-user/.local/bin/uvicorn main_cluster:app \
  --host 0.0.0.0 --port 8000 > uvicorn.log 2>&1 &
