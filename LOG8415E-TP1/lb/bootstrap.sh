#!/bin/bash
sudo yum update -y
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
source ~/.bashrc
nvm install --lts

cd /home/ec2-user/lb
npm install

node index.js > lb.log 2>&1 &