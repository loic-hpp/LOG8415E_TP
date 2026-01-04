#!/bin/bash

echo "Downloading Apache Spark..."
wget https://dlcdn.apache.org/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz > /dev/null 2>&1
echo "Downloaded Apache Spark successfully."
echo "Extracting Spark files..."
tar -xzf spark-4.0.1-bin-hadoop3.tgz > /dev/null 2>&1
echo "Spark files extracted successfully."
echo "Moving Spark files to /usr/local/spark..."
sudo mv spark-4.0.1-bin-hadoop3 /usr/local/spark
echo "Spark moved to /usr/local/spark successfully."

echo "Setting SPARK_HOME environment variable..."
echo "export SPARK_HOME=/usr/local/spark" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> ~/.bashrc
# Without the \ it is considered as a code vairiable not an env variable
source ~/.bashrc
echo "SPARK_HOME environment variable set successfully."