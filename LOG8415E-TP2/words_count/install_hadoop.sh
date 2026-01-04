#!/bin/bash
# Shebang to tell the system to use bash to interpret this script

CHECKSUM="79a383e156022d6690da359120b25db8146452265d92a4e890d9ea78c2078a01b661daf78163ee9b4acef7106b01fd5c8d1a55f7ad284f88b31ab3f402ae3acf"
# checksum got from this link: https://downloads.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz.sha512

echo "Updating package lists..."
sudo apt update > /dev/null 2>&1
echo "Package lists updated."
echo "Installing Java..."
sudo apt install openjdk-21-jdk -y > /dev/null 2>&1

which java > /dev/null 2>&1
if [ $? -ne 0 ]; then  # The result of previous command is accessed via $? and should be 0 if java is found
    echo "Java installation failed and try again."
    exit 1
else
    echo "Java installed successfully."
fi
echo "Downloading Hadoop latest version..."
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz > /dev/null 2>&1
echo "Downloaded Hadoop successfully."
echo "Verifying Hadoop download..."
# Calculate cryptographic hash(checksum) to verify integrity of the download
shasum -a 512 hadoop-3.4.2.tar.gz | grep "$CHECKSUM" > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Hadoop download is corrupted. Try downloading again."
    exit 1
fi
echo "Hadoop download verified successfully."
echo "Extracting Hadoop files..."
tar -xzf hadoop-3.4.2.tar.gz > /dev/null 2>&1
echo "Hadoop files extracted successfully."
echo "Moving Hadoop files to /usr/local/hadoop..."
sudo mv hadoop-3.4.2 /usr/local/hadoop
echo "Hadoop moved to /usr/local/hadoop successfully."
JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
echo "export JAVA_HOME=$JAVA_HOME" >> /usr/local/hadoop/etc/hadoop/hadoop-env.sh
echo "Setting HADOOP_HOME environment variable..."
echo "export HADOOP_HOME=/usr/local/hadoop" >> ~/.bashrc
echo "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin" >> ~/.bashrc
source ~/.bashrc
echo "HADOOP_HOME environment variable set successfully."
