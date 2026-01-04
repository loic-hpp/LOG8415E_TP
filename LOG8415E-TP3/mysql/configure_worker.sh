#!/bin/bash
set -e

if [ "$(id -u)" -ne 0 ]; then
  echo "This script must be run as root. Use sudo ./install_mysql.sh"
  exit 1
fi

# the master ip is the param $1 if no param is given, exit with error
if [ -z "$1" ]; then
  echo "Usage: sudo ./configure_worker.sh <MASTER_IP> <POSITION>"
  exit 1
fi
MASTER_IP=$1
BIN_FILE=$2
POSITION=$3


MYSQL_USER="log8415"
MYSQL_PASSWORD="log8415Pwd@"

echo "Configuring MySQL Slave server..."
echo "relay-log = /var/log/mysql/mysql-relay-bin.log" | sudo tee -a /etc/mysql/mysql.conf.d/mysqld.cnf > /dev/null 2>&1
sudo sed -i "s/^#\s*server-id\s*=.*/server-id = 2/" /etc/mysql/mysql.conf.d/mysqld.cnf
sudo sed -i "s/^#\s*relay-log\s*=.*/relay-log = \/var\/log\/mysql\/mysql-relay-bin.log/" /etc/mysql/mysql.conf.d/mysqld.cnf
sudo systemctl restart mysql > /dev/null 2>&1

sudo mysql -e "CHANGE MASTER TO
  MASTER_HOST='$MASTER_IP',
  MASTER_USER='$MYSQL_USER',
  MASTER_PASSWORD='$MYSQL_PASSWORD',
  MASTER_LOG_FILE='$BIN_FILE',
  MASTER_LOG_POS=$POSITION;"
sudo mysql -e "START SLAVE;"
echo "Slave configuration completed."

echo "Verifying Slave status..."
SLAVE_STATUS=$(sudo mysql -e "SHOW SLAVE STATUS\G")
IO_RUNNING=$(echo "$SLAVE_STATUS" | grep 'Slave_IO_Running:' | awk '{print $2}')
SQL_RUNNING=$(echo "$SLAVE_STATUS" | grep 'Slave_SQL_Running:' | awk '{print $2}')
if [ "$IO_RUNNING" == "Yes" ] && [ "$SQL_RUNNING" == "Yes" ]; then
  echo "Slave is running successfully."
else
  echo "Slave is not running properly. Please check the MySQL slave status."
fi
echo "MySQL Slave configuration completed."