#!/bin/bash
set -e

if [ "$(id -u)" -ne 0 ]; then
  echo "This script must be run as root. Use sudo ./install_mysql.sh"
  exit 1
fi
echo "Configuring MySQL Master server..."
sudo sed -i "s/^#\s*log_bin/log_bin/" /etc/mysql/mysql.conf.d/mysqld.cnf
sudo sed -i "s/^#\s*binlog_do_db\s*=.*/binlog_do_db = sakila/" /etc/mysql/mysql.conf.d/mysqld.cnf
sudo sed -i "s/^#\s*server-id\s*=.*/server-id = 1/" /etc/mysql/mysql.conf.d/mysqld.cnf
sudo systemctl restart mysql > /dev/null 2>&1
echo "MySQL Master configuration completed."
# return MASTER_LOG_POS
MASTER_LOG_POS=$(sudo mysql -e "SHOW MASTER STATUS\G" | grep 'Position:' | awk '{print $2}')
BIN_FILE=$(sudo mysql -e "SHOW MASTER STATUS\G" | grep 'File:' | awk '{print $2}')
echo $BIN_FILE
echo $MASTER_LOG_POS
