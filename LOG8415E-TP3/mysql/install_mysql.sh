#!/bin/bash
set -e

if [ "$(id -u)" -ne 0 ]; then
  echo "This script must be run as root. Use sudo ./install_mysql.sh"
  exit 1
fi

MYSQL_USER="log8415"
MYSQL_PASSWORD="log8415Pwd@"
echo "Updating package lists..."
sudo apt update > /dev/null 2>&1

echo "Installing MySQL server..."
sudo apt-get install mysql-server -y > /dev/null 2>&1
echo "MySQL server installation completed."

echo "Create a new MySQL user and set password..."
sudo mysql -e "CREATE USER '$MYSQL_USER'@'%' IDENTIFIED BY '$MYSQL_PASSWORD'; GRANT ALL PRIVILEGES ON *.* TO '$MYSQL_USER'@'%' WITH GRANT OPTION; FLUSH PRIVILEGES;" > /dev/null 2>&1
sudo sed -i "s/bind-address.*/bind-address = 0.0.0.0/" /etc/mysql/mysql.conf.d/mysqld.cnf
sudo systemctl restart mysql > /dev/null 2>&1
echo "User $MYSQL_USER created with all privileges."

echo "Installing Sakila database..."
wget https://downloads.mysql.com/docs/sakila-db.tar.gz > /dev/null 2>&1
tar -xzf sakila-db.tar.gz > /dev/null 2>&1
sudo mysql -u root < sakila-db/sakila-schema.sql > /dev/null 2>&1
sudo mysql -u root < sakila-db/sakila-data.sql > /dev/null 2>&1
rm -rf sakila-db sakila-db.tar.gz
echo "Sakila database installation completed."

echo "Installing Sysbench..."
sudo apt-get install sysbench -y > /dev/null 2>&1
sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user="$MYSQL_USER" --mysql-password="$MYSQL_PASSWORD" prepare > /dev/null 2>&1
sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user="$MYSQL_USER" --mysql-password="$MYSQL_PASSWORD" run > /dev/null 2>&1
echo "Sysbench installation completed."