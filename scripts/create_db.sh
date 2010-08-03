#!/bin/sh

# this scripts creates the database for robinhood
# it must be launched by root.

who=`whoami`

if [[ $who != root ]]; then
	echo "This script must be executed by root" >&2
	exit 1
fi

echo "#######################################################"
echo "# Configuration script for RobinHood's MySQL database #"
echo "#######################################################"
echo
echo "Checking system configuration..."

if [[ ! -x `which mysqladmin` ]]; then
	echo "Command 'mysqladmin' not found."
	echo "Install 'mysql' and 'mysql-server' packages on your system."
	exit 2 
fi
echo "mysqladmin command OK."

if [[ ! -x `which mysql_config` ]]; then
	echo "Command 'mysql_config' not found."
	echo "Install 'mysql' package on your system."
	exit 2 
fi
echo "mysql_config command OK."

version=`mysql_config --version | cut -d . -f 1`
if (( $? )); then
	echo "Error executing 'mysql_config --version'."
	exit 2 
fi
echo "MySQL version is $version."

/sbin/service mysqld status | grep running >/dev/null 2>/dev/null
if (( $? )); then
 	running=1
 	pgrep mysqld >/dev/null || running=0
 
 	if (( $running == 0 )); then
 		echo "Service 'mysqld' is not running."
 		echo "It must be started to run this script."
 		exit 2 
 	else
 		echo "mysqld is running"
 	fi
else
 	echo "mysqld service OK."
fi

if [[ ! -x `which mysql` ]]; then
	echo "Command 'mysql' not found."
	echo "Install 'mysql' package on your system."
	exit 2 
fi
echo "mysql command OK."


echo
echo -n "Enter a custom identifier for your filesystem. E.g. lustre (max 8 chars): "

while (( 1 )); do
	read fsname
	if [[ "$fsname" =~ "^[a-zA-Z][a-zA-Z0-9_]{0,7}$" ]]; then
		break
	else
		echo
		unmatched=`echo $fsname | sed -e "s/[a-zA-Z0-9_]//g"`
		echo "Error: unexpected '" $unmatched "'."
		echo "Filesystem name must only contain alpha-num chars with no space."
	fi
done

echo
echo "Enter hosts where robinhood commands will run."
echo "You can use '%' as wildcard. E.g: \"%\" for all hosts, \"cluster%\" for all nodes starting with 'cluster'..."

read clienthost

while (( 1 )); do
	echo
	echo -n "Choose a password for connecting to the database (user 'robinhood'): "
	read -s pass1
	echo
	echo -n "Confirm password: "
	read -s pass2
	echo
	
	if [[ "$pass1" = "$pass2" ]]; then
		break
	else
		echo "Passwords don't match."
		echo "Try again."
	fi
done

echo "Write this password to /etc/robinhood.d/.dbpassword file"


DB_NAME="robinhood_$fsname"

echo
echo "Configuration summary:"
echo "- Database name: '$DB_NAME'"
echo "- Client hosts: '$clienthost'"
echo "- Database user name: 'robinhood'"
echo
echo -n "Do you agree? [y/N]"

read -n 1 ok
echo
if [[ $ok != [yY] ]]; then
	echo "aborting."
	exit 1
fi

echo
echo "Creating database '$DB_NAME'..."

mysqladmin create $DB_NAME

if (( $? )); then
	echo "Error creating DB."
	exit 1
fi
sleep 1
echo "done"

echo
echo "Setting access right for user 'robinhood'@'$clienthost'..."

sleep 1

echo
mysql $DB_NAME << EOF
GRANT USAGE ON $DB_NAME.* TO 'robinhood'@'localhost' IDENTIFIED BY '$pass1' ;
GRANT USAGE ON $DB_NAME.* TO 'robinhood'@'$clienthost' IDENTIFIED BY '$pass1' ;
GRANT ALL PRIVILEGES ON $DB_NAME.* TO 'robinhood'@'localhost' IDENTIFIED BY '$pass1' ;
GRANT ALL PRIVILEGES ON $DB_NAME.* TO 'robinhood'@'$clienthost' IDENTIFIED BY '$pass1' ;
FLUSH PRIVILEGES;

SHOW GRANTS FOR 'robinhood'@'$clienthost';
EOF

if (( $? )); then
	echo "Error setting access rights for 'robinhood'@'$clienthost'"
	exit 1
fi

echo
echo "Testing connection to '$DB_NAME'..."
sleep 1
mysql --user=robinhood --password=$pass1 $DB_NAME << EOF
quit
EOF

if (( $? )); then
	echo "Connection to $DB_NAME@localhost failed"
	exit 1
fi

echo "Database sucessfully configured!"
