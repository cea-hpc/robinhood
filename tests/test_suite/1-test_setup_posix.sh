#!/bin/bash

# Test whether the testsuite is running from the source tree or has
# been installed.
if [ -d "../../src/robinhood" ]; then
    RBH_TEST_INSTALLED=0
else
    RBH_TEST_INSTALLED=1
fi

if [ $RBH_TEST_INSTALLED = 0 ]; then
    CFG_SCRIPT="../../scripts/rbh-config"
else
    CFG_SCRIPT="rbh-config"
fi


function start_service {
if [ -x /usr/bin/systemctl ]; then
	#RHEL7
	systemctl start $1
else
	#RHEL6 or less
	service $1 start
fi
}

if rpm -q mariadb; then
	start_service mariadb
else
	start_service mysqld
fi

$CFG_SCRIPT test_db  robinhood_test robinhood || $CFG_SCRIPT create_db robinhood_test localhost robinhood
$CFG_SCRIPT empty_db robinhood_test
$CFG_SCRIPT repair_db robinhood_test


LOOP_FILE=/tmp/rbh.loop.cont
MNT_PT=/tmp/mnt.rbh

echo "Checking test filesystem..."

if [[ ! -d $MNT_PT ]]; then
    echo "creating $MNT_PT"
    mkdir $MNT_PT || exit 1
fi

mnted=`mount | grep $MNT_PT | grep loop | wc -l`

if (( $mnted == 0 )); then
    if [[ ! -s $LOOP_FILE ]]; then
        echo "creating file container $LOOP_FILE..."
        dd if=/dev/zero of=$LOOP_FILE bs=1M count=400 || exit 1
        echo "formatting as ext4..."
        mkfs.ext4 -F $LOOP_FILE || exit 1
    else
        # make sure it is consistent
        mkfs.ext4 -F $LOOP_FILE || exit 1
    fi

    mount -o loop,user_xattr -t ext4 $LOOP_FILE $MNT_PT || exit 1
fi

df $MNT_PT

# create testuser
getent passwd testuser || useradd testuser || exit 1

# create testgroup
getent group testgroup || groupadd testgroup || exit 1
