#!/bin/bash

CFG_SCRIPT="../../scripts/rbh-config"

sudo service mysqld start # sudo has been added before creating base

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
        echo "formatting as ext3..."
        mkfs.ext3 -F $LOOP_FILE || exit 1
    else
        # make sure it is consistent
        mkfs.ext3 -F $LOOP_FILE || exit 1
    fi

    mount -o loop,user_xattr -t ext3 $LOOP_FILE $MNT_PT || exit 1
fi

df $MNT_PT

# create testuser
getent passwd testuser || useradd testuser || exit 1

# create testgroup
getent group testgroup || groupadd testgroup || exit 1
