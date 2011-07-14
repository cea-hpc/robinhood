#!/bin/sh

CFG_SCRIPT="../../scripts/rbh-config"

service mysqld start

$CFG_SCRIPT test_db  robinhood_test robinhood || $CFG_SCRIPT create_db robinhood_test localhost robinhood
$CFG_SCRIPT empty_db robinhood_test

LOOP_FILE=/tmp/rbh.loop.huge_cont
MNT_PT=/tmp/mnt.rbh_huge

CONT_SIZE_MB=4000

echo "Checking test filesystem..."

if [[ ! -d $MNT_PT ]]; then
    echo "creating $MNT_PT"
    mkdir $MNT_PT || exit 1
fi

if [[ ! -s $LOOP_FILE ]]; then
    echo "creating big file container $LOOP_FILE..."

    # check available size
    avail=`df -k /tmp | xargs | awk '{print $(NF-2)}'`
    if (( $avail < $(( $CONT_SIZE_MB * 1024 )) )); then
	echo "Not enough space available in /tmp: free=$avail KB , needed=$(( $CONT_SIZE_MB * 1024 )) KB"
	exit 1
    fi

    dd if=/dev/zero of=$LOOP_FILE bs=1M count=$CONT_SIZE_MB || exit 1
    echo "formatting as ext3..."
    mkfs.ext3 -F $LOOP_FILE -i 1024 || exit 1
fi

# mount
mnted=`mount | grep $MNT_PT | grep loop | wc -l`
if (( $mnted == 0 )); then
    mount -o loop -t ext3 $LOOP_FILE $MNT_PT || exit 1
fi

# fill it when plenty inodes
ino=`df -i $MNT_PT | xargs | awk '{print $(NF-2)}'`
echo "$ino free inodes"
# take 10% margin
ino=$(( $ino * 9/10 ))

echo "creating $ino inodes..."
../fill_fs.sh $MNT_PT $ino || exit 1

