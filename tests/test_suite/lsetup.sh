#!/bin/bash

export OSTSIZE=400000
export OSTCOUNT=4
LUSTRE_SRC_DIR=${LUSTRE_SRC_DIR:-/usr/lib64}

if [[ "$1" == "mount" || -z "$1" ]]; then

	# moving to lustre test dir
	if [[ -d $LUSTRE_SRC_DIR ]]; then
		cd $LUSTRE_SRC_DIR/lustre/tests
	else
		echo "$LUSTRE_SRC_DIR: no such directory"
		exit 1
	fi

	# first check if lustre is already mounted
	mounted=`mount | grep /mnt/lustre | wc -l`
	if (( $mounted > 0 )); then
		echo "Lustre is already mounted:"
		mount | grep /mnt/lustre
		echo "Unmounting previous instance:"
		./llmountcleanup.sh
		umount /mnt/lustre
	fi

	echo "Mounting lustre..."
	./llmount.sh

	mount | grep /mnt/lustre
    exit $?

elif [[ "$1" == "umount" ]]; then

	# moving to lustre test dir
	if [[ -d $LUSTRE_SRC_DIR ]]; then
		cd $LUSTRE_SRC_DIR/lustre/tests
	else
		echo "$LUSTRE_SRC_DIR: no such directory"
		exit 1
	fi

	# first check if lustre is already mounted
	mounted=`mount | grep /mnt/lustre | wc -l`
	if (( $mounted > 0 )); then
		echo "Lustre is mounted:"
		mount | grep /mnt/lustre
		echo "Unmounting Lustre filesystem:"
		./llmountcleanup.sh
		umount /mnt/lustre
	fi
        mount | grep "/mnt/lustre" && ( echo "Filesystem is still mounted"; exit 1 )
        exit 0
else
	echo "Usage: $0 mount|umount"
	exit 1
fi
