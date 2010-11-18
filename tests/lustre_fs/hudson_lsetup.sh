#!/bin/sh

HUDSON_DIR=$HUDSON_HOME
NODE_LABEL=lustre

LUSTRE_ROOT_DIR=$HUDSON_DIR/Lustre/label/$NODE_LABEL

# moving to lustre test dir
if [[ -d $LUSTRE_ROOT_DIR ]]; then
	cd $LUSTRE_ROOT_DIR/lustre/tests
else
	echo "$LUSTRE_ROOT_DIR: no such directory"
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
./llmount.sh || exit 1

mount | grep lustre
