#!/bin/bash

function project_quota_supported()
{
	if [ ! -f ./test-framework.sh ]; then
		echo "test-framework.sh not found"
		exit 1
	fi
	grep "ENABLE_PROJECT_QUOTAS" ./test-framework.sh
}

export OSTSIZE=400000
export OSTCOUNT=4
export ENABLE_QUOTA=true
export ENABLE_PROJECT_QUOTAS=true
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

	export QUOTA_USERS=$(head -n 3 /etc/passwd | cut -d ':' -f 1 | \
		grep -v root | xargs)

	echo "Mounting lustre..."
	./llmount.sh
	if ! project_quota_supported; then
		lctl conf_param lustre.quota.ost=ugp ||
			echo "Could not enable project quota"
		lctl conf_param lustre.quota.mdt=ugp ||
			echo "Could not enable project quota"

		echo "Dismounting to apply project quota"
		./llmountcleanup.sh
		for t in /tmp/lustre-mdt1 /tmp/lustre-ost*; do
			tune2fs -O project,quota $t
			tune2fs -Q prjquota,usrquota,grpquota "$t"

			dumpe2fs $t | grep quota
		done

		echo "Re-mounting lustre..."
		NOFORMAT=1 ./llmount.sh
		lctl conf_param lustre.quota.ost=ugp ||
			echo "Could not enable project quota"
		lctl conf_param lustre.quota.mdt=ugp ||
			echo "Could not enable project quota"
	fi

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
