#!/bin/bash

CFG_SCRIPT="../../scripts/rbh-config"

service mysqld start

$CFG_SCRIPT test_db  robinhood_lustre robinhood || $CFG_SCRIPT create_db robinhood_lustre localhost robinhood
$CFG_SCRIPT empty_db robinhood_lustre

if [[ -z "$NOLOG" || $NOLOG = "0" ]]; then
	$CFG_SCRIPT enable_chglogs lustre
fi

if [[ $PURPOSE = "LUSTRE_HSM" ]]; then
	
	for mdt in /proc/fs/lustre/mdt/lustre-MDT* ; do
		echo -n "checking coordinator status on $(basename $mdt): "
		status=`cat $mdt/hsm_control`
		echo $status

		if [[ $status != "enabled" ]]; then
			lctl set_param mdt.$(basename $mdt).hsm_control=enabled
			sleep 2
		fi

		lctl set_param mdt.$(basename $mdt).hsm/grace_delay=10
	done

	echo "Checking if copytool is already running..."
	if (( `pgrep -f lhsmd_posix | wc -l` > 0 )); then
		echo "Already running"
	else
		lhsmd_posix --hsm_root=/tmp --noshadow lustre &
	fi

fi

# workaround for statahead issues
f=`ls /proc/fs/lustre/llite/lustre-*/statahead_max`
echo 0 > $f

# create testuser
getent passwd testuser || useradd testuser || exit 1

# create testgroup
getent group testgroup || groupadd testgroup || exit 1
