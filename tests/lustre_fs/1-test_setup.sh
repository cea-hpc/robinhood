#!/bin/bash

CFG_SCRIPT="../../scripts/rbh-config"

service mysqld start

$CFG_SCRIPT test_db  robinhood_lustre robinhood || $CFG_SCRIPT create_db robinhood_lustre localhost robinhood
$CFG_SCRIPT empty_db robinhood_lustre

if [[ -z "$NOLOG" || $NOLOG = "0" ]]; then
	$CFG_SCRIPT enable_chglogs lustre
fi

if [[ -z "$PURPOSE" || $PURPOSE = "LUSTRE_HSM" ]]; then
	
	echo -n "checking coordinator status: "
	status=`cat /proc/fs/lustre/mdt/lustre-MDT0000/hsm_control`
	echo $status

	if [[ $status != "enabled" ]]; then
		echo "enabled" >  /proc/fs/lustre/mdt/lustre-MDT0000/hsm_control
		sleep 2
	fi

	echo 10 > /proc/fs/lustre/mdt/lustre-MDT0000/hsm/grace_delay

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
