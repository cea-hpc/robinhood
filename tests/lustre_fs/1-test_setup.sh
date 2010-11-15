#!/bin/sh

CFG_SCRIPT="../../scripts/rbh-config"

service mysqld start

$CFG_SCRIPT create_db robinhood_lustre localhost robinhood
$CFG_SCRIPT empty_db robinhood_lustre
$CFG_SCRIPT enable_chglogs lustre

if [[ -z "$PURPOSE" || $PURPOSE = "LUSTRE_HSM" ]]; then

	echo "Checking if copytool is already running..."
	if (( `pgrep -f lhsmd_posix | wc -l` > 0 )); then
		echo "Already running"
	else
		lhsmd_posix --hsm_root=/tmp --noshadow lustre &
	fi

fi
