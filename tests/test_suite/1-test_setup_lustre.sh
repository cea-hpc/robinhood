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

BKROOT="/tmp/backend"

if [ -z "$LFS" ]; then
	LFS=lfs
	LCTL=lctl
	COPYTOOL=lhsmtool_posix
else
	lutils_dir=$(dirname $LFS)
	LCTL=$lutils_dir/lctl
	COPYTOOL=$lutils_dir/lhsmtool_posix
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

$CFG_SCRIPT test_db  robinhood_lustre robinhood || $CFG_SCRIPT create_db robinhood_lustre localhost robinhood
$CFG_SCRIPT empty_db robinhood_lustre
$CFG_SCRIPT repair_db robinhood_lustre

if [[ -z "$NOLOG" || $NOLOG = "0" ]]; then
	$CFG_SCRIPT enable_chglogs lustre
fi

if [[ $PURPOSE = "LUSTRE_HSM" ]]; then

	for mdt in $(lctl list_param mdt.*) ; do
		echo -n "checking coordinator status on $(basename $mdt): "
		status=`lctl get_param -n $mdt.hsm_control`
		echo $status

		if [[ $status != "enabled" ]]; then
			$LCTL set_param $mdt.hsm_control=enabled
			sleep 2
		fi

		$LCTL set_param $mdt.hsm.grace_delay=10
		$LCTL set_param $mdt.hsm.loop_period=1
		$LCTL set_param $mdt.hsm.max_requests=8
	done

    # start copytool on a distinct mount point
    [ -d "/mnt/lustre2" ] || mkdir "/mnt/lustre2"
    # test if lustre2 is mounted
    mount | grep "/mnt/lustre2 "
    if (( $? != 0 )); then
        mnt_str=$(mount | grep "/mnt/lustre " | awk '{print $1}' | head -n 1)
        if [[ -z "$mnt_str" ]]; then
            echo "/mnt/lustre is not mounted"
            exit 1
        fi
        mnt_opt=$(mount | grep "/mnt/lustre " | sed -e 's/.*(\([^)]*\))/\1/' | head -n 1 | sed -e "s/rw,//" | sed -e "s/seclabel,//")
        mount -t lustre -o $mnt_opt $mnt_str /mnt/lustre2 || exit 1
    fi

	echo "Checking if copytool is already running..."
	if (( `pgrep lhsmtool_posix | wc -l` > 0 )); then
		echo "Already running"
	else
		mkdir -p $BKROOT
		$COPYTOOL --hsm_root=$BKROOT --no-shadow --daemon /mnt/lustre2 2>/dev/null &
	fi
fi

# workaround for statahead issues
lctl set_param llite.lustre-*.statahead_max=0

# lazy statfs make 'df' tests fail
lctl set_param llite.lustre-*.lazystatfs=0
lctl set_param llite.lustre-*.statfs_max_age=0

# create testuser
getent passwd testuser || useradd testuser || exit 1

# create testgroup
getent group testgroup || groupadd testgroup || exit 1
