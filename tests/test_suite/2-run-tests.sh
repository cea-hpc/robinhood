#!/bin/bash
# -*- mode: shell; sh-basic-offset: 4; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=4:tabstop=4:

[ -z "$LFS" ] && LFS=lfs
[ -z "$TESTOPEN" ] && TESTOPEN=/usr/lib64/lustre/tests/openfile

if [ -z "$POSIX_MODE" ]; then
    export RH_ROOT="/mnt/lustre"
    export FS_TYPE=lustre
    export RH_DB=robinhood_lustre
    echo "Lustre test mode"
else
    export RH_ROOT="/tmp/mnt.rbh"
    export FS_TYPE=ext4
    export RH_DB=robinhood_test
    echo "POSIX test mode"
    # force no log
    NOLOG=1
fi

BKROOT="/tmp/backend"
RBH_OPT=""

# Test whether the testsuite is running from the source tree or has
# been installed.
if [ -d "../../src/robinhood" ]; then
    CFG_SCRIPT="../../scripts/rbh-config"
    RBH_BINDIR="../../src/robinhood"
    RBH_SBINDIR="../../src/robinhood"
    RBH_MODDIR=$(readlink -m "../../src/modules/.libs")
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$RBH_MODDIR
    RBH_TEMPLATE_DIR="../../doc/templates"
    RBH_TESTS_DIR=".."
    RBH_CFG_DIR="./cfg"
else
    CFG_SCRIPT="rbh-config"
    RBH_BINDIR=${RBH_BINDIR:-"/usr/bin"}
    RBH_SBINDIR=${RBH_SBINDIR:-"/usr/sbin"}
    RBH_INSTALL_DIR=${RBH_INSTALL_DIR:-"/usr/share/robinhood"}
    RBH_TESTS_DIR="${RBH_INSTALL_DIR}/tests"
    RBH_CFG_DIR="$RBH_TESTS_DIR/test_suite/cfg"
    RBH_TEMPLATE_DIR="$RBH_INSTALL_DIR/doc/templates"
fi

export RBH_TEST_POLICIES="$(pwd)/test_policies.inc"
export RBH_TEST_LAST_ACCESS_ONLY_ATIME=${RH_TEST_LAST_ACCESS_ONLY_ATIME:-no}
export RBH_NUM_UIDGID=${RBH_NUM_UIDGID:-no}

# Retrieve the testuser UID and testgroup GID, as we may need them
# later
if [[ $RBH_NUM_UIDGID = "yes" ]]; then
    testuser_str=$(getent passwd testuser | cut -d: -f3)
    testgroup_str=$(getent group testgroup | cut -d: -f3)
    root_str=0
else
    testuser_str=testuser
    testgroup_str=testgroup
    root_str=root
fi

XML="test_report.xml"
TMPXML_PREFIX="/tmp/report.xml.$$"
TMPERR_FILE="/tmp/err_str.$$"

if [[ ! -d $RH_ROOT ]]; then
	echo "Creating directory $RH_ROOT"
	mkdir -p "$RH_ROOT"
else
	echo "Creating directory $RH_ROOT"
fi

SYNC_OPT="--run=migration(all) --force-all"
PURGE_OPT="--run=purge(all,target-usage=0)"

if [ -z ${WITH_VALGRIND+x} ]; then
    VALGRIND=
else
    # Run all executables under valgrind. Each instance will create a
    # valgrind log file, vg-test_<test number>-<pid>.log
    rm -f vg-test_*.log
    export G_SLICE=always-malloc,debug-blocks
    export G_DEBUG=fatal-warnings,fatal-criticals,gc-friendly
    VALGRIND="valgrind --gen-suppressions=all --suppressions=valgrind.supp --leak-check=full --log-file=vg-test_%q{index}-%p.log"
fi

RH="$VALGRIND $RBH_SBINDIR/robinhood $RBH_OPT"
REPORT="$VALGRIND $RBH_SBINDIR/rbh-report $RBH_OPT"
FIND="$VALGRIND $RBH_BINDIR/rbh-find"
DU="$VALGRIND $RBH_BINDIR/rbh-du"
DIFF="$VALGRIND $RBH_SBINDIR/rbh-diff"
RECOV="$VALGRIND $RBH_SBINDIR/rbh-recov $RBH_OPT"
UNDELETE="$VALGRIND $RBH_SBINDIR/rbh-undelete"
IMPORT="$VALGRIND $RBH_SBINDIR/rbh-import $RBH_OPT"
CMD=robinhood
ARCH_STR="migration success for"
REL_STR="purge success for"
HSMRM_STR="hsm_remove success for"

#default: TMP_FS_MGR
if [[ -z "$PURPOSE" || $PURPOSE = "TMP"* ]]; then
    is_lhsm=0
    is_hsmlite=0
    shook=0
    PURPOSE="TMP_FS_MGR"
    STATUS_MGR=none
    # get include for this flavor
    cp -f ${RBH_TEMPLATE_DIR}/includes/tmpfs.inc $RBH_TEST_POLICIES || exit 1
    # change policy names to the test framework names
    sed -e "s/cleanup/purge/" -i $RBH_TEST_POLICIES
elif [[ $PURPOSE = "LUSTRE_HSM" ]]; then
    is_lhsm=1
    is_hsmlite=0
    shook=0
    STATUS_MGR=lhsm
    # get include for this flavor
    cp -f ${RBH_TEMPLATE_DIR}/includes/lhsm.inc $RBH_TEST_POLICIES || exit 1
    # change policy names to the test framework names
    sed -e "s/lhsm_archive/migration/" -i $RBH_TEST_POLICIES
    sed -e "s/lhsm_release/purge/" -i $RBH_TEST_POLICIES
    sed -e "s/lhsm_remove/hsm_remove/" -i $RBH_TEST_POLICIES

elif [[ $PURPOSE = "BACKUP" ]]; then
    is_lhsm=0
    shook=0
    is_hsmlite=1
    STATUS_MGR=backup
    # get include for this flavor
    cp -f ${RBH_TEMPLATE_DIR}/includes/backup.inc $RBH_TEST_POLICIES || exit 1
    # change policy names to the test framework names
    sed -e "s/backup_archive/migration/" -i $RBH_TEST_POLICIES
    sed -e "s/backup_remove/hsm_remove/" -i $RBH_TEST_POLICIES
    # append a basic purge policy to run some purge tests
    cat >> $RBH_TEST_POLICIES << EOF
define_policy purge {
    scope { type != directory }
    status_manager = none;
    default_action = common.unlink;
    default_lru_sort_attr = none;
}
EOF
    mkdir -p $BKROOT
elif [[ $PURPOSE = "SHOOK" ]]; then
    is_lhsm=0
    is_hsmlite=1
    shook=1
    STATUS_MGR=shook
    # get include for this flavor
    INCLUDE="${RBH_TEMPLATE_DIR}/includes/shook.inc"
    # change policy names to the test framework names
    sed -e "s/shook_archive/migration/" -i $RBH_TEST_POLICIES
    sed -e "s/shook_release/purge/" -i $RBH_TEST_POLICIES
    sed -e "s/shook_remove/hsm_remove/" -i $RBH_TEST_POLICIES
    mkdir -p $BKROOT
fi

# Compatibility macros. Some lfs setstripe options changed in Lustre
# 2.3 (7a454853).
#
# --size|-s became --stripe-size|-S                (use $LFS_SS_SZ_OPT)
# --index|-i|--offset|-o became --stripe-index|-i  (no macro, use -i)
# --count|-c became --stripe-count|-c              (no macro, use -c)
$LFS setstripe 2>&1 | grep stripe-index > /dev/null
if [ $? -eq 0 ]; then
    LFS_SS_SZ_OPT="--stripe-size"
else
    LFS_SS_SZ_OPT="--size"
fi

function flush_data
{
    if [[ -n "$SYNC" ]]; then
      # if the agent is on the same node as the writer, we are not sure
      # data has been flushed to OSTs
      echo "Flushing data to OSTs"
      sync
    fi
}

function clean_caches
{
    echo 3 > /proc/sys/vm/drop_caches
    lctl set_param ldlm.namespaces.lustre-*.lru_size=clear > /dev/null
}

function wait_stable_df
{
    sync
    clean_caches

    $LFS df $RH_ROOT > /tmp/lfsdf.1
    while (( 1 )); do
        # df is updated about every 2sec
        sleep 2
        $LFS df $RH_ROOT > /tmp/lfsdf.2
        diff /tmp/lfsdf.1 /tmp/lfsdf.2 > /dev/null && break
        echo "waiting for df update..."
        mv -f /tmp/lfsdf.2 /tmp/lfsdf.1
    done
}

# Prints all, or part of Lustre's version
#
# lustre_version {all,major}
function lustre_version
{
    local version_file="/sys/fs/lustre/version"
    # Support for older versions of Lustre
    [ -f "$version_file" ] || version_file="${version_file/\/sys//proc}"

    local version="$(grep -o "[1-9].*" "$version_file")"
    if [ -z "$version" ]; then
        printf "Unable to determine Lustre's version\n" >&2
        return 1
    fi

    case "${1:-all}" in
    all)
        printf "$version"
        ;;
    major)
        printf "${version%%.*}"
        ;;
    *)
        printf "Invalid argument: '$1', {all,major} expected\n" >&2
        return 64
        ;;
    esac
}

LVERSION="$(lustre_version)"
if [ -z "$POSIX_MODE" ]; then
    lustre_major=$(lustre_version major) || exit 1
else
    # avoid failing comparisons for POSIX mode
    lustre_major=0
fi

if [[ -z "$NOLOG" || $NOLOG = "0" ]]; then
	no_log=0
else
	no_log=1
fi

PROC=$CMD

LOGS=(rh_chglogs.log rh_migr.log rh_rm.log rh.pid rh_purge.log rh_report.log
      rh_syntax.log recov.log rh_scan.log /tmp/rh_alert.log rh_rmdir.log
      rh.log)

SUMMARY="/tmp/test_${PROC}_summary.$$"

NB_ERROR=0
RC=0
SKIP=0
SUCCESS=0
DO_SKIP=0

function error_reset
{
	NB_ERROR=0
	DO_SKIP=0
	cp /dev/null $TMPERR_FILE
}

function error
{
    grep_ctx_opt="-B 5 -A 1"

	echo "ERROR $@"
    # prefilter false errors
    grep -v "[ (]0 errors" *.log | grep -v "LastScanErrors" | grep -i $grep_ctx_opt error
	NB_ERROR=$(($NB_ERROR+1))

	if (($junit)); then
        # prefilter false errors
        grep -v "[ (]0 errors" *.log | grep -v "LastScanErrors" | grep -i $grep_ctx_opt error  >> $TMPERR_FILE
		echo "ERROR $@" >> $TMPERR_FILE
	fi

    # exit on error
    [ "$EXIT_ON_ERROR" = "1" ] && exit 1

    # avoid displaying the same log many times
    [ "$DEBUG" = "1" ] || clean_logs
}

function set_skipped
{
	DO_SKIP=1
}

function clean_logs
{
    local f
	for f in "${LOGS[@]}"; do
		if [ -s "$f" ]; then
			cp /dev/null "$f"
		fi
	done
}


function wait_done
{
	max_sec=$1
	sec=0
	if [[ -n "$MDS" ]]; then
		cmd="ssh $MDS egrep -v \"SUCCEED|CANCELED\" /proc/fs/lustre/mdt/lustre-MDT0000/hsm/actions"
	else
		cmd="egrep -v SUCCEED|CANCELED /proc/fs/lustre/mdt/lustre-MDT0000/hsm/actions"
	fi

	action_count=`$cmd | wc -l`

	if (( $action_count > 0 )); then
		echo "Current actions:"
		$cmd

		echo -n "Waiting for copy requests to end."
		while (( $action_count > 0 )) ; do
			echo -n "."
			sleep 1;
			((sec=$sec+1))
			(( $sec > $max_sec )) && return 1
			action_count=`$cmd | wc -l`
		done
		$cmd
		echo " Done ($sec sec)"
	fi

	return 0
}

# Wait for a file to reach a given HSM state
# arg 1 = full file name
# arg 2 = HSM state (such as 0x00000001)
function wait_hsm_state {
    # Poll state for 10 seconds
    LIM=$((`date +%s`+10))
    while :
    do
        [ `date +%s` -ge $LIM ] && error "HSM state for file \"$1\" isn't $2"
        $LFS hsm_state $1 | grep --quiet $2 && break
        sleep .5
    done
}

function clean_fs
{
	if (( $is_lhsm != 0 )); then
		echo "Cancelling agent actions..."
		if [[ -n "$MDS" ]]; then
			ssh $MDS "echo purge > /proc/fs/lustre/mdt/lustre-MDT0000/hsm_control"
		else
			echo "purge" > /proc/fs/lustre/mdt/lustre-MDT0000/hsm_control
		fi

		echo "Waiting for end of data migration..."
		wait_done 60
	fi

    [ "$DEBUG" = "1" ] && echo "Cleaning filesystem..."
	if [[ -n "$RH_ROOT" ]]; then
		 find "$RH_ROOT" -mindepth 1 -delete 2>/dev/null
	fi

	if (( $is_hsmlite + $is_lhsm != 0 )); then
		if [[ -n "$BKROOT" ]]; then
			[ "$DEBUG" = "1" ] && echo "Cleaning backend content..."
			find "$BKROOT" -mindepth 1 -delete 2>/dev/null
		fi
	fi

	[ "$DEBUG" = "1" ] && echo "Destroying any running instance of robinhood..."
	pkill robinhood

	if [ -f rh.pid ]; then
		echo "killing remaining robinhood process..."
		kill `cat rh.pid`
		rm -f rh.pid
	fi

	pgrep -f robinhood && sleep 1 && pkill -9 -f robinhood
	[ "$DEBUG" = "1" ] && echo "Cleaning robinhood's DB..."
	$CFG_SCRIPT empty_db $RH_DB > /dev/null

	[ "$DEBUG" = "1" ] && echo "Cleaning changelogs..."
	if (( $no_log==0 )); then
	   $LFS changelog_clear lustre-MDT0000 cl1 0
	fi
}

function ensure_init_backend()
{
	mnted=`mount | grep $BKROOT | grep loop | wc -l`
    if (( $mnted == 0 )); then
        LOOP_FILE=/tmp/rbh_backend.loop.cont
        if [[ ! -s $LOOP_FILE ]]; then
            echo "creating file container $LOOP_FILE..."
            dd if=/dev/zero of=$LOOP_FILE bs=1M count=400 || return 1
            echo "formatting as ext4..."
            mkfs.ext4 -q -F $LOOP_FILE || return 1
        fi

        echo "Mounting $LOOP_FILE as $BKROOT"
        mount -o loop -t ext4 $LOOP_FILE $BKROOT || return 1
    	echo "Cleaning backend content..."
		find "$BKROOT" -mindepth 1 -delete 2>/dev/null
    fi
    return 0
}


function kill_from_pidfile
{
    if [ -f rh.pid ]; then
        kill $(cat rh.pid)
        sleep 1
        # wait a second until it stops
        if [ -f rh.pid ]; then
            kill -9 $(cat rh.pid)
            rm -f rh.pid
        fi
    fi
    # security: drop old process
	pkill -9 $PROC
}

POOL1=ost0
POOL2=ost1
POOL_CREATED=0

function create_pools
{
  if [[ -n "$MDS" ]]; then
	do_mds="ssh $MDS"
  else
	do_mds=""
  fi

  (($POOL_CREATED != 0 )) && return
  $do_mds $LFS pool_list lustre | grep lustre.$POOL1 && POOL_CREATED=1
  $do_mds $LFS pool_list lustre | grep lustre.$POOL2 && ((POOL_CREATED=$POOL_CREATED+1))
  (($POOL_CREATED == 2 )) && return

  $do_mds lctl pool_new lustre.$POOL1 || error "creating pool $POOL1"
  $do_mds lctl pool_add lustre.$POOL1 lustre-OST0000 || error "adding OST0000 to pool $POOL1"
  $do_mds lctl pool_new lustre.$POOL2 || error "creating pool $POOL2"
  $do_mds lctl pool_add lustre.$POOL2 lustre-OST0001 || error "adding OST0001 to pool $POOL2"

  $do_mds $LFS pool_list lustre.$POOL1
  $do_mds $LFS pool_list lustre.$POOL2

  POOL_CREATED=1
}

function check_db_error
{
        grep DB_REQUEST_FAILED $1 && error "DB request error"
}

function get_id
{
    local p="$1"

    if (( $lustre_major >= 2 )); then
         $LFS path2fid "$p" | tr -d '[]'
    else
         stat -c "/%i" "$p"
    fi
}

function create_nostripe
{
    local f=$1
    $TESTOPEN -f O_RDWR:O_CREAT:O_LOV_DELAY_CREATE -m 0644 "$f" || return 1
    $LFS getstripe "$f" | grep "no stripe info" || error "$f should not have stripe info"
}

function migration_test
{
	config_file=$1
	expected_migr=$2
	sleep_time=$3
	policy_str="$4"

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs
	if (( $no_log == 0 )); then
		echo "Initial scan of empty filesystem"
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	fi

	# create and fill 10 files

	echo "1-Modifing files..."
	for i in a `seq 1 10`; do
		dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"
	done

	echo "2-Reading changelogs..."
	# read changelogs
	if (( $no_log )); then
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
    	check_db_error rh_chglogs.log

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should not be migrated this time
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all -l DEBUG -L rh_migr.log || error ""

	nb_migr=`grep "$ARCH_STR" rh_migr.log | wc -l`
	if (($nb_migr != 0)); then
		error "********** TEST FAILED: No migration expected, $nb_migr started"
	else
		echo "OK: no files migrated"
	fi

	if (( $is_lhsm != 0 )); then
	    $REPORT -f $RBH_CFG_DIR/$config_file --status-info $STATUS_MGR --csv -q --count-min=1 > report.out
        [ "$DEBUG" = "1" ] && cat report.out
        check_status_count report.out archiving 0
        check_status_count report.out archived 0
    fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "3-Applying migration policy again ($policy_str)..."
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all -l DEBUG -L rh_migr.log

	nb_migr=`grep "$ARCH_STR" rh_migr.log | wc -l`
	if (($nb_migr != $expected_migr)); then
		error "********** TEST FAILED: $expected_migr migrations expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"
	fi

	if (( $is_lhsm != 0 )); then
	    $REPORT -f $RBH_CFG_DIR/$config_file --status-info $STATUS_MGR --csv -q --count-min=1 > report.out
        [ "$DEBUG" = "1" ] && cat report.out
        check_status_count report.out archiving $expected_migr

		wait_done 60 || error "Migration timeout"
        # get completion log
        $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_chglogs.log || error "readlog"

        # should be archived now
	    $REPORT -f $RBH_CFG_DIR/$config_file --status-info $STATUS_MGR --csv -q --count-min=1 > report.out
        [ "$DEBUG" = "1" ] && cat report.out
        check_status_count report.out synchro $expected_migr
    fi

    rm -f report.out
}

# Create a file with a UUID, archive it and delete. Make sure that its
# UUID makes it to the ENTRIES table. Do the same test with a file
# without UUID to test some bad paths.
function archive_uuid1
{
    config_file=$1

    if (( $is_lhsm == 0 )); then
        echo "Lustre/HSM test only: skipped"
        set_skipped
        return 1
    fi

    clean_logs

    echo "Initial scan of empty filesystem"
    $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""

    # create 2 files
    echo "1-Creating files..."
    for i in a `seq 1 2`; do
        rm -f $RH_ROOT/file.$i
        dd if=/dev/zero of=$RH_ROOT/file.$i bs=1k count=1 >/dev/null 2>/dev/null || error "writing file.$i"
    done

    # Set a fake UUID only on the first file
    local fake_uuid="8bc54fd0-5a7e-49f2-ad32-adc4147d31a2"
    setfattr -n trusted.lhsm.uuid -v "$fake_uuid" $RH_ROOT/file.1
    getfattr -n trusted.lhsm.uuid $RH_ROOT/file.1 || error "UUID wasn't set"

    local fid1=$(get_id "$RH_ROOT/file.1")
    local fid2=$(get_id "$RH_ROOT/file.2")

    echo "2- scan filesystem"
    $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
    $REPORT -f $RBH_CFG_DIR/$config_file -e $fid1 | egrep "lhsm\.uuid\s+:\s+$fake_uuid" || error "UUID not found in ENTRIES for file1"
    $REPORT -f $RBH_CFG_DIR/$config_file -e $fid2 | grep "lhsm\.uuid" && error "UUID found in ENTRIES for file2"

    echo "3-Test rbh-find with UUID"
    $FIND -f $RBH_CFG_DIR/$config_file -printf "%p %Rm{lhsm.uuid}\\n" | grep "$fake_uuid" || error "UUID not found by rbh-find for file1"

    rm -f report.out
}

# Create a file with a UUID, archive it and delete. Make sure that its
# UUID makes it to the ENTRIES and SOFT_RM tables. Do the same test
# for a file without UUID to test some bad paths.
function archive_uuid2
{
    config_file=$1

    if (( $is_lhsm == 0 )); then
        echo "Lustre/HSM test only: skipped"
        set_skipped
        return 1
    fi

    clean_logs

    echo "Initial scan of empty filesystem"
    $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""

    # create 2 files
    echo "1-Creating files..."
    for i in a `seq 1 2`; do
        rm -f $RH_ROOT/file.$i
        dd if=/dev/zero of=$RH_ROOT/file.$i bs=1k count=1 >/dev/null 2>/dev/null || error "writing file.$i"
    done

    # Set a fake UUID only on the first file
    local fake_uuid="2363c3ed-5a7e-49f2-ad32-adc4147d31a2"
    setfattr -n trusted.lhsm.uuid -v "$fake_uuid" $RH_ROOT/file.1
    getfattr -n trusted.lhsm.uuid $RH_ROOT/file.1 || error "UUID wasn't set"

    local fid1=$(get_id "$RH_ROOT/file.1")
    local fid2=$(get_id "$RH_ROOT/file.2")

    echo "2-Reading changelogs..."
    $RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
    check_db_error rh_chglogs.log

    echo "3-Archiving the files"
    $LFS hsm_archive $RH_ROOT/file.1 || error "executing lfs hsm_archive"
    $LFS hsm_archive $RH_ROOT/file.2 || error "executing lfs hsm_archive"

    wait_hsm_state $RH_ROOT/file.1 0x00000009
    wait_hsm_state $RH_ROOT/file.2 0x00000009

    echo "4-Reading changelogs..."
    $RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""

    $REPORT -f $RBH_CFG_DIR/$config_file -e $fid1 | egrep "lhsm\.uuid\s+:\s+$fake_uuid" || error "UUID not found in ENTRIES for file1"
    $REPORT -f $RBH_CFG_DIR/$config_file -e $fid2 | grep "lhsm\.uuid" && error "UUID found in ENTRIES for file2"

    echo "5-Test soft rm"
    rm -f $RH_ROOT/file.1
    rm -f $RH_ROOT/file.2

    $RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""

    mysql $RH_DB -e "SELECT lhsm_uuid FROM SOFT_RM WHERE id='$fid1'" | grep "$fake_uuid" || error "UUID not found in SOFT_RM for file1"
    mysql $RH_DB -e "SELECT lhsm_uuid FROM SOFT_RM WHERE id='$fid2'" | grep "NULL" || error "UUID found in SOFT_RM for file2"

    echo "6-Test undelete"
    $UNDELETE -f $RBH_CFG_DIR/$config_file -L | grep 'file'
    $UNDELETE -f $RBH_CFG_DIR/$config_file -R $RH_ROOT/file.1

    $RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""

    getfattr -n trusted.lhsm.uuid $RH_ROOT/file.1 || error "UUID wasn't set"
    getfattr -n trusted.lhsm.uuid $RH_ROOT/file.1 | grep "$fake_uuid" || error "Bad UUID undeleted"

    local fid1=$(get_id "$RH_ROOT/file.1")
    $REPORT -f $RBH_CFG_DIR/$config_file -e $fid1 | egrep "lhsm\.uuid\s+:\s+$fake_uuid" || error "UUID not found in ENTRIES for file1"
    mysql $RH_DB -e "SELECT lhsm_uuid FROM SOFT_RM WHERE id='$fid2'" | grep "NULL" || error "UUID found in SOFT_RM for file2"

    echo "7-Test rbh-find with UUID"
    $FIND -f $RBH_CFG_DIR/$config_file -printf "%p %Rm{lhsm.uuid}\\n" | grep "$fake_uuid" || error "UUID not found by rbh-find for file1"

    rm -f report.out
}

# migrate a single file
function migration_test_single
{
	config_file=$1
	expected_migr=$2
	sleep_time=$3
	policy_str="$4"

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# create and fill 10 files

	echo "1-Modifing files..."
	for i in a `seq 1 10`; do
		dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"
	done

	count=0
	echo "2-Trying to migrate files before we know them..."
	for i in a `seq 1 10`; do
		$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:$RH_ROOT/file.$i -L rh_migr.log -l DEBUG 2>/dev/null
		grep "$RH_ROOT/file.$i" rh_migr.log | grep "not known in database" && count=$(($count+1))
	done

	if (( $count == $expected_migr )); then
		echo "OK: all $expected_migr files are not known in database"
	else
		error "$count files are not known in database, $expected_migr expected"
	fi

	cp /dev/null rh_migr.log
	sleep 1

	echo "3-Reading changelogs..."
	# read changelogs
	if (( $no_log )); then
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error ""
	else
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error ""
	fi
    	check_db_error rh_chglogs.log

	count=0
	cp /dev/null rh_migr.log
	echo "4-Applying migration policy ($policy_str)..."
	# files should not be migrated this time: do not match policy
	for i in a `seq 1 10`; do
		$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:$RH_ROOT/file.$i -l DEBUG -L rh_migr.log 2>/dev/null
		grep "$RH_ROOT/file.$i" rh_migr.log | grep "doesn't match condition for policy rule" && count=$(($count+1))
	done

	if (( $count == $expected_migr )); then
		echo "OK: all $expected_migr files are not eligible for migration"
	else
		error "$count files are not eligible, $expected_migr expected"
	fi

	nb_migr=`grep "$ARCH_STR" rh_migr.log | wc -l`
	if (($nb_migr != 0)); then
		error "********** TEST FAILED: No migration expected, $nb_migr started"
	else
		echo "OK: no files migrated"
	fi

	cp /dev/null rh_migr.log
	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	count=0
	echo "5-Applying migration policy again ($policy_str)..."
	for i in a `seq 1 10`; do
		$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:$RH_ROOT/file.$i -l DEBUG -L rh_migr.log 2>/dev/null
		grep "$RH_ROOT/file.$i" rh_migr.log | grep "$ARCH_STR" && count=$(($count+1))
	done

	if (( $count == $expected_migr )); then
		echo "OK: all $expected_migr files have been migrated successfully"
	else
		error "$count files migrated, $expected_migr expected"
	fi

	nb_migr=`grep "$ARCH_STR" rh_migr.log | wc -l`
	if (($nb_migr != $expected_migr)); then
		error "********** TEST FAILED: $expected_migr migrations expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"
	fi
}



# migrate a symlink
function migrate_symlink
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	if (( $is_hsmlite == 0 )); then
		echo "Backup test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# create a symlink

	echo "1-Create a symlink"
	ln -s "this is a symlink" "$RH_ROOT/link.1" || error "creating symlink"

	echo "2-Reading changelogs..."
	# read changelogs
	if (( $no_log )); then
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
	else
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
	fi
    	check_db_error rh_chglogs.log

	count=0
	echo "3-Applying migration policy ($policy_str)..."
	# files should not be migrated this time: do not match policy
	$RH -f $RBH_CFG_DIR/$config_file  --run=migration --target=file:$RH_ROOT/link.1 -l DEBUG -L rh_migr.log 2>/dev/null
	nb_migr=`grep "$ARCH_STR" rh_migr.log | wc -l`
	if (($nb_migr != 0)); then
		error "********** TEST FAILED: No migration expected, $nb_migr started"
	else
		echo "OK: no entries migrated"
	fi

	cp /dev/null rh_migr.log
	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	count=0
	echo "5-Applying migration policy again ($policy_str)..."
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:$RH_ROOT/link.1 -l DEBUG -L rh_migr.log 2>/dev/null

	nb_migr=`grep "$ARCH_STR" rh_migr.log | wc -l`
	if (($nb_migr != 1)); then
		error "********** TEST FAILED: 1 migration expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"
	fi

	echo "6-Scanning..."
	$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
    	check_db_error rh_chglogs.log

	$REPORT -f $RBH_CFG_DIR/$config_file --dump-status=$STATUS_MGR:synchro --csv -q > report.out
        [ "$DEBUG" = "1" ] && cat report.out
    	count=$(wc -l report.out | awk '{print $1}')
	if  (($count == 1)); then
		echo "OK: 1 synchro symlink"
	else
		error "1 symlink is expected to be synchro (found $count)"
	fi
    	rm -f report.out

	cp /dev/null rh_migr.log
	echo "7-Applying migration policy again ($policy_str)..."
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:$RH_ROOT/link.1 -l DEBUG -L rh_migr.log

	if grep " 1 entries skipped" rh_migr.log; then
		echo "OK: entry skipped"
	else
		error "1 entry should be skipped"
	fi
}

# helper for test_rmdir
# run rmdir_empty and rmdir_recurse policies
# and check the correct amount of directories is matched
function run_rmdirs
{
    config_file=$1
    policy_str="$2"
    expect_empty=$3
    expect_recurs=$4

    echo "Applying rmdir_empty policy ($policy_str)..."
    $RH -f $RBH_CFG_DIR/$config_file --run=rmdir_empty --target=all --once -l FULL -L rh_purge.log
    [ "$DEBUG" = "1" ] && grep "SELECT ENTRIES" rh_purge.log
    grep "Policy run summary" rh_purge.log | grep rmdir_empty
    cnt_empty=$(grep "Policy run summary" rh_purge.log | grep rmdir_empty | cut -d ';' -f 3 | awk '{print $1}')
    :> rh_purge.log

    echo "Applying rmdir_recurse policy ($policy_str)..."
    $RH -f $RBH_CFG_DIR/$config_file --run=rmdir_recurse --target=all --once -l FULL -L rh_purge.log
    [ "$DEBUG" = "1" ] && grep "SELECT ENTRIES" rh_purge.log
    grep "Policy run summary" rh_purge.log | grep rmdir_recurse
    cnt_recurs=$(grep "Policy run summary" rh_purge.log | grep rmdir_recurse | cut -d ';' -f 3 | awk '{print $1}')
    :> rh_purge.log

    if (( $cnt_empty == $expect_empty )); then
        echo "OK: $cnt_empty empty directories removed"
    else
        error "$cnt_empty empty directories removed ($expect_empty expected)"
    fi
    if (( $cnt_recurs == $expect_recurs )); then
        echo "OK: $cnt_recurs directories removed recursively"
    else
        error "$cnt_recurs directories removed recursively ($expect_recurs expected)"
    fi
}

# test rmdir policies
function test_rmdir
{
    config_file=$1
    sleep_time=$2
    policy_str="$3"

    clean_logs

    EMPTY=empty
    NONEMPTY=smthg
    RECURSE=remove_me
    export MATCH_PATH="$RH_ROOT/$RECURSE.*"

    # initial scan
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log
       check_db_error rh_chglogs.log

    echo "Create test directories"

    # create 3 empty directories
    mkdir "$RH_ROOT/$EMPTY.1" "$RH_ROOT/$EMPTY.2" "$RH_ROOT/$EMPTY.3" || error "creating empty directories"
    # create non-empty directories
    mkdir "$RH_ROOT/$NONEMPTY.1" "$RH_ROOT/$NONEMPTY.2" "$RH_ROOT/$NONEMPTY.3" || error "creating directories"
    touch "$RH_ROOT/$NONEMPTY.1/f" "$RH_ROOT/$NONEMPTY.2/f" "$RH_ROOT/$NONEMPTY.3/f" || error "populating directories"
    # create "deep" directories for testing recurse rmdir
    mkdir "$RH_ROOT/$RECURSE.1"  "$RH_ROOT/$RECURSE.2" || error "creating directories"
    mkdir "$RH_ROOT/$RECURSE.1/subdir.1" "$RH_ROOT/$RECURSE.1/subdir.2" || error "creating directories"
    touch "$RH_ROOT/$RECURSE.1/subdir.1/file.1" "$RH_ROOT/$RECURSE.1/subdir.1/file.2" "$RH_ROOT/$RECURSE.1/subdir.2/file" || error "populating directories"

    echo "Reading changelogs..."
    # read changelogs
    if (( $no_log )); then
        $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
    else
        $RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
    fi
    check_db_error rh_chglogs.log

    run_rmdirs $config_file "$policy_str" 0 0
    echo "Sleeping $sleep_time seconds..."
    sleep $sleep_time

    run_rmdirs $config_file "$policy_str" 0 2
    echo "Sleeping $sleep_time seconds..."
    sleep $sleep_time

    run_rmdirs $config_file "$policy_str" 3 0
}

function test_lru_policy
{
	config_file=$1
	expected_migr_1=$2
	expected_migr_2=$3
    sleep_time=$4
	policy_str="$5"

    nb_expected_migr_1=$(echo $expected_migr_1 | wc -w)
    nb_expected_migr_2=$(echo $expected_migr_2 | wc -w)
    cr_sleep=5

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs
	# initial scan
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log
   	check_db_error rh_chglogs.log

	# create test tree with 10 files
    # time | files         |  0   1   2   3   4   5   6   7   8   9
    # -------------------------------------------------------------
    #  0   | creation      |  x   x   x   x
    #  5s  | creation      |                  x   x   x   x   x   x
    # 10s  | modification  |          x   x   x       x   x
    # 15s  | access        |      x           x   x       x
    # 20s  | 1st archive   |  $expected_migr_1
    # +$4  | 2nd archive   |  $expected_migr_2

	echo "1-Creating test files..."
    # creation times
	echo -n "  Creating files 0 1 2 3, "
	for i in {0..3}; do
		dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i"
	done
	echo "sleeping $cr_sleep seconds..."
    sleep $cr_sleep
	echo -n "  Creating files 4 5 6 7 8 9, "
	for i in {4..9}; do
		dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i"
	done
	echo "sleeping $cr_sleep seconds..."
    sleep $cr_sleep
    # modification times
	echo -n "  Modifying files 2 3 4 6 7, "
	for i in 2 3 4 6 7; do
	    echo "data" > $RH_ROOT/file.$i || error "modifying file.$i"
	done
	echo "sleeping $cr_sleep seconds..."
    sleep $cr_sleep
    # update last access times
	echo -n "  Reading files 1 4 5 7, "
    for i in 1 4 5 7; do
 	    cat $RH_ROOT/file.$i >/dev/null || error "reading file.$i"
    done
	echo "sleeping $cr_sleep seconds..."
    sleep $cr_sleep

	echo "2-Reading changelogs..."
	# read changelogs
    # TODO: creation time is different when scanning (ctime at discovery time) and when reading
    # changelogs (changelog timestamp)
	if (( $no_log )); then
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
    check_db_error rh_chglogs.log
    # md_update of entries must be > 0 for policy application
    sleep 1

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should not be migrated this time

	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all -l FULL -L rh_migr.log   || error ""
    [ "$DEBUG" = "1" ] && grep "SELECT ENTRIES" rh_migr.log

    # Retrieve the names of migrated files.
    migr=`egrep -o "$ARCH_STR '[^']+'" rh_migr.log | sed "s/.*'\(.*\)'/\1/" | \
        awk -F. '{print $NF}' | sort | tr '\n' ' ' | xargs` # xargs does the trimming
    nb_migr=$(echo $migr | wc -w)
	if [[ "$migr" != "$expected_migr_1" ]]; then
        error "********** TEST FAILED: $nb_expected_migr_1 migration expected ${expected_migr_1:+(files $expected_migr_1)}, $nb_migr started ${migr:+(files $migr)}"
	else
		echo "OK: $nb_expected_migr_1 files migrated"
	fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time
    # empty log file to prevent from counting previous action twice
    :> rh_migr.log

	echo "5-Applying migration policy again ($policy_str)..."
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all -l DEBUG -L rh_migr.log
    [ "$DEBUG" = "1" ] && grep "SELECT ENTRIES" rh_migr.log

    # Retrieve the names of migrated files.
    #   "2015/02/18 15:54:31 [17821/5] migration | migration success for '/mnt/lustre/file.1', matching rule 'default', creation_time 41s ago, size=1.00 MB"
    migr=`egrep -o "$ARCH_STR '[^']+'" rh_migr.log | sed "s/.*'\(.*\)'/\1/" | \
        awk -F. '{print $NF}' | sort | tr '\n' ' ' | xargs` # xargs does the trimming
    nb_migr=$(echo $migr | wc -w)
	if [[ "$migr" != "$expected_migr_2" ]]; then
        error "********** TEST FAILED: $nb_expected_migr_2 migration expected ${expected_migr_2:+(files $expected_migr_2)}, $nb_migr started ${migr:+(files $migr)}"
	else
        echo "OK: $nb_migr files migrated"
	fi
}

function lru_order_of
{
    l="$1"
    f="$2"
    grep "$REL_STR" $l | grep -n "'$f'" | cut -d ':' -f 1
}


function test_purge_lru
{
	config_file=$1
    export SORT_PARAM=$2
	policy_str="$3"

	clean_logs

    # initial scan
    $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""

    # create 6 files
  	for i in {1..6}; do
		dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i"
        sleep 1
	done

    # access 4 files
  	for i in {1..4}; do
		dd if=$RH_ROOT/file.$i of=/dev/null bs=1M count=1 >/dev/null 2>/dev/null || error "reading file.$i"
        sleep 1
	done

 	# read changelogs
	if (( $no_log )); then
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi

	# flush data for HSM flavors
    if (( ($is_hsmlite != 0) || ($is_lhsm != 0) )); then
		echo "Archiving files"
		$RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG  -L rh_migr.log || error "archiving files"

        if (( $is_lhsm != 0 )); then
    		echo "Waiting for end of data migration..."
    		wait_done 60

            # archive is asynchronous: read changelog to get the archive status
            $RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
        fi
	fi

    # md_update for purge must be > previous md updates
    sleep 1

    $RH -f $RBH_CFG_DIR/$config_file $PURGE_OPT --once -l DEBUG  -L rh_purge.log || error "purging files"

    # if sorted: order should be 5 6 1 2 3 4
    exp_rank=(3 4 5 6 1 2)
    # if not: can be any order

    if [[ $SORT_PARAM != "none" ]]; then
      	for i in {1..6}; do
            idx=$(($i-1))
            rank=$(lru_order_of rh_purge.log $RH_ROOT/file.$i)
            echo "file.$i purge rank #${exp_rank[$idx]}"
            [[ $rank == ${exp_rank[$idx]} ]] || error "file.$i should have been purged in #${exp_rank[$idx]} (got $rank)"
        done

        # DB request must have access time criteria
        grep "new request" rh_purge.log | grep access || error "access should be in request criteria"

    else
        # all entries must be found
        cnt=$(grep "$REL_STR" rh_purge.log | wc -l)
        [[ $cnt == 6 ]] || error "All entries should have been purged"

        # DB request must not have access time criteria
        grep "new request" rh_purge.log | grep access && error "access shouldn't be in request criteria"
    fi

}

function test_suspend_on_error
{
	config_file=$1
    sleep_time=$2
	policy_str="$3"

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

    # must reach 50% error with at least 5 errors
    nb_files_ok=5
    nb_files_error=10 # must stop migrating before
    # migrating by creation order, so create them in order
    # to increase the error rate sightly
    # and reach this condition before the whole migration is finished
	echo "1-Creating test files..."
	for i in $(seq 1 ${nb_files_ok}); do
		dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i"
	done
    sleep 1
	for i in $(seq 1 ${nb_files_error}); do
		dd if=/dev/zero of=$RH_ROOT/file.$i.fail bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i.fail"
	done

    # read fs content
	if (( $no_log )); then
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
    check_db_error rh_chglogs.log

	echo "2-Sleeping $sleep_time sec..."
    sleep $sleep_time

	echo "3-Applying migration policy ($policy_str)..."

	$RH -f $RBH_CFG_DIR/$config_file --run=migration --force --target=all -l DEBUG -L rh_migr.log   || error ""

    [ "$DEBUG" = "1" ] && grep action_params rh_migr.log
    nb_fail_match=$(count_action_params rh_migr.log arg=fail)
    nb_ok_match=$(count_action_params rh_migr.log arg=ok)

    echo "$nb_fail_match failed copies, $nb_ok_match successful copies"
    (($nb_ok_match == $nb_files_ok)) || error "expected $nb_files_ok successful copies (got $nb_ok_match)"
    # migration should have been stopped before migrating all
    (($nb_fail_match == $nb_files_error)) && error "migration should have stopped before migrating all"
    grep "suspending policy run" rh_migr.log || error "migration should have been suspended"
}




function xattr_test
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs
	if (( $no_log == 0 )); then
		echo "Initial scan of empty filesystem"
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	fi

	# create and fill 10 files
	echo "1-Modifing files..."
	for i in `seq 1 3`; do
		dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"
	done

	echo "2-Setting xattrs..."
	echo "$RH_ROOT/file.1: xattr.user.foo=1"
	setfattr -n user.foo -v 1 $RH_ROOT/file.1
	echo "$RH_ROOT/file.2: xattr.user.bar=1"
	setfattr -n user.bar -v 1 $RH_ROOT/file.2
	echo "$RH_ROOT/file.3: none"

	# read changelogs
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs..."
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
    	check_db_error rh_chglogs.log

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should not be migrated this time
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all -l DEBUG -L rh_migr.log   || error ""

	nb_migr=`grep "$ARCH_STR" rh_migr.log | wc -l`
	if (($nb_migr != 0)); then
		error "********** TEST FAILED: No migration expected, $nb_migr started"
	else
		echo "OK: no files migrated"
	fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "3-Applying migration policy again ($policy_str)..."
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all -l DEBUG -L rh_migr.log

	nb_migr=`grep "$ARCH_STR" rh_migr.log |  wc -l`
	if (($nb_migr != 3)); then
		error "********** TEST FAILED: $expected_migr migrations expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"

        # checking policy rule
        nb_migr_arch1=$(count_action_params rh_migr.log class=xattr_bar)
        nb_migr_arch2=$(count_action_params rh_migr.log class=xattr_foo)
        nb_migr_arch3=`grep "matches the condition for policy rule 'default'" rh_migr.log | wc -l`

        if (( $nb_migr_arch1 != 1 || $nb_migr_arch2 != 1 || $nb_migr_arch3 != 1 )); then
            error "********** wrong policy cases: 1x$nb_migr_arch1/2x$nb_migr_arch2/3x$nb_migr_arch3 (1x1/2x1/3x1 expected)"
            cp rh_migr.log /tmp/xattr_test.$$
            echo "Log saved as /tmp/xattr_test.$$"
        else
            echo "OK: 1 file for each policy case"
        fi

        # checking archive nums
        nb_migr_arch1=$(count_action_params rh_migr.log archive_id=1)
        nb_migr_arch2=$(count_action_params rh_migr.log archive_id=2)
        nb_migr_arch3=$(count_action_params rh_migr.log archive_id=3)

        if (( $nb_migr_arch1 != 1 || $nb_migr_arch2 != 1 || $nb_migr_arch3 != 1 )); then
            error "********** wrong archive_ids: 1x$nb_migr_arch1/2x$nb_migr_arch2/3x$nb_migr_arch3 (1x1/2x1/3x1 expected)"
        else
            echo "OK: 1 file to each archive_id"
        fi
	fi

}

function link_unlink_remove_test
{
	config_file=$1
	expected_rm=$2
	sleep_time=$3
	policy_str="$4"
    cl_delay=6 # time between action and its impact on rbh-report

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi
	if (( $no_log )); then
		echo "changelog disabled: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "1-Start reading changelogs in background..."
	# read changelogs
	$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --detach --pid-file=rh.pid || error ""

	sleep 1

	# write file.1 and force immediate migration
	echo "2-Writing data to file.1..."
	dd if=/dev/zero of=$RH_ROOT/file.1 bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.1"

	sleep $cl_delay

	if (( $is_lhsm != 0 )); then
		echo "3-Archiving file....1"
		flush_data
		$LFS hsm_archive $RH_ROOT/file.1 || error "executing lfs hsm_archive"

		echo "3bis-Waiting for end of data migration..."
		wait_done 60 || error "Migration timeout"
	elif (( $is_hsmlite != 0 )); then
		$RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG  -L rh_migr.log || error "executing $CMD --sync"
	fi

	# create links on file.1 files
	echo "4-Creating hard links to $RH_ROOT/file.1..."
	ln $RH_ROOT/file.1 $RH_ROOT/link.1 || error "ln"
	ln $RH_ROOT/file.1 $RH_ROOT/link.2 || error "ln"

	sleep 1

	# removing all files
        echo "5-Removing all links to file.1..."
	rm -f $RH_ROOT/link.* $RH_ROOT/file.1

	sleep $cl_delay

	echo "Checking report..."
	$REPORT -f $RBH_CFG_DIR/$config_file --deferred-rm --csv -q > rh_report.log
	nb_ent=`wc -l rh_report.log | awk '{print $1}'`
	if (( $nb_ent != $expected_rm )); then
		error "Wrong number of deferred rm reported: $nb_ent"
	fi
	grep "$RH_ROOT/file.1" rh_report.log > /dev/null || error "$RH_ROOT/file.1 not found in deferred rm list"

	# deferred remove delay is not reached: nothing should be removed
	echo "6-Performing HSM remove requests (before delay expiration)..."
	$RH -f $RBH_CFG_DIR/$config_file --run=hsm_remove --target=all --force -l DEBUG -L rh_rm.log  || error "hsm_remove"

	nb_rm=`grep "$HSMRM_STR" rh_rm.log | wc -l`
	if (($nb_rm != 0)); then
		echo "********** test failed: no removal expected, $nb_rm done"
	else
		echo "OK: no rm done"
	fi

	echo "7-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "8-Performing HSM remove requests (after delay expiration)..."
	$RH -f $RBH_CFG_DIR/$config_file --run=hsm_remove --target=all --force -l DEBUG -L rh_rm.log  || error "hsm_remove"

	nb_rm=`grep "$HSMRM_STR" rh_rm.log | wc -l`
	if (($nb_rm != $expected_rm)); then
		error "********** TEST FAILED: $expected_rm removals expected, $nb_rm done"
	else
		echo "OK: $nb_rm files removed from archive"
	fi

    grep "Performing new request with a limit" rh_rm.log && error "No request splitting is expected for SOFT_RM table"

	# kill event handler
	pkill -9 $PROC

}

function test_hsm_remove
{
    config_file=$1
    expected_rm=$2
    sleep_time=$3
    policy_str="$4"

    if (( $is_lhsm + $is_hsmlite == 0 )); then
        echo "HSM test only: skipped"
        set_skipped
        return 1
    fi
    if (( $no_log )); then
        echo "changelog disabled: skipped"
        set_skipped
        return 1
    fi

    clean_logs

    nb_files=$((2*$expected_rm))

    # write 2 x expected_rm
    echo "Writing $nb_files files..."
    for i in $(seq 1 $nb_files); do
        dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i"
    done

    # initial scan (files are known as 'new')
    $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log  --once || error ""
    check_db_error rh_scan.log

    # create 2 more files that robinhood won't know before their removal
    # (1 archived, 1 not archived)
    dd if=/dev/zero of=$RH_ROOT/file.a bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.a"
    dd if=/dev/zero of=$RH_ROOT/file.b bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.b"

    extra=0
    tolerance=0
    extra_list=()
    extra_excl=()
    # archive them all
    if (( $is_lhsm != 0 )); then
        extra=1 # +1 for file.a
        extra_list=(a) # should be in softrm
        extra_excl=(b) # shouldn't be in softrm
        echo "Archiving $expected_rm files..."
        flush_data
        for i in $(seq 1 $expected_rm) a; do
            $LFS hsm_archive $RH_ROOT/file.$i || error "executing lfs hsm_archive"
        done

        echo "Waiting for end of data migration..."
        wait_done 60 || error "Migration timeout"
    elif (( $is_hsmlite != 0 )); then
        # allow 2 extra entries in SOFTRM (robinhood may doubt about file.a and file.b)
        tolerance=2
        for i in $(seq 1 $expected_rm); do
            $RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:$RH_ROOT/file.$i --ignore-conditions -l DEBUG  -L rh_migr.log || error "migrating $RH_ROOT/file.$i"
        done
    fi

    # removing all files
    echo "Removing all files"
    rm -f $RH_ROOT/file.*

    # make sure rm operations are in the changelog
    sleep 1

    # robinhood reads the log but entries no longer exist: make sure it takes the right decision in this case
    $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_chglogs.log  --once || error "reading changelogs"
    check_db_error rh_chglogs.log

    echo "Checking report..."
    $REPORT -f $RBH_CFG_DIR/$config_file --deferred-rm --csv -q > rh_report.log
    [ "$DEBUG" = "1" ] && cat rh_report.log
    nb_ent=`wc -l rh_report.log | awk '{print $1}'`
    if (( $nb_ent > $expected_rm  + $extra + $tolerance )) || (( $nb_ent < $expected_rm  + $extra )); then
        error "Wrong number of deferred rm reported: $nb_ent / $expected_rm  + $extra (tolerance $tolerance)"
    fi
    for i in $(seq 1 $expected_rm) $extra_list; do
        grep "$RH_ROOT/file.$i" rh_report.log || error "$RH_ROOT/file.$i not found in deferred rm list"
    done

    for i in $(seq $(($expected_rm+1)) $nb_files) $extra_excl; do
        grep "$RH_ROOT/file.$i" rh_report.log && error "$RH_ROOT/file.$i shouldn't be in deferred rm list"
    done

    # deferred remove delay is not reached: nothing should be removed
    echo "Performing HSM remove requests (before delay expiration)..."
    $RH -f $RBH_CFG_DIR/$config_file --run=hsm_remove --target=all --force -l DEBUG -L rh_rm.log  || error "hsm_remove"

    nb_rm=`grep "$HSMRM_STR" rh_rm.log | wc -l`
    if (($nb_rm != 0)); then
        echo "********** test failed: no removal expected, $nb_rm done"
    else
        echo "OK: no rm done"
    fi

    echo "Sleeping $sleep_time seconds..."
    sleep $sleep_time

    echo "Performing HSM remove requests (after delay expiration)..."
    $RH -f $RBH_CFG_DIR/$config_file --run=hsm_remove --target=all --force -l DEBUG -L rh_rm.log  || error "hsm_remove"

    nb_rm=`grep "$HSMRM_STR" rh_rm.log | wc -l`
    if (( $nb_rm > $expected_rm  + $extra + $tolerance )) || (( $nb_rm < $expected_rm  + $extra )); then
        error "********** TEST FAILED: $expected_rm+$extra removals expected (tolerance $tolerance), $nb_rm done"
    else
        echo "OK: $nb_rm files removed from archive"
    fi

    grep "Performing new request with a limit" rh_rm.log && error "No request splitting is expected for SOFT_RM table"

}

# test that hsm_remove requests are sent to the right archive
function test_lhsm_remove
{
    config_file=$1
    nb_archive=$2
    sleep_time=$3
    policy_str="$4"

    if (( $is_lhsm == 0 )); then
        echo "Lustre/HSM test only: skipped"
        set_skipped
        return 1
    fi
    if (( $no_log )); then
        echo "changelog disabled: skipped"
        set_skipped
        return 1
    fi

    clean_logs

    local default_archive=$(cat /proc/fs/lustre/mdt/lustre-MDT0000/hsm/default_archive_id)

    # create nb_archive + 3 more files to test:
    # - hsm_archive with no option
    # - hsm_archive with -a 0
    # - file that will be deleted before robinhood gets its archive_id

    id=()
    name=()
    echo "Writing files..."
    for i in $(seq 1 $nb_archive) no_opt 0 x ; do
        dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i"
        name+=( "$i" )
        id+=( "$(get_id "$RH_ROOT/file.$i")" )
    done

    # initial scan (files are known as 'new')
    $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log  --once || error ""
    check_db_error rh_scan.log

    # archive then
    echo "Archiving files..."
    flush_data
    for i in $(seq 1 $nb_archive); do
        $LFS hsm_archive -a $i $RH_ROOT/file.$i || error "executing lfs hsm_archive"
    done
    $LFS hsm_archive $RH_ROOT/file.no_opt || error "executing lfs hsm_archive"
    $LFS hsm_archive -a 0 $RH_ROOT/file.0 || error "executing lfs hsm_archive"

    echo "Waiting for end of data migration..."
    wait_done 60 || error "Migration timeout"

    # make sure rm operations are in the changelog
    sleep 1

    # robinhood reads the archive_id
    $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_chglogs.log  --once || error "reading changelogs"
    check_db_error rh_chglogs.log

    # now archive and remove the last file
    $LFS hsm_archive -a 2 $RH_ROOT/file.x || error "executing lfs hsm_archive"
    echo "Waiting for end of data migration..."
    wait_done 60 || error "Migration timeout"

    echo "Removing all files"
    rm -f $RH_ROOT/file.*

    # make sure rm operations are in the changelog
    sleep 1

    # read unlink records
    $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_chglogs.log  --once || error "reading changelogs"
    check_db_error rh_chglogs.log


    echo "Checking report..."
    $REPORT -f $RBH_CFG_DIR/$config_file --deferred-rm --csv -q > rh_report.log

    nb_ent=`wc -l rh_report.log | awk '{print $1}'`
    if (( $nb_ent != $nb_archive + 3 )); then
        error "Wrong number of deferred rm reported: $nb_ent"
    fi

    for i in $(seq 1 ${#id[@]}); do
        n=${name[$((i-1))]}
        fid=${id[$((i-1))]}
        grep "$fid" rh_report.log | grep $RH_ROOT/file.$n || error "$RH_ROOT/file.$n ($fid) not found in deferred rm list"
    done

    echo "Applying deferred remove operations"
    $RH -f $RBH_CFG_DIR/$config_file --run=hsm_remove --target=all --force-all -l DEBUG -L rh_rm.log  || error "hsm_remove"

    for i in $(seq 1 ${#id[@]}); do
        n=${name[$((i-1))]}
        fid=${id[$((i-1))]}

        echo $n
        # specific cases
        if [[ "$n" == "0" ]] || [[ "$n" == "no_opt" ]]; then
            # robinhood should know the entry was in default archive
            grep "action REMOVE" rh_rm.log | grep $fid | grep "archive_id=$default_archive" ||
                error "REMOVE action for $RH_ROOT/file.$n ($fid) should be sent to default archive $default_archive"
        elif [[ "$n" == "x" ]]; then
            # robinhood doesn't know in was archive was the entry
            # send to archive 0 (must be interpreted by coordinator as a broadcast to all archives)
            grep "action REMOVE" rh_rm.log | grep $fid | grep "archive_id=0" ||
                error "REMOVE action for $RH_ROOT/file.$n ($fid) should be sent to archive 0 (broadcast)"
        else
            # should be send to archive $i
            grep "action REMOVE" rh_rm.log | grep "$fid" | grep "archive_id=$i" ||
                error "REMOVE action for $RH_ROOT/file.$n ($fid) should be sent to archive_id $i"
        fi
    done

    nb_rm=`grep "$HSMRM_STR" rh_rm.log | wc -l`
    if (($nb_rm != $nb_archive + 3)); then
        error "********** TEST FAILED: $nb_archive + 3 removals expected, $nb_rm done"
    else
        echo "OK: $nb_rm files removed from archive"
    fi

    grep "Performing new request with a limit" rh_rm.log && error "No request splitting is expected for SOFT_RM table"
}

function populate
{
	local entries=$1
	local i
	for i in `seq 1 $entries`; do
		((dir_c=$i % 10))
		((subdir_c=$i % 100))
		dir=$RH_ROOT/dir.$dir_c/subdir.$subdir_c
		mkdir -p $dir || error "creating directory $dir"
		echo "file.$i" > $dir/file.$i || error "creating file $dir/file.$i"
	done
}


function mass_softrm
{
	config_file=$1
	sleep_time=$2
	entries=$3
	policy_str="$4"

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# populate filesystem
	echo "1-Populating filesystem..."
	populate $entries

	# how many subdirs in dir.1?
	nbsubdirs=$( ls $RH_ROOT/dir.1 | grep subdir | wc -l )

	echo "2-Initial scan..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning filesystem"
    	check_db_error rh_scan.log

	grep "Full scan of" rh_scan.log | tail -1

	sleep 1

	# archiving files
	echo "3-Archiving files..."

	if (( $is_lhsm != 0 )); then
		flush_data
		$RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log || error "flushing data to backend"

		echo "3bis-Waiting for end of data migration..."
		wait_done 120 || error "Migration timeout"
		echo "update db content..."
		$RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_chglogs.log || error "reading chglog"

	elif (( $is_hsmlite != 0 )); then
		$RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log || error "flushing data to backend"
	fi
	grep "Migration summary" rh_migr.log

	echo "Checking stats after 1st scan..."
	$REPORT -f $RBH_CFG_DIR/$config_file --status-info $STATUS_MGR --csv -q --count-min=1 | grep -v ' dir,' > fsinfo.1
	cat fsinfo.1
	$REPORT -f $RBH_CFG_DIR/$config_file --deferred-rm --csv -q > deferred.1
	(( `wc -l fsinfo.1 | awk '{print $1}'` == 1 )) || error "a single file status is expected after data migration"
	status=`cat fsinfo.1 | cut -d "," -f 2 | tr -d ' '`
	nb=`cat fsinfo.1 | grep synchro | cut -d "," -f 3 | tr -d ' '`
	[[ -n $nb ]] || nb=0
	[[ "$status"=="synchro" ]] || error "status expected after data migration: synchro, got $status"
	(( $nb == $entries )) || error "$entries entries expected, got $nb"
	(( `wc -l deferred.1 | awk '{print $1}'`==0 )) || error "no deferred rm expected after first scan"
	rm -f fsinfo.1 deferred.1

	# removing some files
        echo "4-Removing files in $RH_ROOT/dir.1..."
	rm -rf "$RH_ROOT/dir.1" || error "removing files in $RH_ROOT/dir.1"

	# at least 1 second must be elapsed since last entry change (sync)
	sleep 1

	echo "5-Update DB with a new scan..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning filesystem"
    	check_db_error rh_scan.log

	grep "Full scan of" rh_scan.log | tail -1

	echo "Checking stats after 2nd scan..."
	$REPORT -f $RBH_CFG_DIR/$config_file --status-info $STATUS_MGR --csv -q --count-min=1 | grep -v ' dir,' > fsinfo.2
	cat fsinfo.2
	$REPORT -f $RBH_CFG_DIR/$config_file --deferred-rm --csv -q > deferred.2
	# 100 files were in the removed directory
	(( `wc -l fsinfo.2 | awk '{print $1}'` == 1 )) || error "a single file status is expected after data migration"
	status=`cat fsinfo.2 | cut -d "," -f 2 | tr -d ' '`
	nb=`cat fsinfo.2 | grep synchro | cut -d "," -f 3 | tr -d ' '`
	[[ "$status"=="synchro" ]] || error "status expected after data migration: synchro, got $status"
	(( $nb == $entries - 100 )) || error $(($entries - 100)) " entries expected, got $nb"
	nb=`wc -l deferred.2 | awk '{print $1}'`
	((expect=100 + $nbsubdirs + 1))
	(( $nb == $expect )) || error "$expect deferred rm expected after first scan, got $nb"
	rm -f fsinfo.2 deferred.2

}

function purge_test
{
	config_file=$1
	expected_purge=$2
	sleep_time=$3
	policy_str="$4"

	clean_logs

	# initial scan
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log
    	check_db_error rh_chglogs.log

	# fill 10 files and archive them

	echo "1-Modifing files..."
	for i in a `seq 1 10`; do
		dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "$? writing file.$i"

		if (( $is_lhsm != 0 )); then
			flush_data
			$LFS hsm_archive $RH_ROOT/file.$i || error "lfs hsm_archive"
		fi
	done
	if (( $is_lhsm != 0 )); then
		wait_done 60 || error "Copy timeout"
	fi

	sleep 1
	if (( $no_log )); then
		echo "2-Scanning the FS again to update file status (after 1sec)..."
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs to update file status (after 1sec)..."
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""

		if (($is_lhsm != 0)); then
			((`grep "archive,rc=0" rh_chglogs.log | wc -l` == 11)) || error "Not enough archive events in changelog!"
		fi
	fi
    	check_db_error rh_chglogs.log

	# use robinhood for flushing
	if (( $is_hsmlite != 0 )); then
		echo "2bis-Archiving files"
		$RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG  -L rh_migr.log || error "executing migration policy"
		arch_count=`grep "$ARCH_STR" rh_migr.log | wc -l`
		(( $arch_count == 11 )) || error "$11 archive commands expected"
	fi

	echo "3-Applying purge policy ($policy_str)..."
	# no purge expected here
	$RH -f $RBH_CFG_DIR/$config_file --run=purge --target=all --no-limit -l DEBUG -L rh_purge.log  || error ""

        nb_purge=`grep "$REL_STR" rh_purge.log | wc -l`

        if (($nb_purge != 0)); then
                error "********** TEST FAILED: No release actions expected, $nb_purge done"
        else
                echo "OK: no file released"
        fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "5-Applying purge policy again ($policy_str)..."
	$RH -f $RBH_CFG_DIR/$config_file $PURGE_OPT -l DEBUG -L rh_purge.log --once || error ""

    nb_purge=`grep "$REL_STR" rh_purge.log | wc -l`

    if (($nb_purge != $expected_purge)); then
            error "********** TEST FAILED: $expected_purge release actions expected, $nb_purge done"
    else
            echo "OK: $nb_purge files released"
    fi

	# stop RH in background
#	kill %1
}

function test_custom_purge
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	clean_logs

	# initial scan
	echo "Populating filesystem..."
	for i in `seq 1 10`; do
		dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"
	done
    # create malicious file names to test vulnerability
    touch "$RH_ROOT/foo1 \`pkill -9 $CMD\`" || error "couldn't create file"
    touch "$RH_ROOT/foo2 ; exit 1" || error "couldn't create file"
    touch "$RH_ROOT/foo3' ';' 'exit' '1'" || error "couldn't create file"

	echo "Initial scan..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log
	check_db_error rh_scan.log

    if (( $is_lhsm != 0 )); then
        # Archive files to be able to release them afterward
        flush_data
        $RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log || error "flushing data to backend"

        echo "Waiting for end of data migration..."
        wait_done 120 || error "Migration timeout"
        echo "update db content..."
        $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_chglogs.log || error "reading chglog"
    fi

	echo "Sleeping $sleep_time seconds..."
	sleep $sleep_time

    if [ -z "$POSIX_MODE" ]; then
        fsname=$(df $RH_ROOT/. | xargs | awk '{print $(NF-5)}' | awk -F '/' '{print $(NF)}')
    else
        fsname=$(df $RH_ROOT/. | xargs | awk '{print $(NF-5)}')
    fi
	if (( $no_log == 0 )); then
		# get fids of entries
		fids=()
		for i in `seq 1 10`; do
			fids[$i]=$(get_id "$RH_ROOT/file.$i")
        done
        i=11
        for f in  "$RH_ROOT/foo1 \`pkill -9 $CMD\`" "$RH_ROOT/foo2 ; exit 1" "$RH_ROOT/foo3' ';' 'exit' '1'" ; do
			fids[$i]=$(get_id "$f")
            ((i=$i+1))
        done
        [ "$DEBUG" = "1" ] && echo "fsname=$fsname, fids=${fids[*]}"
    fi

	echo "Applying purge policy ($policy_str)..."
	$RH -f $RBH_CFG_DIR/$config_file $PURGE_OPT -l FULL -L rh_purge.log --once || error "purging files"
	check_db_error rh_purge.log

	nb_purge=`grep "$REL_STR" rh_purge.log | wc -l`
	if (($nb_purge != 13)); then
		error "********** TEST FAILED: 13 purge actions expected, $nb_purge done"
	else
		echo "OK: 13 actions done"
	fi

	# checking that the custom command was called for each file
	for  i in `seq 1 10`; do
		line=$(grep "action: cmd" rh_purge.log | grep 'rm_script' | grep $RH_ROOT/file.$i)
        if [ -z "$line" ]; then
            error "No action found on $RH_ROOT/file.$i"
            continue
        fi
        # split args
	#2016/05/10 10:17:08 [5529/4] purge | [0x200000400:0x133ac:0x0]: action: cmd(./rm_script lustre 0x200000400:0x133ac:0x0 /mnt/lustre/file.1)
        args=($(echo "$line" | sed -e "s/.*rm_script//" -e "s/)$//"))
        fn=${args[0]}
        id=${args[1]}
        p=${args[2]}
        [ "$DEBUG" = "1" ] && echo "action: fsname=$fn, fid=$id, path=$p"

        [ $fn = $fsname ] || error "invalid fsname $fn != $fsname"
        # only compare fids for lustre 2.x
        if (( $no_log == 0 )); then
            [ $id = ${fids[$i]} ] || error "invalid fid $id != ${fids[$i]}"
        fi
        [ $p = $RH_ROOT/file.$i ] || error "invalid path $p != $RH_ROOT/file.$i"

        [ -f $RH_ROOT/file.$i ] && error "$RH_ROOT/file.$i still exists after purge command"
	done

    # same test for special file names
    i=11
    for f in  "$RH_ROOT/foo1 \`pkill -9 $CMD\`" "$RH_ROOT/foo2 ; exit 1" "$RH_ROOT/foo3' ';' 'exit' '1'" ; do
        f0=$(echo "$f" | awk '{print $1}')
		line=$(grep "action: cmd" rh_purge.log | grep 'rm_script' | grep "$f0")
        if [ -z "$line" ]; then
            error "No action found on $f"
            continue
        fi
        # split args
        args=($(echo "$line" | sed -e "s/.*rm_script//" -e "s/)$//"))
        fn=${args[0]}
        id=${args[1]}
        unset args[0]
        unset args[1]
        p=${args[@]}
        [ "$DEBUG" = "1" ] && echo "action: fsname=$fn, fid=$id, path=$p"

        [ $fn = $fsname ] || error "invalid fsname $fn != $fsname"
        # only compare fids for lustre 2.x
        if (( $no_log == 0 )); then
            [ $id = ${fids[$i]} ] || error "invalid fid $id != ${fids[$i]}"
        fi
        [ "$p" = "$f" ] || error "invalid path $p != $f"

        [ -f "$f" ] && error "$f still exists after purge command"
        ((i=$i+1))
    done

	return 0
}


function test_default
{
	config_file=$1
	policy_str="$2"

	clean_logs

    # matrix of files (m)igration/(p)urge:
    #       *.A  *.B  *.C
    #   X*        m
    #   Y*   p    m/p   p
    #   Z*        m
    for pre in X Y Z; do
        for suf in A B C; do
            touch $RH_ROOT/$pre.$suf || error "touch $RH_ROOT/$pre.$suf"
        done
    done

    # wait for entries to be eligible
    sleep 1

	# initial scan
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log || error "Initial scan"
    check_db_error rh_chglogs.log

	# archive the file (if applicable)
	if (( $is_hsmlite + $is_lhsm != 0 )); then
        $RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log || error "Migration"

        # check archived files
        # *.B files must be archived. other files should be.
        nb_b=$(grep "$ARCH_STR" rh_migr.log | grep -E "$RH_ROOT/[XYZ]\.B"| wc -l)
        nb_ac=$(grep "$ARCH_STR" rh_migr.log | grep -E "$RH_ROOT/[XYZ]\.[AC]"| wc -l)

        [ "$DEBUG" = "1" ] && grep "$ARCH_STR" rh_migr.log

        (( $nb_b != 3 )) && error "unexpected number of migrated *.B files: $nb_b != 3"
        (( $nb_ac != 0 )) && error "unexpected number of migrated *.[AC] files: $nb_ac != 0"
    fi

    # purge the files (if applicable)
    if (( ($is_hsmlite == 0) || ($shook != 0) )); then

        if (($is_lhsm != 0)); then
    		wait_done 60 || error "Migration timeout"

            # read changelogs to be aware of migration success
            :> rh_chglogs.log
            $RH -f $RBH_CFG_DIR/$config_file --readlog --once  -l DEBUG -L rh_chglogs.log || error "reading changelog"
            check_db_error rh_chglogs.log
        fi

        # wait for entries to be eligible
        sleep 1

        $RH -f $RBH_CFG_DIR/$config_file $PURGE_OPT --once -l DEBUG -L rh_purge.log || error "Purge"

        # check purged files
        # tmpfs: all Y* files can be purged
        # hsm: only archived files can be purged: Y.B
	    if (( $is_lhsm + $shook != 0 )); then
            nb_purge=1
            purge_pat="Y.B"
        else
            nb_purge=3
            purge_pat="Y*"
        fi
        other=$(( 9 - $nb_purge ))

        nbp=$(grep "$REL_STR" rh_purge.log | grep -E "$RH_ROOT/$purge_pat"| wc -l)
        nbnp=$(grep "$REL_STR" rh_purge.log | grep -vE "$RH_ROOT/$purge_pat" | wc -l)

        [ "$DEBUG" = "1" ] && grep "$REL_STR" rh_purge.log

        (( $nbp != $nb_purge )) && error "unexpected number of purged files matching $purge_pat : $nbp != $nb_purge"
        (( $nbnp != 0 )) && error "unexpected number of purged files matching $purge_pat: $nbnp != 0"
    fi


	# stop RH in background
#	kill %1
}

function test_undelete
{
    local config_file="$1"
    local policy_str="$2"

    clean_logs

    if (( $is_hsmlite + $is_lhsm == 0 )); then
        echo "No undelete for this flavor"
        set_skipped
        return 1
    fi

    # Using two level of directories allows to fully test the mkdir_recurse()
    # function defined in src
    local files=()
    for path in dir0/dir{1,2}/file{1,2}; do
        files+=( "$RH_ROOT/$path" )
    done

    mkdir -p "$RH_ROOT"/dir0/dir{1,2} || error "mkdir"
    for f in "${files[@]}"; do
        echo 123 > "$f" || error "write"
    done
    local sz1=$(stat -c '%s' "${files[0]}")
    local fid=$(get_id "$RH_ROOT/dir0/dir1/file1")

    # initial scan + archive all
    $RH -f "$RBH_CFG_DIR/$config_file" --scan --once $SYNC_OPT -l DEBUG -L rh_chglogs.log || error "Initial scan and sync"
    check_db_error rh_chglogs.log

    if (( $is_lhsm != 0 )); then
        wait_done 60 || error "Copy timeout"

        # archive is asynchronous: read the changelog to get archive completion status
        $RH -f "$RBH_CFG_DIR/$config_file" --readlog --once -l DEBUG -L rh_chglogs.log || error "Reading changelog"
    fi

    # remove all and read the changelog
    rm -rf "$RH_ROOT/dir0"
    $RH -f "$RBH_CFG_DIR/$config_file" --readlog --once -l DEBUG -L rh_chglogs.log || error "Reading changelog"
    check_db_error rh_chglogs.log

    # list all deleted entries
    # details about output format:
    #   - last field of each line is last entry path in filesystem
    #   - test suite uses a single status manager at once: '-s' option not needed
    $UNDELETE -f "$RBH_CFG_DIR/$config_file" -L | grep 'file' | awk '{print $(NF)}' > rh_report.log
    [ "$DEBUG" = "1" ] && cat rh_report.log
    diff <(sort rh_report.log) <(printf '%s\n' "${files[@]}" | sort) ||
        error "undelete list does not match the expected output"

    # list all deleted entried from dir1
    $UNDELETE -f "$RBH_CFG_DIR/$config_file" -L "$RH_ROOT/dir0/dir1" | grep "file" | awk '{print $(NF)}' > rh_report.log
    diff <(sort rh_report.log) <(printf '%s\n' "${files[@]:0:2}" | sort) ||
        error "undelete list does not match the expected output"

    # query a single file by path
    $UNDELETE -f "$RBH_CFG_DIR/$config_file" -L "$RH_ROOT/dir0/dir1/file2" \
        > rh_report.log || error "list softrm by path"
    grep "$RH_ROOT/dir0/dir1/file2" rh_report.log || error "entry missing in report"

    # query a single file by fid
    $UNDELETE -f "$RBH_CFG_DIR/$config_file" -L "$fid" > rh_report.log ||
        error "list softrm by fid"
    grep "$RH_ROOT/dir0/dir1/file1" rh_report.log || error "entry missing in report"

    # recover all deleted entries from dir2
    local undeleted_files=( "${files[@]:2}" )
    $UNDELETE -f "$RBH_CFG_DIR/$config_file" -R "$RH_ROOT/dir0/dir2" | grep Restoring | cut -d "'" -f 2 > rh_report.log
    diff <(sort rh_report.log) \
        <(printf '%s\n' "${undeleted_files[@]}" | sort) ||
        error "list of undeleted file does not match the expected output"

    # query a single file by path
    $UNDELETE -f "$RBH_CFG_DIR/$config_file" -R "$RH_ROOT/dir0/dir1/file2" || error "undelete by path"
    undeleted_files+=( "$RH_ROOT/dir0/dir1/file2" )

    # query a single file by fid
    $UNDELETE -f "$RBH_CFG_DIR/$config_file" -R "$fid" || error "undelete by fid"
    undeleted_files+=( "$RH_ROOT/dir0/dir1/file1" )

    for f in "${undeleted_files[@]}"; do
        [ -f "$f"  ] || error "Missing $f in FS after undelete"
    done

    # check final size
    local sz2=$(stat -c '%s' "${undeleted_files[0]}")
    (( $sz1 == $sz2 )) || error "final size $sz2 doesn't match $sz1"

    # Lustre/HSM specific checks
    if (( $is_lhsm != 0 )); then
        # files must be imported as 'released'
        for f in "${undeleted_files[@]}"; do
            $LFS hsm_state "$f" | grep released ||
                error "$f should be released"
        done

        # check if restore command succeeds
        for f in "${undeleted_files[@]}"; do
            $LFS hsm_restore "$f" || error "hsm_restore"
        done
        wait_done 60 || error "Restore timeout"

        # check final size
        sz2=$(stat -c '%s' "${undeleted_files[0]}")
        (( $sz1 == $sz2 )) || error "final size $sz2 doesn't match $sz1"

        # files must be online now
        for f in "${undeleted_files[@]}"; do
            $LFS hsm_state "$f" | grep released && error "$f should be online"
        done
    fi
}

function purge_size_filesets
{
	config_file=$1
	sleep_time=$2
	count=$3
	policy_str="$4"

	clean_logs

	# initial scan
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log
    	check_db_error rh_chglogs.log

	# fill 3 files of different sizes and mark them archived non-dirty

	j=1
	for size in 0 1 10 200; do
		echo "1.$j-Writing files of size " $(( $size*10 )) "kB..."
		((j=$j+1))
		for i in `seq 1 $count`; do
			dd if=/dev/zero of=$RH_ROOT/file.$size.$i bs=10k count=$size >/dev/null 2>/dev/null || error "writing file.$size.$i"

			if (( $is_lhsm != 0 )); then
				flush_data
				$LFS hsm_archive $RH_ROOT/file.$size.$i || error "lfs hsm_archive"
				wait_done 60 || error "Copy timeout"
			fi
		done
	done

	sleep 1
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs to update file status (after 1sec)..."
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
    	check_db_error rh_chglogs.log

	if (( $is_hsmlite != 0 )); then
		$RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG  -L rh_migr.log || error "executing $CMD --sync"
    fi

	echo "3-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "4-Applying purge policy ($policy_str)..."
	# no purge expected here
	$RH -f $RBH_CFG_DIR/$config_file $PURGE_OPT -l DEBUG -L rh_purge.log --once || error ""

	# counting each matching policy $count of each
	for policy in very_small mid_file default; do
	        nb_purge=`grep 'matching rule' rh_purge.log | grep $policy | wc -l`
		if (($nb_purge != $count)); then
			error "********** TEST FAILED: $count release actions expected matching rule $policy, $nb_purge done"
		else
			echo "OK: $nb_purge files released matching rule $policy"
		fi
	done

	# stop RH in background
#	kill %1
}

function test_maint_mode
{
	config_file=$1
	window=$2 		# in seconds
	migr_policy_delay=$3  	# in seconds
	delay_min=$4  		# in seconds
	policy_str="$5"

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

    # initial scan
    $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error "scanning filesystem"

	# writing data
	echo "1-Writing files..."
	for i in `seq 1 4`; do
		echo "file.$i" > $RH_ROOT/file.$i || error "creating file $RH_ROOT/file.$i"
	done
	t0=`date +%s`

	# read changelogs
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error "scanning filesystem"
	else
		echo "2-Reading changelogs..."
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error "reading changelogs"
	fi
    	check_db_error rh_chglogs.log

    	# migrate (nothing must be migrated, no maint mode reported)
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all -l DEBUG -L rh_migr.log   || error "executing --run=migration action"
	grep "Maintenance time" rh_migr.log && error "No maintenance mode expected"
	grep "Currently in maintenance mode" rh_migr.log && error "No maintenance mode expected"

	# set maintenance mode (due is window +10s)
	maint_time=`perl -e "use POSIX; print strftime(\"%Y%m%d%H%M%S\" ,localtime($t0 + $window + 10))"`
	$REPORT -f $RBH_CFG_DIR/$config_file --next-maintenance=$maint_time || error "setting maintenance time"

	# right now, migration window is in the future
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all -l DEBUG -L rh_migr.log   || error "executing --run=migration action"
	grep "maintenance window will start in" rh_migr.log || error "Future maintenance not report in the log"

	# sleep enough to be in the maintenance window
	sleep 11

	# split maintenance window in 4
	((delta=$window / 4))
	(( $delta == 0 )) && delta=1

	arch_done=0

	# start migrations while we do not reach maintenance time
	while (( `date +%s` < $t0 + $window + 10 )); do
		cp /dev/null rh_migr.log
		$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all -l DEBUG -L rh_migr.log   || error "executing --run=migration action"
		grep "Currently in maintenance mode" rh_migr.log || error "Should be in maintenance window now"

		# check that files are migrated after min_delay and before the policy delay
		if grep "$ARCH_STR" rh_migr.log ; then
			arch_done=1
			now=`date +%s`
			# delay_min must be elapsed
			(( $now >= $t0 + $delay_min )) || error "file migrated before dealy min"
			# migr_policy_delay must not been reached
			(( $now < $t0 + $migr_policy_delay )) || error "file already reached policy delay"
		fi
		sleep $delta
	done
	cp /dev/null rh_migr.log

	(($arch_done == 1)) || error "Files have not been migrated during maintenance window"

	(( `date +%s` > $t0 + $window + 15 )) || sleep $(( $t0 + $window + 15 - `date +%s` ))
	# shouldn't be in maintenance now
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all -l DEBUG -L rh_migr.log   || error "executing --run=migration action"
	grep "Maintenance time is in the past" rh_migr.log || error "Maintenance window should be in the past now"
}

# test reporting function with path filter
function test_rh_report
{
	config_file=$1
	dircount=$2
	sleep_time=$3
	descr_str="$4"

	clean_logs

	for i in `seq 1 $dircount`; do
		mkdir $RH_ROOT/dir.$i
		echo "1.$i-Writing files to $RH_ROOT/dir.$i..."
		# write i MB to each directory
		for j in `seq 1 $i`; do
			dd if=/dev/zero of=$RH_ROOT/dir.$i/file.$j bs=1M count=1 >/dev/null 2>/dev/null || error "writing $RH_ROOT/dir.$i/file.$j"
		done
	done

	echo "1bis. Wait for IO completion..."
	sync

	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs..."
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
    	check_db_error rh_chglogs.log

	echo "3.Checking reports..."
	for i in `seq 1 $dircount`; do
	    # posix FS do some block preallocation, so we don't know the exact space used:
    	# compare with 'du -b' instead.
        if [ -n "$POSIX_MODE" ]; then
		    real=`du -b -c $RH_ROOT/dir.$i/* | grep total | awk '{print $1}'`
    		#real=`echo "$real*512" | bc -l`
        else
            real=$(($i*1024*1024))
        fi
		$REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -U 1 -P "$RH_ROOT/dir.$i/*" > rh_report.log
		used=`tail -n 1 rh_report.log | cut -d "," -f 3`
		if (( $used != $real )); then
			error ": $used != $real"
		else
			echo "OK: space used by files in $RH_ROOT/dir.$i is $real bytes"
        fi
	done
}

#test report using accounting table
function test_rh_acct_report
{
        config_file=$1
        dircount=$2
        # used in acct.conf
        export ACCT_SWITCH=$3
        descr_str="$4"

        clean_logs

        for i in `seq 1 $dircount`; do
                mkdir $RH_ROOT/dir.$i
                echo "1.$i-Writing files to $RH_ROOT/dir.$i..."
                # write i MB to each directory
                for j in `seq 1 $i`; do
                        dd if=/dev/zero of=$RH_ROOT/dir.$i/file.$j bs=1M count=1 >/dev/null 2>/dev/null || error "$? writing $RH_ROOT/dir.$i/file.$j"
                done
        done

        echo "1bis. Wait for IO completion..."
        sync

        echo "2-Scanning..."
        $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

        echo "3.Checking reports..."
        $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv --force-no-acct --top-user > rh_no_acct_report.log
        $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv --top-user > rh_acct_report.log

        nbrowacct=` awk -F ',' 'END {print NF}' rh_acct_report.log`;
        nbrownoacct=` awk -F ',' 'END {print NF}' rh_no_acct_report.log`;
        for i in `seq 1 $nbrowacct`; do
                rowchecked=0;
                for j in `seq 1 $nbrownoacct`; do
                        if [[ `cut -d "," -f $i rh_acct_report.log` == `cut -d "," -f $j rh_no_acct_report.log`  ]]; then
                                rowchecked=1
                                break
                        fi
                done
                if (( $rowchecked == 1 )); then
                        echo "Row `awk -F ',' 'NR == 1 {print $'$i';}' rh_acct_report.log | tr -d ' '` OK"
                else
                        error "Row `awk -F ',' 'NR == 1 {print $'$i';}' rh_acct_report.log | tr -d ' '` is different with acct "
                fi
        done
        rm -f rh_no_acct_report.log
        rm -f rh_acct_report.log
}

#test --split-user-groups option
function test_rh_report_split_user_group
{
        config_file=$1
        dircount=$2
        option=$3
        descr_str="$4"

        clean_logs

        for i in `seq 1 $dircount`; do
                mkdir $RH_ROOT/dir.$i || error "creating directory $RH_ROOT/dir.$i"
                echo "1.$i-Writing files to $RH_ROOT/dir.$i..."
                # write i MB to each directory
                for j in `seq 1 $i`; do
                        dd if=/dev/zero of=$RH_ROOT/dir.$i/file.$j bs=1M count=1 >/dev/null 2>/dev/null || error "writing $RH_ROOT/dir.$i/file.$j"
                done
        done

        echo "1bis. Wait for IO completion..."
        sync

        echo "2-Scanning..."
        $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

        echo "3.Checking reports..."
        $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv --user-info $option | head --lines=-2 > rh_report_no_split.log
        $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv --user-info --split-user-groups $option | head --lines=-2 > rh_report_split.log

        nbrow=` awk -F ',' 'END {print NF}' rh_report_split.log`
        nb_uniq_user=`sed "1d" rh_report_split.log | cut -d "," -f 1 | uniq | wc -l `
        for i in `seq 1 $nb_uniq_user`; do
                check=1
                user=`sed "1d" rh_report_split.log | awk -F ',' '{print $1;}' | uniq | awk 'NR=='$i'{ print }'`
                for j in `seq 1 $nbrow`; do
                        curr_row=`sed "1d" rh_report_split.log | awk -F ',' 'NR==1 { print $'$j'; }' | tr -d ' '`
                        curr_row_label=` awk -F ',' 'NR==1 { print $'$j'; }' rh_report_split.log | tr -d ' '`
                        if [[ "$curr_row" =~ "^[0-9]*$" && "$curr_row_label" != "avg_size" ]]; then
				if [[ `grep -e "dir" rh_report_split.log` ]]; then
					sum_split_dir=`egrep -e "^$user.*dir.*" rh_report_split.log | awk -F ',' '{array[$1]+=$'$j'}END{for (name in array) {print array[name]}}'`
					sum_no_split_dir=`egrep -e "^$user.*dir.*" rh_report_no_split.log | awk -F ',' '{array[$1]+=$'$((j-1))'}END{for (name in array) {print array[name]}}'`
					sum_split_file=`egrep -e "^$user.*file.*" rh_report_split.log | awk -F ',' '{array[$1]+=$'$j'}END{for (name in array) {print array[name]}}'`
					sum_no_split_file=`egrep -e "^$user.*file.*" rh_report_no_split.log | awk -F ',' '{array[$1]+=$'$((j-1))'}END{for (name in array) {print array[name]}}'`
                                        if (( $sum_split_dir != $sum_no_split_dir || $sum_split_file != $sum_no_split_file )); then
						error "Unexpected value: dircount=$sum_split_dir/$sum_no_split_dir, filecount: $sum_split_file/$sum_no_split_file"
						echo "Split report: "
						cat rh_report_split.log
						echo "Summed report: "
						cat rh_report_no_split.log
                                                check=0
                                        fi
				else
                                        sum_split=`egrep -e "^$user" rh_report_split.log | awk -F ',' '{array[$1]+=$'$j'}END{for (name in array) {print array[name]}}'`
                                        sum_no_split=`egrep -e "^$user" rh_report_no_split.log | awk -F ',' '{array[$1]+=$'$((j-1))'}END{for (name in array) {print array[name]}}'`
					if (( $sum_split != $sum_no_split )); then
						error "Unexpected value: filecount: $sum_split/$sum_no_split"
						echo "Split report: "
						cat rh_report_split.log
						echo "Summed report: "
						cat rh_report_no_split.log
                                        	check=0
                                	fi
				fi
                        fi
                done
                if (( $check == 1 )); then
                        echo "Report for user $user: OK"
                else
                        error "Report for user $user is wrong"
                fi
        done

        rm -f rh_report_no_split.log
        rm -f rh_report_split.log

}

#test acct table and triggers creation
function test_acct_table
{
        config_file=$1
        dircount=$2
        # used in acct.conf
        export ACCT_SWITCH=$3
        descr_str="$4"

        clean_logs

        for i in `seq 1 $dircount`; do
	        mkdir $RH_ROOT/dir.$i
                echo "1.$i-Writing files to $RH_ROOT/dir.$i..."
                # write i MB to each directory
                for j in `seq 1 $i`; do
                        dd if=/dev/zero of=$RH_ROOT/dir.$i/file.$j bs=1M count=1 >/dev/null 2>/dev/null || error "writing $RH_ROOT/dir.$i/file.$j"
                done
        done

        echo "1bis. Wait for IO completion..."
        sync

        echo "2-Scanning..."
        $RH -f $RBH_CFG_DIR/$config_file --scan -l VERB -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

        if [[ "$ACCT_SWITCH" != "no" ]]; then
            echo "3.Checking acct table and triggers creation"
            grep -q "Table ACCT_STAT created successfully" rh_scan.log && echo "ACCT table creation: OK" || error "creating ACCT table"
            grep -q "Trigger ACCT_ENTRY_INSERT created successfully" rh_scan.log && echo "ACCT_ENTRY_INSERT trigger creation: OK" || error "creating ACCT_ENTRY_INSERT trigger"
            grep -q "Trigger ACCT_ENTRY_UPDATE created successfully" rh_scan.log && echo "ACCT_ENTRY_INSERT trigger creation: OK" || error "creating ACCT_ENTRY_UPDATE trigger"
            grep -q "Trigger ACCT_ENTRY_DELETE created successfully" rh_scan.log && echo "ACCT_ENTRY_INSERT trigger creation: OK" || error "creating ACCT_ENTRY_DELETE trigger"
        else
            echo "3. Checking no ACCT table or trigger have been created"
            grep -q "Table ACCT_STAT created successfully" rh_scan.log && error "table ACCT created"
            grep -q "Trigger ACCT_ENTRY_INSERT created successfully" rh_scan.log && error "ACCT_ENTRY_INSERT trigger created"
            grep -q "Trigger ACCT_ENTRY_UPDATE created successfully" rh_scan.log && error "ACCT_ENTRY_INSERT trigger created"
            grep -q "Trigger ACCT_ENTRY_DELETE created successfully" rh_scan.log && error "ACCT_ENTRY_INSERT trigger created"
        fi
}

#test accounting with borderline cases (NULL fields, etc...)
function test_acct_borderline
{
    config_file=$1
    export ACCT_SWITCH=$2

    :>rh.log

    # create DB schema
    $RH -f $RBH_CFG_DIR/$config_file --alter-db -L rh.log

    # insert 2 records with NULL uid, gid, size...
    mysql $RH_DB -e "INSERT INTO ENTRIES (id, type) VALUES ('id1','file')" || error "INSERT ERROR"
    mysql $RH_DB -e "INSERT INTO ENTRIES (id, type) VALUES ('id2','file')" || error "INSERT ERROR"
    $REPORT -f $RBH_CFG_DIR/$config_file -u '*' -S --csv -q --szprof > rh_report.log || error "report error"
    [ "$DEBUG" = "1" ] && cat rh_report.log && echo "------"

    line_values=($(grep "unknown,    unknown" rh_report.log | tr ',' ' '))
    [[ "${line_values[3]}" == 2 ]] || error "expected count: 2"
    [[ "${line_values[4]}" == 0 ]] || error "expected size: 0"
    [[ "${line_values[7]}" == 2 ]] || error "expected count for size0: 2"
    [[ "${line_values[8]}" == 0 ]] || error "expected count for size1-31: 0"

    # change size of this record (to sz32)
    mysql $RH_DB -e "UPDATE ENTRIES SET size=123 WHERE id='id1'" || error "UPDATE ERROR"
    $REPORT -f $RBH_CFG_DIR/$config_file -u '*' -S --csv -q --szprof > rh_report.log || error "report error"
    [ "$DEBUG" = "1" ] && cat rh_report.log && echo "------"

    line_values=($(grep "unknown,    unknown" rh_report.log | tr ',' ' '))
    [[ "${line_values[3]}" == 2 ]] || error "expected count: 2"
    [[ "${line_values[4]}" == 123 ]] || error "expected size: 123"
    [[ "${line_values[7]}" == 1 ]] || error "expected count for size0: 1"
    [[ "${line_values[9]}" == 1 ]] || error "expected count for size32-1K: 1"

    # change size of this record (to sz1M)
    mysql $RH_DB -e "UPDATE ENTRIES SET size=2000000 WHERE id='id1'" || error "UPDATE ERROR"
    $REPORT -f $RBH_CFG_DIR/$config_file -u '*' -S --csv -q --szprof > rh_report.log || error "report error"
    [ "$DEBUG" = "1" ] && cat rh_report.log && echo "------"

    line_values=($(grep "unknown,    unknown" rh_report.log | tr ',' ' '))
    [[ "${line_values[3]}" == 2 ]] || error "expected count: 2"
    [[ "${line_values[4]}" == 2000000 ]] || error "expected size: 2000000"
    [[ "${line_values[7]}" == 1 ]] || error "expected count for size0: 1"
    [[ "${line_values[9]}" == 0 ]] || error "expected count for size32-1K: 0"
    [[ "${line_values[12]}" == 1 ]] || error "expected count for size1M-32M: 1"

    # change record owner
    mysql $RH_DB -e "UPDATE ENTRIES SET uid='foo' WHERE id='id1'" || error "UPDATE ERROR"
    $REPORT -f $RBH_CFG_DIR/$config_file -u '*' -S --csv -q --szprof > rh_report.log || error "report error"
    [ "$DEBUG" = "1" ] && cat rh_report.log && echo "------"

    # only id2 remains with unknown uid/unknown gid
    line_values=($(grep "unknown,    unknown" rh_report.log | tr ',' ' '))
    [[ "${line_values[3]}" == 1 ]] || error "expected count: 1"
    [[ "${line_values[4]}" == 0 ]] || error "expected size: 0"
    [[ "${line_values[7]}" == 1 ]] || error "expected count for size0: 1"
    [[ "${line_values[9]}" == 0 ]] || error "expected count for size32-1K: 0"
    [[ "${line_values[12]}" == 0 ]] || error "expected count for size1M-32M: 0"

    # only id1 is now foo/unknown gid
    line_values=($(grep " foo,    unknown" rh_report.log | tr ',' ' '))
    [[ "${line_values[3]}" == 1 ]] || error "expected count: 1"
    [[ "${line_values[4]}" == 2000000 ]] || error "expected size: 2000000"
    [[ "${line_values[7]}" == 0 ]] || error "expected count for size0: 0"
    [[ "${line_values[9]}" == 0 ]] || error "expected count for size32-1K: 0"
    [[ "${line_values[12]}" == 1 ]] || error "expected count for size1M-32M: 1"
    
    # change record group
    mysql $RH_DB -e "UPDATE ENTRIES SET gid='bar' WHERE id='id1'" || error "UPDATE ERROR"
    $REPORT -f $RBH_CFG_DIR/$config_file -u '*' -S --csv -q --szprof > rh_report.log || error "report error"
    [ "$DEBUG" = "1" ] && cat rh_report.log && echo "------"

    # only id1 is now foo/bar
    line_values=($(grep " foo,        bar" rh_report.log | tr ',' ' '))
    [[ "${line_values[3]}" == 1 ]] || error "expected count: 1"
    [[ "${line_values[4]}" == 2000000 ]] || error "expected size: 2000000"
    [[ "${line_values[7]}" == 0 ]] || error "expected count for size0: 0"
    [[ "${line_values[9]}" == 0 ]] || error "expected count for size32-1K: 0"
    [[ "${line_values[12]}" == 1 ]] || error "expected count for size1M-32M: 1"

    # nothing remains as foo/unknown (no report line, or 0)
    line_values=($(grep " foo,    unknown" rh_report.log | tr ',' ' '))
    [ -z "$line_values" ] || [[ "${line_values[3]}" == 0 ]] || error "no entries expected for foo/unknown (${line_values[3]})"

    # delete records
    mysql $RH_DB -e "DELETE FROM ENTRIES WHERE id='id1'" || error "DELETE ERROR"
    mysql $RH_DB -e "DELETE FROM ENTRIES WHERE id='id2'" || error "DELETE ERROR"
    $REPORT -f $RBH_CFG_DIR/$config_file -u '*' -S --csv -q --szprof > rh_report.log || error "report error"
    [ "$DEBUG" = "1" ] && cat rh_report.log && echo "------"

    # nothing remains as foo/bar or unknown/unknown
    line_values=($(grep " foo,        bar" rh_report.log | tr ',' ' '))
    [ -z "$line_values" ] || [[ "${line_values[3]}" == 0 ]] || error "no entries expected for foo/bar (${line_values[3]})"
    line_values=($(grep "unknown,    unknown" rh_report.log | tr ',' ' '))
    [ -z "$line_values" ] || [[ "${line_values[3]}" == 0 ]] || error "no entries expected for unknown/unknown (${line_values[3]})"

    # Add new records and check ACCT is correctly populated
    mysql $RH_DB -e "INSERT INTO ENTRIES (id, type) VALUES ('id3','file')" || error "INSERT ERROR"
    mysql $RH_DB -e "INSERT INTO ENTRIES (id, type) VALUES ('id4','file')" || error "INSERT ERROR"
    mysql $RH_DB -e "INSERT INTO ENTRIES (id, type, size) VALUES ('id5','file', 123)" || error "INSERT ERROR"
    mysql $RH_DB -e "INSERT INTO ENTRIES (id, type, uid, gid, size) VALUES ('id6','file','foo','bar', 456)" || error "INSERT ERROR"
    mysql $RH_DB -e "INSERT INTO ENTRIES (id, type, uid, gid, size) VALUES ('id7','file','foo','bar', 123456)" || error "INSERT ERROR"
    mysql $RH_DB -e "DROP TABLE ACCT_STAT" || error "DROP ERROR"

    :>rh.log

    $RH -f $RBH_CFG_DIR/$config_file --alter-db -L rh.log
    # check if ACCT_STAT has been populated
    grep "Populating accounting table" rh.log || error "ACCT_STAT should have been populated"

    $REPORT -f $RBH_CFG_DIR/$config_file -u '*' -S --csv -q --szprof > rh_report.log || error "report error"
    [ "$DEBUG" = "1" ] && cat rh_report.log && echo "------"

    # check records 
    line_values=($(grep "unknown,    unknown" rh_report.log | tr ',' ' '))
    [[ "${line_values[3]}" == 3 ]] || error "expected count: 3"
    [[ "${line_values[4]}" == 123 ]] || error "expected size: 123"
    [[ "${line_values[7]}" == 2 ]] || error "expected count for size0: 2"
    [[ "${line_values[9]}" == 1 ]] || error "expected count for size32-1K: 1"

    line_values=($(grep " foo,        bar" rh_report.log | tr ',' ' '))
    [[ "${line_values[3]}" == 2 ]] || error "expected count: 2"
    [[ "${line_values[4]}" == 123912 ]] || error "expected size: 123912"
    [[ "${line_values[7]}" == 0 ]] || error "expected count for size0: 0"
    [[ "${line_values[9]}" == 1 ]] || error "expected count for size32-1K: 1"
    [[ "${line_values[11]}" == 1 ]] || error "expected count for size32K-1M: 1"
}

#test dircount reports
function test_dircount_report
{
	config_file=$1
	dircount=$2
	descr_str="$3"
	emptydir=5

	clean_logs

	# initial scan
	$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
	check_db_error rh_chglogs.log

	# create several dirs with different entry count (+10 for each)

    match_empty1=0
    match_dir1=0
	for i in `seq 1 $dircount`; do
                mkdir $RH_ROOT/dir.$i
                [[ $i == 1* ]] && ((match_dir1++))
                echo "1.$i-Creating files in $RH_ROOT/dir.$i..."
                # write i MB to each directory
                for j in `seq 1 $((10*$i))`; do
                        dd if=/dev/zero of=$RH_ROOT/dir.$i/file.$j bs=1 count=$i 2>/dev/null || error "creating $RH_ROOT/dir.$i/file.$j"
                done
        done

    echo "1bis. Creating empty directories..."
    # create 5 empty dirs
    for i in `seq 1 $emptydir`; do
        mkdir $RH_ROOT/empty.$i
        [[ $i == 1* ]] && ((match_empty1++))
    done


	# read changelogs
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
	else
		echo "2-Reading changelogs..."
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
	fi
	check_db_error rh_chglogs.log

	echo "3.Checking dircount report..."
	# dircount+1 because $RH_ROOT may be returned
	$REPORT -f $RBH_CFG_DIR/$config_file --topdirs=$((dircount+1)) --csv > report.out

    [ "$DEBUG" = "1" ] && cat report.out

	# check that dircount is right for each dir

	# check if $RH_ROOT is in topdirs. If so, check its position
	is_root=0
	line=`grep "$RH_ROOT," report.out`
	[[ -n $line ]] && is_root=1
	if (( ! $is_root )); then
		id=`stat -c "%D/%i" $RH_ROOT/. | tr '[:lower:]' '[:upper:]'`
		line=`grep "$id," report.out`
		[[ -n $line ]] && is_root=1
	fi
	if (( $is_root )); then
		root_rank=`echo $line | cut -d ',' -f 1 | tr -d ' '`
		echo "FS root $RH_ROOT was returned in top dircount (rank=$root_rank)"
	fi
	for i in `seq 1 $dircount`; do
		line=`grep "$RH_ROOT/dir.$i," report.out` || error "$RH_ROOT/dir.$i not found in report"
		rank=`echo $line | cut -d ',' -f 1 | tr -d ' '`
		count=`echo $line | cut -d ',' -f 3 | tr -d ' '`
		avg=`echo $line | cut -d ',' -f 4 | tr -d ' '`
        [ "$DEBUG" = "1" ] && echo "rank=$rank, count=$count, avg_sz=$avg"
		# if expected_rank >= root_rank, shift expected rank
		(($is_root )) && (($rank >= $root_rank)) && rank=$rank-1
		(($rank == $(( 20 - $i +1 )) )) || error "Invalid rank $rank for dir.$i"
		(($count == $(( 10 * $i )) )) || error "Invalid dircount $count for dir.$i"
		(($avg == $i)) || error "Invalid avg size $avg for dir.$i ($i expected)"
	done

    echo "3b. Checking topdirs + filterpath"
	$REPORT -f $RBH_CFG_DIR/$config_file --topdirs=$((dircount+1)) --filter-path="$RH_ROOT/dir.1" --csv -q > report.out
    [ "$DEBUG" = "1" ] && echo && cat report.out
    # only one line expected
    lines=$(wc -l report.out | awk '{print $1}')
    (( $lines == 1 )) || error "1 single dir expected in output (found $lines)"
    line=`grep "$RH_ROOT/dir.1," report.out` || error "$RH_ROOT/dir.1 not found in report"

    $REPORT -f $RBH_CFG_DIR/$config_file --topdirs=$((dircount+1)) --filter-path="$RH_ROOT/dir.1*" --csv -q > report.out
    [ "$DEBUG" = "1" ] && echo && cat report.out
    lines=$(wc -l report.out | awk '{print $1}')
    (( $lines == $match_dir1 )) || error "$match_dir1 expected in output (found $lines)"

    echo "4. Check empty dirs..."
    # check empty dirs
    $REPORT -f $RBH_CFG_DIR/$config_file --oldest-empty-dirs --csv > report.out
    [ "$DEBUG" = "1" ] && echo && cat report.out
    for i in `seq 1 $emptydir`; do
        grep "$RH_ROOT/empty.$i" report.out > /dev/null || error "$RH_ROOT/empty.$i not found in empty dirs"
    done

    # test with filterpath
    $REPORT -f $RBH_CFG_DIR/$config_file --oldest-empty-dirs --csv -q --filter-path="$RH_ROOT/empty.1" > report.out
    [ "$DEBUG" = "1" ] && echo && cat report.out
    # only one line expected
    lines=$(wc -l report.out | awk '{print $1}')
    (( $lines == 1 )) || error "1 single dir expected in output (found $lines)"
    line=`grep "$RH_ROOT/empty.1," report.out` || error "$RH_ROOT/empty.1 not found in report"

    $REPORT -f $RBH_CFG_DIR/$config_file --oldest-empty-dirs --csv -q --filter-path="$RH_ROOT/empty.1*" > report.out
    [ "$DEBUG" = "1" ] && echo && cat report.out
    lines=$(wc -l report.out | awk '{print $1}')
    (( $lines == $match_empty1 )) || error "$match_empty1 expected in output (found $lines)"

    [ "$DEBUG" = "1" ] || rm -f report.out
}

# test report options: avg_size, by-count, count-min and reverse
function    test_sort_report
{
    config_file=$1
    dummy=$2
    descr_str="$3"

    clean_logs

    # get 3 different users (from /etc/passwd)
    if [[ $RBH_NUM_UIDGID = "yes" ]]; then
        users=( $(head -n 3 /etc/passwd | cut -d ':' -f 3) )
    else
        users=( $(head -n 3 /etc/passwd | cut -d ':' -f 1) )
    fi

    echo "1-Populating filesystem with test files..."

    # populate the filesystem with data of these users
    for i in `seq 0 2`; do
        u=${users[$i]}
        mkdir $RH_ROOT/dir.$u || error "creating directory  $RH_ROOT/dir.$u"
        if (( $i == 0 )); then
            # first user:  20 files of size 1k to 20k
            for f in `seq 1 20`; do
                dd if=/dev/zero of=$RH_ROOT/dir.$u/file.$f bs=1k count=$f 2>/dev/null || error "writing $f KB to $RH_ROOT/dir.$u/file.$f"
            done
        elif (( $i == 1 )); then
            # second user: 10 files of size 10k to 100k
            for f in `seq 1 10`; do
                dd if=/dev/zero of=$RH_ROOT/dir.$u/file.$f bs=10k count=$f 2>/dev/null || error "writing $f x10 KB to $RH_ROOT/dir.$u/file.$f"
            done
        else
            # 3rd user:    5 files of size 100k to 500k
            for f in `seq 1 5`; do
                dd if=/dev/zero of=$RH_ROOT/dir.$u/file.$f bs=100k count=$f 2>/dev/null || error "writing $f x100 KB to $RH_ROOT/dir.$u/file.$f"
            done
        fi
        chown -R $u $RH_ROOT/dir.$u || error "changing owner of $RH_ROOT/dir.$u"
    done

    # flush data to OSTs
    sync

    # scan!
    echo "2-Scanning..."
    $RH -f $RBH_CFG_DIR/$config_file --scan -l VERB -L rh_scan.log  --once || error "scanning filesystem"
    check_db_error rh_scan.log

    echo "3-checking reports..."

    # sort users by volume
    $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -q --top-user > report.out || error "generating topuser report by volume"
    first=$(head -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    last=$(tail -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    [ $first = ${users[2]} ] || error "first user expected in top volume: ${users[2]} (got $first)"
    [ $last = ${users[0]} ] || error "last user expected in top volume: ${users[0]} (got $last)"

    # sort users by volume (reverse)
    $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -q --top-user --reverse > report.out || error "generating topuser report by volume (reverse)"
    first=$(head -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    last=$(tail -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    [ $first = ${users[0]} ] || error "first user expected in top volume: ${users[0]} (got $first)"
    [ $last = ${users[2]} ] || error "last user expected in top volume: ${users[2]} (got $last)"

    # sort users by count
    $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -q --top-user --by-count > report.out || error "generating topuser report by count"
    first=$(head -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    last=$(tail -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    [ $first = ${users[0]} ] || error "first user expected in top count: ${users[0]} (got $first)"
    [ $last = ${users[2]} ] || error "last user expected in top count: ${users[2]} (got $last)"

    # sort users by count (reverse)
    $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -q --top-user --by-count --reverse > report.out || error "generating topuser report by count (reverse)"
    first=$(head -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    last=$(tail -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    [ $first = ${users[2]} ] || error "first user expected in top count: ${users[2]} (got $first)"
    [ $last = ${users[0]} ] || error "last user expected in top count: ${users[0]} (got $last)"

    # sort users by avg size
    $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -q --top-user --by-avgsize > report.out || error "generating topuser report by avg size"
    first=$(head -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    last=$(tail -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    [ $first = ${users[2]} ] || error "first user expected in top avg size: ${users[2]} (got $first)"
    [ $last = ${users[0]} ] || error "last user expected in top avg size: ${users[0]} (got $last)"

    # sort users by avg size (reverse)
    $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -q --top-user --by-avgsize --reverse > report.out || error "generating topuser report by avg size (reverse)"
    first=$(head -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    last=$(tail -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    [ $first = ${users[0]} ] || error "first user expected in top avg size: ${users[0]} (got $first)"
    [ $last = ${users[2]} ] || error "last user expected in top avg size: ${users[2]} (got $last)"

    # filter users by min count
    # only user 0 and 1 have 10 entries or more
    $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -q --top-user --count-min=10 > report.out || error "generating topuser with at least 10 entries"
    (( $(wc -l report.out | awk '{print$1}') == 2 )) || error "only 2 users expected with more than 10 entries"
    egrep "^\s+[0-9]+,\s+${users[2]}," report.out && error "${users[2]} is not expected to have more than 10 entries"

    rm -f report.out
}

function count_action_params # log, pattern
{
    grep action_params $1 | grep "$2" | wc -l
}

function path_test
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "hsm test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs
	if (( $no_log == 0 )); then
		echo "Initial scan of empty filesystem"
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	fi

	# create test tree
	mkdir -p $RH_ROOT/dir1
	mkdir -p $RH_ROOT/dir1/subdir1
	mkdir -p $RH_ROOT/dir1/subdir2
	mkdir -p $RH_ROOT/dir1/subdir3/subdir4
	# 2 matching files for fileclass absolute_path
	echo "data" > $RH_ROOT/dir1/subdir1/A
	echo "data" > $RH_ROOT/dir1/subdir2/A
	# 2 unmatching
	echo "data" > $RH_ROOT/dir1/A
	echo "data" > $RH_ROOT/dir1/subdir3/subdir4/A

	mkdir -p $RH_ROOT/dir2
	mkdir -p $RH_ROOT/dir2/subdir1
	# 2 matching files for fileclass absolute_tree
	echo "data" > $RH_ROOT/dir2/X
	echo "data" > $RH_ROOT/dir2/subdir1/X

	mkdir -p $RH_ROOT/one_dir/dir3
	mkdir -p $RH_ROOT/other_dir/dir3
	mkdir -p $RH_ROOT/yetanother_dir
	mkdir -p $RH_ROOT/dir3
	mkdir -p $RH_ROOT/one_dir/one_dir/dir3
	# 2 matching files for fileclass path_depth2
	echo "data" > $RH_ROOT/one_dir/dir3/X
	echo "data" > $RH_ROOT/other_dir/dir3/Y
	# 2 unmatching files for fileclass path_depth2
	echo "data" > $RH_ROOT/dir3/X
	echo "data" > $RH_ROOT/one_dir/one_dir/dir3/X

	mkdir -p $RH_ROOT/one_dir/dir4/subdir1
	mkdir -p $RH_ROOT/other_dir/dir4/subdir1
	mkdir -p $RH_ROOT/dir4
	mkdir -p $RH_ROOT/one_dir/one_dir/dir4
	# 3 matching files for fileclass tree_depth2
	echo "data" > $RH_ROOT/one_dir/dir4/subdir1/X
	echo "data" > $RH_ROOT/other_dir/dir4/subdir1/X
    echo "data" > $RH_ROOT/yetanother_dir/dir4 # tree root should match too!
	# unmatching files for fileclass tree_depth2
	echo "data" > $RH_ROOT/dir4/X
	echo "data" > $RH_ROOT/one_dir/one_dir/dir4/X

	mkdir -p $RH_ROOT/dir5
	mkdir -p $RH_ROOT/subdir/dir5
	# 2 matching files for fileclass relative_path
	echo "data" > $RH_ROOT/dir5/A
	echo "data" > $RH_ROOT/dir5/B
	# 2 unmatching files for fileclass relative_path
	echo "data" > $RH_ROOT/subdir/dir5/A
	echo "data" > $RH_ROOT/subdir/dir5/B

	mkdir -p $RH_ROOT/dir6/subdir
	mkdir -p $RH_ROOT/subdir/dir6
	# 3 matching files for fileclass relative_tree
	echo "data" > $RH_ROOT/dir6/A
	echo "data" > $RH_ROOT/dir6/subdir/A
    echo "data" > $RH_ROOT/file.6 # tree root should match too!
	# 2 unmatching files for fileclass relative_tree
	echo "data" > $RH_ROOT/subdir/dir6/A
	echo "data" > $RH_ROOT/subdir/dir6/B


	mkdir -p $RH_ROOT/dir7/subdir
	mkdir -p $RH_ROOT/dir71/subdir
	mkdir -p $RH_ROOT/subdir/subdir/dir7
	mkdir -p $RH_ROOT/subdir/subdir/dir72
	# 3 matching files for fileclass any_root_tree
	echo "data" > $RH_ROOT/dir7/subdir/file
	echo "data" > $RH_ROOT/subdir/subdir/dir7/file
    echo "data" > $RH_ROOT/yetanother_dir/dir7 # tree root should match too!
	# 2 unmatching files for fileclass any_root_tree
	echo "data" > $RH_ROOT/dir71/subdir/file
	echo "data" > $RH_ROOT/subdir/subdir/dir72/file

	mkdir -p $RH_ROOT/dir8
	mkdir -p $RH_ROOT/dir81/subdir
	mkdir -p $RH_ROOT/subdir/subdir/dir8
	# 2 matching files for fileclass any_root_path
	echo "data" > $RH_ROOT/dir8/file.1
	echo "data" > $RH_ROOT/subdir/subdir/dir8/file.1
	# 3 unmatching files for fileclass any_root_path
	echo "data" > $RH_ROOT/dir8/file.2
	echo "data" > $RH_ROOT/dir81/file.1
	echo "data" > $RH_ROOT/subdir/subdir/dir8/file.2

	mkdir -p $RH_ROOT/dir9/subdir/dir10/subdir
	mkdir -p $RH_ROOT/dir9/subdir/dir10x/subdir
	mkdir -p $RH_ROOT/dir91/subdir/dir10
	# 3 matching files for fileclass any_level_tree
	echo "data" > $RH_ROOT/dir9/subdir/dir10/file
	echo "data" > $RH_ROOT/dir9/subdir/dir10/subdir/file
	echo "data" > $RH_ROOT/dir9/subdir/dir10x/dir10  # tree root should match too!
	# 2 unmatching files for fileclass any_level_tree
	echo "data" > $RH_ROOT/dir9/subdir/dir10x/subdir/file
	echo "data" > $RH_ROOT/dir91/subdir/dir10/file

	mkdir -p $RH_ROOT/dir11/subdir/subdir
	mkdir -p $RH_ROOT/dir11x/subdir
	# 2 matching files for fileclass any_level_path
	echo "data" > $RH_ROOT/dir11/subdir/file
	echo "data" > $RH_ROOT/dir11/subdir/subdir/file
	# 2 unmatching files for fileclass any_level_path
	echo "data" > $RH_ROOT/dir11/subdir/file.x
	echo "data" > $RH_ROOT/dir11x/subdir/file


	echo "1bis-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	# read changelogs
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs..."
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
	check_db_error rh_chglogs.log


	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should not be migrated this time
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all -l DEBUG -L rh_migr.log   || error ""

	# count the number of file for each policy
	nb_pol1=$(count_action_params rh_migr.log class=absolute_path)
	nb_pol2=$(count_action_params rh_migr.log  class=absolute_tree)
	nb_pol3=$(count_action_params rh_migr.log  class=path_depth2)
	nb_pol4=$(count_action_params rh_migr.log  class=tree_depth2)
	nb_pol5=$(count_action_params rh_migr.log  class=relative_path)
	nb_pol6=$(count_action_params rh_migr.log  class=relative_tree)

	nb_pol7=$(count_action_params rh_migr.log  class=any_root_tree)
	nb_pol8=$(count_action_params rh_migr.log  class=any_root_path)
	nb_pol9=$(count_action_params rh_migr.log  class=any_level_tree)
	nb_pol10=$(count_action_params rh_migr.log  class=any_level_path)

	nb_unmatch=$(count_action_params rh_migr.log  class=unmatch)

	(( $nb_pol1 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'absolute_path': $nb_pol1"
	(( $nb_pol2 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'absolute_tree': $nb_pol2"
	(( $nb_pol3 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'path_depth2': $nb_pol3"
	(( $nb_pol4 == 3 )) || error "********** TEST FAILED: wrong count of matching files for policy 'tree_depth2': $nb_pol4"
	(( $nb_pol5 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'relative_path': $nb_pol5"
	(( $nb_pol6 == 3 )) || error "********** TEST FAILED: wrong count of matching files for policy 'relative_tree': $nb_pol6"

	(( $nb_pol7 == 3 )) || error "********** TEST FAILED: wrong count of matching files for policy 'any_root_tree': $nb_pol7"
	(( $nb_pol8 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'any_root_path': $nb_pol8"
	(( $nb_pol9 == 3 )) || error "********** TEST FAILED: wrong count of matching files for policy 'any_level_tree': $nb_pol9"
	(( $nb_pol10 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'any_level_tree': $nb_pol10"
	(( $nb_unmatch == 19 )) || error "********** TEST FAILED: wrong count of unmatching files: $nb_unmatch"

	(( $nb_pol1 == 2 )) && (( $nb_pol2 == 2 )) && (( $nb_pol3 == 2 )) && (( $nb_pol4 == 3 )) \
        	&& (( $nb_pol5 == 2 )) && (( $nb_pol6 == 3 )) && (( $nb_pol7 == 3 )) \
		&& (( $nb_pol8 == 2 )) && (( $nb_pol9 == 3 )) && (( $nb_pol10 == 2 )) \
		&& (( $nb_unmatch == 19 )) \
		&& echo "OK: test successful"
}



function update_test
{
    config_file=$1
    event_updt_min=$2
    update_period=$3
    policy_str="$4"

    init=`date "+%s"`

    LOG=rh_chglogs.log

    if (( $no_log )); then
        echo "changelog disabled: skipped"
        set_skipped
        return 1
    fi

    for i in `seq 1 3`; do
        # force emptying the log
        $LFS changelog_clear lustre-MDT0000 cl1 0

        t=$(( `date "+%s"` - $init ))
        echo "loop 1.$i: many 'touch' within $event_updt_min sec (t=$t)"
        clean_logs

        # start log reader (DEBUG level displays needed attrs)
        $RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L $LOG --detach \
            --pid-file=rh.pid 2>/dev/null || error ""

        start=`date "+%s"`
        # generate a lot of MTIME events within 'event_updt_min'
        # => must only update once
        while (( `date "+%s"` - $start < $event_updt_min - 2 )); do
            touch $RH_ROOT/file
            usleep 10000
        done

        # force flushing log
        sleep 1
        pkill $PROC
        sleep 1
        t=$(( `date "+%s"` - $init ))

        nb_getattr=`grep getattr=1 $LOG | wc -l`
        egrep -e "getattr=1|needed because" $LOG
        echo "nb attr update: $nb_getattr"

        expect_attr=1
        (( $shook != 0 && $i == 1 )) && expect_attr=4 # .shook dir, .shook/restripe dir, .shook/locks dir

        (( $nb_getattr == $expect_attr )) || error "********** TEST FAILED: wrong count of getattr: $nb_getattr (t=$t), expected=$expect_attr"
        # the path may be retrieved at the first loop (at creation)
        # but not during the next loop (as long as elapsed time < update_period)
        if (( $i > 1 )) && (( `date "+%s"` - $init < $update_period )); then
            nb_getpath=`grep getpath=1 $LOG | wc -l`
            grep "getpath=1" $LOG
            echo "nb path update: $nb_getpath"
            (( $nb_getpath == 0 )) || error "********** TEST FAILED: wrong count of getpath: $nb_getpath (t=$t), expected=0"
        fi

        # wait for 5s to be fully elapsed
        while (( `date "+%s"` - $start <= $event_updt_min )); do
            usleep 100000
        done
    done

    init=`date "+%s"`

    for i in `seq 1 3`; do
        # force emptying the log
        $LFS changelog_clear lustre-MDT0000 cl1 0

        echo "loop 2.$i: many 'rename' within $event_updt_min sec"
        clean_logs

        # start log reader (DEBUG level displays needed attrs)
        $RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L $LOG \
            --detach --pid-file=rh.pid 2>/dev/null || error ""

        start=`date "+%s"`
        # generate a lot of TIME events within 'event_updt_min'
        # => must only update once
        while (( `date "+%s"` - $start < $event_updt_min - 2 )); do
            mv $RH_ROOT/file $RH_ROOT/file.2
            usleep 10000
            mv $RH_ROOT/file.2 $RH_ROOT/file
            usleep 10000
        done

        # force flushing log
        sleep 1
        pkill $PROC
        sleep 1

        nb_getpath=`grep getpath=1 $LOG | wc -l`
        echo "nb path update: $nb_getpath"
        # no getpath expected as rename records provide name info
        (( $nb_getpath == 0 )) || error "********** TEST FAILED: wrong count of getpath: $nb_getpath"

        # attributes may be retrieved at the first loop (at creation)
        # but not during the next loop (as long as elapsed time < update_period)
        if (( $i > 1 )) && (( `date "+%s"` - $init < $update_period )); then
            nb_getattr=`grep getattr=1 $LOG | wc -l`
            echo "nb attr update: $nb_getattr"
            (( $nb_getattr == 0 )) || error "********** TEST FAILED: wrong count of getattr: $nb_getattr"
        fi
    done

    # force emptying the log
    $LFS changelog_clear lustre-MDT0000 cl1 0

    echo "Waiting $update_period seconds..."
    clean_logs

    # check that getattr+getpath are performed after update_period, even if the event is not related:
    $RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L $LOG --detach \
        --pid-file=rh.pid 2>/dev/null || error ""
    sleep $update_period

    if (( $is_lhsm != 0 )); then
        # chg something different that path or POSIX attributes
        $LFS hsm_set --noarchive $RH_ROOT/file
    else
        touch $RH_ROOT/file
    fi

    # force flushing log
    sleep 1
    pkill $PROC
    sleep 1

    nb_getattr=`grep getattr=1 $LOG | wc -l`
    echo "nb attr update: $nb_getattr"
    (( $nb_getattr == 1 )) || error "********** TEST FAILED: wrong count of getattr: $nb_getattr"
    nb_getpath=`grep getpath=1 $LOG | wc -l`
    echo "nb path update: $nb_getpath"
    (( $nb_getpath == 1 )) || error "********** TEST FAILED: wrong count of getpath: $nb_getpath"

    if (( $is_lhsm != 0 )); then
        # also check that the status is to be retrieved
        nb_getstatus=`grep "getstatus(lhsm)" $LOG | wc -l`
        echo "nb status update: $nb_getstatus"
        (( $nb_getstatus == 1 )) || error "********** TEST FAILED: wrong count of getstatus: $nb_getstatus"
    fi

    # kill remaining event handler
    sleep 1
    pkill -9 $PROC
}

function periodic_class_match_migr
{
	config_file=$1
	update_period=$2
	policy_str="$3"

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	#create test tree
	touch $RH_ROOT/ignore1
	touch $RH_ROOT/whitelist1
	touch $RH_ROOT/migrate1
	touch $RH_ROOT/default1

	# scan
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log
	check_db_error rh_chglogs.log

	# now apply policies
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all --dry-run -l FULL -L rh_migr.log  || error ""

	#we must have 4 lines like this: Entry xxx matches target file class
	nb_updt=`grep "matches target file class" rh_migr.log | wc -l`
	nb_whitelist=`grep "matches ignored target" rh_migr.log | wc -l`
	nb_migr_match=`grep "matches the condition for policy rule 'migr_match'" rh_migr.log | wc -l`
	nb_default=`grep "matches the condition for policy rule 'default'" rh_migr.log | wc -l`

	(( $nb_updt == 1 )) || error "********** TEST FAILED: wrong count of matched fileclasses: $nb_updt/1"
	(( $nb_whitelist == 2 )) || error "********** TEST FAILED: wrong count of ignored entries : $nb_whitelist/2"
	(( $nb_migr_match == 1 )) || error "********** TEST FAILED: wrong count of files matching 'migr_match': $nb_migr_match"
	(( $nb_default == 1 )) || error "********** TEST FAILED: wrong count of files matching 'default': $nb_default"

        (( $nb_updt == 1 )) && (( $nb_whitelist == 2 ))  && (( $nb_migr_match == 1 )) && (( $nb_default == 1 )) \
		&& echo "OK: fileclass matching successful"

	echo "Waiting $update_period sec..."
	sleep $update_period

	# rematch entries: should update all fileclasses
	clean_logs
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all --dry-run -l FULL -L rh_migr.log  || error ""

	nb_updt=`grep "matches target file class" rh_migr.log | wc -l`
	nb_whitelist=`grep "matches ignored target" rh_migr.log | wc -l`

	(( $nb_updt == 1 )) || error "********** TEST FAILED: wrong count of matched fileclasses: $nb_updt/1"
	(( $nb_whitelist == 2 )) || error "********** TEST FAILED: wrong count of ignored entries : $nb_whitelist/2"

    (( $nb_updt == 1 )) && (( $nb_whitelist == 2 )) && echo "OK: all fileclasses updated"
}

function policy_check_migr
{
    # check that migr fileclasses are properly matched at scan time,
    # then at application time
	config_file=$1
	update_period=$2
	policy_str="$3"

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	#create test tree
	touch $RH_ROOT/ignore1
	touch $RH_ROOT/whitelist1
	touch $RH_ROOT/migrate1
	touch $RH_ROOT/default1

    echo "1. scan..."
	# scan
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log || error "scanning"
	check_db_error rh_chglogs.log
    # check that all files have been properly matched

    $REPORT -f $RBH_CFG_DIR/$config_file --dump -q  > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    st1=`grep ignore1 report.out | cut -d ',' -f 5 | tr -d ' '`
    st2=`grep whitelist1 report.out  | cut -d ',' -f 5 | tr -d ' '`
    st3=`grep migrate1 report.out  | cut -d ',' -f 5 | tr -d ' '`
    st4=`grep default1 report.out  | cut -d ',' -f 5 | tr -d ' '`

    [ "$st1" = "to_be_ignored" ] || error "file should be in class 'to_be_ignored'"
    [ "$st2" = "" ] || error "file should not match a class"
    [ "$st3" = "to_be_migr" ] || error "file should be in class 'to_be_migr'"
    [ "$st4" = "" ] || error "file should not match a class"

    echo "2. migrate..."

	# now apply policies
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all --dry-run -l FULL -L rh_migr.log  || error "running migration"

    $REPORT -f $RBH_CFG_DIR/$config_file --dump -q  > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    st1=`grep ignore1 report.out | cut -d ',' -f 5 | tr -d ' '`
    st2=`grep whitelist1 report.out  | cut -d ',' -f 5 | tr -d ' '`
    st3=`grep migrate1 report.out  | cut -d ',' -f 5 | tr -d ' '`
    st4=`grep default1 report.out  | cut -d ',' -f 5 | tr -d ' '`

    [ "$st1" = "to_be_ignored" ] || error "file should be in class 'to_be_ignored'"
    [ "$st2" = "" ] || error "file should not match a class"
    [ "$st3" = "to_be_migr" ] || error "file should be in class 'to_be_migr'"
    [ "$st4" = "" ] || error "file should not match a class"

	#we must have 4 lines like this: "Need to update fileclass (not set)"
	nb_migr_match=`grep "matches the condition for policy rule 'migr_match'" rh_migr.log | wc -l`
	nb_default=`grep "matches the condition for policy rule 'default'" rh_migr.log | wc -l`

	(( $nb_migr_match == 1 )) || error "********** TEST FAILED: wrong count of files matching 'migr_match': $nb_migr_match"
	(( $nb_default == 1 )) || error "********** TEST FAILED: wrong count of files matching 'default': $nb_default"

    (( $nb_migr_match == 1 )) && (( $nb_default == 1 )) \
		&& echo "OK: initial fileclass matching successful"

	# rematch entries
	clean_logs
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all --dry-run -l FULL -L rh_migr.log  || error "running $RH --run=migration"

    # check effectively migrated files
    m1_arch=`grep "$ARCH_STR" rh_migr.log | grep migrate1 | wc -l`
    d1_arch=`grep "$ARCH_STR" rh_migr.log | grep default1 | wc -l`
    w1_arch=`grep "$ARCH_STR" rh_migr.log | grep whitelist1 | wc -l`
    i1_arch=`grep "$ARCH_STR" rh_migr.log | grep ignore1 | wc -l`

    (( $w1_arch == 0 )) || error "whitelist1 should not have been migrated"
    (( $i1_arch == 0 )) || error "ignore1 should not have been migrated"
    (( $m1_arch == 1 )) || error "migrate1 should have been migrated"
    (( $d1_arch == 1 )) || error "default1 should have been migrated"

    (( $w1_arch == 0 )) && (( $i1_arch == 0 )) && (( $m1_arch == 1 )) \
    && (( $d1_arch == 1 )) && echo "OK: All expected files migrated"

    rm -f report.out
}

function policy_check_purge
{
    # check that purge fileclasses are properly matched at scan time,
    # then at application time
    config_file=$1
    update_period=$2
    policy_str="$3"

    stf=5

    clean_logs

    #create test tree
    touch $RH_ROOT/ignore1
    touch $RH_ROOT/whitelist1
    touch $RH_ROOT/purge1
    touch $RH_ROOT/default1

    echo "1. scan..."
    # scan
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log || error "scanning"
    check_db_error rh_chglogs.log
    # check that all files have been properly matched

    $REPORT -f $RBH_CFG_DIR/$config_file --dump -q  > report.out
    [ "$DEBUG" = "1" ] && cat report.out

    st1=`grep ignore1 report.out | cut -d ',' -f $stf | tr -d ' '`
    st2=`grep whitelist1 report.out  | cut -d ',' -f $stf | tr -d ' '`
    st3=`grep purge1 report.out  | cut -d ',' -f $stf | tr -d ' '`
    st4=`grep default1 report.out  | cut -d ',' -f $stf | tr -d ' '`

    [ "$st1" = "to_be_ignored" ] || error "file should be in class 'to_be_ignored'"
    [ "$st2" = "" ] || error "file should not match a fileclass"
    [ "$st3" = "to_be_released" ] || error "file should be in class 'to_be_released'"
    [ "$st4" = "" ] || error "file should not match a fileclass"

    if (( $is_lhsm + $is_hsmlite > 0 )); then
        echo "1bis. migrate..."

        # now apply policies
        if (( $is_lhsm != 0 )); then
                flush_data
                $RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log || error "flushing data to backend"

                echo "1ter. Waiting for end of data migration..."
                wait_done 120 || error "Migration timeout"
        echo "update db content..."
        $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_chglogs.log || error "reading chglog"

        elif (( $is_hsmlite != 0 )); then
                $RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log || error "flushing data to backend"
        fi

        # check that release class is still correct
        $REPORT -f $RBH_CFG_DIR/$config_file --dump -q  > report.out
        [ "$DEBUG" = "1" ] && cat report.out

        st1=`grep ignore1 report.out | cut -d ',' -f $stf | tr -d ' '`
        st2=`grep whitelist1 report.out  | cut -d ',' -f $stf | tr -d ' '`
        st3=`grep purge1 report.out  | cut -d ',' -f $stf | tr -d ' '`
        st4=`grep default1 report.out  | cut -d ',' -f $stf | tr -d ' '`

        [ "$st1" = "to_be_ignored" ] || error "file should be in class 'to_be_ignored'"
        [ "$st2" = "" ] || error "file should not match a fileclass"
        [ "$st3" = "to_be_released" ] || error "file should be in class 'to_be_released'"
        [ "$st4" = "" ] || error "file should not match a fileclass"
    fi
    sleep 1
    echo "2. purge/release..."

    # now apply policies
    $RH -f $RBH_CFG_DIR/$config_file $PURGE_OPT -l FULL -L rh_purge.log --once || error "running purge"

    $REPORT -f $RBH_CFG_DIR/$config_file --dump -q  > report.out
    [ "$DEBUG" = "1" ] && cat report.out

    st1=`grep ignore1 report.out | cut -d ',' -f $stf | tr -d ' '`
    st2=`grep whitelist1 report.out  | cut -d ',' -f $stf | tr -d ' '`

    [ "$st1" = "to_be_ignored" ] || error "file should be in class 'to_be_ignored'"
    [ "$st2" = "" ] || error "file should not match a fileclass: $st2"

    #we must have 2 lines like this: "Need to update fileclass (not set)"
    nb_purge_match=`grep "matches the condition for policy rule 'purge_match'" rh_purge.log | wc -l`
    nb_default=`grep "matches the condition for policy rule 'default'" rh_purge.log | wc -l`

    (( $nb_purge_match == 1 )) || error "********** TEST FAILED: wrong count of files matching 'purge_match': $nb_purge_match"
    (( $nb_default == 1 )) || error "********** TEST FAILED: wrong count of files matching 'default': $nb_default"

    (( $nb_purge_match == 1 )) && (( $nb_default == 1 )) \
        && echo "OK: initial fileclass matching successful"

    # check effectively purged files
    p1_arch=`grep "$REL_STR" rh_purge.log | grep purge1 | wc -l`
    d1_arch=`grep "$REL_STR" rh_purge.log | grep default1 | wc -l`
    w1_arch=`grep "$REL_STR" rh_purge.log | grep whitelist1 | wc -l`
    i1_arch=`grep "$REL_STR" rh_purge.log | grep ignore1 | wc -l`

    (( $w1_arch == 0 )) || error "whitelist1 should not have been purged"
    (( $i1_arch == 0 )) || error "ignore1 should not have been purged"
    (( $p1_arch == 1 )) || error "purge1 should have been purged"
    (( $d1_arch == 1 )) || error "default1 should have been purged"

    (( $w1_arch == 0 )) && (( $i1_arch == 0 )) && (( $p1_arch == 1 )) \
        && (( $d1_arch == 1 )) && echo "OK: All expected purge actions triggered"

    if (( $is_lhsm + $shook > 0 )); then
        st1=$(grep purge1 report.out | cut -d ',' -f 6 | tr -d ' ')
        st2=$(grep default1 report.out | cut -d ',' -f 6 | tr -d ' ')

        [ "$st1" = "released" ] || [ "$st1" = "release_pending" ] ||
            error "purge1 should be 'released' or 'release_pending' (actual: $st1)"
        [ "$st2" = "released" ] || [ "$st2" = "release_pending" ] ||
            error "default1 should be 'released' or 'release_pending' (actual: $st2)"
    else
        # entries should be removed
        grep purge1 report.out && error "purge1 should have been removed from DB"
        grep default1 report.out && error "default1 should have been removed from DB"

        [ -f $RH_ROOT/purge1 ] && error "purge1 should have been removed from filesystem"
        [ -f $RH_ROOT/default1 ] && error "default1 should have been removed from filesystem"
    fi

    rm -f report.out

    # check that purge fileclasses are properly matched at scan time,
    # then at application time
    return 0
}


function periodic_class_match_purge
{
	config_file=$1
	update_period=$2
	policy_str="$3"

	clean_logs

	echo "Writing and archiving files..."
	#create test tree of archived files
	for file in ignore1 whitelist1 purge1 default1 ; do
		touch $RH_ROOT/$file

		if (( $is_lhsm != 0 )); then
			flush_data
			$LFS hsm_archive $RH_ROOT/$file
		fi
	done
	if (( $is_lhsm != 0 )); then
		wait_done 60 || error "Copy timeout"
	fi

	echo "FS Scan..."
	if (( $is_hsmlite != 0 )); then
		$RH -f $RBH_CFG_DIR/$config_file --scan $SYNC_OPT -l DEBUG  -L rh_migr.log || error "executing $CMD --sync"
		check_db_error rh_migr.log
	    else
   		$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log || error "executing $CMD --scan"
		check_db_error rh_chglogs.log
	fi

	# now apply policies
	$RH -f $RBH_CFG_DIR/$config_file $PURGE_OPT --dry-run -l FULL -L rh_purge.log  || error ""

	nb_updt=`grep "matches target file class" rh_purge.log | wc -l`
	nb_whitelist=`grep "matches ignored target" rh_purge.log | wc -l`
	nb_purge_match=`grep "matches the condition for policy rule 'purge_match'" rh_purge.log | wc -l`
	nb_default=`grep "matches the condition for policy rule 'default'" rh_purge.log | wc -l`

	# we must have 4 lines like this: "Need to update fileclass (not set)"
	(( $nb_updt == 1 )) || error "********** TEST FAILED: wrong count of matched fileclasses: $nb_updt/1"
	(( $nb_whitelist == 2 )) || error "********** TEST FAILED: wrong count of ignored entries : $nb_whitelist/2"
	(( $nb_purge_match == 1 )) || error "********** TEST FAILED: wrong count of files matching 'purge_match': $nb_purge_match"
	(( $nb_default == 1 )) || error "********** TEST FAILED: wrong count of files matching 'default': $nb_default"

        (( $nb_updt == 1 )) && (( $nb_whitelist == 2 )) && (( $nb_purge_match == 1 )) && (( $nb_default == 1 )) \
		&& echo "OK: initial fileclass matching successful"

	# TMP_FS_MGR:  whitelisted status is always checked at scan time (?)
	# 	2 entries are new (default and to_be_released)
	if (( $is_lhsm + $is_hsmlite == 0 )); then
		already=0
		new=2
	else
		already=0
		new=0
	fi

	# update db content and rematch entries: should update all fileclasses
	clean_logs
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log
	check_db_error rh_chglogs.log

	echo "Waiting $update_period sec..."
	sleep $update_period

	$RH -f $RBH_CFG_DIR/$config_file $PURGE_OPT --dry-run -l FULL -L rh_purge.log  || error ""

	nb_updt=`grep "matches target file class" rh_purge.log | wc -l`
	nb_whitelist=`grep "matches ignored target" rh_purge.log | wc -l`

	(( $nb_updt == 1 )) || error "********** TEST FAILED: wrong count of matched fileclasses: $nb_updt/1"
	(( $nb_whitelist == 2 )) || error "********** TEST FAILED: wrong count of ignored entries : $nb_whitelist/2"

    (( $nb_updt == 1 )) && (( $nb_whitelist == 2 )) && echo "OK: all fileclasses updated"
}

function test_size_updt
{
	config_file=$1
	event_read_delay=$2
	policy_str="$3"
    cl_delay=6 # time between action and its impact on rbh-report

	init=`date "+%s"`

	LOG=rh_chglogs.log

	if (( $no_log )); then
		echo "changelog disabled: skipped"
		set_skipped
		return 1
	fi

    # create a log reader
	$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L $LOG --detach || error "starting chglog reader"
    sleep 1

    # create a small file and write it (20 bytes, incl \n)
    echo "qqslmdkqslmdkqlmsdk" > $RH_ROOT/file
    sleep 1

    [ "$DEBUG" = "1" ] && $FIND $RH_ROOT/file -f $RBH_CFG_DIR/$config_file -ls
    size=$($FIND $RH_ROOT/file -f $RBH_CFG_DIR/$config_file -ls | awk '{print $(NF-3)}')
    if [ -z "$size" ]; then
       echo "db not yet updated, waiting changelog processing delay ($cl_delay sec)..."
       sleep $cl_delay
       size=$($FIND $RH_ROOT/file -f $RBH_CFG_DIR/$config_file -ls | awk '{print $(NF-3)}')
    fi

    if (( $size != 20 )); then
        error "unexpected size value: $size != 20 (is Lustre version < 2.3?)"
    fi

    # now appending the file (+20 bytes, incl \n)
    echo "qqslmdkqslmdkqlmsdk" >> $RH_ROOT/file
    sleep 1

    [ "$DEBUG" = "1" ] && $FIND $RH_ROOT/file -f $RBH_CFG_DIR/$config_file -ls
    size=$($FIND $RH_ROOT/file -f $RBH_CFG_DIR/$config_file -ls | awk '{print $(NF-3)}')
    if [ -z "$size" ]; then
       echo "db not yet updated, waiting one more second..."
       sleep 1
       size=$($FIND $RH_ROOT/file -f $RBH_CFG_DIR/$config_file -ls | awk '{print $(NF-3)}')
    fi

    if (( $size != 40 )); then
        error "unexpected size value: $size != 40"
    fi

    pkill -9 $PROC
}

#used by test_action_params
function check_action_param # $log $id $name $value
{
    local log=$1
    local id=$2
    local name=$3
    local val=$4

    line=$(grep 'action_params:' $log | grep "\[$id\]" | grep " $name=")
    if [ -z "$line" ]; then
        error "parameter '$name' not found for $id"
        return 1
    fi

    local found=$(echo "$line" | sed -e "s/.* $name=\([^,]*\).*/\1/")
    if [[ "$val" == "$found" ]]; then
        echo "OK: $id: $name = $val"
        return 0
    else
        error "$id: invalid value for parameter '$name': got '$found', '$val' expected."
        return 1
    fi
}

#used by test_action_params
function check_rule_and_class #  $log $path $rule $class
{
    local log=$1
    local path=$2
    local rule=$3
    local class=$4

    if [[ -n "$class" ]]; then
        grep "success for '$path', matching rule '$rule' (fileset=$class)" $log || error "action success not found for $path, rule $rule, class $class"
    else
        grep "success for '$path', matching rule '$rule'," $log || error "action success not found for $path, rule $rule"
    fi
}


#used by test_action_params
function check_action_patterns # $log $id $pattern...
{
    local log=$1
    local id=$2
    shift 2

    local act=$(grep 'action:' $log | grep "\[$id\]"| sed -e "s/.*cmd(\(.*\))$/\1/" | tr -d "'")

    for pattern in "$@"; do
        echo "$act" | grep -- "$pattern" || error "pattern '$pattern' not found in cmd '$act'"
    done
}

#used by test_action_params
function check_action_function # $log $id $name
{
    local log=$1
    local id=$2
    local name=$3

    grep "action:" $log | grep "\[$id\]" | grep $name || error "action $name expected"
}

function test_action_params
{
    config_file=$1

	clean_logs

    if (( $is_lhsm == 0 )); then
       echo "This test uses Lustre/HSM actions. Skipping for current test mode $PURPOSE."
       set_skipped
       return 1
    fi

    # create 1 file in each class + default
    touch $RH_ROOT/file.1a
    touch $RH_ROOT/file.1b
    touch $RH_ROOT/file.2
    touch $RH_ROOT/file.3
    touch $RH_ROOT/file.4

    id1a=$(get_id $RH_ROOT/file.1a)
    id1b=$(get_id $RH_ROOT/file.1b)
    id2=$(get_id $RH_ROOT/file.2)
    id3=$(get_id $RH_ROOT/file.3)
    id4=$(get_id $RH_ROOT/file.4)

	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scan error"
    check_db_error rh_scan.log

    # check classinfo report
    $REPORT -f $RBH_CFG_DIR/$config_file --class-info -q > rh_report.log || error "report error"
    # find_valueInCSVreport $logFile $typeValues $countValues $colSearch
    find_valueInCSVreport rh_report.log class1a 1  2 || error "invalid count for class1a"
    find_valueInCSVreport rh_report.log class1b 1  2 || error "invalid count for class1b"
    find_valueInCSVreport rh_report.log class2  1  2 || error "invalid count for class2"
    find_valueInCSVreport rh_report.log class3  1  2 || error "invalid count for class3"

    # run migration policy (force run to ignore rule time condition)
    $RH -f $RBH_CFG_DIR/$config_file --run=migration --force-all -l DEBUG -L rh_migr.log || error "policy run error"
    check_db_error rh_migr.log

    [ "$DEBUG" = "1" ] && egrep -e 'action:|action_params:' rh_migr.log

    # check selected action and action_params for each file

    # file.1a (class1a, rule migr1)
    #   'prio = 4' from fileclass
    #   'cos = 2' from rule
    #   'archive_id = 1' (policy default)
    #   'mode = trigger' from trigger
    # action: lfs hsm_archive -a {archive_id} {fullpath} --data cos={cos},class={fileclass}"
    ## BUG: the whole argument should be quoted (not the replaced part) in "cos={cos}").
    check_action_param rh_migr.log $id1a prio 4
    check_action_param rh_migr.log $id1a cos 2
    check_action_param rh_migr.log $id1a archive_id 1
    check_action_param rh_migr.log $id1a mode trigger
    check_rule_and_class rh_migr.log $RH_ROOT/file.1a "migr1" "class1a"
    check_action_patterns rh_migr.log $id1a "-a 1" "$RH_ROOT/file.1a" "cos=2" "class=class1a"

    # file.1b (class1b, rule migr1)
    #   'prio = 5' from fileclass
    #   'cos = 2' from rule
    #   'archive_id = 1' (policy default)
    #   'mode = trigger' from trigger
    check_action_param rh_migr.log $id1b prio 5
    check_action_param rh_migr.log $id1b cos 2
    check_action_param rh_migr.log $id1b archive_id 1
    check_action_param rh_migr.log $id1b mode trigger
    check_rule_and_class rh_migr.log $RH_ROOT/file.1b "migr1" "class1b"
    check_action_patterns rh_migr.log $id1b "-a 1" "$RH_ROOT/file.1b" "cos=2" "class=class1b"

    # file.2 (class2, rule migr2)
    #   'cos = 4' from fileclass (override 'cos = 3' from rule).
    #   'archive_id = 1' (policy default)
    #   'mode = trigger' from trigger
    # action: lhsm.archive (rule)
    check_action_param rh_migr.log $id2 cos 4
    check_action_param rh_migr.log $id2 archive_id 1
    check_action_param rh_migr.log $id2 mode trigger
    check_rule_and_class rh_migr.log $RH_ROOT/file.2 "migr2" "class2"
    check_action_function rh_migr.log "$id2" "lhsm.archive"

    # file.3 (class3, rule migr3)
    #   'archive_id = 2' (override 'archive_id = 1' from policy)
    #   'cos = 1' (policy default)
    #   'mode = over1' (override 'mode = trigger' from trigger)
    # action from policy: lfs hsm_archive -a {archive_id} /mnt/lustre/.lustre/fid/{fid} --data cos={cos}
    check_action_param rh_migr.log $id3 cos 1
    check_action_param rh_migr.log $id3 archive_id 2
    check_action_param rh_migr.log $id3 mode over1
    check_action_patterns rh_migr.log $id3 "-a 2" "$RH_ROOT/.lustre/fid/$id3" "cos=1"

    # file.4 (no class, rule default)
    #   'archive_id = 1' (policy default)
    #   'cos = 1' (policy default)
    #   'mode = over2' (override 'mode = trigger' from trigger)
    check_action_param rh_migr.log $id4 cos 1
    check_action_param rh_migr.log $id4 archive_id 1
    check_action_param rh_migr.log $id4 mode over2
    check_rule_and_class rh_migr.log $RH_ROOT/file.4 "default" ""
    check_action_patterns rh_migr.log $id4 "-a 1" "$RH_ROOT/.lustre/fid/$id4" "cos=1"

    if (($is_lhsm != 0)); then
		wait_done 60 || error "Copy timeout"
    fi

    # check copy completion
    $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l EVENT -L rh_chglogs.log || error "readlog"
    check_db_error rh_chglogs.log

	clean_logs

    # now purge all (not by trigger)
    ## BUG: don't quote again if an argument is already quoted
    ## BUG: don't redirect &1 and &2 if the command already does
    $RH -f $RBH_CFG_DIR/$config_file $PURGE_OPT --force-all -l DEBUG -L rh_purge.log || error "policy run error"
    check_db_error rh_purge.log

    [ "$DEBUG" = "1" ] && egrep -e 'action:|action_params:' rh_purge.log

    # no mode expected (not triggered)
    grep "action_params:" rh_purge.log | grep " mode=" && error "no mode parameter expected"

    # file.1a (class1a, rule purge1)
    #   'arg = 2' from rule
    # action: rm -f {fullpath}
    check_action_param rh_purge.log $id1a arg 2
    check_rule_and_class rh_purge.log $RH_ROOT/file.1a "purge1" "class1a"
    check_action_patterns rh_purge.log $id1a "rm -f" "$RH_ROOT/file.1a"

    # file.1b (class1b, rule purge1)
    #   'arg = 55' from fileclass
    # action: rm -f {fullpath}
    check_action_param rh_purge.log $id1b arg 55
    check_rule_and_class rh_purge.log $RH_ROOT/file.1b "purge1" "class1b"
    check_action_patterns rh_purge.log $id1b "rm -f" "$RH_ROOT/file.1b"

    # file.2 (class2, rule purge2)
    #   'arg = 3' from rule
    # action: echo '{fid}' '{rule}' '{arg}'  >> /tmp/purge.log
    check_action_param rh_purge.log $id2 arg 3
    check_rule_and_class rh_purge.log $RH_ROOT/file.2 "purge2" "class2"
    check_action_patterns rh_purge.log $id2 "echo" "$id2" "purge2" "3"
    # FIXME grep the line in /tmp/purge.log

    # file.3 (class3, rule purge3)
    #   'arg = 1' from policy
    # action: lhsm.release (policy definition default)
    check_action_param rh_purge.log $id3 arg 1
    check_rule_and_class rh_purge.log $RH_ROOT/file.3 "purge3" "class3"
    check_action_function rh_purge.log "$id3" "lhsm.release"

    # file.4 (rule default)
    #   'arg = 4' from rule
    # action: lhsm.release (policy definition default)
    check_action_param rh_purge.log $id4 arg 4
    check_rule_and_class rh_purge.log $RH_ROOT/file.4 "default" ""
    check_action_patterns rh_purge.log $id4 "echo" "$id4" "default" "4"
}

function test_manual_run
{
	config_file=$1
    run_interval=$2
    flavor=$3

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

    # create test files (2 for each rule)
    for i in 1 2 3 4 5 11 12 13 14 15 ; do
        touch $RH_ROOT/file.$i
    done

    # initial scan
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log ||
        error "scan error"
    check_db_error rh_scan.log

    # policy rules specifies last_mod >= 1
    sleep 1

    case "$flavor" in
    run)
        cmd="--run" # run with arguments
        bgr=1       # background run?
        nb_run_migr=2  # nb migration run expected
        nb_run_purge=2 # nb purge run expected
        nb_items_migr=10 # nb migration actions
        ;;
    run_all)
        cmd="--run=all"
        bgr=1
        nb_run_migr=2
        nb_run_purge=2
        nb_items_migr=10
        ;;
    run_migr)
        cmd="--run=migration"
        bgr=1
        nb_run_migr=2
        nb_run_purge=0
        nb_items_migr=10
        ;;
    run_migr_tgt)
        cmd="--run=migration(target=class:file2)"
        bgr=0
        nb_run_migr=1
        nb_run_purge=0
        nb_items_migr=2
        ;;
    run_migr_usage)
         cmd="--run=migration(target=class:file1,target-usage=0%)"
         bgr=0
         nb_run_migr=1
         nb_run_purge=0
         nb_items_migr=2
        ;;
    run_both)
        # run the 2 policies as one-shot cmds (first tgt1, then tgt2?)
        cmd="--run=migration(user:root),purge(target=class:file1)"
        bgr=0
        nb_run_migr=1
        nb_run_purge=1
        nb_items_migr=10
        ;;
    ## --run=policy(limits) => to be added with later patch (daemon)
    ## --run=policy1(target1,limit1),policy1(target2,limit2) => to be added with later patch (once shot)

    esac

    echo "run options: $cmd"
    if [ $bgr = 1 ]; then
        $RH -f $RBH_CFG_DIR/$config_file $cmd -l DEBUG -L rh_migr.log --detach \
            --pid-file=rh.pid || error "starting background run"

        # sleep 1.5 the run interval (2 runs should be started)
        sleep $((3*$run_interval/2))
        kill_from_pidfile
    else
        $RH -f $RBH_CFG_DIR/$config_file $cmd -l DEBUG -L rh_migr.log || \
            error "starting run"
    fi

    c=$(grep migration rh_migr.log | grep "Starting policy run" | wc -l)
    (( c == $nb_run_migr )) || error "$nb_run_migr migration runs expected (found: $c)"
    c=$(grep purge rh_migr.log | grep "Starting policy run" | wc -l)
    (( c == $nb_run_purge )) || error "$nb_purge purge runs expected (found: $c)"

    c=$(grep migration rh_migr.log | grep "Executing policy action" | wc -l)
    (( c == $nb_items_migr )) || error "$nb_items_migr migration actions expected (found: $c)"
}

# test policy limits at all levels (trigger, policy parameters, manual run...)
function test_limits
{
	config_file=$1
    flavor=$2

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

    # create test files (2 for each rule)
    for i in 1 2 3 4 5 11 12 13 14 15 ; do
        # 1MB each
        dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=1 2>/dev/null
    done

    # 0 for no limit
    export trig_cnt=0
    export trig_vol=0
    export param_cnt=0
    export param_vol=0

    case "$flavor" in
    trig_cnt)
        # check that a count limit specified in a trigger is taken into account
        run_opt="--run=migration --once"
        export trig_cnt=5
        ;;
    trig_vol)
        # check that a size limit specified in a trigger is taken into account
        run_opt="--run=migration --once"
        export trig_vol="5MB"
        ;;
    param_cnt)
        # check that a count limit specified in a policy parameter is taken into account
        run_opt="--run=migration --once"
        export param_cnt=5
        ;;
    param_vol)
        # check that a size limit specified in a policy parameter is taken into account
        run_opt="--run=migration --once"
        export param_vol="5MB"
        ;;
    run_cnt)
        # check that a count limit specified in a manual run is taken into account
        run_opt="--run=migration(all,max-count=5)"
        ;;
    run_vol)
        # check that a size limit specified in a manual run is taken into account
        run_opt="--run=migration(all,max-vol=5MB)"
        ;;
    trig_param)
        # check that if a limit is specified in trigger+policy parameter, we take the min
        run_opt="--run=migration --once"
        export trig_vol="7MB"
        export param_vol="5MB"
        ;;
    trig_run)
        # check that if a limit is specified in trigger+command line, we take the min
        run_opt="--run=migration(all,max-count=5)"
        export trig_vol="7MB"
        ;;
    param_run)
        # check that if a limit is specified in command line+policy parameter, we take the min
        run_opt="--run=migration(all,max-vol=7MB)"
        export param_cnt="5"
        ;;
    esac

    # initial scan
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log ||
        error "scan error"
    check_db_error rh_scan.log

    # policy rules specifies last_mod >= 1
    sleep 1

    # we should get 5 actions / 5MB in all cases
    $RH -f $RBH_CFG_DIR/$config_file $run_opt -l DEBUG -L rh_migr.log || \
        error "starting run"

    [ "$DEBUG" = "1" ] && grep "run summary" rh_migr.log

    c=$(grep "run summary" rh_migr.log | cut -d ";" -f 3 | awk '{print $1}')

    (( c == 5 )) || error "5 actions expected (got $c)"
}

# test limits using max_per_run scheduler
function test_sched_limits
{
    config_file=$1
    flavor=$2

    if (( $is_lhsm + $is_hsmlite == 0 )); then
        echo "HSM test only: skipped"
        set_skipped
        return 1
    fi

    clean_logs

    # create test files (2 for each rule)
    for i in 1 2 3 4 5 11 12 13 14 15 ; do
        # 1MB each
        dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=1 2>/dev/null
    done

    # 0 for no limit
    export trig_cnt=0
    export trig_vol=0
    export param_cnt=0
    export param_vol=0
    export sched_max_cnt=0;
    export sched_max_vol=0;

    case "$flavor" in
    sched_max_cnt)
        run_opt="--run=migration --once"
        export trig_vol="7MB"
        export param_vol="6MB"
        export sched_max_cnt="5";
        ;;
    sched_max_vol)
        run_opt="--run=migration(all,max-count=6)"
        export trig_vol="7MB"
        export sched_max_vol="5MB";
        ;;
    trigger)
        run_opt="--run=migration --once"
        export trig_vol="5MB"
        export param_vol="6MB"
        export sched_max_cnt=7
        ;;
    param)
        run_opt="--run=migration(all,max-vol=7MB)"
        export param_cnt="5"
        export sched_max_vol="6MB";
        ;;
    cmd)
        run_opt="--run=migration(all,max-vol=5MB)"
        export param_cnt=7
        export sched_max_cnt=6
    esac

    # initial scan
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log ||
        error "scan error"
    check_db_error rh_scan.log

    # policy rules specifies last_mod >= 1
    sleep 1

    # we should get 5 actions / 5MB in all cases
    $RH -f $RBH_CFG_DIR/$config_file $run_opt -l DEBUG -L rh_migr.log || \
        error "starting run"

    [ "$DEBUG" = "1" ] && grep "run summary" rh_migr.log

    c=$(grep "run summary" rh_migr.log | cut -d ";" -f 3 | awk '{print $1}')

    (( c == 5 )) || error "5 actions expected (got $c)"
}

function test_basic_sm
{
    local config_file=$1

    clean_logs

    local nb_files_ok=10
    local nb_files_error=5
    local nb_all=$(( $nb_files_ok + $nb_files_error ))

    # create 2 sets of files:
    # - for file.<i> the action will succeed
    # - for file.<i>.fail the action will fail
    echo "1-Creating test files..."
    for i in $(seq ${nb_files_ok}); do
        dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=1 2>/dev/null ||
            error "writing file.$i"
    done
    for i in $(seq ${nb_files_error}); do
        dd if=/dev/zero of=$RH_ROOT/file.$i.fail bs=1M count=1 2>/dev/null ||
            error "writing file.$i.fail"
    done

    echo "2-Scanning"
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l VERB -L rh_scan.log ||
        error "scan error"
    check_db_error rh_scan.log

    echo "3-Checking initial 'basic' status"
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info=touch --csv -q \
            --count-min=1 > rh_report.log
    [ "$DEBUG" = "1" ] && cat rh_report.log
    check_status_count rh_report.log "ok" 0
    check_status_count rh_report.log "failed" 0
    check_status_count rh_report.log "" $nb_all

    # make sure md_update of scan < now
    sleep 1
    echo "4-Running basic policy"
    $RH -f $RBH_CFG_DIR/$config_file --run=touch --once -l VERB -L rh_migr.log ||
        error "policy run error"
    check_db_error rh_migr.log

    # check policy actions are executed on all entries
    local actions=$(grep "Executing policy action" rh_migr.log | wc -l)
    (($actions == $nb_all)) || error "$nb_all actions expected"

    echo "5-Checking final 'basic' status"
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info=touch --csv -q \
            --count-min=1 > rh_report.log
    [ "$DEBUG" = "1" ] && cat rh_report.log
    check_status_count rh_report.log "ok" $nb_files_ok
    check_status_count rh_report.log "failed" $nb_files_error

    return 0
}

function test_modeguard_sm_dir
{
    local config_file=$1

    clean_logs

    local nb_dir_ok=10
    local nb_dir_invalid=5
    local nb_all=$(( $nb_dir_ok + $nb_dir_invalid ))

    # test_modeguard_dir.conf will set mode 2000 and clear 0002

    # create 2 sets of directories:
    # - dir.<i>.ok with the setgid bit and not writable by other
    # - dir.<i>.invalid without the setgid bit or writable by other
    echo "1-Creating test directories..."
    for i in $(seq ${nb_dir_ok}); do
        mkdir $RH_ROOT/dir.$i.ok || error "creating dir.$i.ok"
        chmod g+s,o-w $RH_ROOT/dir.$i.ok || error "chmod dir.$i.ok"
    done
    for i in $(seq 1 2 ${nb_dir_invalid}); do
        mkdir $RH_ROOT/dir.$i.invalid || error "creating dir.$i.ok"
        chmod g-s,o-w $RH_ROOT/dir.$i.invalid || error "chmod dir.$i.ok"
    done
    for i in $(seq 2 2 ${nb_dir_invalid}); do
        mkdir $RH_ROOT/dir.$i.invalid || error "creating dir.$i.ok"
        chmod g+s,o+w $RH_ROOT/dir.$i.invalid || error "chmod dir.$i.ok"
    done

    echo "2-Scanning"
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l VERB -L rh_scan.log ||
        error "scan error"
    check_db_error rh_scan.log

    echo "3-Checking initial 'modeguard' status"
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info=modeguard --csv -q \
            --count-min=1 > rh_report.log
    [ "$DEBUG" = "1" ] && cat rh_report.log
    check_status_count rh_report.log "ok" $nb_dir_ok
    check_status_count rh_report.log "invalid" $nb_dir_invalid

    # make sure md_update of scan < now
    sleep 1
    echo "4-Running modeguard policy"
    $RH -f $RBH_CFG_DIR/$config_file --run=modeguard --once -l VERB \
        -L rh_migr.log || error "policy run error"
    check_db_error rh_migr.log

    # check policy actions are executed on invalid entries
    local actions=$(grep "Executing policy action" rh_migr.log | wc -l)
    [ "$DEBUG" = "1" ] && cat rh_migr.log
    (($actions == $nb_dir_invalid)) || error "$nb_dir_invalid actions expected"

    if (( $no_log )); then
        echo "5-Scanning"
        $RH -f $RBH_CFG_DIR/$config_file --scan --once -l VERB -L rh_scan.log ||
            error "scan error"
        check_db_error rh_scan.log
    else
        echo "5-Reading changelogs"
        $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l VERB \
            -L rh_chglogs.log || error "readlog error"
        check_db_error rh_chglogs.log
    fi

    echo "6-Checking final 'modeguard' status"
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info=modeguard --csv -q \
            --count-min=1 > rh_report.log
    [ "$DEBUG" = "1" ] && cat rh_report.log
    check_status_count rh_report.log "ok" $nb_all
    check_status_count rh_report.log "invalid" 0

    return 0
}

function test_modeguard_sm_file
{
    local config_file=$1

    clean_logs

    local nb_file_ok=10
    local nb_file_invalid=5
    local nb_all=$(( $nb_file_ok + $nb_file_invalid ))

    # test_modeguard_file.conf will clear mode bits 0007

    # create 2 sets of files:
    # - dir.<i>.ok with mode 770
    # - dir.<i>.invalid either with mode 771 or 777
    echo "1-Creating test files..."
    for i in $(seq ${nb_file_ok}); do
        touch $RH_ROOT/file.$i.ok || error "creating file.$i.ok"
        chmod 0770 $RH_ROOT/file.$i.ok || error "chmod file.$i.ok"
    done
    for i in $(seq 1 2 ${nb_file_invalid}); do
        touch $RH_ROOT/file.$i.invalid || error "creating file.$i.ok"
        chmod 0771 $RH_ROOT/file.$i.invalid || error "chmod file.$i.ok"
    done
    for i in $(seq 2 2 ${nb_file_invalid}); do
        touch $RH_ROOT/file.$i.invalid || error "creating file.$i.ok"
        chmod 0777 $RH_ROOT/file.$i.invalid || error "chmod file.$i.ok"
    done

    echo "2-Scanning"
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l VERB -L rh_scan.log ||
        error "scan error"
    check_db_error rh_scan.log

    echo "3-Checking initial 'modeguard' status"
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info=modeguard --csv -q \
            --count-min=1 > rh_report.log
    [ "$DEBUG" = "1" ] && cat rh_report.log
    check_status_count rh_report.log "ok" $nb_file_ok
    check_status_count rh_report.log "invalid" $nb_file_invalid

    # make sure md_update of scan < now
    sleep 1
    echo "4-Running modeguard policy"
    $RH -f $RBH_CFG_DIR/$config_file --run=modeguard --once -l VERB \
        -L rh_migr.log || error "policy run error"
    check_db_error rh_migr.log

    # check policy actions are executed on invalid entries
    local actions=$(grep "Executing policy action" rh_migr.log | wc -l)
    [ "$DEBUG" = "1" ] && cat rh_migr.log
    (($actions == $nb_file_invalid)) || error "$nb_file_invalid actions expected"

    if (( $no_log )); then
        echo "5-Scanning"
        $RH -f $RBH_CFG_DIR/$config_file --scan --once -l VERB -L rh_scan.log ||
            error "scan error"
        check_db_error rh_scan.log
    else
        echo "5-Reading changelogs"
        $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l VERB \
            -L rh_chglogs.log || error "readlog error"
        check_db_error rh_chglogs.log
    fi

    echo "6-Checking final 'modeguard' status"
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info=modeguard --csv -q \
            --count-min=1 > rh_report.log
    [ "$DEBUG" = "1" ] && cat rh_report.log
    check_status_count rh_report.log "ok" $nb_all
    check_status_count rh_report.log "invalid" 0

    return 0
}

function action_executed_on
{
    local target="$1"
    local log_file="$2"

    grep "Executing policy action" "$log_file" | grep "$target"
}

function assert_action_on
{
    action_executed_on "$1" "$2" ||
        error "Action expected on '$1'"
}

function assert_no_action_on
{
    action_executed_on "$1" "$2" &&
        error "No action expected on '$1'"
}

# test pre check matching behaviors
function test_prepost_sched
{
    config_file=$1
    export pre_sched=$2
    export post_sched=$3
    export sched=$4

    if (( $is_lhsm + $is_hsmlite == 0 )); then
        echo "HSM test only: skipped"
        set_skipped
        return 1
    fi

    # only pre_sched or post_sched != none
    local check_mode=$pre_sched
    [ $pre_sched = "none" ] && check_mode=$post_sched

    clean_logs

    mkdir $RH_ROOT/subdir
    # create test files (1 for each rule)
    # make sure these files match the policy condition (older than 1day)
    touch -d "now-1day" $RH_ROOT/file.{2..4}
    touch -d "now-1day" $RH_ROOT/subdir/file.1

    # initial scan
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG \
        -L rh_scan.log 2>/dev/null || error "scan error"
    check_db_error rh_scan.log

    # file 4 is younger than previously created ones
    touch $RH_ROOT/file.4
    # change file depth
    mv $RH_ROOT/file.2 $RH_ROOT/subdir/file.2

    $RH -f $RBH_CFG_DIR/$config_file --run=migration --once -l FULL \
        -L rh_migr.log 2>/dev/null

    case "$check_mode" in
    none)
        # depth criteria is not checked => files in subdir are migrated
        assert_action_on "subdir/file.1" rh_migr.log
        assert_action_on "file.2" rh_migr.log
        assert_action_on "file.3" rh_migr.log
        grep "Updating info about" rh_migr.log &&
            error "No attr update expected"
        ;;

    cache_only)
        # mtime is not refreshed => file.4 is migrated
        assert_action_on "file.4" rh_migr.log
        # depth is from DB (move not taken into account) => file 2 is migrated
        assert_action_on "file.2" rh_migr.log
        # depth comes from DB, it is checked
        assert_no_action_on "subdir/file.1" rh_migr.log
        # action should run on file.3
        assert_action_on "file.3" rh_migr.log
        grep "Updating info about" rh_migr.log &&
            error "No attr update expected"
        ;;

    # no big difference between these 2, as the used policy needs to update
    # everything
    auto_update|force_update)
        # any needed criteria is refreshed: subdir/file.1 file2, and file.4 are
        # not migrated
        assert_no_action_on "subdir/file.1" rh_migr.log
        assert_no_action_on "file.2" rh_migr.log
        assert_no_action_on "file.4" rh_migr.log
        assert_action_on "file.3" rh_migr.log
        grep -q "Updating POSIX info" rh_migr.log ||
            error "Attr update expected"
        ;;
    esac

    $RH -f $RBH_CFG_DIR/$config_file --run=cleanup --once -l FULL \
        -L rh_purge.log 2>/dev/null
    case "$check_mode" in
    none|cache_only)
        # no update expected
        grep "Updating info" rh_purge.log && error "No attr update expected"
        ;;
    auto_update)
        # POSIX attr update expected, but no path
        grep -q "Updating POSIX info" rh_purge.log ||
            error "Attr update expected"
        grep "Updating path info" rh_purge.log &&
            error "No path update expected"
        ;;
    force_update)
        # all updates expected
        grep -q "Updating POSIX info" rh_purge.log ||
            error "Attr update expected"
        grep -q "Updating path info" rh_purge.log ||
            error "Path update expected"
        ;;
    esac

    return 0
}

function grep_matched_rule
{
    log_file=$1
    policy_name=$2
    rule_name=$3

    grep "$policy_name \|" $log_file | grep " matches the condition for policy rule '$rule_name'"
}

function test_checker
{
    config_file=$1
    nb_files=4

    clean_logs

    # default dataversion is mtime+size
    # use data_version with lustre 2.4+
    if [ $FS_TYPE = "lustre" ]; then
        $LFS help | grep -q data_version && export RBH_CKSUM_DV_CMD="lfs data_version"
    fi

    # create initial version of files
    for i in `seq 1 $nb_files`; do
        dd if=/dev/urandom of=$RH_ROOT/file.$i bs=16k count=$i >/dev/null 2>/dev/null || error "writing file.$i"
    done
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log ||
        error "scan error"
    check_db_error rh_scan.log

    # if robinhood is not installed, use rbh_cksum.sh from script directory 
    if [ -d "../../src/robinhood" ]; then
        export PATH="$PATH:../../scripts/"
    # else use the installed one
    fi

    # run before 5s (no checksumming: last_mod < 5)
    echo "No sum (last_mod criteria for new entries)"
    $RH -f $RBH_CFG_DIR/$config_file --run=checksum --target=all -l DEBUG -L rh_migr.log ||
        error "running checksum"
    init=$(grep_matched_rule rh_migr.log checksum initial_check | wc -l)
    rematch=$(grep_matched_rule rh_migr.log checksum default | wc -l)
    [[ $init == 0 && $rematch == 0 ]] || error "No matching rule expected ($init, $rematch)"

    :> rh_migr.log
    # initial checksum after 5s
    sleep 5
    echo "Initial sum"
    $RH -f $RBH_CFG_DIR/$config_file --run=checksum --target=all -l DEBUG -L rh_migr.log ||
        error "running checksum"
    init=$(grep_matched_rule rh_migr.log checksum initial_check | wc -l)
    rematch=$(grep_matched_rule rh_migr.log checksum default | wc -l)
    [[ $init == 4 && $rematch == 0 ]] || error "4 initial_check rule expected ($init, $rematch)"

    for i in `seq 1 $nb_files`; do
        [ "$DEBUG" = "1" ] && $REPORT  -f $RBH_CFG_DIR/$config_file -e $RH_ROOT/file.$i | grep checksum
        status=$($REPORT  -f $RBH_CFG_DIR/$config_file -e $RH_ROOT/file.$i | grep checksum\.status | awk '{print $(NF)}')
        [ "$status" = "ok" ] || error "Unexpected status '$status' for $RH_ROOT/file.$i (ok expected)"
    done

    :> rh_migr.log
    # re-run (no checksumming: last_check < 5)
    echo "No sum (last_check criteria)"
    $RH -f $RBH_CFG_DIR/$config_file --run=checksum --target=all -l DEBUG -L rh_migr.log ||
        error "running checksum"
    init=$(grep_matched_rule rh_migr.log checksum initial_check | wc -l)
    rematch=$(grep_matched_rule rh_migr.log checksum default | wc -l)
    [[ $init == 0 && $rematch == 0 ]] || error "No matching rule expected ($init, $rematch)"

    rm -f $RH_ROOT/file.1
    echo "sqdkqlsdk" >> $RH_ROOT/file.2
    touch $RH_ROOT/file.3

    :> rh_migr.log
    # rerun (changes occurred!)
    sleep 5
    echo "New sum (last_check OK)"
    $RH -f $RBH_CFG_DIR/$config_file --run=checksum --target=all -l DEBUG -L rh_migr.log ||
        error "running checksum"
    init=$(grep_matched_rule rh_migr.log checksum initial_check | wc -l)
    rematch=$(grep_matched_rule rh_migr.log checksum default | wc -l)
    [[ $init == 0 && $rematch == 3 ]] || error "3 default rule expected ($init, $rematch)"

    for i in `seq 2 $nb_files`; do # was removed
        [ "$DEBUG" = "1" ] && $REPORT  -f $RBH_CFG_DIR/$config_file -e $RH_ROOT/file.$i | grep checksum
        status=$($REPORT  -f $RBH_CFG_DIR/$config_file -e $RH_ROOT/file.$i | grep checksum\.status | awk '{print $(NF)}')
        [ "$status" = "ok" ] || error "Unexpected status '$status' for $RH_ROOT/file.$i (ok expected)"
    done
}

function test_action_check
{
    # Test the check of outstanding actions
    local config_file=$1
    local FCOUNT=50
    local ACT_TIMEO=4

    if (( $is_lhsm == 0 )); then
        echo "No asynchronous archive for this purpose: skipped"
        set_skipped
        return 1
    fi

    clean_logs

    echo "Create Files ..."
    for i in `seq 1 $FCOUNT` ; do
        dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i"
    done

    echo "Start asynchonous actions..."
    local t0=$(date +%s)
    $RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=all -I -l DEBUG -L rh_migr.log

    # check status of files in DB:
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info lhsm --csv -q | tee report.out

    find_valueInCSVreport report.out archiving $FCOUNT 3 || error "Invalid count of entries with status 'archiving'"

    :>rh_migr.log
    $RH -f $RBH_CFG_DIR/$config_file --run=migration -l VERB -L rh_migr.log &
    local pid=$!

    sleep 1

    # is the check done?
    grep -q "Checking status of outstanding actions" rh_migr.log ||
        error "No check of outstanding actions was performed"

    # early check of entries status (hopefully, no timeout reached yet? (3s))
    local t1=$(date +%s)
    local elapsed=$(($t1-$t0))
    local nb_check=$(grep "Updating status of" rh_migr.log | wc -l)
    if (( $elapsed < $ACT_TIMEO && $nb_check > 0 )); then
        error "No action check should be done after $elapsed sec < $ACT_TIMEO"
    else
        echo "Elapsed: $elapsed, nb_check=$nb_check"
    fi

    # next check is after 10 sec
    sleep 10

    local run_check=$(grep "Checking status of outstanding actions" rh_migr.log | wc -l)
    (( $run_check == 2 )) || error "No 2nd check was done after 10 sec"

    t1=$(date +%s)
    elapsed=$(($t1-$t0))
    nb_check=$(grep "Updating status of" rh_migr.log | wc -l)
    local nb_sync=$(grep "changed: now 'synchro'" rh_migr.log | wc -l)
    if (( $nb_check != $FCOUNT )); then
        error "All actions should have been checked now (elapsed: $elapsed, nb_check=$nb_check)"
    else
        echo "Elapsed: $elapsed, nb_check=$nb_check, $nb_sync changed to 'synchro'"
    fi

    # wait for all files to be synchro
    if (( $nb_sync < $FCOUNT )); then
        # once all actions are finished, check entry status changed accordingly
        (( $is_lhsm != 0 )) && wait_done 30

        # wait for next status check
        local t2=$(date +%s)
        (( $t2-$t1 < 10)) && sleep $((10 - ($t2-$t1)))
        nb_sync=$(grep "changed: now 'synchro'" rh_migr.log | wc -l)
        echo "$nb_sync entries/$FCOUNT changed to status 'synchro'"
        (( $nb_sync != $FCOUNT )) && error "All entries status should have been set to 'synchro'"
    fi

    # double-check in report
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info lhsm --csv -q | tee report.out
    find_valueInCSVreport report.out synchro $FCOUNT 3 || error "Invalid count of entries with status 'synchro'"

    kill -9 $pid
}


function test_cnt_trigger
{
	config_file=$1
	file_count=$2
	exp_purge_count=$3
	policy_str="$4"

	clean_logs

	if (( $is_hsmlite != 0 )); then
        # this mode may create an extra inode in filesystem: initial scan
        # to take it into account
		$RH -f $RBH_CFG_DIR/$config_file --scan --once -l MAJOR -L rh_scan.log \
            2>/dev/null || error "executing $CMD --scan"
		check_db_error rh_scan.log
    fi

	# initial inode count
	empty_count=`df -i $RH_ROOT/ | grep "$RH_ROOT" | xargs | awk '{print $(NF-3)}'`
    export high_cnt=$((file_count + $empty_count))
    export low_cnt=$(($high_cnt - $exp_purge_count))

    [ "$DEBUG" = "1" ] && echo "Initial inode count $empty_count, creating additional $file_count files"

	#create test tree of archived files (1M each)
	for i in `seq 1 $file_count`; do
		dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=1 >/dev/null \
            2>/dev/null || error "writing $RH_ROOT/file.$i"

		if (( $is_lhsm != 0 )); then
			$LFS hsm_archive $RH_ROOT/file.$i
		fi
	done

	if (( $is_lhsm != 0 )); then
		wait_done 60 || error "Copy timeout"
	fi

	# wait for df sync
	sync; sleep 1

	if (( $is_hsmlite != 0 )); then
        # scan and sync
		$RH -f $RBH_CFG_DIR/$config_file --scan $SYNC_OPT -l DEBUG \
            -L rh_migr.log 2>/dev/null || error "executing $CMD --sync"
		check_db_error rh_migr.log
    else
       	# scan
	    	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG \
            -L rh_chglogs.log 2>/dev/null || error "executing $CMD --scan"
		check_db_error rh_chglogs.log
    fi

	# apply purge trigger
	$RH -f $RBH_CFG_DIR/$config_file --run=purge --once -l FULL -L rh_purge.log

	nb_release=`grep "$REL_STR" rh_purge.log | wc -l`

	if (($nb_release == $exp_purge_count)); then
		echo "OK: $nb_release files released"
	else
		error ": $nb_release files released, $exp_purge_count expected"
	fi
}


function test_ost_trigger
{
	config_file=$1
	mb_h_threshold=$2
	mb_l_threshold=$3
	policy_str="$4"

    export ost_high_vol="${mb_h_threshold}MB"
    export ost_low_vol="${mb_l_threshold}MB"

    if [ -n "$POSIX_MODE" ]; then
        echo "No OST support for POSIX mode"
        set_skipped
        return 1
    fi

	clean_logs

    # reset df values
    wait_stable_df

	empty_vol=`$LFS df $RH_ROOT | grep OST0000 | awk '{print $3}'`
	empty_vol=$(($empty_vol/1024))

    if (($empty_vol >= $mb_h_threshold)); then
        error "OST IS ALREADY OVER HIGH THRESHOLD (cannot run test)"
        return 1
    fi

    [ "$DEBUG" = "1" ] && echo "empty_vol OST0000: $empty_vol MB, HW: $mb_h_threshold MB"

	$LFS setstripe -c 2 -i 0 $LFS_SS_SZ_OPT 1m $RH_ROOT || echo "error setting stripe_count=2"

	#create test tree of archived files (2M each=1MB/ost) until we reach high threshold
	((count=$mb_h_threshold - $empty_vol + 1))
	for i in `seq $empty_vol $mb_h_threshold`; do
		dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=2  >/dev/null 2>/dev/null || error "writing $RH_ROOT/file.$i"

		if (( $is_lhsm != 0 )); then
			flush_data
			$LFS hsm_archive $RH_ROOT/file.$i
		fi
	done
	if (( $is_lhsm != 0 )); then
		wait_done 60 || error "Copy timeout"
	fi

	if (( $is_hsmlite != 0 )); then
		$RH -f $RBH_CFG_DIR/$config_file --scan $SYNC_OPT -l DEBUG  -L rh_migr.log || error "executing $CMD --sync"
    fi

	# wait for df sync
    wait_stable_df

	if (( $is_lhsm != 0 )); then
		arch_count=`$LFS hsm_state $RH_ROOT/file.* | grep "exists archived" | wc -l`
		(( $arch_count == $count )) || error "File count $count != archived count $arch_count"
	fi

    # sometimes there are orphan objects on some OSTs which may make the
    # OST0001 a little more filled with data, and make the test fail.
    # get the fillest OST:
    idx=$(lfs df "$RH_ROOT" | grep OST00 | sort -k 3nr | head -n 1 | cut -d ':' -f 2 | tr -d ']')
    [ "$DEBUG" = "1" ] && lfs df "$RH_ROOT" | grep OST00
    [ "$DEBUG" = "1" ] && echo "=> MAX OST is OST#$idx"
    # 0 or 1 expected
    [[ $idx == "0" ]] || [[ $idx == "1" ]] || error "fullest OST should be 0 or 1 (actual: $idx)"

	full_vol=`$LFS df  $RH_ROOT | grep OST000$idx | awk '{print $3}'`
	full_vol=$(($full_vol/1024))
	delta=$(($full_vol-$empty_vol))
	echo "OST#$idx usage increased of $delta MB (total usage = $full_vol MB)"
	((need_purge=$full_vol-$mb_l_threshold))
	echo "Need to purge $need_purge MB on OST#$idx"

	# scan
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log
	check_db_error rh_chglogs.log

	$REPORT -f $RBH_CFG_DIR/$config_file -i

	# apply purge trigger
	$RH -f $RBH_CFG_DIR/$config_file --run=purge --once -l DEBUG -L rh_purge.log || error "applying purge policy"

	grep summary rh_purge.log || error "No purge was done"
    [ "$DEBUG" = "1" ] && cat rh_purge.log

    # Retrieve the size purged
    # "2015/02/18 12:09:03 [5536/4] purge | Policy run summary: time=01s; target=OST#0; 42 successful actions (42.00/sec); volume: 84.00 MB (84.00 MB/sec); 0 entries skipped; 0 errors."
    purged_total=`grep summary rh_purge.log | grep "OST#$idx;" | awk '{print $(NF-8)}' | sed -e "s/\.[0-9]\+//g"`

    [ "$DEBUG" = "1" ] && echo "total_purged=$purged_total"

	# checks
    (( $purged_total > $need_purge )) || error ": invalid amount of data purged ($purged_total <= $need_purge)"
    (( $purged_total <= 2*($need_purge + 1) )) || error ": invalid amount of data purged ($purged_total < 2*($need_purge + 1)"

    # Check that RH knows all OST are now below the high threshold.
    grep "Top OSTs are all under high threshold" rh_purge.log || error "An OST is still above high threshold"

    # sync df values before checking df return
    wait_stable_df

	full_vol1=`$LFS df $RH_ROOT | grep OST0001 | awk '{print $3}'`
	full_vol1=$(($full_vol1/1024))
    # 1-idx => 0 or 1
    altidx=$((1-$idx))
	purge_ost1=`grep summary rh_purge.log | grep "OST#$altidx" | wc -l`

	if (($full_vol1 > $mb_h_threshold )); then
		error ": OST#$altidx is not expected to exceed high threshold!"
	elif (($purge_ost1 != 0)); then
		error ": no purge expected on OST#$altidx"
	else
		echo "OK: no purge on OST#$altidx (usage=$full_vol1 MB)"
	fi

	# restore default striping
	$LFS setstripe -c 2 -i -1 $RH_ROOT
}

function test_ost_order
{
	config_file=$1
	policy_str="$2"
	clean_logs

    if [ -n "$POSIX_MODE" ]; then
		echo "No OST support for POSIX mode"
        set_skipped
        return 1
    fi

    # reset df values
    wait_stable_df

    # nb OSTs?
    nbost=`$LFS df $RH_ROOT | grep OST | wc -l`
    maxidx=$((nbost -1))

    # get low watermark = max current OST usage
    local min_kb=0
    for i in $(seq 0 $maxidx); do
    	empty_vol=`$LFS df $RH_ROOT | grep OST000$i | awk '{print $3}'`
        (( $empty_vol > $min_kb )) && min_kb=$empty_vol
    done

    export ost_low_vol="${min_kb}KB"
    local trig_kb=$(($min_kb + 1024 )) # low thresh. +1MB
    export ost_high_vol="${trig_kb}KB"

    [ "$DEBUG" = "1" ] && $LFS df $RH_ROOT
    echo "setting low threshold = $ost_low_vol, high_threshold = $ost_high_vol"

    # create nothing on OST0000 (should not be purged)
    # ensure OST1 usage is trig_kb + 1M
    # ensure OST2 usage is trig_kb + 2M
    # etc...
    for i in $(seq 1 $maxidx); do
        vol=`$LFS df $RH_ROOT | grep OST000$i | awk '{print $3}'`
        nbkb=$(($trig_kb + 1024*$i - $vol))
        nbmb=$(($nbkb/1024+1))
        for f in $(seq 1 $nbmb); do
            $LFS setstripe -c 1 -i $i $RH_ROOT/test_ost_order.ost_$i.$f || error "lfs setstripe"
            dd if=/dev/zero of=$RH_ROOT/test_ost_order.ost_$i.$f bs=1M count=$nbmb || error "dd"
        done
    done

    wait_stable_df

    # check thresholds only, then purge
    for opt in "--check-thresholds=purge" "--run=purge"; do
        :> rh_purge.log
        $RH -f $RBH_CFG_DIR/$config_file $opt --once -l DEBUG -L rh_purge.log || error "command $opt error"
        [ "$DEBUG" = "1" ] && cat rh_purge.log

        # OSTs != 0 should be stated from the higher index to the lower
        lastline=0
        for i in $(seq 1 $maxidx); do
            grep "High threshold reached on OST #$i" rh_purge.log || error "OST #$i should be reported over high threshold"
            line=$(grep -n "High threshold reached on OST #$i" rh_purge.log | cut -d ':' -f 1)
            if (( $lastline > 0 && $line > $lastline )); then
                error "OST #$i: a lower OST idx has been reported in a previous line $lastline"
            else
                last_line=$line
            fi
        done

        # OST0 should not be reported
        grep "High threshold reached on OST #0" rh_purge.log && error "OST #0 should not be reported over threshold"
    done

}


function test_trigger_check
{
	config_file=$1
	max_count=$2
	max_vol_mb=$3
	policy_str="$4"
	target_count=$5
	target_fs_vol=$6
	target_user_vol=$7
	max_user_vol=$8
        target_user_count=$9

	clean_logs

    wait_stable_df

	if (( $is_hsmlite != 0 )); then
        # this mode may create an extra inode in filesystem: initial scan
        # to take it into account
		$RH -f $RBH_CFG_DIR/$config_file --scan --once -l MAJOR -L rh_scan.log || error "executing $CMD --scan"
		check_db_error rh_scan.log
    fi

	# triggers to be checked
	# - inode count > max_count
	# - fs volume	> max_vol
	# - root quota  > user_quota

	# initial inode count
	empty_count=`df -i $RH_ROOT/ | xargs | awk '{print $(NF-3)}'`
    empty_count_user=0

#	((file_count=$max_count-$empty_count))
	file_count=$max_count

	# compute file size to exceed max vol and user quota
	empty_vol=`df -k $RH_ROOT  | xargs | awk '{print $(NF-3)}'`
	((empty_vol=$empty_vol/1024))

	if (( $empty_vol < $max_vol_mb )); then
		((missing_mb=$max_vol_mb-$empty_vol))
	else
		missing_mb=0
	fi

	if (($missing_mb < $max_user_vol )); then
		missing_mb=$max_user_vol
	fi

	# file_size = missing_mb/file_count + 1
	((file_size=$missing_mb/$file_count + 1 ))

	echo "$file_count files missing, $file_size MB each"

	#create test tree of archived files (file_size MB each)
	for i in `seq 1 $file_count`; do
		dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=$file_size  >/dev/null 2>/dev/null || error "writing $RH_ROOT/file.$i"

		if (( $is_lhsm != 0 )); then
			flush_data
			$LFS hsm_archive $RH_ROOT/file.$i
		fi
	done

	if (( $is_lhsm != 0 )); then
		wait_done 60 || error "Copy timeout"
	fi

	# wait for df sync
    wait_stable_df

	if (( $is_hsmlite != 0 )); then
        # scan and sync
		$RH -f $RBH_CFG_DIR/$config_file --scan $SYNC_OPT -l DEBUG  -L rh_migr.log || error "executing $CMD --sync"
		check_db_error rh_migr.log
    else
	  # scan
  	  $RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log
  		check_db_error rh_chglogs.log
    fi

	# check purge triggers
	$RH -f $RBH_CFG_DIR/$config_file --check-thresholds=purge --once -l FULL -L rh_purge.log

	((expect_count=$empty_count+$file_count-$target_count))
	((expect_vol_fs=$empty_vol+$file_count*$file_size-$target_fs_vol))
	((expect_vol_user=$file_count*$file_size-$target_user_vol))
	((expect_count_user=$file_count+$empty_count_user-$target_user_count))

	echo "over trigger limits: $expect_count entries, $expect_vol_fs MB, $expect_vol_user MB for user root, $expect_count_user entries for user root"

	nb_release=`grep "$REL_STR" rh_purge.log | wc -l`

	count_trig=`grep " entries must be processed in Filesystem" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`
	[ -n "$count_trig" ] || count_trig=0

	vol_fs_trig=`grep " blocks (x512) must be processed on Filesystem" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`
	((vol_fs_trig_mb=$vol_fs_trig/2048)) # /2048 == *512/1024/1024

	vol_user_trig=`grep " blocks (x512) must be processed for user" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`
	((vol_user_trig_mb=$vol_user_trig/2048)) # /2048 == *512/1024/1024

	cnt_user_trig=`grep " files to be processed for user" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`
	[ -n "$cnt_user_trig" ] || cnt_user_trig=0

	echo "triggers reported: $count_trig entries (global), $cnt_user_trig entries (user), $vol_fs_trig_mb MB (global), $vol_user_trig_mb MB (user)"

	# check then was no actual purge
	if (($nb_release > 0)); then
		error ": $nb_release files released, no purge expected"
	elif (( $count_trig != $expect_count )); then
		error ": trigger reported $count_trig files over threshold, $expect_count expected"
	elif (( $vol_fs_trig_mb != $expect_vol_fs )); then
		error ": trigger reported $vol_fs_trig_mb MB over threshold, $expect_vol_fs expected"
	elif (( $vol_user_trig_mb != $expect_vol_user )); then
		error ": trigger reported $vol_user_trig_mb MB over threshold, $expect_vol_user expected"
        elif ((  $cnt_user_trig != $expect_count_user )); then
                error ": trigger reported $cnt_user_trig files over threshold, $expect_count_user expected"

	else
		echo "OK: all checks successful"
	fi
}

function check_released
{
	if (($is_lhsm != 0)); then
		$LFS hsm_state $1 | grep released || return 1
    elif (($shook != 0 )); then
        # check that nb blocks is 0
        bl=`stat -c "%b" $1`
        [ "$DEBUG" = "1" ] && echo "$1: $bl blocks"
        [[ -n $bl ]] && (( $bl == 0 )) || return 1
        # check that shook_state is "released"
        st=`getfattr -n security.shook_state $1 --only-values 2>/dev/null`
        [ "$DEBUG" = "1" ] && echo "$1: status $st"
        [[ "x$st" = "xreleased" ]] || return 1
	else
		[ -f $1 ] && return 1
	fi
	return 0
}

function wait_run_count
{
    local log=$1
    local cnt=$2

    # wait for end of run
    while (( $(grep "End of current pass" $log | wc -l) < $cnt )); do
        echo "waiting end of pass..."
        sleep 1
        continue
    done
}

function test_periodic_trigger
{
    config_file=$1
    sleep_time=$2
    policy_str=$3

    clean_logs

    echo "1-Populating filesystem..."
    # create 3 files of each type
    # (*.1, *.2, *.3, *.4)
    for i in `seq 1 4`; do
        dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "$? writing $RH_ROOT/file.$i"
        dd if=/dev/zero of=$RH_ROOT/foo.$i bs=1M count=1 >/dev/null 2>/dev/null || error "$? writing $RH_ROOT/foo.$i"
        dd if=/dev/zero of=$RH_ROOT/bar.$i bs=1M count=1 >/dev/null 2>/dev/null || error "$? writing $RH_ROOT/bar.$i"

        flush_data
        if (( $is_lhsm != 0 )); then
            $LFS hsm_archive $RH_ROOT/file.$i $RH_ROOT/foo.$i $RH_ROOT/bar.$i
        fi
    done

    if (( $is_lhsm != 0 )); then
        wait_done 20 || error "Copy timeout"
    fi

    # ensure their access time is all the same
    touch -a $RH_ROOT/file.{1..4}
    touch -a $RH_ROOT/foo.{1..4}
    touch -a $RH_ROOT/bar.{1..4}
    t0=`date +%s`

    # scan
    echo "2-Populating robinhood database (scan)..."
    if (( $is_hsmlite != 0 )); then
        # scan and sync
        $RH -f $RBH_CFG_DIR/$config_file --scan $SYNC_OPT -l DEBUG \
            -L rh_migr.log 2>/dev/null || error "executing $CMD --sync"
          check_db_error rh_migr.log
    else
        $RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log \
            2>/dev/null || error "executing $CMD --scan"
          check_db_error rh_scan.log
    fi

    # make sure files *.1 are old enough
    sleep 1

    # start periodic trigger in background
    echo "3.1-checking trigger for first policy run..."
    $RH -f $RBH_CFG_DIR/$config_file --run=purge -l DEBUG -L rh_purge.log \
        2>/dev/null &
    sleep 1

    t1=`date +%s`
    ((delta=$t1 - $t0))

    clean_caches # blocks is cached
    # it first must have purged *.1 files (not others)

    wait_run_count rh_purge.log 1

    # make sure the policy delay is not elapsed
    check_released "$RH_ROOT/file.1" || error "$RH_ROOT/file.1 should have been released after $delta s"
    check_released "$RH_ROOT/foo.1"  || error "$RH_ROOT/foo.1 should have been released after $delta s"
    check_released "$RH_ROOT/bar.1"  || error "$RH_ROOT/bar.1 should have been released after $delta s"

    if (( $delta <= 5 )); then
        check_released "$RH_ROOT/file.2" && error "$RH_ROOT/file.2 shouldn't have been released after $delta s"
        check_released "$RH_ROOT/foo.2"  && error "$RH_ROOT/foo.2 shouldn't have been released after $delta s"
        check_released "$RH_ROOT/bar.2"  && error "$RH_ROOT/bar.2 shouldn't have been released after $delta s"
    else
        echo "WARNING: more than 5s elapsed, check skipped"
    fi

    ((sleep_time=$sleep_time-$delta))
    sleep $(( $sleep_time + 2 ))
    # now, *.2 must have been purged
    echo "3.2-checking trigger for second policy run..."

    wait_run_count rh_purge.log 2

    t2=`date +%s`
    ((delta=$t2 - $t0))

    clean_caches # blocks is cached
    check_released "$RH_ROOT/file.2" || error "$RH_ROOT/file.2 should have been released after $delta s ($(date))"
    check_released "$RH_ROOT/foo.2" || error "$RH_ROOT/foo.2 should have been released after $delta s ($(date))"
    check_released "$RH_ROOT/bar.2" || error "$RH_ROOT/bar.2 should have been released after $delta s ($(date))"

    if (( $delta <= 10 )); then
        check_released "$RH_ROOT/file.3" && error "$RH_ROOT/file.3 shouldn't have been released after $delta s"
        check_released "$RH_ROOT/foo.3"  && error "$RH_ROOT/foo.3 shouldn't have been released after $delta s"
        check_released "$RH_ROOT/bar.3" && error "$RH_ROOT/bar.3 shouldn't have been released after $delta s"
    else
        echo "WARNING: more than 10s elapsed, check skipped"
    fi

    # wait 4 more secs (so another purge policy is applied)
    sleep 4
    # now, it's *.3
    # *.4 must be preserved
    echo "3.3-checking trigger for third policy..."

    wait_run_count rh_purge.log 3
    t3=`date +%s`
    ((delta=$t3 - $t0))

    clean_caches # blocks is cached
    check_released "$RH_ROOT/file.3" || error "$RH_ROOT/file.3 should have been released after $delta s"
    check_released "$RH_ROOT/foo.3"  || error "$RH_ROOT/foo.3 should have been released after $delta s"
    check_released "$RH_ROOT/bar.3"  || error "$RH_ROOT/bar.3 should have been released after $delta s"
    check_released "$RH_ROOT/file.4" && error "$RH_ROOT/file.4 shouldn't have been released after $delta s"
    check_released "$RH_ROOT/foo.4"  && error "$RH_ROOT/foo.4 shouldn't have been released after $delta s"
    check_released "$RH_ROOT/bar.4"  && error "$RH_ROOT/bar.4 shouldn't have been released after $delta s"

    # final check: 3x "Policy run summary: [...] 3 successful actions"
    nb_pass=$(grep -c "Checking trigger " rh_purge.log)
    # trig count should be (elapsed/period) +/- 1
    min_trig=$(($delta/5 - 1))
    max_trig=$(($delta/5 + 1))
    if (( $nb_pass < $min_trig )) || (( $nb_pass > $max_trig )); then
        error "unexpected trigger count $nb_pass (in $delta sec)"
    else
        echo "OK: triggered $nb_pass times in $delta sec"
    fi

    # terminate
    pkill -9 $PROC
}

function fileclass_test
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

    # initial scan
    $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error "running initial scan"
	check_db_error rh_chglogs.log

	# create test tree

	mkdir -p $RH_ROOT/dir_A # odd or A
	mkdir -p $RH_ROOT/dir_B # none
	mkdir -p $RH_ROOT/dir_C # none

	# classes are:
	# 1) even_and_B
	# 2) even_and_not_B
	# 3) odd_or_A
	# 4) none

	echo "data" > $RH_ROOT/dir_A/file.0 #2+3
	echo "data" > $RH_ROOT/dir_A/file.1 #3
	echo "data" > $RH_ROOT/dir_A/file.2 #2+3
	echo "data" > $RH_ROOT/dir_A/file.3 #3
	echo "data" > $RH_ROOT/dir_A/file.x #3
	echo "data" > $RH_ROOT/dir_A/file.y #3

	echo "data" > $RH_ROOT/dir_B/file.0 #1
	echo "data" > $RH_ROOT/dir_B/file.1 #3
	echo "data" > $RH_ROOT/dir_B/file.2 #1
	echo "data" > $RH_ROOT/dir_B/file.3 #3

	echo "data" > $RH_ROOT/dir_C/file.0 #2
	echo "data" > $RH_ROOT/dir_C/file.1 #3
	echo "data" > $RH_ROOT/dir_C/file.2 #2
	echo "data" > $RH_ROOT/dir_C/file.3 #3
	echo "data" > $RH_ROOT/dir_C/file.x #4
	echo "data" > $RH_ROOT/dir_C/file.y #4

	# policies => 2x 1), 4x 2), 8x 3), 2x 4)
	# matching => 2x 1), 2x 2) 2x 2+3) 9x3) 4x 4)

	echo "1bis-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	# read changelogs
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs..."
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
	check_db_error rh_chglogs.log

    # check classinfo report
    $REPORT -f $RBH_CFG_DIR/$config_file --class-info -q > rh_report.log || error "report error"
    [ "$DEBUG" = "1" ] && cat rh_report.log

    # fileclasses with 'report = no' are not expected in the report
    for f in  even_files odd_files in_dir_A in_dir_B; do
        egrep "[ +]$f[,+]" rh_report.log && error "non matchable fileclass '$f' should not be in report"
    done

    # check other fileclasses
    # find_valueInCSVreport $logFile $typeValues $countValues $colSearch
	# matching => 2x 1), 2x 2) 8x 3) 2x 2+3) 4x 4)
    expect=( 2 2 9 2 4 )
    i=0
    for f in  even_and_B even_and_not_B odd_or_A 'odd_or_A\+even_and_not_B' none; do
        val=$(egrep "[^+]$f[^+]" rh_report.log | cut -d ',' -f 2 | tr -d ' ')
        echo "$f: $val"
        [ "$val" = "${expect[$i]}" ] || error "$f: ${expect[$i]} expected, got $val"
        ((i++))
    done

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should not be migrated this time
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=all -l FULL -L rh_migr.log   || error ""

    [ "$DEBUG" = "1" ] && grep action_params rh_migr.log

	# count the number of file for each policy
	nb_pol1=`grep action_params rh_migr.log | grep class=even_and_B | wc -l`
	nb_pol2=`grep action_params rh_migr.log | grep class=even_and_not_B | wc -l`
	nb_pol3=`grep action_params rh_migr.log | grep class=odd_or_A | wc -l`
	nb_pol4=`grep action_params rh_migr.log | grep class=unmatched | wc -l`

	(( $nb_pol1 == 2 )) || error "********** TEST FAILED: wrong count of matching files for fileclass 'even_and_B': $nb_pol1"
	(( $nb_pol2 == 4 )) || error "********** TEST FAILED: wrong count of matching files for fileclass 'even_and_not_B': $nb_pol2"
	(( $nb_pol3 == 8 )) || error "********** TEST FAILED: wrong count of matching files for fileclass 'odd_or_A': $nb_pol3"
	(( $nb_pol4 == 2 )) || error "********** TEST FAILED: wrong count of matching files for fileclass 'unmatched': $nb_pol4"

    # test rbh-find -class option
    cfg=$RBH_CFG_DIR/$config_file
    check_find "" "-f $cfg -class even_and_B -lsclass" 2
    check_find "" "-f $cfg -b -class even_and_B -lsclass" 2
    check_find $RH_ROOT "-f $cfg -class even_and_B -lsclass" 2
    check_find $RH_ROOT "-f $cfg -b -class even_and_B -lsclass" 2
    check_find $RH_ROOT "-f $cfg -class even* -lsclass" 6
    check_find $RH_ROOT "-f $cfg -b -class even* -lsclass" 6
    check_find $RH_ROOT "-f $cfg -not -class even* -lsclass" 14
    check_find $RH_ROOT "-f $cfg -b -not -class even* -lsclass" 14
}

function test_info_collect
{
	config_file=$1
	sleep_time1=$2
	sleep_time2=$3
	policy_str="$4"

	clean_logs

	# test reading changelogs or scanning with strange names, etc...
	mkdir $RH_ROOT'/dir with blanks'
	mkdir $RH_ROOT'/dir with "quotes"'
	mkdir "$RH_ROOT/dir with 'quotes'"

	touch $RH_ROOT'/dir with blanks/file 1'
	touch $RH_ROOT'/dir with blanks/file with "double" quotes'
	touch $RH_ROOT'/dir with "quotes"/file with blanks'
	touch "$RH_ROOT/dir with 'quotes'/file with 1 quote: '"

	sleep $sleep_time1

	# read changelogs
	if (( $no_log )); then
		echo "1-Scanning..."
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log \
            --once 2>/dev/null || error "scan"
		nb_cr=0
	else
        [ "$DEBUG" = "1" ] && $LFS changelog lustre
		echo "1-Reading changelogs..."
		#$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l FULL -L rh_chglogs.log  \
            --once 2>/dev/null || error "readlog"
		nb_cr=4
	fi
	check_db_error rh_chglogs.log

	sleep $sleep_time2

	grep "DB query failed" rh_chglogs.log && error ": a DB query failed when reading changelogs"

	nb_create=`grep ChangeLog rh_chglogs.log | grep 01CREAT | wc -l`
	nb_close=`grep ChangeLog rh_chglogs.log | grep 11CLOSE | wc -l`
	nb_db_apply=`grep ': DB_APPLY' rh_chglogs.log | tail -1 | cut -d '|' -f 6 | cut -d ':' -f 2 |
                 cut -d ',' -f 1 | tr -d ' '`

    # (directories are always inserted since robinhood 2.4)
    # 4 file + 3 dirs -> 7 changelogs
    # (all close are suppressed)
    ((db_expect=7))
    close_expect=4

	if (( $no_log == 0 )); then
        if (( $nb_close != $close_expect )); then
            if [[ $LVERSION = 2.[01]* ]] ; then
                # CLOSE record is only expected since Lustre 2.2
                # for previous versions, just display a warning
                echo "warning: $nb_close close record (lustre version $LVERSION), $close_expect expected"
            elif [[ $LVERSION = 2.[234]* ]] ; then
                # CLOSE is expected from 2.2 to 2.4
                error ": unexpected number of close: $nb_close / $close_expect"
            else
                echo "warning: $nb_close close record (lustre version $LVERSION), $close_expect expected"
            fi
            return 1
        fi
    fi

	if (( $nb_create == $nb_cr && $nb_db_apply == $db_expect )); then
		echo "OK: $nb_cr files created, $db_expect database operations"
	else
		error ": unexpected number of operations: $nb_create files created/$nb_cr, $nb_db_apply database operations/$db_expect"
		return 1
	fi

	clean_logs

	echo "2-Scanning..."
	$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log \
        --once 2>/dev/null || error "scan"
	check_db_error rh_chglogs.log

	grep "DB query failed" rh_chglogs.log && error ": a DB query failed when scanning"
	nb_db_apply=`grep ': DB_APPLY' rh_chglogs.log | tail -1 | cut -d '|' -f 6 | cut -d ':' -f 2 |
                 cut -d ',' -f 1 | tr -d ' '`

	# 7 db operations expected (1 for each file and dir)
	if (( $nb_db_apply == $db_expect )); then
		echo "OK: $db_expect database operations"
	else
#		grep ENTRIES rh_chglogs.log
		error ": unexpected number of operations: $nb_db_apply database operations/$db_expect"
	fi
}


function readlog_chk
{
	local config_file=$1

	echo "Reading changelogs..."
	$RH -f $RBH_CFG_DIR/$config_file --readlog -l FULL -L rh_chglogs.log \
        --once 2>/dev/null || error "reading logs"
	grep "DB query failed" rh_chglogs.log &&
        error ": a DB query failed:" \
              "`grep 'DB query failed' rh_chglogs.log | tail -1`"
	clean_logs
}

function scan_chk
{
	local config_file=$1

	echo "Scanning..."
        $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log \
            --once 2>/dev/null || error "scanning filesystem"
	grep "DB query failed" rh_chglogs.log &&
        error ": a DB query failed:" \
            "`grep 'DB query failed' rh_chglogs.log | tail -1`"
	clean_logs
}

function diff_chk
{
    local config_file=$1

    echo "Scanning with rbh-diff..."
    $DIFF -f $RBH_CFG_DIR/$config_file --apply=db -l DEBUG > rh_chglogs.log  2>&1 || error "scanning filesystem"
    grep "DB query failed" rh_chglogs.log && error ": a DB query failed: `grep 'DB query failed' rh_chglogs.log | tail -1`"
    clean_logs
}

function check_fcount
{
    local nb=$1

    nbfile=$($REPORT -f $RBH_CFG_DIR/$config_file -icq | grep file | awk -F ',' '{print $2}' | tr -d ' ')
    [[ -z $nbfile ]] && nbfile=0
    [ "$DEBUG" = "1" ] && echo "nb_files=$nbfile"

    [[ $nbfile != $nb ]] && error "Unexpected file count: $nbfile/$nb"
}

function empty_fs
{
    if [[ -n "$RH_ROOT" ]]; then
        find "$RH_ROOT" -mindepth 1 -delete 2>/dev/null
    fi
}

function test_info_collect2
{
	local config_file=$1
	local flavor=$2
	local policy_str="$3"

    local fcount=2000

	clean_logs

	if (($no_log != 0 && $flavor != 1 )); then
		echo "Changelogs not supported on this config: skipped"
		set_skipped
		return 1
	fi

	# create 5k entries
    echo "Creating $fcount files..."
	$RBH_TESTS_DIR/fill_fs.sh $RH_ROOT $fcount >/dev/null

	# flavor 1: scan only x3
	# flavor 2: mixed (readlog/scan/readlog/scan)
	# flavor 3: mixed (readlog/readlog/scan/scan)
	# flavor 4: mixed (scan/scan/readlog/readlog)
	# flavor 5: diff --apply=db x2

	if (( $flavor == 1 )); then
		scan_chk $config_file
        check_fcount $fcount
		scan_chk $config_file
        check_fcount $fcount
        empty_fs
        # sleep 1 to ensure md_update >= 1s
        sleep 1
		scan_chk $config_file
        check_fcount 0
	elif (( $flavor == 2 )); then
		readlog_chk $config_file
        check_fcount $fcount
		scan_chk    $config_file
        check_fcount $fcount
		# touch entries before reading log
		$RBH_TESTS_DIR/fill_fs.sh $RH_ROOT $fcount >/dev/null
		readlog_chk $config_file
        check_fcount $fcount
        empty_fs
        # sleep 1 to ensure md_update >= 1s
        sleep 1
		scan_chk    $config_file
        check_fcount 0
	elif (( $flavor == 3 )); then
		readlog_chk $config_file
        check_fcount $fcount
		# touch entries before reading log again
		$RBH_TESTS_DIR/fill_fs.sh $RH_ROOT $fcount >/dev/null
		readlog_chk $config_file
        check_fcount $fcount
		scan_chk    $config_file
        check_fcount $fcount
        empty_fs
        # sleep 1 to ensure md_update >= 1s
        sleep 1
		scan_chk    $config_file
        check_fcount 0
	elif (( $flavor == 4 )); then
		scan_chk    $config_file
        check_fcount $fcount
		scan_chk    $config_file
        check_fcount $fcount
		readlog_chk $config_file
        check_fcount $fcount
        empty_fs
		readlog_chk $config_file
        check_fcount 0
	elif (( $flavor == 5 )); then
        diff_chk $config_file
        check_fcount $fcount
        empty_fs
        # sleep 1 to ensure md_update >= 1s
        sleep 1
        diff_chk $config_file
        check_fcount 0
	else
		error "Unexpexted test flavor '$flavor'"
	fi

}

function get_db_info
{
    local config_file=$1
    local field=$2
    local entry=$3

    $REPORT -f $RBH_CFG_DIR/$config_file -e $entry -c | egrep -E "^$field," | cut -d ',' -f 2 | sed -e 's/^ //g'
}

function test_root_changelog
{
	config_file=$1
	clean_logs

	if (( $no_log )); then
    	echo "Changelogs not supported on this config: skipped"
		set_skipped
		return 1
    fi

    # create a directory and a file
    local d=$RH_ROOT/subdir
    local f=$RH_ROOT/subdir/file
    mkdir $d || error "creating directory $d"
    id1=$(get_id $d)
    touch $f || error "creating file $f"
    id2=$(get_id $f)
    idr=$(get_id $RH_ROOT/.)

    [ "$DEBUG" = "1" ] && echo -e "$RH_ROOT: $idr\n$d: $id1\n$f: $id2"

    # read the changelog
    readlog_chk $config_file

    # check the id, path and parent for $RH_ROOT, $d and $f
    idrb=$(get_db_info $config_file id $idr | tr -d '[]')
    [ "$idr" = "$idrb" ] || error "id doesn't match: $idr != $idrb"
    pathr=$(get_db_info $config_file path $idr)
    # path must be empty or match $RH_ROOT
    [ "$pathr" = "" ] || [ "$pathr" = "$RH_ROOT" ] || error "path doesn't match: $RH_ROOT != $pathr"

    # name and parent are supposed to be empty for ROOT
    nr=$(get_db_info $config_file name $idr)
    [ "$nr" = "" ] || error "name for $RH_ROOT is not empty: '$nr'"
    pr=$(get_db_info $config_file parent_id $idr)
    [ "$pr" = "" ] || error "parent_id for $RH_ROOT is not empty: '$pr'"

    id1b=$(get_db_info $config_file id $id1 | tr -d '[]')
    [ "$id1" = "$id1b" ] || error "id doesn't match: '$id1' != '$id1b'"
    path1=$(get_db_info $config_file path $id1)
    [ "$DEBUG" = "1" ] && echo "$d: path=$path1"
    [ "$path1" = "$d" ] || error "path doesn't match: $d != $path1"
    parent1=$(get_db_info $config_file parent_id $id1 | tr -d '[]')
    [ "$DEBUG" = "1" ] && echo "$d: parent=$parent1"
    [ "$parent1" = "$idr" ] || error "parent doesn't match: $idr != $parent1"

    id2b=$(get_db_info $config_file id $id2 | tr -d '[]')
    [ "$id2" = "$id2b" ] || error "id doesn't match: '$id2' != '$id2b'"
    path2=$(get_db_info $config_file path $id2)
    [ "$DEBUG" = "1" ] && echo "$f: path=$path2"
    [ "$path2" = "$f" ] || error "path doesn't match: $f != $path2"
    parent2=$(get_db_info $config_file parent_id $id2 | tr -d '[]')
    [ "$DEBUG" = "1" ] && echo "$f: parent=$parent2"
    [ "$parent2" = "$id1" ] || error "parent doesn't match: $id1 != $parent2"

    # generate an event on $RH_ROOT and do the checks again
    touch $RH_ROOT/.
    sleep 1
    # read the changelog
    readlog_chk $config_file

    # check the id, path and parent for $RH_ROOT, $d and $f
    idrb=$(get_db_info $config_file id $idr | tr -d '[]')
    [ "$idr" = "$idrb" ] || error "id doesn't match: $idr != $idrb"
    pathr=$(get_db_info $config_file path $idr)
    # path must be empty or match $RH_ROOT
    [ "$pathr" = "" ] || [ "$pathr" = "$RH_ROOT" ] || error "path doesn't match: $RH_ROOT != $pathr"

    # name and parent are supposed to be empty for ROOT
    nr=$(get_db_info $config_file name $idr)
    [ "$nr" = "" ] || error "name for $RH_ROOT is not empty: '$nr'"
    pr=$(get_db_info $config_file parent_id $idr)
    [ "$pr" = "" ] || error "parent_id for $RH_ROOT is not empty: '$pr'"

    id1b=$(get_db_info $config_file id $id1 | tr -d '[]')
    [ "$id1" = "$id1b" ] || error "id doesn't match: '$id1' != '$id1b'"
    path1=$(get_db_info $config_file path $id1)
    [ "$DEBUG" = "1" ] && echo "$d: path=$path1"
    [ "$path1" = "$d" ] || error "path doesn't match: $d != $path1"
    parent1=$(get_db_info $config_file parent_id $id1 | tr -d '[]')
    [ "$DEBUG" = "1" ] && echo "$d: parent=$parent1"
    [ "$parent1" = "$idr" ] || error "parent doesn't match: $idr != $parent1"

    id2b=$(get_db_info $config_file id $id2 | tr -d '[]')
    [ "$id2" = "$id2b" ] || error "id doesn't match: '$id2' != '$id2b'"
    path2=$(get_db_info $config_file path $id2)
    [ "$DEBUG" = "1" ] && echo "$f: path=$path2"
    [ "$path2" = "$f" ] || error "path doesn't match: $f != $path2"
    parent2=$(get_db_info $config_file parent_id $id2 | tr -d '[]')
    [ "$DEBUG" = "1" ] && echo "$f: parent=$parent2"
    [ "$parent2" = "$id1" ] || error "parent doesn't match: $id1 != $parent2"
}

function partial_paths
{
	config_file=$1
	clean_logs

    # create a tree
    mkdir -p $RH_ROOT/dir1/dir2
    mkdir -p $RH_ROOT/dir3
    touch $RH_ROOT/file1
    touch $RH_ROOT/dir1/file2
    touch $RH_ROOT/dir1/dir2/file3
    touch $RH_ROOT/dir3/file4

    # initial scan
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing initial scan"
    check_db_error rh_scan.log

    # remove a path component from the DB
    id=$(get_id $RH_ROOT/dir1/dir2)
    [ -z $id ] && error "could not get id"
    # FIXME only for Lustre 2.x
    mysql $RH_DB -e "DELETE FROM NAMES WHERE id='$id'" || error "DELETE request"

	if (( $is_hsmlite + $is_lhsm > 0 )); then
        # check how a child entry is archived
        $RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log
        check_db_error rh_migr.log
        if (( $is_hsmlite > 0 )); then
            name=$(find $BKROOT -type f -name "file3__*")
            cnt=$(echo $name | wc -w)
            (( $cnt == 1 )) || error "1 file expected to match file 3 in backend, $cnt found"
            echo "file3 archived as $name"
	    else
		    wait_done 60
        fi
    fi

    # check what --dump reports
    f3=$($REPORT -f $RBH_CFG_DIR/$config_file --dump --csv -q | grep "file3" | awk '{print $(NF)}')
    echo "file3 reported with path $f3"
    [[ $f3 = /* ]] && [[ $f3 != $RH_ROOT/dir1/dir2/file3 ]] && error "$f3 : invalid fullpath"

    # check filter path behavior
    # should report at least file2 (and optionnally file3 : must check its path is valid)
    f2=$($REPORT -f $RBH_CFG_DIR/$config_file --dump --csv -q -P "$RH_ROOT/dir1" | grep file2 | awk '{print $(NF)}')
    [[ -n $f2 ]] && echo "file2 reported with path $f2"
    [[ $f2 != $RH_ROOT/dir1/file2 ]] && error "wrong path reported for file2: $f2"

    f3=$($REPORT -f $RBH_CFG_DIR/$config_file --dump --csv -q -P "$RH_ROOT/dir1" | grep file3 | awk '{print $(NF)}')
    [[ -n $f3 ]] && echo "file3 reported with path $f3"
    [[ $f3 = /* ]] && [[ $f3 != $RH_ROOT/dir1/dir2/file3 ]] && error "$f3 : invalid fullpath"

    f3=$($REPORT -f $RBH_CFG_DIR/$config_file --dump --csv -q -P "$RH_ROOT/dir1/dir2" | grep file)
    [[ -n $f3 ]] && echo "file3 reported with path $f3"
    [[ $f3 = /* ]] && [[ $f3 != $RH_ROOT/dir1/dir2/file3 ]] && error "$f3 : invalid fullpath"

    # check find behavior
    # find cannot go into dir2
    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT/dir1 | grep dir2 && echo "$RH_ROOT/dir1/dir2 reported?!"
    # starting from dir2 fid, it can list file3 in it
    f3=$($FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT/dir1/dir2 | grep file3)
    echo "find: $f3"
    [[ $f3 = $RH_ROOT/dir1/dir2/file3 ]] || error "$f3 : invalid fullpath"

    # like find, should count file3
    fc=$($DU -d -f $RBH_CFG_DIR/$config_file $RH_ROOT/dir1/dir2 | grep "file count" | cut -d ':' -f 2 | cut -d ',' -f 1)
    [[ $fc = 1 ]] || error "expected filecount in $RH_ROOT/dir1/dir2: 1 (got $fc)"

    # check -e report
    # dir2 should be in DB, even with no path
    $REPORT -f $RBH_CFG_DIR/$config_file --csv -e "$RH_ROOT/dir1/dir2" | grep "md updt" || error "$RH_ROOT/dir1/dir2 should have a DB entry"

    $REPORT -f $RBH_CFG_DIR/$config_file --csv -e "$RH_ROOT/dir1/dir2/file3"  > report.log || error "report error for $RH_ROOT/dir1/dir2/file3"
    grep "md updt" report.log || error "$RH_ROOT/dir1/dir2/file3 should have a DB entry"
    f3=$(egrep "^path," report.log)
    [[ $f3 = /* ]] && [[ $f3 != $RH_ROOT/dir1/dir2/file3 ]] && error "$f3 : invalid fullpath"
	if (( $is_hsmlite > 0 )); then
        b3=$(grep "backend_path," report.log | cut -d ',' -f 2)
        # b3 should be in 'dir2' or in '__unknown_path'
        echo $b3 | egrep "dir1/dir2|unknown_path" || error "unexpected backend path $b3"
    fi

    # check what rm does (+undelete)
	if (( $no_log==0 )); then
	   $LFS changelog_clear lustre-MDT0000 cl1 0

        rm -f $RH_ROOT/dir1/dir2/file3
        readlog_chk $config_file

	    if (( $is_lhsm + $is_hsmlite > 0 )); then
            $REPORT -f $RBH_CFG_DIR/$config_file -Rcq > report.log
            [ "$DEBUG" = "1" ] && cat report.log
            nb=$(cat report.log | grep file3 | wc -l)
            (($nb == 1)) || error "file3 not reported in remove-pending list"
            f3=$(cat report.log | grep file3 | awk '{print $(NF)}')
            [[ $f3 = /* ]] && [[ $f3 != $RH_ROOT/dir1/dir2/file3 ]] && error "$f3 : invalid fullpath"
        fi

        if (( $is_hsmlite + $is_lhsm > 0 )); then
            $UNDELETE -f $RBH_CFG_DIR/$config_file -R '*/file3' -l DEBUG || error "undeleting file3"
            find $RH_ROOT -name "file3" -ls | tee report.log
            (( $(wc -l report.log | awk '{print $1}') == 1 )) || error "file3 not restored"
        fi
	fi

    # TODO check disaster recovery

    rm -f report.log
}

function test_mnt_point
{
	config_file=$1
	clean_logs

    export fs_path=$RH_ROOT/subdir # retrieved from env when parsing config file

    local dir_rel="dir1 dir2"
    local file_rel="dir1/file.1 dir1/file.2 dir2/file.3 file.4"

    for d in $dir_rel; do
        mkdir -p $fs_path/$d || error mkdir
    done
    for f in $file_rel; do
        touch $fs_path/$f || error touch
    done

    # scan the filesystem
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing initial scan"
    check_db_error rh_scan.log

    # check that rbh-find output is correct (2 methods)
    for opt in "-nobulk $fs_path" "$fs_path" "-nobulk" ""; do
        echo "checking output for rbh-find $opt..."
        $FIND -f $RBH_CFG_DIR/$config_file $opt > rh_report.log
        for e in $dir_rel $file_rel; do
            egrep -E "^$fs_path/$e$" rh_report.log || error "$e not found in rbh-find output"
        done
    done

    # check that rbh-report output is correct
    $REPORT -f $RBH_CFG_DIR/$config_file -q --dump | awk '{print $(NF)}'> rh_report.log
    [ "$DEBUG" = "1" ] && cat rh_report.log
    for e in $dir_rel $file_rel; do
        egrep -E "^$fs_path/$e$" rh_report.log || error "$e not found in report output"
    done

    # backup: check that backend path is correct
    if (( $is_hsmlite > 0 )); then
        # wait atime > 1s
        sleep 1
        $RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log
        check_db_error rh_migr.log

        for e in $file_rel; do
            ls -d $BKROOT/${e}__* || error "$BKROOT/$e* not found in backend"
        done
    fi
}

function uid_gid_as_numbers
{
	config_file=$1

	clean_logs
    $CFG_SCRIPT empty_db $RH_DB > /dev/null

    # create the following files with different owners/groups:
    #
    #   -rw-r--r-- 1       0      0     10 Jun  7 13:40 file1
    #   -rw-r--r-- 1      12     16    100 Jun  7 13:40 file2
    #   -rw-r--r-- 1 7856568 345654   1000 Jun  7 13:40 file3
    #   -rw-r--r-- 1       0 645767  10000 Jun  7 13:40 file4
    #   -rw-r--r-- 1 3476576      0 100000 Jun  7 13:40 file5

    echo "1-Creating files..."
    rm -f $RH_ROOT/file[1-4]
    dd if=/dev/zero of=$RH_ROOT/file1 bs=10 count=1 >/dev/null 2>/dev/null || error "writing file"
    dd if=/dev/zero of=$RH_ROOT/file2 bs=100 count=1 >/dev/null 2>/dev/null || error "writing file"
    dd if=/dev/zero of=$RH_ROOT/file3 bs=1000 count=1 >/dev/null 2>/dev/null || error "writing file"
    dd if=/dev/zero of=$RH_ROOT/file4 bs=10000 count=1 >/dev/null 2>/dev/null || error "writing file"
    dd if=/dev/zero of=$RH_ROOT/file5 bs=100000 count=1 >/dev/null 2>/dev/null || error "writing file"

    chown 0:0 $RH_ROOT/file1
    chown 12:16 $RH_ROOT/file2
    chown 7856568:345654 $RH_ROOT/file3
    chown 0:645767 $RH_ROOT/file4
    chown 3476576:0 $RH_ROOT/file5

    echo "2-Initial scan of empty filesystem"
    $RH -f $RBH_CFG_DIR/$config_file --scan -l FULL -L rh_scan.log  --once || error ""

    echo "3-Check report"
	$REPORT -f $RBH_CFG_DIR/$config_file -D --csv > rh_report.log
    egrep --quiet "\s0,\s+0,.*/file1" rh_report.log || error "bad for file1"
    egrep --quiet "\s12,\s+16,.*/file2" rh_report.log || error "bad for file2"
    egrep --quiet "\s7856568,\s+345654,.*/file3" rh_report.log || error "bad for file3"
    egrep --quiet "\s0,\s+645767,.*/file4" rh_report.log || error "bad for file4"
    egrep --quiet "\s3476576,\s+0,.*/file5" rh_report.log || error "bad for file5"

    $REPORT -f $RBH_CFG_DIR/$config_file --top-user > rh_report.log
    # spc used (4th field) depends on filesystem preallocation algorithm: don't rely on it.
    grep --quiet -e "1,    3476576,   97.66 KB, [^,]*,[ ]* 1,   97.66 KB" rh_report.log || error "bad top user1"
    grep --quiet -e "2,          0,    9.78 KB, [^,]*,[ ]* 2,    4.89 KB" rh_report.log || error "bad top user3"
    grep --quiet -e "3,    7856568,       1000, [^,]*,[ ]* 1,       1000" rh_report.log || error "bad top user3"
    grep --quiet -e "4,         12,        100, [^,]*,[ ]* 1,        100" rh_report.log || error "bad top user3"

    $REPORT -f $RBH_CFG_DIR/$config_file --top-size > rh_report.log
    grep --quiet -e "1, [ ]* $RH_ROOT/file5,   97.66 KB,    3476576," rh_report.log || error "bad top size1"
    grep --quiet -e "5, [ ]* $RH_ROOT/file1,         10,          0," rh_report.log || error "bad top size2"

    $REPORT -f $RBH_CFG_DIR/$config_file --dump > rh_report.log
    grep --quiet "file,       1000,    7856568,     345654,.*/file3" rh_report.log || error "bad dump1"
    grep --quiet "Total: 5 entries, 111110 bytes" rh_report.log || error "bad dump2"

    $REPORT -f $RBH_CFG_DIR/$config_file --dump-user=root > rh_report.log
    grep --quiet "Total: 2 entries, 10010 bytes (9.78 KB)" rh_report.log || error "bad dump user root"

    $REPORT -f $RBH_CFG_DIR/$config_file --dump-user=0 > rh_report.log
    grep --quiet "Total: 2 entries, 10010 bytes (9.78 KB)" rh_report.log || error "bad dump user 0"

    $REPORT -f $RBH_CFG_DIR/$config_file --dump-user=7856568 > rh_report.log
    grep --quiet "Total: 1 entries, 1000 bytes" rh_report.log || error "bad dump user 7856568"

    $REPORT -f $RBH_CFG_DIR/$config_file --dump-group=root > rh_report.log
    grep --quiet "Total: 2 entries, 100010 bytes (97.67 KB)" rh_report.log || error "bad dump group root"

    $REPORT -f $RBH_CFG_DIR/$config_file --dump-group=0 > rh_report.log
    grep --quiet "Total: 2 entries, 100010 bytes (97.67 KB)" rh_report.log || error "bad dump group root"

    $REPORT -f $RBH_CFG_DIR/$config_file --dump-group=645767 > rh_report.log
    grep --quiet "Total: 1 entries, 10000 bytes" rh_report.log || error "bad dump group 645767"

    $REPORT -f $RBH_CFG_DIR/$config_file --fs-info > rh_report.log
    grep --quiet "Total: 5 entries, volume: 111110 bytes" rh_report.log || error "bad fs info"

    $REPORT -f $RBH_CFG_DIR/$config_file --user-info=root > rh_report.log
    grep --quiet "Total: 2 entries, volume: 10010 bytes" rh_report.log || error "bad info for user root"

    $REPORT -f $RBH_CFG_DIR/$config_file --user-info=0 > rh_report.log
    grep --quiet "Total: 2 entries, volume: 10010 bytes" rh_report.log || error "bad info for user 0"

    $REPORT -f $RBH_CFG_DIR/$config_file --user-info=7856568 > rh_report.log
    grep --quiet "Total: 1 entries, volume: 1000 bytes" rh_report.log || error "bad info for user 7856568"

    $REPORT -f $RBH_CFG_DIR/$config_file --group-info=0 > rh_report.log
    grep --quiet "Total: 2 entries, volume: 100010 bytes" rh_report.log || error "bad info for group 0"

    $REPORT -f $RBH_CFG_DIR/$config_file --group-info=root > rh_report.log
    grep --quiet "Total: 2 entries, volume: 100010 bytes" rh_report.log || error "bad info for group root"

    $REPORT -f $RBH_CFG_DIR/$config_file --group-info=645767 > rh_report.log
    grep --quiet "Total: 1 entries, volume: 10000 bytes" rh_report.log || error "bad info for group 645767"

    $REPORT -f $RBH_CFG_DIR/$config_file --entry-info=$RH_ROOT/file1 > rh_report.log
    grep --quiet "user           : 	0$" rh_report.log || error "bad user for entry file1"
    grep --quiet "group          : 	0$" rh_report.log || error "bad group for entry file1"

    $REPORT -f $RBH_CFG_DIR/$config_file --entry-info=$RH_ROOT/file3 > rh_report.log
    grep --quiet "user           : 	7856568$" rh_report.log || error "bad user for entry file3"
    grep --quiet "group          : 	345654$" rh_report.log || error "bad group for entry file3"

    $REPORT -f $RBH_CFG_DIR/$config_file --classinfo > rh_report.log
    wc -l < rh_report.log | grep --quiet "^8$" || error "bad classinfo report"

    $REPORT -f $RBH_CFG_DIR/$config_file --classinfo=uroot1 > rh_report.log
    grep --quiet "Total: 2 entries, volume: 10010 bytes" rh_report.log || error "bad info for class root1"

    $REPORT -f $RBH_CFG_DIR/$config_file --classinfo=uroot2 > rh_report.log
    grep --quiet "Total: 2 entries, volume: 10010 bytes" rh_report.log || error "bad info for class root2"

    $REPORT -f $RBH_CFG_DIR/$config_file --classinfo=u7856568 > rh_report.log
    grep --quiet "Total: 1 entries, volume: 1000 bytes" rh_report.log || error "bad info for class u7856568"

    $REPORT -f $RBH_CFG_DIR/$config_file --classinfo=g645767 > rh_report.log
    grep --quiet "Total: 1 entries, volume: 10000 bytes" rh_report.log || error "bad info for class g645767"

    $REPORT -f $RBH_CFG_DIR/$config_file --classinfo=groot > rh_report.log
    grep --quiet "Total: 2 entries, volume: 100010 bytes" rh_report.log || error "bad info for class groot"

    $REPORT -f $RBH_CFG_DIR/$config_file --classinfo=mix > rh_report.log
    grep --quiet "Total: 3 entries, volume: 101010 bytes" rh_report.log || error "bad info for class mix"

    echo "4-Check rbh-find"
    # rbh-find will also find the mount point which belong to root.
    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT -ls > find.out
    egrep --quiet "\s0\s+0\s.*/file1" find.out || error "bad for file1"
    egrep --quiet "\s12\s+16\s.*/file2" find.out || error "bad for file2"
    egrep --quiet "\s7856568\s+345654\s.*/file3" find.out || error "bad for file3"
    egrep --quiet "\s0\s+645767\s.*/file4" find.out || error "bad for file4"
    egrep --quiet "\s3476576\s+0\s.*/file5" find.out || error "bad for file5"

    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT -ls -user 12 > find.out
    wc -l < find.out | grep --quiet "^1$" || error "incorrect number of files found1"

    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT -ls -user 0 > find.out
    wc -l < find.out | grep --quiet "^3$" || error "incorrect number of files found2"

    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT -ls -not -user 7856568 > find.out
    wc -l < find.out | grep --quiet "^5$" || error "incorrect number of files found3"

    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT -ls -group 0 > find.out
    wc -l < find.out | grep --quiet "^3$" || error "incorrect number of files found4"

    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT -ls -group 645767 > find.out
    wc -l < find.out | grep --quiet "^1$" || error "incorrect number of files found5"

    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT -ls -not -group 345654 > find.out
    wc -l < find.out | grep --quiet "^5$" || error "incorrect number of files found6"

    echo "5-Check rbh-find with printf"
    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT -printf "%p:%g:%u\n" > find.out
    grep --quiet "$RH_ROOT:0:0" find.out
    grep --quiet "$RH_ROOT/file1:0:0" find.out || error "bad for file1"
    grep --quiet "$RH_ROOT/file2:16:12" find.out || error "bad for file2"
    grep --quiet "$RH_ROOT/file3:345654:7856568" find.out || error "bad for file3"
    grep --quiet "$RH_ROOT/file4:645767:0" find.out || error "bad for file4"
    grep --quiet "$RH_ROOT/file5:0:3476576" find.out || error "bad for file5"

    echo "6-Check rbh-du"
    # Each file has a precise size, so we know what the result in
    # bytes will be for any combination
    $DU -f $RBH_CFG_DIR/$config_file -t f -b $RH_ROOT | egrep --quiet "^111110\s" || error "bad sum1"
    $DU -f $RBH_CFG_DIR/$config_file -t f -b -u 0 $RH_ROOT | egrep  --quiet "^10010\s" || error "bad sum2"
    $DU -f $RBH_CFG_DIR/$config_file -t f -b -u 12 $RH_ROOT | egrep  --quiet "^100\s" || error "bad sum3"
    $DU -f $RBH_CFG_DIR/$config_file -t f -b -u 1234 $RH_ROOT | egrep  --quiet "^0\s" || error "bad sum4"
    $DU -f $RBH_CFG_DIR/$config_file -t f -b -g 0 $RH_ROOT | egrep  --quiet "^100010\s" || error "bad sum5"
    $DU -f $RBH_CFG_DIR/$config_file -t f -b -g 645767 $RH_ROOT | egrep  --quiet "^10000\s" || error "bad sum6"
    $DU -f $RBH_CFG_DIR/$config_file -t f -b -g 1234 $RH_ROOT | egrep  --quiet "^0\s" || error "bad sum7"
    $DU -f $RBH_CFG_DIR/$config_file -t f -b -u 0 -g 16 $RH_ROOT | egrep --quiet "^0\s" || error "bad sum8"
    $DU -f $RBH_CFG_DIR/$config_file -t f -b -u 0 -g 0 $RH_ROOT | egrep --quiet "^10\s" || error "bad sum9"
}

# Create a file and touch it to set atime/mtime. Check that crtime and
# ctime are properly set, using the search criteria of rbh-find, and
# display of rbh-report. Check that creation_time never changes while
# ctime does.
function posix_acmtime
{
    config_file=$1
    local cfg=$RBH_CFG_DIR/$config_file
    clean_logs

    local org_RBH_TEST_LAST_ACCESS_ONLY_ATIME=${RBH_TEST_LAST_ACCESS_ONLY_ATIME}
    export RBH_TEST_LAST_ACCESS_ONLY_ATIME=yes

    # create file
    echo "1-Creating file..."
    rm -f $RH_ROOT/file
    dd if=/dev/zero of=$RH_ROOT/file bs=10 count=1 >/dev/null 2>/dev/null || error "writing file"

    # Set a given atime and mtime. touch can't change ctime.
    touch -m -t 201004171230 $RH_ROOT/file
    touch -a -t 201004171300 $RH_ROOT/file
    stat $RH_ROOT/file | grep --quiet "Modify: 2010-04-17 12:30:00" || error "bad mtime"
    stat $RH_ROOT/file | grep --quiet "Access: 2010-04-17 13:00:00" || error "bad atime"

    echo "2-Initial scan of filesystem"
    $RH -f $cfg --scan -l FULL -L rh_scan.log  --once || error ""

    $REPORT -f $RBH_CFG_DIR/$config_file -e $RH_ROOT/file > report.out

    # Check that the DB has the correct atime and mtime
    egrep --quiet "last_mod\s+:\s+2010/04/17 12:30:00" report.out || error "bad mtime"
    egrep --quiet "last_access\s+:\s+2010/04/17 13:00:00" report.out || error "bad atime"

    # Ensure that the DB and FS agree on atime/mtime and ctime, this
    # time using rbh-find.
    local real_acmtime=$(stat -c '%X %Y %Z' $RH_ROOT/file)
    local db_acmtime=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%As %Ts %Cs")
    [[ $real_acmtime == $db_acmtime ]] || error "FS and DB times don't match1"

    local crtime=$(egrep "creation\s+:" report.out)
    local ctime=$(egrep "last_mdchange\s+:" report.out)

    # Check crtime and ctime in a small time interval
    [[ $($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -crtime -30s) ]] || error "file not found1"
    [[ $($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -crtime +1s) ]] && error "file found1"

    [[ $($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -ctime -30s) ]] || error "file not found2"
    [[ $($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -ctime +1s) ]] && error "file found2"

    echo "3-Change mtime"

    # Sleep lomg enough the time to change by at least one second, so ctime will be
    # updatedwhen the file is touched.
    sleep 5

    # Again, check crtime and ctime. Both must fail as the file is at
    # least 5s old now.
    [[ $($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -crtime -1s) ]] && error "file found3"
    [[ $($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -ctime -1s) ]] && error "file found4"

    # Make mtime > atime. Normally last_access would take the value of
    # the most recent of atime and mtime, but with the
    # last_access_only_atime option, it should stay at atime.
    touch -m -t 201004171400 $RH_ROOT/file
    stat $RH_ROOT/file | grep --quiet "Modify: 2010-04-17 14:00:00" || error "bad mtime"
    stat $RH_ROOT/file | grep --quiet "Access: 2010-04-17 13:00:00" || error "bad atime"
    $RH -f $cfg --scan -l FULL -L rh_scan.log  --once || error ""
    $REPORT -f $RBH_CFG_DIR/$config_file -e $RH_ROOT/file > report.out

    # Again, check crtime and ctime. Hopefully less than 5 seconds
    # elapsed between touch and this command.
    [[ $($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -crtime -4s) ]] && error "file found1"
    [[ $($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -ctime -4s) ]] || error "file not found2"

    egrep --quiet "last_mod\s+:\s+2010/04/17 14:00:00" report.out || error "bad mtime"
    egrep --quiet "last_access\s+:\s+2010/04/17 13:00:00" report.out || error "bad atime"

    # Check that FS and DB agree, using rbh-find
    local real_acmtime=$(stat -c '%X %Y %Z' $RH_ROOT/file)
    local db_acmtime=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%As %Ts %Cs")
    [[ $real_acmtime == $db_acmtime ]] || error "FS and DB times don't match2"

    # Check that FS and DB agree, using rbh-report
    local newcrtime=$(egrep "creation\s+:" report.out)
    local newctime=$(egrep "last_mdchange\s+:" report.out)

    [[ $ctime != $newctime ]] || error "ctime hasn't changed"
    [[ $crtime = $newcrtime ]] || error "creation time has changed"

    ctime=$newctime

    echo "4-Change atime"
    touch -a -t 201004171600 $RH_ROOT/file

    stat $RH_ROOT/file | grep --quiet "Modify: 2010-04-17 14:00:00" || error "bad mtime"
    stat $RH_ROOT/file | grep --quiet "Access: 2010-04-17 16:00:00" || error "bad atime"

    $RH -f $cfg --scan -l FULL -L rh_scan.log  --once || error ""

    # Check that FS and DB agree, using rbh-find
    local real_acmtime=$(stat -c '%X %Y %Z' $RH_ROOT/file)
    local db_acmtime=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%As %Ts %Cs")
    [[ $real_acmtime == $db_acmtime ]] || error "FS and DB times don't match3"

    # Check that FS and DB agree, using rbh-report
    local newcrtime=$(egrep "creation\s+:" report.out)
    local newctime=$(egrep "last_mdchange\s+:" report.out)

    [[ $ctime = $newctime ]] || error "ctime has changed"
    [[ $crtime = $newcrtime ]] || error "creation time has changed"

    export RBH_TEST_LAST_ACCESS_ONLY_ATIME=${org_RBH_TEST_LAST_ACCESS_ONLY_ATIME}
}

# check that changing ACCT schema updates triggers code
function check_acct_update_triggers
{
    local log=$1

    grep "dropping and repopulating table ACCT_STAT" $log || return 0

    # there was an ACCT_STAT change, triggers should have been updated
    grep "trigger ACCT_ENTRY" $log || error "Triggers should have been updated"
}

# test various scenarios of DB schema changes
function db_schema_convert
{
    local schema25=$RBH_CFG_DIR/rbh25.sql
    local cfg1=$RBH_CFG_DIR/test1.conf
    local cfg2=$RBH_CFG_DIR/test_checker.conf
    local cfg3=$RBH_CFG_DIR/test_checker_invert.conf

    # import Robinhood 2.5 DB schema
    mysql $RH_DB < $schema25 || error "importing DB schema"
    # set the right FS path to allow running robinhood commands
    mysql $RH_DB -e "UPDATE VARS SET value='$RH_ROOT' WHERE varname='FS_Path'"

    local nbent=100
    populate $nbent

    :> rh.log
    echo "rbh-report"
    # run rbhv3 report
    $REPORT -f $cfg1 -i -l FULL 2>&1 > rh.log
    [ "$DEBUG" = "1" ] && cat rh.log

    # robinhood should have suggested to run '--alter-db'
    grep "Run 'robinhood --alter-db'" rh.log || error "robinhood should have reported DB schema changes"

    # no ALTER TABLE expected
    grep "ALTER TABLE" rh.log && error "no ALTER TABLE expected"

    :> rh.log
    echo "robinhood (no alter-db)"
    # run a simple rbhv3 over this initial schema
    $RH -f $cfg1 --scan --once -l FULL -L rh.log

    # robinhood should have suggested to run '--alter-db'
    grep "Run 'robinhood --alter-db'" rh.log || error "robinhood should have reported DB schema changes"
    # it should change the default size
    grep "Changing default value of 'ENTRIES.size'" rh.log || error "default value should have been changed"
    grep "ALTER COLUMN size SET DEFAULT 0" rh.log || error "change of default size expected"

    # no other ALTER TABLE expected
    grep -v "SET DEFAULT" rh.log | grep "ALTER TABLE" && error "no ALTER TABLE expected"

    :> rh.log
    echo "robinhood --alter-db"
    # run alter DB on initial schema
    $RH -f $cfg1 --alter-db -l FULL -L rh.log

    [ "$DEBUG" = "1" ] && cat rh.log
    grep "ALTER TABLE" rh.log || error "ALTER TABLE expected"
    check_acct_update_triggers rh.log

    # after alter, no more DB change should be reported
    :> rh.log

    echo "robinhood on converted DB"
    $RH -f $cfg1 --scan --once -l VERB -L rh.log
    grep "DB schema change detected" rh.log && error "DB should be right"
    check_db_error rh.log
    [ "$DEBUG" = "1" ] && cat rh.log
    :> rh.log
    $REPORT -f $cfg1 -i > rh.log || error "Report should succeed"
    grep "Run 'robinhood --alter-db'" rh.log && error "DB should be right"
    [ "$DEBUG" = "1" ] && cat rh.log
    config_file=$(basename $cfg1) check_fcount $nbent

    # now use cfg2
    :> rh.log
    echo "cfg2: robinhood (no alter-db)"
    # run a simple rbhv3 over this initial schema
    $RH -f $cfg2 --scan --once -l FULL -L rh.log

    # robinhood should have suggested to run '--alter-db'
    grep "Run 'robinhood --alter-db'" rh.log || error "robinhood should have reported DB schema changes"

    # no ALTER TABLE expected
    grep "ALTER TABLE" rh.log && error "no ALTER TABLE expected"

    :> rh.log
    echo "cfg2: robinhood --alter-db"
    # run alter DB on initial schema
    $RH -f $cfg2 --alter-db -l FULL -L rh.log

    [ "$DEBUG" = "1" ] && cat rh.log
    grep "ALTER TABLE" rh.log || error "ALTER TABLE expected"
    check_acct_update_triggers rh.log

    # after alter, no more DB change should be reported
    :> rh.log
    echo "cfg2: robinhood on converted DB"
    $RH -f $cfg2 --scan --once -l VERB -L rh.log
    grep "DB schema change detected" rh.log && error "DB should be right"
    check_db_error rh.log
    [ "$DEBUG" = "1" ] && cat rh.log
    :> rh.log
    $REPORT -f $cfg2 -i > rh.log || error "Report should succeed"
    grep "Run 'robinhood --alter-db'" rh.log && error "DB should be right"
    [ "$DEBUG" = "1" ] && cat rh.log
    config_file=$(basename $cfg2) check_fcount $nbent

    # test inversion only if the tested mode has a status manager
    if [[ "$STATUS_MGR" != "none" ]]; then
	    # now test inversion with cfg3
	    :> rh.log
	    echo "cfg3: robinhood (no alter-db)"
	    # run a simple rbhv3 over this initial schema
	    $RH -f $cfg3 --scan --once -l FULL -L rh.log

	    # robinhood must report field shuffling
	    grep "Shuffled DB fields" rh.log || error "lismgr should report shuffled fields"
	    # alter is only required for the ACCT_STAT table
	    grep "Run 'robinhood --alter-db'" rh.log | grep -v "modification in ACCT_STAT" && error "lismgr should deal with field shuffling"
	    # no ALTER TABLE expected
	    grep "ALTER TABLE" rh.log && error "no ALTER TABLE expected"

	    :> rh.log
	    # scan successful?
	    echo "cfg3: scan"
	    $RH -f $cfg3 --scan --once --alter-db -l MAJOR -L rh.log || error "scan failed"
	    grep "FS Scan finished" rh.log || error "Scan failed?"
	    # DB errors reported during scan?
	    check_db_error rh.log

	    # report should work
	    echo "cfg3: report"
	    $REPORT -f $cfg3 -i -l FULL 2>&1 > rh.log || error "Report should work"
	    grep "Run 'robinhood --alter-db'" rh.log && error "DB should be right"
	    [ "$DEBUG" = "1" ] && cat rh.log
	    config_file=$(basename $cfg3) check_fcount $nbent
    fi

    # back to cfg1
    echo "cfg1: report"
    $REPORT -f $cfg1 -i -l FULL 2>&1 > rh.log || error "Report should work"
    [ "$DEBUG" = "1" ] && cat rh.log
    grep "Run 'robinhood --alter-db'" rh.log | grep -v "modification in ACCT_STAT" && error "report should deal with policy removal"
    config_file=$(basename $cfg1) check_fcount $nbent

    :> rh.log
    echo "cfg1: robinhood (no alter-db)"
    # run a simple rbhv3 over this initial schema
    $RH -f $cfg1 --scan --once -l FULL -L rh.log

    # robinhood should have suggested to run '--alter-db'
    grep "Run 'robinhood --alter-db'" rh.log || error "robinhood should have reported DB schema changes"

    # no ALTER TABLE expected
    grep "ALTER TABLE" rh.log && error "no ALTER TABLE expected"

    :> rh.log
    echo "cfg1: robinhood --alter-db"
    # run alter DB on initial schema
    $RH -f $cfg1 --alter-db -l FULL -L rh.log

    [ "$DEBUG" = "1" ] && cat rh.log
    grep "ALTER TABLE" rh.log || error "ALTER TABLE expected"
    grep "dropping and repopulating table ACCT_STAT" rh.log
    grep "trigger ACCT_ENTRY" rh.log

    # after alter, no more DB change should be reported
    :> rh.log
    echo "cfg1: robinhood on converted DB"
    $RH -f $cfg1 --scan --once -l FULL -L rh.log
    grep "DB schema change detected" rh.log && error "DB should be right"
    check_db_error rh.log
    [ "$DEBUG" = "1" ] && cat rh.log
    :> rh.log
    $REPORT -f $cfg1 -i > rh.log || error "Report should succeed"
    grep "Run 'robinhood --alter-db'" rh.log && error "DB should be right"
    [ "$DEBUG" = "1" ] && cat rh.log
    config_file=$(basename $cfg1) check_fcount $nbent

    # Test conversion from numeric to text uids.
    # This is not supposed to be an upgrade path, but it is convenient
    # to test type conversion routines.
    if [[ $RBH_NUM_UIDGID = "yes" ]]; then
        :> rh.log
        echo "Numeric to text conversion (no alter)..."
        RBH_NUM_UIDGID=no $RH -f $cfg1 --scan --once -l FULL -L rh.log
        # robinhood should have suggested to run '--alter-db'
        grep "Run 'robinhood --alter-db'" rh.log || error "robinhood should have reported DB schema changes"

        :> rh.log
        echo "Numeric to text conversion (alter)..."
        RBH_NUM_UIDGID=no $RH -f $cfg1 --alter-db -l FULL -L rh.log
        [ "$DEBUG" = "1" ] && cat rh.log
        grep "ALTER TABLE" rh.log || error "ALTER TABLE expected"
        grep "dropping and repopulating table ACCT_STAT" rh.log
        grep "trigger ACCT_ENTRY" rh.log
    fi
}

# Create files with random names, and use rbh-find on them
function random_names
{
    config_file=$1

    local num_files=500

    clean_logs
    $CFG_SCRIPT empty_db $RH_DB > /dev/null

    echo "1-Creating files..."
    rm -rf $RH_ROOT/random/
    mkdir $RH_ROOT/random/

    echo Creating $num_files files with random names
    $(dirname $0)/create-random $num_files 200 $RH_ROOT/random || error "creating files failed"
    echo Done creating files

    echo "2-Scan of filesystem"
    $RH -f $RBH_CFG_DIR/$config_file --scan -l FULL -L rh_scan.log --once || error ""

    echo "3-Find tests"
    $FIND -f $RBH_CFG_DIR/$config_file -type f $RH_ROOT/random/ > find.out || error "find failed1"
    $FIND -f $RBH_CFG_DIR/$config_file -type f -printf "file=%p\n" $RH_ROOT/random/ > find.out || error "find failed2"

    # When the names are escaped, we will get 1 line per file
    $FIND -f $RBH_CFG_DIR/$config_file -type f -printf "file=%p\n" --escaped $RH_ROOT/random/ > find.out || error "find failed3"
    wc -l < find.out | grep --quiet "^${num_files}$" || error "should have found ${num_files} files"

    echo "4-Cleanup"
    rm -rf $RH_ROOT/random/
}

function check_status_count
{
    report=$1
    status=$2
    count=$3

    nst=$(grep -E "^([ ]*)$status" $report | cut -d ',' -f 3 | tr -d ' ')
    [ -z "$nst" ] && nst=0

    [ "$DEBUG" = "1" ] && echo "$status: $nst"
    [ "$nst" = "$count" ] || error "Expected $count $status, got $nst"
}

function test_compress
{
	config_file=$1
	clean_logs

	if (( $is_hsmlite == 0 )); then
    	echo "compression is only available with backup mode"
		set_skipped
		return 1
    fi

    local dir_rel="dir1 dir2"
    local file_rel="dir1/file.1 dir1/file.2 dir2/file.3 file.4"
    local file_rel_mod="dir1/file.1 file.4"
    local file_rel_new="dir1/file.5 dir1/file.6 dir2/file.7"

    src_file="/etc/hosts"

    # populate the filesystem
    for d in $dir_rel; do
        mkdir -p $RH_ROOT/$d || error mkdir
    done
    for f in $file_rel; do
        /bin/cp $src_file $RH_ROOT/$f || error cp
    done

    # scan the filesystem (compress=no)
    export compress=no
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing initial scan"
    check_db_error rh_scan.log

    # check file status
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info $STATUS_MGR -q > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    check_status_count report.out new 4

    # check how a child entries is archived
    $RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log
    check_db_error rh_migr.log

    # check file status
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info $STATUS_MGR -q  > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    check_status_count report.out synchro 4

    # no compressed file names expected xxxx__<fid>z
    name_comp=$(find $BKROOT -type f -name "*z" | wc -l)
    name_ncomp=$(find $BKROOT -type f -name "*[0-9]" | wc -l)
    type_comp=$(find $BKROOT -type f -exec file {} \; | grep "gzip compressed data" | wc -l)
    type_ncomp=$(find $BKROOT -type f -exec file {} \; | grep "ASCII" | wc -l)

    (( $name_comp == 0 )) || error "No compressed file name expected in backend: found $name_comp"
    (( $type_comp == 0 )) || error "No compressed file data expected in backend: found $type_comp"
    (( $name_ncomp == 4 )) || error "4 non-compressed file names expected in backend: found $name_ncomp"
    (( $type_ncomp == 4 )) || error "4 ASCII file data expected in backend: found $type_ncomp"

    # turn compression on
    export compress=yes

    # modify some files, create new files
    for f in $file_rel_mod; do
        cat $src_file >> $RH_ROOT/$f || error "appending $f"
    done
    for f in $file_rel_new; do
        /bin/cp $src_file $RH_ROOT/$f || error "creating $f"
    done

    # scan the file system and check file status
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing 2nd scan"
    check_db_error rh_scan.log

    # check file status
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info $STATUS_MGR -q  > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    check_status_count report.out synchro 2
    check_status_count report.out modified 2
    check_status_count report.out new 3

    # archive all dirty data and check status
    $RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log
    check_db_error rh_migr.log

    # check file status
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info $STATUS_MGR -q  > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    check_status_count report.out synchro 7

    # check backend files
    name_comp=$(find $BKROOT -type f -name "*z" | wc -l)
    name_ncomp=$(find $BKROOT -type f -name "*[0-9]" | wc -l)
    type_comp=$(find $BKROOT -type f -exec file {} \; | grep "gzip compressed data" | wc -l)
    type_ncomp=$(find $BKROOT -type f -exec file {} \; | grep "ASCII" | wc -l)

    # 2 already archived: uncompresssed
    # 2 modified: compressed
    # 3 new: compressed
    (( $name_comp == 5 )) || error "5 compressed file names expected in backend: found $name_comp"
    (( $type_comp == 5 )) || error "5 compressed file data expected in backend: found $type_comp"
    (( $name_ncomp == 2 )) || error "2 non-compressed file names expected in backend: found $name_ncomp"
    (( $type_ncomp == 2 )) || error "2 ASCII file data expected in backend: found $type_ncomp"

    # turn compression off compression, make some changes and check status again
    for f in $file_rel_mod; do
        cat $src_file >> $RH_ROOT/$f || error "appending $f"
    done

    export compress=no
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing initial scan"
    check_db_error rh_scan.log

    # check file status
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info $STATUS_MGR -q  > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    check_status_count report.out synchro 5
    check_status_count report.out modified 2

    $RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log
    check_db_error rh_migr.log

    # check backend files
    name_comp=$(find $BKROOT -type f -name "*z" | wc -l)
    name_ncomp=$(find $BKROOT -type f -name "*[0-9]" | wc -l)
    type_comp=$(find $BKROOT -type f -exec file {} \; | grep "gzip compressed data" | wc -l)
    type_ncomp=$(find $BKROOT -type f -exec file {} \; | grep "ASCII" | wc -l)

    # 2 archived at first: uncompresssed
    # 2 modified: uncompressed
    # 3 archived at step 2: compressed
    (( $name_comp == 3 )) || error "3 compressed file names expected in backend: found $name_comp"
    (( $type_comp == 3 )) || error "3 compressed file data expected in backend: found $type_comp"
    (( $name_ncomp == 4 )) || error "4 non-compressed file names expected in backend: found $name_ncomp"
    (( $type_ncomp == 4 )) || error "4 ASCII file data expected in backend: found $type_ncomp"

    # check file status
    $REPORT -f $RBH_CFG_DIR/$config_file --status-info $STATUS_MGR -q  > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    check_status_count report.out synchro 7

    # test disaster recovery with compressed files
    local before=/tmp/before.$$
    local after=/tmp/after.$$
    local diff=/tmp/diff.$$
    # shots before disaster (time is only significant for files)
    find $RH_ROOT -type f -printf "%n %m %T@ %g %u %s %p %l\n" > $before
    find $RH_ROOT -type d -printf "%n %m %g %u %s %p %l\n" >> $before
    find $RH_ROOT -type l -printf "%n %m %g %u %s %p %l\n" >> $before

    # perform 2 disaster recovery with compress=yes and compress=no
    for c in yes no; do
        export compress=$c
        # FS disaster
        if [[ -n "$RH_ROOT" ]]; then
            echo "Disaster: all FS content is lost"
            rm  -rf $RH_ROOT/*
        fi

        # perform the recovery
        echo "Performing recovery (compress=$c)..."
        cp /dev/null recov.log
        $RECOV -f $RBH_CFG_DIR/$config_file --start -l DEBUG >> recov.log 2>&1 || error "Error starting recovery"
        $RECOV -f $RBH_CFG_DIR/$config_file --resume -l DEBUG >> recov.log 2>&1 || error "Error performing recovery"
        $RECOV -f $RBH_CFG_DIR/$config_file --complete -l DEBUG >> recov.log 2>&1 || error "Error completing recovery"

        find $RH_ROOT -type f -printf "%n %m %T@ %g %u %s %p %l\n" > $after
        find $RH_ROOT -type d -printf "%n %m %g %u %s %p %l\n" >> $after
        find $RH_ROOT -type l -printf "%n %m %g %u %s %p %l\n" >> $after

        diff  $before $after > /tmp/diff.$$ || error "unexpected differences between initial and final state"
        [ "$DEBUG" = "1" ] && cat /tmp/diff.$$

        # check that no file in Lustre is restored as compressed file
        lucomp=$(find $RH_ROOT -type f -exec file {} \; | grep "gzip compressed data" | wc -l)
        (( $lucomp == 0 )) || error "No compressed file expected in Lustre"

        # check backend files
        # check all *z files are compressed
        type_comp=$(find $BKROOT -type f -name "*z" -exec file {} \; | grep -v "gzip compressed data" | wc -l)
        (( $type_comp == 0 )) || error "Some __<fid>z files are not compressed data"
        # check all *0x0 files are uncompressed
        type_ncomp=$(find $BKROOT -type f -name "*[0-9]" -exec file {} \; | grep "gzip compressed data" | wc -l)
        (( $type_ncomp == 0 )) || error "Some __<fid> files are actually compressed data"
        # check counts
        name_comp=$(find $BKROOT -type f -name "*z" | wc -l)
        name_ncomp=$(find $BKROOT -type f -name "*[0-9]" | wc -l)
        (( $name_comp == 3 )) || error "3 compressed file names expected in backend: found $name_comp"
        (( $name_ncomp == 4 )) || error "4 non-compressed file names expected in backend: found $name_ncomp"
    done

    rm -f report.out $before $after $diff
}

function test_enoent
{
	config_file=$1

	if [[ $RBH_NUM_UIDGID = "yes" ]]; then
		echo "Incompatible configuration for numerical UID/GID: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	if (($no_log != 0)); then
		echo "Changelogs not supported on this config: skipped"
		set_skipped
		return 1
	fi

	echo "1-Start reading changelogs in background..."
	# read changelogs
	$RH -f $RBH_CFG_DIR/$config_file --readlog -l FULL -L rh_chglogs.log  --detach --pid-file=rh.pid || error "could not start cl reader"

	echo "2-create/unlink sequence"
    for i in $(seq 1 1000); do
        touch $RH_ROOT/file.$i
        rm -f $RH_ROOT/file.$i
        touch $RH_ROOT/file.$i
        rm -f $RH_ROOT/file.$i
    done

    # wait for consumer to read all records
    sleep 2
	check_db_error rh_chglogs.log

    # TODO add addl checks here

	$REPORT -f $RBH_CFG_DIR/$config_file --dump-all -cq > report.out || error "report cmd failed"
    lines=$(cat report.out | wc -l)
    [ "$DEBUG" = "1" ] && cat report.out
    (($lines == 0)) || error "no entries expected after create/rm"
    rm -f report.out

	# kill event handler
	pkill -9 $PROC
}

function test_diff
{
	config_file=$1
	flavor=$2
	policy_str="$3"

	clean_logs

    # diff: diff (various), no apply
    # diffapply: diff (various) + apply to DB
    # scan: scan with diff option (various)

    # populate filesystem
    mkdir $RH_ROOT/dir.1 || error "mkdir"
    chmod 0750 $RH_ROOT/dir.1 || error "chmod"
    mkdir $RH_ROOT/dir.2 || error "mkdir"
    mkdir $RH_ROOT/dir.3 || error "mkdir"
    touch $RH_ROOT/dir.1/a $RH_ROOT/dir.1/b $RH_ROOT/dir.1/c || error "touch"
    touch $RH_ROOT/dir.2/d $RH_ROOT/dir.2/e $RH_ROOT/dir.2/f || error "touch"
    touch $RH_ROOT/file || error "touch"

    # initial scan
    echo "1-Initial scan..."
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing initial scan"

    # new entry (file & dir)
    touch $RH_ROOT/dir.1/file.new || error "touch"
    mkdir $RH_ROOT/dir.new	       || error "mkdir"

    # rm'd entry (file & dir)
    rm -f $RH_ROOT/dir.1/b	|| error "rm"
    rmdir $RH_ROOT/dir.3	|| error "rmdir"

    # apply various changes
    chmod 0700 $RH_ROOT/dir.1 		|| error "chmod"
    chown testuser $RH_ROOT/dir.2		|| error "chown"
    chgrp testgroup $RH_ROOT/dir.1/a	|| error "chgrp"
    echo "zqhjkqshdjkqshdjh" >>  $RH_ROOT/dir.1/c || error "append"
    mv $RH_ROOT/dir.2/d  $RH_ROOT/dir.1/d     || error "mv"
    mv $RH_ROOT/file $RH_ROOT/fname           || error "rename"

    # is swap layout feature available?
    has_swap=0
    if [ -z "$POSIX_MODE" ]; then
        $LFS help | grep swap_layout > /dev/null && has_swap=1
        # if so invert stripe for e and f
        if [ $has_swap -eq 1 ]; then
            $LFS swap_layouts $RH_ROOT/dir.2/e  $RH_ROOT/dir.2/f || error "lfs swap_layouts"
        fi
    fi

    # need 1s difference for md and name GC
    sleep 1

    echo "2-diff ($policy_str)..."
    if [ "$flavor" = "diff" ]; then
        $DIFF -f $RBH_CFG_DIR/$config_file -l FULL > report.out \
            2> rh_report.log || error "performing diff"
    elif [ "$flavor" = "partdiff" ]; then
        # the triggered bug returns a retryable error
        # use a timeout to make this test finish
        timeout 10 $DIFF -f $RBH_CFG_DIR/$config_file -l FULL \
            --scan=$RH_ROOT/dir.1 > report.out 2> rh_report.log ||
                error "performing partial diff"
    elif [ "$flavor" = "diffapply" ]; then
        $DIFF --apply=db -f $RBH_CFG_DIR/$config_file -l FULL > report.out \
            2> rh_report.log || error "performing diff"
    elif [ "$flavor" = "scan" ]; then
        $RH -f $RBH_CFG_DIR/$config_file -l FULL --scan --once --diff=all \
            -L rh_report.log > report.out || error "performing scan+diff"
    fi

    [ "$DEBUG" = "1" ] && cat report.out

    # must get:
    # new entries dir.1/file.new and dir.new
    egrep '^++' report.out | grep -v '+++' | grep -E "name='file.new'|path='$RH_ROOT/dir.1/file.new'" | grep type=file || error "missing create dir.1/file.new"
    if [ "$flavor" != "partdiff" ]; then
        egrep '^++' report.out | grep -v '+++' | grep -E "name='dir.new'|path='$RH_ROOT/dir.new'" | grep type=dir || error "missing create dir.new"
    fi
    # rmd entries dir.1/b and dir.3
    if [ "$flavor" = "partdiff" ]; then
        rm_expect=1
    else
        rm_expect=2
    fi
    nbrm=$(egrep -e '^--' report.out | grep -v -- '---' | wc -l)
    [ $nbrm  -eq $rm_expect ] || error "$nbrm/$rm_expect removal"
    # changes
    grep "^+[^ ]*"$(get_id "$RH_ROOT/dir.1") report.out  | grep mode= || error "missing chmod $RH_ROOT/dir.1"
    if [ "$flavor" != "partdiff" ]; then
        grep "^+[^ ]*"$(get_id "$RH_ROOT/dir.2") report.out | grep owner=$testuser_str || error "missing chown $RH_ROOT/dir.2"
    fi
    grep "^+[^ ]*"$(get_id "$RH_ROOT/dir.1/a") report.out | grep group=$testgroup_str || error "missing chgrp $RH_ROOT/dir.1/a"
    grep "^+[^ ]*"$(get_id "$RH_ROOT/dir.1/c") report.out | grep size= || error "missing size change $RH_ROOT/dir.1/c"

    # dir2/d -> dir1/d
    old_parent=$(grep "^-[^ ]*"$(get_id "$RH_ROOT/dir.1/d") report.out | sed -e "s/.*parent=\[\([^]]*\).*/\1/" )
    new_parent=$(grep "^+[^ ]*"$(get_id "$RH_ROOT/dir.1/d") report.out | sed -e "s/.*parent=\[\([^]]*\).*/\1/" )
    [ -z "$old_parent" ] && error "cannot get old parent of $RH_ROOT/dir.1/d"
    [ -z "$new_parent" ] && error "cannot get new parent of $RH_ROOT/dir.1/d"
    [ $old_parent = $new_parent ] && error "$RH_ROOT/dir.1/d still has the same parent"

    # file -> fname
    file_fid=$(get_id "$RH_ROOT/fname")
    old_file=$(grep "^-[^ ]*${file_fid}.*name='file'" report.out)
    new_file=$(grep "^+[^ ]*${file_fid}.*name='fname'" report.out)
    [ -z old_file ] && error "missing path change $RH_ROOT/fname"
    [ -z new_file ] && error "missing path change $RH_ROOT/fname"

    if [ "$flavor" != "partdiff" ] && [ $has_swap -eq 1 ]; then
        grep "^+[^ ]*"$(get_id "$RH_ROOT/dir.2/e") report.out | grep stripe || error "missing stripe change $RH_ROOT/dir.2/e"
        grep "^+[^ ]*"$(get_id "$RH_ROOT/dir.2/f") report.out | grep stripe || error "missing stripe change $RH_ROOT/dir.2/f"
    fi

    # TODO check the content of the DB for scan and diff --apply
}

function test_diff_apply_fs # test diff --apply=fs in particular for entry recovery
{
    config_file=$1
    flavor=$2
    policy_str="$3"

    clean_logs
    # clean any previous files used for this test
    rm -f diff.out diff.log find.out find2.out lovea fid_remap

    # copy 2 instances /bin in the filesystem
    echo "Populating filesystem..."
    $LFS setstripe -c 2 $RH_ROOT/.
    cp -ar /bin $RH_ROOT/bin.1 || error "copy failed"
    cp -ar /bin $RH_ROOT/bin.2 || error "copy failed"

    # run initial scan
    echo "Initial scan..."
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing initial scan"

    # save contents of bin.1
    find $RH_ROOT/bin.1 -printf "%n %y %m %T@ %g %u %p %l\n" > find.out || error "find error"

    # remove it
    echo "removing objects"
    rm -rf "$RH_ROOT/bin.1"

    # cause 1 sec bw initial creation and recovery
    # to check robinhood restore the original date
    sleep 1

    echo "running recovery..."
    strace -f $DIFF -f $RBH_CFG_DIR/$config_file --apply=fs > diff.out 2> diff.log || error "rbh-diff error"

    cr1=$(grep -E '^\+\+[^+]' diff.out | wc -l)
    cr2=$(grep -E 'create|mkdir' diff.log | wc -l)
    cr3=$(wc -l find.out | awk '{print $1}')
    rmhl=0
    if (($cr1 != $cr2)) || (($cr1 != $cr3)); then
        miss=0
        for h in $(grep "type=file" diff.out | grep -E "nlink=[^1]"| sed -e "s/.*nlink=\([0-9]*\),.*/\1/"); do
            ((miss=$h-1+$miss))
        done
        echo "detected $miss missing hardlinks"
        rmhl=1
        if (($cr3 == $cr1 + $miss)); then
            echo "WARNING: $miss hardlinks not restored"
        else
            error "Unexpected number of objects created: rbh-diff displayed $cr1, rbh-diff log indicates $cr2, expected $cr3 according to find"
        fi
    else
        echo "OK: $cr1 objects created"
    fi

    find $RH_ROOT/bin.1 -printf "%n %y %m %T@ %g %u %p %l\n" > find2.out || error "find error"

    if (($rmhl == 1)); then
        # remove file hardlinks from diff as their are erroneous
        for f in $(grep -E "^[^1]* f" find.out | awk '{print $(NF)}'); do
            grep -Ev " $f " find.out > find.out.new
            grep -Ev " $f " find2.out > find2.out.new
            /bin/mv find.out.new find.out
            /bin/mv find2.out.new find2.out
        done
    fi

    # diff non-files: don't compare time as it can't be set
    sed -e "s/\([0-9]* [^f] [0-7]* \)[0-9.]* /\1/" find.out | sort > find.out.new
    sed -e "s/\([0-9]* [^f] [0-7]* \)[0-9.]* /\1/" find2.out | sort > find2.out.new
    /bin/mv find.out.new find.out
    /bin/mv find2.out.new find2.out

    diff find.out find2.out || error "unexpected differences between initial and final state"


    lvers=$(cat /proc/fs/lustre/version | grep "lustre:" | awk '{print $2}' | cut -d '.' -f 1,2)
    if [[ "$lvers" == "2.1" ]]; then
        # lovea and fid_remap must have been generated for newly created files
        [[ -f lovea ]] || error "lovea not generated"
        [[ -f fid_remap ]] || error "fid_remap not generated"
    elif [[ "$lustre_major" == "2" ]]; then
        # not tested for those versions: display a warning for reminder
        [[ -f lovea ]] || echo  "WARNING: lovea not generated"
        [[ -f fid_remap ]] || echo "WARNING: fid_remap not generated"
    fi
    if [[ -f lovea ]] && [[ -f fid_remap ]]; then
        nbf=$(grep -E '^\+\+[^+]' diff.out | grep "type=file" | wc -l)
        nbso=$(grep -E '^\+\+[^+]' diff.out | grep "type=file" | sed -e "s/.*stripe_count=\([0-9]*\),.*/\1/" | xargs | tr " " "+" | bc)
        # check their contents
        nbl=$(wc -l lovea | awk '{print $1}')
        nbo=$(wc -l fid_remap | awk '{print $1}')

        echo "$nbl items in lovea, $nbo items in fid_remap"
        [[ "$nbf" == "$nbl" ]] || error "unexpected number of items in lovea $nbl: $nbf expected"
        [[ "$nbso" == "$nbo" ]] || error "unexpected number of items in fid_remap $nbo: $nbso expected"
    fi

    rm -f  diff.out diff.log find.out find2.out lovea fid_remap
}

function test_completion
{
	config_file=$1
	flavor=$2
	policy_str="$3"

	clean_logs
    # clean existing "out.*" files
    rm -f out.1 out.2

    done_str="Executing scan completion command"
    fail_str="Invalid scan completion command"

    # flavors:
    case "$flavor" in
        OK)
            export TEST_CMD="$RBH_TESTS_DIR/completion.sh {cfg} {fspath} out"
            ;;
        unmatched)
            export TEST_CMD="$RBH_TESTS_DIR/completion.sh {cfg"
            err="ERROR: unmatched '{' in scan completion command"
            ;;
        invalid_ctx_id)
            export TEST_CMD="$RBH_TESTS_DIR/completion.sh {fid}"
            err="fid is not available in this context"
            ;;
        invalid_ctx_attr)
            export TEST_CMD="$RBH_TESTS_DIR/completion.sh {fullpath}"
            err="entry attributes are not available in this context"
            ;;
        invalid_attr)
            export TEST_CMD="$RBH_TESTS_DIR/completion.sh {foo}"
            err="unexpected variable 'foo' in scan completion command"
            ;;
    esac

    # populate filesystem
    for i in `seq 1 10`; do
        touch $RH_ROOT/file.$i || error "creating entry"
    done

    # do the scan
    echo "scan..."
    $RH -f $RBH_CFG_DIR/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing initial scan"

    # if flavor is OK: completion command must have been called
    if [ "$flavor" = "OK" ]; then
        grep "$done_str" rh_scan.log || error "Completion command not executed"

        [ -f out.1 ] || error "file out.1 not found"
        [ -f out.2 ] || error "file out.2 not found"
        # out.1 contains cfg
        grep $config_file out.1 || error "out.1 has unexpected content: $(cat out.1)"
        # out.2 contains fspath
        grep $RH_ROOT out.2 || error "out.2 has unexpected content: $(cat out.2)"
    else
        grep "$fail_str" rh_scan.log || error "Completion command should fail"
        grep "$err" rh_scan.log || error "unreported cmd error"
    fi

    rm -f out.1 out.2
}


function test_rename
{
    config_file=$1
    flavor=$2

    clean_logs

	if (( $no_log )) && [ "$flavor" = "readlog" ]; then
            echo "Changelogs not supported on this config: skipped"
            set_skipped
            return 1
    fi

    dirs="$RH_ROOT/dir.1 $RH_ROOT/dir.2 $RH_ROOT/dir.3 $RH_ROOT/dir.3/subdir"
    files="$RH_ROOT/dir.1/file.1  $RH_ROOT/dir.1/file.2  $RH_ROOT/dir.2/file.1 $RH_ROOT/dir.2/file.2 $RH_ROOT/dir.2/file.4 $RH_ROOT/dir.3/subdir/file.1"
    hlink_ref="$RH_ROOT/dir.2/file.3"
    hlink="$RH_ROOT/dir.2/link_file" # initially points to file.3, then file.4

    dirs_tgt="$RH_ROOT/dir.1 $RH_ROOT/dir.2 $RH_ROOT/dir.3 $RH_ROOT/dir.3/subdir.rnm"
    files_tgt="$RH_ROOT/dir.1/file.1.rnm  $RH_ROOT/dir.2/file.2.rnm  $RH_ROOT/dir.2/file.2  $RH_ROOT/dir.2/file.3  $RH_ROOT/dir.2/link_file $RH_ROOT/dir.3/subdir.rnm/file.1"
    deleted="$RH_ROOT/dir.2/file.2"

    # create several files/dirs
    echo "1. Creating initial objects..."
    mkdir $dirs || error "mkdir $dirs"
    touch $files $hlink_ref || error "touch $files $hlink_ref"
    ln $hlink_ref $hlink || error "hardlink $hlink_ref $hlink"

    # get fid of deleted entries
    rmid=`get_id "$deleted"`

    # readlog or scan
    if [ "$flavor" = "readlog" ]; then
        echo "2. Reading changelogs..."
    	$RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
    elif [ "$flavor" = "diff" ]; then
        echo "2. Diff..."
    	$DIFF -f $RBH_CFG_DIR/$config_file --apply=db -l DEBUG > rh_scan.log 2>&1 || error "scanning"
    else
        echo "2. Scanning initial state..."
    	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning"
    fi

	if (( $is_lhsm != 0 )); then
		echo "  -archiving all data"
		flush_data
		$LFS hsm_archive $files || error "executing lfs hsm_archive"
		echo "  -Waiting for end of data migration..."
		wait_done 60 || error "Migration timeout"
	elif (( $is_hsmlite != 0 )); then
		echo "  -archiving all data"
		$RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log || error "executing $CMD --sync"
        [ "$DEBUG" = "1" ] && find $BKROOT -type f -ls
	fi

    $REPORT -f $RBH_CFG_DIR/$config_file --dump-all -q > report.out || error "$REPORT"
    [ "$DEBUG" = "1" ] && cat report.out

    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT -ls -nobulk > find.out || error "$FIND"
    [ "$DEBUG" = "1" ] && cat find.out

    # checking all objects in reports
    for o in $dirs $files; do
        grep -E " $o$" report.out > /dev/null || error "$o not found in report"
        grep -E " $o$" find.out > /dev/null || error "$o not found in find"
    done

    # hlink_ref hlink must be in report.out
    grep -E " $hlink$" report.out > /dev/null ||  grep -E " $hlink_ref$" report.out > /dev/null || error "$hlink or $hlink_ref must be in report output"
    # both hlink_ref hlink must be in find.out
    grep -E " $hlink$" find.out > /dev/null || error "$hlink must be in rbh-find output"
    grep -E " $hlink_ref$" find.out > /dev/null || error "$hlink must be in rbh-find output"

    count_nb_init=$(wc -l report.out | awk '{print $1}')
    count_path_init=$(wc -l find.out | awk '{print $1}')

    # get entry fid before they are unlinked, moved...
    name_from=(dir.1/file.1 dir.1/file.2 dir.2/file.1 dir.3/subdir dir.2/file.4)
    id_from=()
    for f in ${name_from[*]}; do
        id_from+=( "$(get_id $RH_ROOT/$f)" )
    done

    name_unlnk=(dir.2/file.2 dir.2/link_file)
    id_unlnk=()
    for f in ${name_unlnk[*]}; do
        id_unlnk+=( "$(get_id $RH_ROOT/$f)" )
    done

    # rename entries
    echo "3. Renaming objects..."
    # 1) simple file rename
    mv $RH_ROOT/dir.1/file.1 $RH_ROOT/dir.1/file.1.rnm
    # 2) cross directory file rename
    mv $RH_ROOT/dir.1/file.2 $RH_ROOT/dir.2/file.2.rnm
    # 3) rename that deletes the target
    mv -f $RH_ROOT/dir.2/file.1 $RH_ROOT/dir.2/file.2
    # 4) upper level directory rename
    mv $RH_ROOT/dir.3/subdir $RH_ROOT/dir.3/subdir.rnm
    # 5) overwriting a hardlink
    mv -f $RH_ROOT/dir.2/file.4 $hlink

    # get target fids
    name_to=(dir.1/file.1.rnm dir.2/file.2.rnm dir.2/file.2 dir.3/subdir.rnm dir.3/subdir.rnm dir.2/link_file)
    id_to=()
    for f in ${name_to[*]}; do
        id_to+=( "$(get_id $RH_ROOT/$f)" )
    done

    # namespace GC needs 1s difference
    sleep 1

    # readlog or re-scan
    if [ "$flavor" = "readlog" ]; then
        echo "4. Reading changelogs..."
    	$RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"

        ## check the "fake" records are correctly built

        # "rename from"
        for i in $(seq 1 ${#name_from[@]}); do
            n=${name_from[$((i-1))]}
            id=${id_from[$((i-1))]}
            grep "RECORD:" rh_scan.log | egrep "RENME|RNMFM" | grep $(basename $n) | grep $id || error "Missing RENME $n"
        done

        # "rename to"
        for i in $(seq 1 ${#name_to[@]}); do
            n=${name_to[$((i-1))]}
            id=${id_to[$((i-1))]}
            grep "RECORD:" rh_scan.log | grep RNMTO | grep $(basename $n) | grep $id || error "Missing RNMTO $n"
        done

        # unlinked targets
        for i in $(seq 1 ${#name_unlnk[@]}); do
            n=${name_unlnk[$((i-1))]}
            id=${id_unlnk[$((i-1))]}
            grep "RECORD:" rh_scan.log | grep UNLNK | grep $(basename $n) | grep $id || error "Missing UNLNK $n"
        done

    elif [ "$flavor" = "scan" ]; then
        echo "4. Scanning again..."
    	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning"
    elif [ "$flavor" = "diff" ]; then
        echo "4. Diffing again..."
    	$DIFF -f $RBH_CFG_DIR/$config_file --apply=db -l DEBUG > rh_scan.log 2>&1 || error "scanning"
    elif [ "$flavor" = "partial" ]; then
        i=0
        for d in $dirs_tgt; do
            # namespace GC needs 1s difference
            sleep 1
            ((i++))
            echo "4.$i Partial scan ($d)..."
        	$RH -f $RBH_CFG_DIR/$config_file --scan=$d --once -l DEBUG -L rh_scan.log || error "scanning $d"
        done
    elif [ "$flavor" = "partdiff" ]; then
        i=0
        for d in $dirs_tgt; do
            # namespace GC needs 1s difference
            sleep 1
            ((i++))
            echo "4.$i Partial diff+apply ($d)..."
        	$DIFF -f $RBH_CFG_DIR/$config_file --scan=$d --apply=db -l DEBUG  > rh_scan.log 2>&1 || error "scanning $d"
        done
    fi

    $REPORT -f $RBH_CFG_DIR/$config_file --dump-all -q > report.out || error "$REPORT"
    [ "$DEBUG" = "1" ] && cat report.out

    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT -nobulk -ls > find.out || error "$FIND"
    [ "$DEBUG" = "1" ] && cat find.out

    # checking all objects in reports
    for o in $dirs_tgt $files_tgt; do
        grep -E " $o$" report.out > /dev/null || error "$o not found in report"
        grep -E " $o$" find.out > /dev/null || error "$o not found in report"
    done

    grep "\[$rmid\]" find.out && error "id of deleted file ($rmid) found in rbh-find output"
    unset count_nb_final
	if (( $is_lhsm + $is_hsmlite == 1 )); then
		# additionally check that the entry is scheduled for deferred rm (and only this one)
	    $REPORT -f $RBH_CFG_DIR/$config_file --deferred-rm --csv -q > rh_report.log

        # The following test is not critical for partial scanning
        # In the worst case, the deleted entry remains in the archive.
        if [ "$flavor" = "partial" ] || [ "$flavor" = "partdiff" ]; then
            grep "\[$rmid\]" rh_report.log > /dev/null || echo "WARNING: $rmid should be in HSM rm list"
            # Conservative behavior for partial scans in front of an archive: allow n/a in paths
            count_nb_final=$(awk '{print $(NF)}' report.out | grep -v 'n/a' | wc -l)
        else
            # in the other cases, raise an error
            grep "\[$rmid\]" rh_report.log > /dev/null || error "$rmid should be in HSM rm list"
        fi

        # the following is the most CRITICAL, as this would result in removing archived entries
        # for existing file!
        grep -v "\[$rmid\]" rh_report.log && error "Existing entries are in HSM rm list!!!"
	fi

    [ -z "$count_nb_final" ] && count_nb_final=$(wc -l report.out | awk '{print $1}')
    count_path_final=$(wc -l find.out | awk '{print $1}')

    (( $count_nb_final == $count_nb_init - 1)) || error "1 entry should have been removed (rename target), got $(($count_nb_init - $count_nb_final))"
    (( $count_path_final == $count_path_init - 2)) || error "2 paths should have been removed (rename target), got $(( $count_path_init - $count_path_final ))"

    rm -f report.out find.out
}

function test_unlink
{
    config_file=$1
    flavor=$2

    clean_logs

	if (( $no_log )); then
            echo "Changelogs not supported on this config: skipped"
            set_skipped
            return 1
    fi

	# Create one file and a hardlink
    touch "$RH_ROOT/foo1"
	ln "$RH_ROOT/foo1" "$RH_ROOT/foo2"

	# Check nlink == 2
    $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
	$FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT/foo1 -nobulk -ls > report.out || error "$REPORT"
	nlink=$( cat report.out | awk '{ print $4; }' )
	(( $nlink == 2 )) || error "nlink should be 2 instead of $nlink"

	# Remove one file and check nlink == 1
	rm "$RH_ROOT/foo2"
    $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
	$FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT/foo1 -nobulk -ls > report.out || error "$REPORT"
	nlink=$( cat report.out | awk '{ print $4; }' )
	(( $nlink == 1 )) || error "nlink should be 1 instead of $nlink"

	# Add a new hard link and check nlink == 2
	ln "$RH_ROOT/foo1" "$RH_ROOT/foo3"
    $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
	$FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT/foo1 -nobulk -ls > report.out || error "$REPORT"
	nlink=$( cat report.out | awk '{ print $4; }' )
	(( $nlink == 2 )) || error "nlink should be 1 instead of $nlink"

	# Remove one file and check nlink == 1
	rm "$RH_ROOT/foo3"
    $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
	$FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT/foo1 -nobulk -ls > report.out || error "$REPORT"
	nlink=$( cat report.out | awk '{ print $4; }' )
	(( $nlink == 1 )) || error "nlink should be 1 instead of $nlink"

    # Now create one hardlink, then remove it, but do not run RH in between.
	ln "$RH_ROOT/foo1" "$RH_ROOT/foo2"
	rm "$RH_ROOT/foo2"
	# check nlink == 1
	$RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
	$FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT/foo1 -nobulk -ls > report.out || error "$REPORT"
	nlink=$( cat report.out | awk '{ print $4; }' )
	(( $nlink == 1 )) || error "nlink should be 1 instead of $nlink"

    rm -f report.out find.out
}

function test_layout
{
    config_file=$1
    flavor=$2

    has_swap=0
    $LFS help | grep swap_layout > /dev/null && has_swap=1

    if (( $has_swap == 0 )); then
        echo "Layout change no supported on this config: skipped"
        set_skipped
        return 1
    fi

    clean_logs

	if (( $no_log )); then
        echo "Changelogs not supported on this config: skipped"
        set_skipped
        return 1
    fi

    # Create a file and change its layout.
    DSTFILE="$RH_ROOT/foo1"
    $LFS setstripe -c 1 $DSTFILE
    dd if=/dev/zero of=$DSTFILE bs=1M count=10
    $LFS migrate -c 2 $DSTFILE

	# Check if a CL_LAYOUT record was emitted and triggered a getstripe().
    $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
    ngetstripe_zero=$(grep LYOUT rh_scan.log | grep -c "getstripe=0")
    ngetstripe=$(grep LYOUT rh_scan.log | grep -c "getstripe=1")
    (( $ngetstripe_zero == 0 && $ngetstripe > 0 )) || error "CL_LAYOUT should trigger a getstripe() operation."

    $LFS migrate -c 1 $DSTFILE
    fsdiff=$($RH -f $RBH_CFG_DIR/$config_file --readlog --diff=stripe --once -l DEBUG -L rh_scan.log)
    (( $? == 0 )) || error "reading changelog (diff)"

    [ "$DEBUG" = "1" ] && echo "$fsdiff"
    echo $fsdiff | egrep "\-.*,stripe_count=2,.* \+.*,stripe_count=1,.*" > /dev/null || error "missed layout change"

    rm -f $DSTFILE
}

function flavor2rbh_cmd
{
    case "$1" in
        scan)
            echo "$RH --scan --once -L stderr"
            ;;
        scandiff1) # default
            echo "$RH --scan --once --diff=all -L stderr"
            ;;
        scandiff2) # explicit nostripe
            echo "$RH --scan --once --diff=posix -L stderr"
            ;;
        scandiff3) # explicit stripe
            echo "$RH --scan --once --diff=stripe -L stderr"
            ;;
        diffna1) # default
            echo "$DIFF --diff=all"
            ;;
        diffna2) # explicit nostripe
            echo "$DIFF --diff=posix"
            ;;
        diffna3) # explicit stripe
            echo "$DIFF --diff=stripe"
            ;;
        diff1) # default
            echo "$DIFF --apply=db"
            ;;
        diff2) # explicit nostripe
            echo "$DIFF --diff=posix --apply=db"
            ;;
        diff3) # explicit stripe
            echo "$DIFF --diff=stripe --apply=db"
            ;;
        cl)
            echo "$RH --readlog --once -L stderr"
            ;;
        cldiff1) # default
            echo "$RH --readlog --once --diff=all -L stderr"
            ;;
        cldiff2) # explicit nostripe
            echo "$RH --readlog --once --diff=posix -L stderr"
            ;;
        cldiff3) # explicit stripe
            echo "$RH --readlog --once --diff=stripe -L stderr"
            ;;
    esac
}

function run_scan_cmd
{
    local cfg=$1
    local mode=$2

    local cmd=$(flavor2rbh_cmd $mode)

    :> rh.out
    $cmd -f $RBH_CFG_DIR/$cfg -l FULL > rh.out 2>> rh.log || error "running $cmd"
	check_db_error rh.log
    grep -E "Warning" rh.log && grep -E "doesn't match stripe count" rh.log > /dev/null && error "Stripe count mismatch detected"
}

function scan_check_no_update
{
    cfg=$1
    mode=$2

    # no stripe update expected for 2nd run
    :> rh.log
    :> rh.out
    run_scan_cmd $cfg $mode
    grep STRIPE_I rh.log | egrep -i "INSERT|DELETE|UPDATE" && error "No stripe update expected during second run"
}

# check diff output (rh.out) when [[ $flavor = *"diff"* ]]
function check_stripe_diff
{
    old="$1"
    new="$2"
    expect=$3
    if [ $expect = 1 ]; then
        if [ -n "$old" ]; then
            egrep "^\-" rh.out | egrep "$old"  || error "pattern '- ... $old' not found in diff output"
        fi
        if [ -n "$new" ]; then
            egrep "^\+" rh.out | egrep "$new"  || error "pattern '+ ... $new' not found in diff output"
        fi
    else
        if [ -n "$old" ]; then
            egrep "^\-" rh.out | egrep "$old"  && error "pattern '- ... $old' not expected in diff output"
        fi
        if [ -n "$new" ]; then
            egrep "^\+" rh.out | egrep "$new"  && error "pattern '+ ... $new' not expected in diff output"
        fi
    fi
}

function check_stripe
{
    local cfg=$1
    local f=$2
    local pattern=$3

    :> rh.out
    $REPORT -f $RBH_CFG_DIR/$cfg -c -e $f > rh.out 2>> rh.log || error "$f not in RBH DB"
	check_db_error rh.log
    egrep "^stripes," rh.out | egrep "$pattern" || error "pattern \"$pattern\" not found in report output: $(cat rh.out)"
}

function stripe_update
{
    config_file=$1
    flavor=$2 # way to update stripe info (scan, scan diff <mask1>, scan diff <mask2>,
              # diff (no apply, apply=db)x(mask1, ..maskN), changelog...)
              # see function flavor2rbh_opt().

    :> rh.out
    :> rh.log

	if [ -n "$POSIX_MODE" ]; then
        echo "No stripe information in POSIX mode"
        set_skipped
        return 1
    fi

    has_swap=0
    $LFS help | grep swap_layout > /dev/null && has_swap=1
    getstripe=1 # allow getstripe
    [ $has_swap = 1 ] && getstripe=0 # no getstripe expected
    diff=0

	if [[ $flavor = "cl"* ]] && (( $no_log )); then
        echo "Changelogs not supported on this config: skipped"
        set_skipped
        return 1
    fi

    [[ $flavor = "cl"* ]] && clean_logs
    [[ $flavor = "cl"* ]] && getstripe=1 # getstripe allowed

    # only diff1 and 3 should display stripe changes
    [[ $flavor = *"diff"* ]] && [[ $flavor != *"2" ]] && diff=1
    rm -f $RH_ROOT/file.*

    echo "test setup: checking diff=$diff, getstripe allowed=$getstripe, has_swap=$has_swap"

    echo "- non-striped file"
    # case 1 (all Lustre versions): create an unstriped file, then stripe it
    create_nostripe $RH_ROOT/file.1 || error "creating unstriped file"
    run_scan_cmd $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "" "stripe_count=0" 1
    check_stripe $config_file $RH_ROOT/file.1 "none"

    # no update expected for second run
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe" "stripe" 0 # no stripe change expected
    check_stripe $config_file $RH_ROOT/file.1 "none"

    # check if "getstripe -g" exists
    has_gen=0
    $LFS getstripe -g $RH_ROOT/ 2>/dev/null && has_gen=1

    # stripe it
    echo "- stripe file"
    $LFS setstripe -c 1 $RH_ROOT/file.1 || error "setting file stripe"
    idx=$($LFS getstripe -i $RH_ROOT/file.1)
    [ "$DEBUG" = "1" ] && echo "$RH_ROOT/file.1: ost$idx"
    [ "$DEBUG" = "1" ] && [ "$has_gen" = "1" ] && echo "$RH_ROOT/file.1: gen $($LFS getstripe -g $RH_ROOT/file.1)"
    run_scan_cmd $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe_count=0" "stripe_count=1" 1
    check_stripe $config_file $RH_ROOT/file.1 "ost#$idx"

    # no update expected for second run
    [ "$DEBUG" = "1" ] &&  [ "$has_gen" = "1" ] && echo "$RH_ROOT/file.1: gen $($LFS getstripe -g $RH_ROOT/file.1)"
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe" "stripe" 0 # no stripe change expected
    check_stripe $config_file $RH_ROOT/file.1 "ost#$idx"

    # other cases: play with layout_swap (skip for Lustre < 2.4)
    if (( $has_swap == 0 )); then
        echo "No layout swap: skipping the end of the test"
        return 0
    fi

    # swap with another striped file
    $LFS setstripe -c 1 $RH_ROOT/file.2 || error "creating striped file"
    idx2=$($LFS getstripe -i $RH_ROOT/file.2)
    [ "$DEBUG" = "1" ] && echo "$RH_ROOT/file.2: ost$idx2"
    echo "- swap it with striped file"
    $LFS swap_layouts $RH_ROOT/file.1 $RH_ROOT/file.2 || error "swapping file layouts"
    [ "$DEBUG" = "1" ] &&  [ "$has_gen" = "1" ] && echo "$RH_ROOT/file.1: gen $($LFS getstripe -g $RH_ROOT/file.1)"
    run_scan_cmd $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripes={ost#$idx" "stripes={ost#$idx2" 1
    check_stripe $config_file $RH_ROOT/file.1 "ost#$idx2"

    # no update expected for second run
    [ "$DEBUG" = "1" ] &&  [ "$has_gen" = "1" ] && echo "$RH_ROOT/file.1: gen $($LFS getstripe -g $RH_ROOT/file.1)"
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe" "stripe" 0 # no stripe change expected
    check_stripe $config_file $RH_ROOT/file.1 "ost#$idx2"

    # swap with non-striped file
    create_nostripe $RH_ROOT/file.3 || error "creating unstriped file"
    echo "- swap it with non-striped file"
    $LFS swap_layouts $RH_ROOT/file.1 $RH_ROOT/file.3 || error "swapping file layouts"
    [ "$DEBUG" = "1" ] &&  [ "$has_gen" = "1" ] && echo "$RH_ROOT/file.1: gen $($LFS getstripe -g $RH_ROOT/file.1)"
    run_scan_cmd $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe_count=1" "stripe_count=0" 1
    check_stripe $config_file $RH_ROOT/file.1 "none"

    [ "$DEBUG" = "1" ] &&  [ "$has_gen" = "1" ] && echo "$RH_ROOT/file.1: gen $($LFS getstripe -g $RH_ROOT/file.1)"
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe" "stripe" 0 # no stripe change expected
    check_stripe $config_file $RH_ROOT/file.1 "none"

    return 0
}

function stripe_no_update
{
    config_file=$1
    flavor=$2 # way to update stripe info (scan, scan diff <mask1>, scan diff <mask2>,
              # diff (no apply, apply=db)x(mask1, ..maskN), changelog...)
              # see function flavor2rbh_opt().

    :> rh.out
    :> rh.log

	if [ -n "$POSIX_MODE" ]; then
        echo "No stripe information in POSIX mode"
        set_skipped
        return 1
    fi

    has_swap=0
    $LFS help | grep swap_layout > /dev/null && has_swap=1
    getstripe=1 # allow getstripe
    [ $has_swap = 1 ] && getstripe=0 # no getstripe expected
    # only diff1 and 3 should display stripe changes
    diff=0
    [[ $flavor = *"diff"* ]] && [[ $flavor != *"2" ]] && diff=1

	if [[ $flavor = "cl"* ]] && (( $no_log )); then
        echo "Changelogs not supported on this config: skipped"
        set_skipped
        return 1
    fi

    rm -f $RH_ROOT/file.*

    echo "test setup: checking diff=$diff, getstripe allowed=$getstripe, has_swap=$has_swap"

    # initial scan
    run_scan_cmd $config_file "scan"

    echo "- non-striped file"
    # case 1 (all Lustre versions): create an unstriped file, then stripe it
    create_nostripe $RH_ROOT/file.1 || error "creating unstriped file"
    # no update expected for the given specified run
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "" "stripe_count=0" 1
    # update db contents
    run_scan_cmd $config_file "scan"
    check_stripe $config_file $RH_ROOT/file.1 "none"

    # check if "getstripe -g" exists
    has_gen=0
    $LFS getstripe -g $RH_ROOT/ 2>/dev/null && has_gen=1

    # stripe it
    echo "- stripe file"
    $LFS setstripe -c 1 $RH_ROOT/file.1 || error "setting file stripe"
    idx=$($LFS getstripe -i $RH_ROOT/file.1)
    [ "$DEBUG" = "1" ] && echo "$RH_ROOT/file.1: ost$idx"
    [ "$DEBUG" = "1" ] && [ "$has_gen" = "1" ] && echo "$RH_ROOT/file.1: gen $($LFS getstripe -g $RH_ROOT/file.1)"
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe_count=0" "stripe_count=1" 1
    run_scan_cmd $config_file "scan"
    check_stripe $config_file $RH_ROOT/file.1 "ost#$idx"

    # other cases: play with layout_swap (skip for Lustre < 2.4)

    if (( $has_swap == 0 )); then
        echo "No layout swap: skipping the end of the test"
        return 0
    fi

    # swap with another striped file
    $LFS setstripe -c 1 $RH_ROOT/file.2 || error "creating striped file"
    idx2=$($LFS getstripe -i $RH_ROOT/file.2)
    [ "$DEBUG" = "1" ] && echo "$RH_ROOT/file.2: ost$idx2"
    echo "- swap it with striped file"
    $LFS swap_layouts $RH_ROOT/file.1 $RH_ROOT/file.2 || error "swapping file layouts"
    [ "$DEBUG" = "1" ] && [ "$has_gen" = "1" ] && echo "$RH_ROOT/file.1: gen $($LFS getstripe -g $RH_ROOT/file.1)"
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripes={ost#$idx" "stripes={ost#$idx2" 1
    run_scan_cmd $config_file "scan"
    check_stripe $config_file $RH_ROOT/file.1 "ost#$idx2"

    # swap with non-striped file
    create_nostripe $RH_ROOT/file.3 || error "creating unstriped file"
    echo "- swap it with non-striped file"
    $LFS swap_layouts $RH_ROOT/file.1 $RH_ROOT/file.3 || error "swapping file layouts"
    [ "$DEBUG" = "1" ] && [ "$has_gen" = "1" ] && echo "$RH_ROOT/file.1: gen $($LFS getstripe -g $RH_ROOT/file.1)"
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe_count=1" "stripe_count=0" 1
    run_scan_cmd $config_file "scan"
    check_stripe $config_file $RH_ROOT/file.1 "none"

    return 0
}


# test link/unlink/rename
# flavors=readlog, scan, partial scan
function test_hardlinks
{
    config_file=$1
    flavor=$2

    clean_logs

	if (( $no_log )) && [ "$flavor" = "readlog" ]; then
            echo "Changelogs not supported on this config: skipped"
            set_skipped
            return 1
    fi

    dirs="$RH_ROOT/dir.1 $RH_ROOT/dir.2 $RH_ROOT/dir.3 $RH_ROOT/dir.3/subdir $RH_ROOT/dir.4"
    files="$RH_ROOT/dir.1/file.1  $RH_ROOT/dir.1/file.2  $RH_ROOT/dir.2/file.1 $RH_ROOT/dir.2/file.2 $RH_ROOT/dir.2/file.4 $RH_ROOT/dir.3/subdir/file.1 $RH_ROOT/dir.4/file.3"
    hlink_refs=("$RH_ROOT/dir.2/file.3" "$RH_ROOT/dir.4/file.1" "$RH_ROOT/dir.4/file.2")
    hlinks=("$RH_ROOT/dir.2/link_file" "$RH_ROOT/dir.1/link.1 $RH_ROOT/dir.2/link.1" "$RH_ROOT/dir.2/link.2")
    #[0] file.4 will over write it, [1] one more link will be created, [2]previous path ($RH_ROOT/dir.4/file.2) will be removed

    dirs_tgt="$RH_ROOT/dir.1 $RH_ROOT/dir.2 $RH_ROOT/dir.3 $RH_ROOT/dir.3/subdir.rnm $RH_ROOT/dir.4"
    files_tgt="$RH_ROOT/dir.1/file.1.rnm  $RH_ROOT/dir.2/file.2.rnm  $RH_ROOT/dir.2/file.2  $RH_ROOT/dir.2/file.3  $RH_ROOT/dir.2/link_file $RH_ROOT/dir.3/subdir.rnm/file.1 $RH_ROOT/dir.2/link.2 $RH_ROOT/dir.1/new"
    hlink_refs_tgt=("$RH_ROOT/dir.4/file.1" "$RH_ROOT/dir.2/new")
    hlinks_tgt=("$RH_ROOT/dir.1/link.1 $RH_ROOT/dir.2/link.1 $RH_ROOT/dir.4/link.1" "$RH_ROOT/dir.4/link.new")
        # only previous [1] remaining as [0], [1] is a new link

    deleted="$RH_ROOT/dir.2/file.2 $RH_ROOT/dir.4/file.3"

    # create several files/dirs
    echo "1. Creating initial objects..."
    mkdir $dirs || error "mkdir $dirs"
    touch $files ${hlink_refs[*]} || error "touch $files ${hlink_refs[*]}"
    i=0
    nb_ln=0
    while [ -n "${hlink_refs[$i]}" ]; do
        for l in ${hlinks[$i]}; do
            ln ${hlink_refs[$i]} $l || error "hardlink ${hlink_refs[$i]} $l"
            ((nb_ln++))
        done
        ((i++))
    done

    # get id of deleted entries
    rmids=""
    for f in $deleted; do
        rmids="$rmids `get_id $f`"
    done
    [ "$DEBUG" = "1" ] && echo "ids to be deleted: $rmids"

    # readlog or scan
    if [ "$flavor" = "readlog" ]; then
        echo "2. Reading changelogs..."
    	$RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
    elif [ "$flavor" = "diff" ]; then
        echo "2. Diff..."
    	$DIFF -f $RBH_CFG_DIR/$config_file --apply=db -l DEBUG > rh_scan.log 2>&1 || error "scanning"
    else
        echo "2. Scanning initial state..."
    	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning"
    fi

	if (( $is_lhsm != 0 )); then
		echo "  -archiving all data"
		flush_data
		$LFS hsm_archive $files || error "executing lfs hsm_archive"
		echo "  -Waiting for end of data migration..."
		wait_done 60 || error "Migration timeout"
	elif (( $is_hsmlite != 0 )); then
		echo "  -archiving all data"
		$RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log || error "executing $CMD --sync"
        [ "$DEBUG" = "1" ] && find $BKROOT -type f -ls
	fi

    $REPORT -f $RBH_CFG_DIR/$config_file --dump-all -q > report.out || error "$REPORT"
    [ "$DEBUG" = "1" ] && cat report.out

    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT -nobulk -ls > find.out || error "$FIND"
    [ "$DEBUG" = "1" ] && cat find.out

    # checking all objects in reports
    for o in $dirs $files; do
        grep -E " $o$" report.out > /dev/null || error "$o not found in report"
        grep -E " $o$" find.out > /dev/null || error "$o not found in find"
    done

    i=0
    while [ -n "${hlink_refs[$i]}" ]; do
        file="${hlink_refs[$i]}"
        ok=0
        grep -E " $file$" find.out > /dev/null || error "$file must be in rbh-find output"
        grep -E " $file$" report.out > /dev/null && ok=1
        for l in ${hlinks[$i]}; do
            grep -E " $l$" find.out > /dev/null || error "$l must be in rbh-find output"
            grep -E " $l$" report.out  > /dev/null && ok=1
        done
        [ "$ok" = "0" ] && error "$file or its hardlinks (${hlinks[$i]}) must be in report output"
        ((i++))
    done

    count_nb_init=$(wc -l report.out | awk '{print $1}')
    count_path_init=$(grep -v "$RH_ROOT$" find.out | wc -l)
    echo "nbr_inodes=$count_nb_init, nb_paths=$count_path_init, nb_ln=$nb_ln"
    (( $count_path_init == $count_nb_init + $nb_ln )) || error "nb path != nb_inode + nb_ln"

    # rename entries
    echo "3. Linking/unlinking/renaming objects..."
    # 1) simple file rename
    mv $RH_ROOT/dir.1/file.1 $RH_ROOT/dir.1/file.1.rnm
    # 2) cross directory file rename
    mv $RH_ROOT/dir.1/file.2 $RH_ROOT/dir.2/file.2.rnm
    # 3) rename that deletes the target
    mv -f $RH_ROOT/dir.2/file.1 $RH_ROOT/dir.2/file.2
    # 4) upper level directory rename
    mv $RH_ROOT/dir.3/subdir $RH_ROOT/dir.3/subdir.rnm
    # 5) overwriting a hardlink
    mv -f $RH_ROOT/dir.2/file.4 ${hlinks[0]}
    ((nb_ln--))
    # 6) creating new link to "dir.4/file.1"
    ln "$RH_ROOT/dir.4/file.1" "$RH_ROOT/dir.4/link.1"
    ((nb_ln++))
    # 7) removing 1 link (dir.2/link.2 remains)
    rm "$RH_ROOT/dir.4/file.2"
    ((nb_ln--))
    # 8) removing 1 file
    rm "$RH_ROOT/dir.4/file.3"
    # 9) creating 1 file
    touch "$RH_ROOT/dir.1/new"
    # 10) creating 1 file with hardlink
    touch "$RH_ROOT/dir.2/new"
    ln "$RH_ROOT/dir.2/new" "$RH_ROOT/dir.4/link.new"
    ((nb_ln++))

    # namespace GC needs 1s difference
    sleep 1

    # readlog or re-scan
    if [ "$flavor" = "readlog" ]; then
        echo "4. Reading changelogs..."
    	$RH -f $RBH_CFG_DIR/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
    elif [ "$flavor" = "scan" ]; then
        echo "4. Scanning again..."
    	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning"
    elif [ "$flavor" = "diff" ]; then
        echo "4. Diffing again..."
    	$DIFF -f $RBH_CFG_DIR/$config_file --apply=db -l DEBUG > rh_scan.log 2>&1 || error "scanning"
    elif [ "$flavor" = "partial" ]; then
        i=0
        for d in $dirs_tgt; do
            # namespace GC needs 1s difference
            sleep 1
            ((i++))
            echo "4.$i Partial scan ($d)..."
        	$RH -f $RBH_CFG_DIR/$config_file --scan=$d --once -l DEBUG -L rh_scan.log || error "scanning $d"
        done
    elif [ "$flavor" = "partdiff" ]; then
        i=0
        for d in $dirs_tgt; do
            # namespace GC needs 1s difference
            sleep 1
            ((i++))
            echo "4.$i Partial diff+apply ($d)..."
        	$DIFF -f $RBH_CFG_DIR/$config_file --scan=$d --apply=db -l DEBUG  > rh_scan.log 2>&1 || error "scanning $d"
        done
    fi

    $REPORT -f $RBH_CFG_DIR/$config_file --dump-all -q > report.out || error "$REPORT"
    [ "$DEBUG" = "1" ] && cat report.out

    $FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT -nobulk -ls > find.out || error "$FIND"
    [ "$DEBUG" = "1" ] && cat find.out



    # checking all objects in reports
    for o in $dirs_tgt $files_tgt; do
        grep -E " $o$" report.out > /dev/null || error "$o not found in report"
        grep -E " $o$" find.out > /dev/null || error "$o not found in find"
    done

    for f in $rmids; do
        grep "\[$f\]" find.out && error "deleted id ($f) found in find output"
    done

    unset count_nb_final
    if (( $is_lhsm + $is_hsmlite == 1 )); then
        # check that removed entries are scheduled for HSM rm
        $REPORT -f $RBH_CFG_DIR/$config_file --deferred-rm --csv -q > rh_report.log
        for f in $rmids; do

            # The following test is not critical for partial scanning
            # In the worst case, the deleted entry remains in the archive.
            if [ "$flavor" = "partial" ] || [ "$flavor" = "partdiff" ]; then
                grep "\[$f\]" rh_report.log > /dev/null || echo "WARNING: $f should be in HSM rm list"
                # Conservative behavior for partial scans in front of an archive: allow n/a in paths
                count_nb_final=$(awk '{print $(NF)}' report.out | grep -v 'n/a' | wc -l)
            else
                # in the other cases, raise an error
                grep "\[$f\]" rh_report.log > /dev/null || error "$f should be in HSM rm list"
            fi

            grep -v "\[$f\]" rh_report.log > rh_report.log.1
            mv rh_report.log.1 rh_report.log
        done
        left=$(wc -l rh_report.log | awk '{print $1}')
        if (($left > 0)); then
            error "Some existing entries are scheduled for HSM rm!!!"
            cat rh_report.log
        fi
    fi


    i=0
    while [ -n "${hlink_refs_tgt[$i]}" ]; do
        file="${hlink_refs_tgt[$i]}"
        ok=0
        grep -E " $file$" find.out > /dev/null || error "$file must be in rbh-find output"
        grep -E " $file$" report.out > /dev/null && ok=1
        for l in ${hlinks_tgt[$i]}; do
            grep -E " $l$" find.out > /dev/null || error "$l must be in rbh-find output"
            grep -E " $l$" report.out  > /dev/null && ok=1
        done
        [ "$ok" = "0" ] && error "$file or its hardlinks (${hlinks_tgt[$i]}) must be in report output"
        ((i++))
    done
    [ -z "$count_nb_final" ] && count_nb_final=$(wc -l report.out | awk '{print $1}')
    count_path_final=$(grep -v "$RH_ROOT$" find.out | wc -l)

    echo "nbr_inodes=$count_nb_final, nb_paths=$count_path_final, nb_ln=$nb_ln"
    (( $count_nb_final == $count_nb_init)) || error "same entry count ($count_nb_init) expected (2 deleted, 2 created)"
    (( $count_path_final == $count_nb_final + $nb_ln )) || error "nb path != nb_inode + nb_ln"

    rm -f report.out find.out
}

function test_hl_count
{
	config_file=$1
    dcount=3
    fcount=2

    clean_logs
    # populate file system with simple files

    for d in $(seq 1 $dcount); do
        mkdir $RH_ROOT/dir.$d || error "cannot create $RH_ROOT/dir.$d"
    for f in $(seq 1 $fcount); do
        touch $RH_ROOT/dir.$d/file.$f || error "cannot create $RH_ROOT/dir.$d/file.$f"
    done
    done

    # scan
   	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning $RH_ROOT"

    ino=$(( $dcount * $fcount + $dcount ))
    ino_subdir=$(($fcount + 1))

    # reports to be checked:
    #   dump report (9 entries, no root)
    (($($REPORT -f $RBH_CFG_DIR/$config_file -D -q | wc -l) == $ino )) || error "wrong count in 'rbh-report -D' output"
    #   dump report with path filter (3 entries)
    (($($REPORT -f $RBH_CFG_DIR/$config_file -D -q -P $RH_ROOT/dir.1 | wc -l) == $ino_subdir )) || error "wrong count in 'rbh-report -D -P <path>' output"
    #   dump find output (whole FS) (10 entries, incl. root)
    (($($FIND -f $RBH_CFG_DIR/$config_file -nobulk | wc -l) == $ino + 1))  || error "wrong count in 'rbh-find' output"
    #   dump find output (subdir: 3 entries)
    (($($FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT/dir.1 -nobulk | wc -l) == $ino_subdir )) || error "wrong count in 'rbh-find <path>' output"

    #   dump summary (9 entries)
    $REPORT -f $RBH_CFG_DIR/$config_file -icq > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    typeValues="dir;file"
  	countValues="$dcount;$(($dcount * $fcount))"
    # type counts are in 2nd column
   	colSearch=2
	find_allValuesinCSVreport report.out $typeValues $countValues $colSearch || error "wrong count in 'rbh-report -i' output"

    #   dump summary with path filter (3 entries)
    $REPORT -f $RBH_CFG_DIR/$config_file -iq -P $RH_ROOT/dir.1 > report.out
    [ "$DEBUG" = "1" ] && cat report.out
  	countValues="1;$fcount"
	find_allValuesinCSVreport report.out $typeValues $countValues $colSearch || error "wrong count in 'rbh-report -i -P <path>' output"

    # create 1 hardlink per file and recheck
    for d in $(seq 1 $dcount); do
    for f in $(seq 1 $fcount); do
        ln $RH_ROOT/dir.$d/file.$f $RH_ROOT/dir.$d/link.$f || error "cannot create hardlink $RH_ROOT/dir.$d/link.$f -> $RH_ROOT/dir.$d/file.$f"
    done
    done

    # rescan
   	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning $RH_ROOT"

    paths=$(( $dcount * $fcount * 2 + $dcount ))
    paths_subdir=$(($fcount * 2 + 1))

    #   dump report (still 9 entries, no root)
    (($($REPORT -f $RBH_CFG_DIR/$config_file -D -q | wc -l) == $ino )) || error "wrong count in 'rbh-report -D' output"
    #   dump report with path filter (still 3 entries)
    (($($REPORT -f $RBH_CFG_DIR/$config_file -D -q -P $RH_ROOT/dir.1 | wc -l) == $ino_subdir )) || error "wrong count in 'rbh-report -D -P <path>' output"
    #   dump find output (whole FS) (
    (($($FIND -f $RBH_CFG_DIR/$config_file -nobulk | wc -l) == $paths + 1 ))  || error "wrong count in 'rbh-find' output"
    #   dump find output (subdir: 3 entries)
    (($($FIND -f $RBH_CFG_DIR/$config_file $RH_ROOT/dir.1 -nobulk | wc -l) == $paths_subdir )) || error "wrong count in 'rbh-find <path>' output"

    #   dump summary (9 entries)
    $REPORT -f $RBH_CFG_DIR/$config_file -icq > report.out
    [ "$DEBUG" = "1" ] && cat report.out
  	countValues="$dcount;$(($dcount * $fcount))"
	find_allValuesinCSVreport report.out $typeValues $countValues $colSearch || error "wrong count in 'rbh-report -i' output"

    #   dump summary with path filter (3 entries)
    $REPORT -f $RBH_CFG_DIR/$config_file -iq -P $RH_ROOT/dir.1 > report.out
    [ "$DEBUG" = "1" ] && cat report.out
  	countValues="1;$fcount"
	find_allValuesinCSVreport report.out $typeValues $countValues $colSearch || error "wrong count in 'rbh-report -i -P <path>' output"


    rm -f report.out
}

function test_pools
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

    if [ -n "$POSIX_MODE" ]; then
        echo "No pools support in POSIX mode"
        set_skipped
        return 1
    fi

	create_pools

	clean_logs

	# create files in different pools (or not)
	touch $RH_ROOT/no_pool.1 || error "creating file"
	touch $RH_ROOT/no_pool.2 || error "creating file"
	$LFS setstripe -p lustre.$POOL1 $RH_ROOT/in_pool_1.a || error "creating file in $POOL1"
	$LFS setstripe -p lustre.$POOL1 $RH_ROOT/in_pool_1.b || error "creating file in $POOL1"
	$LFS setstripe -p lustre.$POOL2 $RH_ROOT/in_pool_2.a || error "creating file in $POOL2"
	$LFS setstripe -p lustre.$POOL2 $RH_ROOT/in_pool_2.b || error "creating file in $POOL2"

	sleep $sleep_time

	# read changelogs
	if (( $no_log )); then
		echo "1.1-scan and match..."
		$RH -f $RBH_CFG_DIR/$config_file --scan -l VERB -L rh_chglogs.log  --once || error ""
	else
		echo "1.1-read changelog and match..."
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l VERB -L rh_chglogs.log  --once || error ""
	fi


	echo "1.2-checking report output..."
	# check classes in report output
	$REPORT -f $RBH_CFG_DIR/$config_file --dump-all -c > report.out || error ""
	cat report.out

	echo "1.3-checking robinhood log..."
	grep "Missing attribute" rh_chglogs.log && error "missing attribute when matching classes"

	# fileclass field index
    pf=5

	for i in 1 2; do
        ( [ "`grep "$RH_ROOT/no_pool.$i" report.out | cut -d ',' -f $pf | tr -d ' '`" = "" ] || error "bad fileclass for no_pool.$i" )
	done

	for i in a b; do
	    ( [ "`grep "$RH_ROOT/in_pool_1.$i" report.out | cut -d ',' -f $pf  | tr -d ' '`" = "pool_1" ] || error "bad fileclass for in_pool_1.$i" )

		( [ "`grep "$RH_ROOT/in_pool_2.$i" report.out  | cut -d ',' -f $pf | tr -d ' '`" = "pool_2" ] || error "bad fileclass for in_pool_2.$i" )
	done

	# rematch and recheck
	echo "2.1-scan and match..."
	# read changelogs
	$RH -f $RBH_CFG_DIR/$config_file --scan -l VERB -L rh_chglogs.log  --once || error ""

	echo "2.2-checking report output..."
	# check classes in report output
	$REPORT -f $RBH_CFG_DIR/$config_file --dump-all -c  > report.out || error ""
	cat report.out

	for i in 1 2; do
        ( [ "`grep "$RH_ROOT/no_pool.$i" report.out | cut -d ',' -f $pf | tr -d ' '`" = "" ] || error "bad fileclass for no_pool.$i" )
	done

	for i in a b; do
	    ( [ "`grep "$RH_ROOT/in_pool_1.$i" report.out | cut -d ',' -f $pf  | tr -d ' '`" = "pool_1" ] || error "bad fileclass for in_pool_1.$i" )

		( [ "`grep "$RH_ROOT/in_pool_2.$i" report.out  | cut -d ',' -f $pf | tr -d ' '`" = "pool_2" ] || error "bad fileclass for in_pool_2.$i" )
	done


	echo "2.3-checking robinhood log..."
	grep "Missing attribute" rh_chglogs.log && error "missing attribute when matching classes"

}

function test_logs
{
	config_file=$1
	flavor=$2
	policy_str="$3"

	sleep_time=430 # log rotation time (300) + scan interval (100) + scan duration (30)

	clean_logs
	rm -f /tmp/test_log.1 /tmp/test_report.1 /tmp/test_alert.1 /tmp/extract_all /tmp/extract_log /tmp/extract_report /tmp/extract_alert

	# test flavors (x=supported):
	# x	file_nobatch
	# x 	file_batch
	# x	syslog_nobatch
	# x	syslog_batch
	# x	stdio_nobatch
	# x	stdio_batch
	# 	mix
	files=0
	syslog=0
	batch=0
	stdio=0
	echo $flavor | grep nobatch > /dev/null || batch=1
	echo $flavor | grep syslog_ > /dev/null && syslog=1
	echo $flavor | grep file_ > /dev/null && files=1
	echo $flavor | grep stdio_ > /dev/null && stdio=1
	echo "Test parameters: files=$files, syslog=$syslog, stdio=$stdio, batch=$batch"

	# create files
	touch $RH_ROOT/file.1 || error "creating file"
	touch $RH_ROOT/file.2 || error "creating file"
	touch $RH_ROOT/file.3 || error "creating file"
	touch $RH_ROOT/file.4 || error "creating file"

	if (( $is_lhsm != 0 )); then
		flush_data
		$LFS hsm_archive $RH_ROOT/file.*
		wait_done 60 || error "Copy timeout"
	fi

	if (( $syslog )); then
		init_msg_idx=`wc -l /var/log/messages | awk '{print $1}'`
	fi

    if (( $is_hsmlite != 0 )); then
        extra_action="$SYNC_OPT"
    else
        extra_action=""
    fi

	# run a scan + alert check
	if (( $stdio )); then
		$RH -f $RBH_CFG_DIR/$config_file --scan $extra_action --run=alert -I -l DEBUG --once >/tmp/rbh.stdout 2>/tmp/rbh.stderr || error "scan error $(cat /tmp/rbh.stderr)"
	else
        # detach and wait, else it will log to stderr by default
		$RH -f $RBH_CFG_DIR/$config_file --scan --run=alert -I -l DEBUG --once -d -p pidfile|| error "scan error"
        sleep 2
        [ -f pidfile ] && wait $(cat pidfile)
        ps -edf | grep $RH | grep -v grep
	fi

	if (( $files )); then
		log="/tmp/test_log.1"
		alert="/tmp/test_alert.1"
		report="/tmp/test_report.1"
	elif (( $stdio )); then
                log="/tmp/rbh.stderr"

		if (( $batch )); then
			# batch output to file has no ALERT header on each line
			# we must extract between "ALERT REPORT" and "END OF ALERT REPORT"
        		local old_ifs="$IFS"
        		IFS=$'\t\n :'
			alert_lines=(`grep -n ALERT /tmp/rbh.stdout | cut -d ':' -f 1 | xargs`)
			IFS="$old_ifs"
		#	echo ${alert_lines[0]}
		#	echo ${alert_lines[1]}
			((nbl=${alert_lines[1]}-${alert_lines[0]}+1))
			# extract nbl lines stating from line alert_lines[0]:
			tail -n +${alert_lines[0]} /tmp/rbh.stdout | head -n $nbl > /tmp/extract_alert
		else
			grep ALERT /tmp/rbh.stdout > /tmp/extract_alert
		fi
		# grep 'robinhood\[' => don't select lines with no headers
		grep -v ALERT /tmp/rbh.stdout | egrep -e "($CMD|shook)[^ ]*\[" > /tmp/extract_report
		alert="/tmp/extract_alert"
		report="/tmp/extract_report"
	elif (( $syslog )); then
        # wait for syslog to flush logs to disk
        sync; sleep 2

		tail -n +"$init_msg_idx" /var/log/messages | egrep -e "($CMD|shook)[^ ]*\[" > /tmp/extract_all
		egrep -v 'ALERT' /tmp/extract_all | grep  ': [A-Za-z0-9_ ]* \|' > /tmp/extract_log
		egrep -v 'ALERT|: [A-Za-z0-9_ ]* \|' /tmp/extract_all > /tmp/extract_report
		grep 'ALERT' /tmp/extract_all > /tmp/extract_alert

		log="/tmp/extract_log"
		alert="/tmp/extract_alert"
		report="/tmp/extract_report"
	else
		error ": unsupported test option"
		return 1
	fi

	# check if there is something written in the log
	if [[ -s $log ]]; then
		echo "OK: log file is not empty"
	else
		error ": empty log file"
	fi

	if (( $batch )); then
		#check summary
		sum=`grep "alert summary" $alert | wc -l`
		(($sum==1)) || (error ": no summary found" ; cat $alert)
		# check alerts about file.1 and file.2
		# search for line ' * 1 alert_file1', ' * 1 alert_file2'
		a1=`egrep -e "[0-9]* entry matches 'file1'" $alert | sed -e 's/.* \([0-9]*\) entry.*/\1/' | xargs`
		a2=`egrep -e "[0-9]* entry matches 'file2'" $alert | sed -e 's/.* \([0-9]*\) entry.*/\1/' | xargs`
		e1=`grep ${RH_ROOT}'/file\.1' $alert | wc -l`
		e2=`grep ${RH_ROOT}'/file\.2' $alert | wc -l`
		# search for alert count: "2 alerts:"
		if (($syslog)); then
			all=`egrep -e "\| [0-9]* alerts:" $alert | sed -e 's/.*| \([0-9]*\) alerts:/\1/' | xargs`
		else
			all=`egrep -e "^[0-9]* alerts:" $alert | sed -e 's/^\([0-9]*\) alerts:/\1/' | xargs`
		fi
		if (( $a1 == 1 && $a2 == 1 && $e1 == 1 && $e2 == 1 && $all == 2)); then
			echo "OK: 2 alerts"
		else
			error ": invalid alert counts: $a1,$a2,$e1,$e2,$all"
			cat $alert
		fi
	else
		# check alerts about file.1 and file.2
		a1=`grep file1 $alert | wc -l`
		a2=`grep file2 $alert | wc -l`
		e1=`grep 'Entry: '${RH_ROOT}'/file\.1' $alert | wc -l`
		e2=`grep 'Entry: '${RH_ROOT}'/file\.2' $alert | wc -l`
		all=`grep "Robinhood alert" $alert | wc -l`
		if (( $a1 == 1 && $a2 == 1 && $e1 == 1 && $e2 == 1 && $all == 2)); then
			echo "OK: 2 alerts"
		else
			error ": invalid alert counts: $a1,$a2,$e1,$e2,$all"
			cat $alert
		fi
	fi

	# no purge for now
	if (( `wc -l $report | awk '{print $1}'` == 0 )); then
                echo "OK: no action reported"
        else
                error ": there are reported actions after a scan"
  	            cat $report
        fi

	if (( $is_hsmlite == 0 )); then

		# reinit msg idx
		if (( $syslog )); then
			init_msg_idx=`wc -l /var/log/messages | awk '{print $1}'`
		fi

		# run a purge
		rm -f $log $report $alert

		if (( $stdio )); then
			$RH -f $RBH_CFG_DIR/$config_file $PURGE_OPT -l DEBUG --once --dry-run >/tmp/rbh.stdout 2>/tmp/rbh.stderr || error "run failed"
		else
			$RH -f $RBH_CFG_DIR/$config_file $PURGE_OPT -l DEBUG --once --dry-run -d -p pidfile || error "run failed"
            sleep 2
            [ -f pidfile ] && wait $(cat pidfile)
            ps -edf | grep $RH | grep -v grep
        fi

		# extract new syslog messages
		if (( $syslog )); then
            # wait for syslog to flush logs to disk
            sync; sleep 2
			tail -n +"$init_msg_idx" /var/log/messages | grep $CMD > /tmp/extract_all

			egrep -v 'ALERT' /tmp/extract_all | grep  ': [A-Za-Z0-9_ ]* \|' > /tmp/extract_log
			egrep -v 'ALERT|: [A-Za-Z0-9_ ]* \|' /tmp/extract_all > /tmp/extract_report
			grep 'ALERT' /tmp/extract_all > /tmp/extract_alert

            if [ "$DEBUG" = "1" ]; then
                echo "----- syslog alerts:" ; cat /tmp/extract_alert
                echo "----- syslog actions:" ; cat /tmp/extract_report
                echo "----- syslog traces:" ; cat /tmp/extract_log
            fi
		elif (( $stdio )); then
			grep ALERT /tmp/rbh.stdout > /tmp/extract_alert
			# grep [22909/8] => don't select lines with no headers
			grep -v ALERT /tmp/rbh.stdout | grep "\[[0-9]*/[0-9]*\]" > /tmp/extract_report
            if [ "$DEBUG" = "1" ]; then
                echo "----- stdio alerts:" ; cat /tmp/extract_alert
                echo "----- stdio actions:" ; cat /tmp/extract_report
                echo "----- stdio (all):" ; cat /tmp/rbh.stdout
            fi
		fi

		# check that there is something written in the log
	    if [[ -s $log ]]; then
			echo "OK: log file is not empty"
		else
			error ": empty log file"
		fi

        egrep "summary|Warning" $log

		grep "could not reach the specified" $log > /dev/null
		if (($?)); then
			error ": a warning should have been issued for impossible purge"
		else
			echo "OK: warning issued"
		fi

		# all files must have been purged
		if (( `wc -l $report | awk '{print $1}'` == 4 )); then
			echo "OK: 4 actions reported"
		else
			error ": unexpected count of actions"
			cat $report
		fi

	fi
	(($files==1)) || return 0

	if [[ "x$SLOW" != "x1" ]]; then
		echo "Quick tests only: skipping log rotation test (use SLOW=1 to enable this test)"
		return 1
	fi

	# start a FS scanner with FS_Scan period = 100
	$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -d -p pidfile

	# rotate the logs
	for l in /tmp/test_log.1 /tmp/test_report.1 /tmp/test_alert.1; do
		mv $l $l.old
	done

	sleep $sleep_time

	# check that there is something written in the log
    if [[ -s $log ]]; then
		echo "OK: log file is not empty"
	else
		error ": empty log file"
	fi

	# check alerts about file.1 and file.2
	a1=`grep alert_file1 /tmp/test_alert.1 | wc -l`
	a2=`grep alert_file2 /tmp/test_alert.1 | wc -l`
	e1=`grep 'Entry: '${RH_ROOT}'/file\.1' /tmp/test_alert.1 | wc -l`
	e2=`grep 'Entry: '${RH_ROOT}'/file\.2' /tmp/test_alert.1 | wc -l`
	all=`grep "Robinhood alert" /tmp/test_alert.1 | wc -l`
	if (( $a1 > 0 && $a2 > 0 && $e1 > 0 && $e2 > 0 && $all >= 2)); then
		echo "OK: $all alerts"
	else
		error ": invalid alert counts: $a1,$a2,$e1,$e2,$all"
		cat /tmp/test_alert.1
	fi

	# no purge during scan
	if (( `wc -l /tmp/test_report.1 | awk '{print $1}'` == 0 )); then
                echo "OK: no action reported"
        else
                error ": there are reported actions after a scan"
		cat /tmp/test_report.1
        fi

	[ -f pidfile ] && kill -9 $(cat pidfile)
	rm -f /tmp/test_log.1 /tmp/test_report.1 /tmp/test_alert.1 pidfile
	rm -f /tmp/test_log.1.old /tmp/test_report.1.old /tmp/test_alert.1.old
}

function test_cfg_parsing
{
	flavor=$1
	dummy=$2
	policy_str="$3"

	clean_logs

    # needed for reading password file
    if [[ ! -f /etc/robinhood.d/.dbpassword ]]; then
        if [[ ! -d /etc/robinhood.d ]]; then
            mkdir /etc/robinhood.d
        fi
        echo robinhood > /etc/robinhood.d/.dbpassword
    fi

	GEN_TEMPLATE="/tmp/template.$CMD"
	if [[ $flavor == "basic" ]]; then
		cp -f "$RBH_TEMPLATE_DIR"/basic.conf "$GEN_TEMPLATE"
		sed -i "s/fs_type = .*;/fs_type = $FS_TYPE;/" $GEN_TEMPLATE
	elif [[ $flavor == "example" ]]; then
		# example contains references to Lustre/HSM actions
		if (( lhsm == 0 )); then
			echo "Example uses Lustre/HSM"
		        set_skipped
		        return 1
		fi
		cp -f "$RBH_TEMPLATE_DIR"/example.conf "$GEN_TEMPLATE"
		sed -i "s/fs_type = .*;/fs_type = $FS_TYPE;/" $GEN_TEMPLATE
	elif [[ $flavor == "generated" ]]; then
		$RH --template=$GEN_TEMPLATE || error "generating config template"
	else
		error "invalid test flavor"
		return 1
	fi
	# link to needed files for %includes
	rm -f "/tmp/includes"
	ln -s "$(readlink -m $RBH_TEMPLATE_DIR)"/includes /tmp/includes

	# test parsing
	$RH --test-syntax -f "$GEN_TEMPLATE" 2>rh_syntax.log >rh_syntax.log || error " reading config file \"$GEN_TEMPLATE\""

	cat rh_syntax.log
	grep "unknown parameter" rh_syntax.log > /dev/null && error "unexpected parameter"
	grep "read successfully" rh_syntax.log > /dev/null && echo "OK: parsing succeeded"

	rm -f "$GEN_TEMPLATE"
	rm -f "/tmp/includes"
}

function check_recov_status
{
    local log="$1"
    local p="$2"
    local exp="$3"

    grep "Restoring $p" $log | egrep -e "$exp" > /dev/null || error "Bad status for $p (expected: <$exp> in <$(grep "Restoring $p" $log)>)"
    return $?
}

function recovery_test
{
    config_file=$1
    flavor=$2
    arch_slink=$3
    policy_str="$4"

    if (( $is_hsmlite == 0 )); then
        echo "Backup test only: skipped"
        set_skipped
        return 1
    fi

    clean_logs

    # flavors:
    # full: all entries fully recovered
    # delta: all entries recovered but some with deltas
    # rename: some entries have been renamed since they have been saved
    # partial: some entries can't be recovered
    # mixed: all of them
    if [[ $flavor == "full" ]]; then
        nb_full=20
        nb_empty=2
        nb_rename=0
        nb_empty_rename=0
        nb_delta=0
        nb_nobkp=0
    elif [[ $flavor == "delta" ]]; then
        nb_full=10
        nb_empty=2
        nb_rename=0
        nb_empty_rename=2
        nb_delta=10
        nb_nobkp=0
    elif [[ $flavor == "rename" ]]; then
        nb_full=10
        nb_empty=2
        nb_rename=10
        nb_empty_rename=2
        nb_delta=0
        nb_nobkp=0
    elif [[ $flavor == "partial" ]]; then
        nb_full=10
        nb_empty=2
        nb_rename=0
        nb_empty_rename=0
        nb_delta=0
        nb_nobkp=10
    elif [[ $flavor == "mixed" ]]; then
        nb_full=5
        nb_empty=2
        nb_rename=5
        nb_empty_rename=2
        nb_delta=5
        nb_nobkp=5
    else
        error "Invalid arg in recovery_test"
        return 1
    fi
    # read logs


    # create files
    ((total=$nb_full + $nb_rename + $nb_delta + $nb_nobkp + $nb_empty + $nb_empty_rename))
    ((total_empty=$nb_empty + $nb_empty_rename))
    echo "1.1-creating files..."

    for i in `seq 1 $total`; do
        mkdir "$RH_ROOT/dir.$i" || error "$? creating directory $RH_ROOT/dir.$i"
        if (( $i % 3 == 0 )); then
            chmod 755 "$RH_ROOT/dir.$i" || error "$? setting mode of $RH_ROOT/dir.$i"
        elif (( $i % 3 == 1 )); then
            chmod 750 "$RH_ROOT/dir.$i" || error "$? setting mode of $RH_ROOT/dir.$i"
        elif (( $i % 3 == 2 )); then
            chmod 700 "$RH_ROOT/dir.$i" || error "$? setting mode of $RH_ROOT/dir.$i"
        fi

        if (($i > $total - $total_empty)); then
            # last total_empty are empty...
            touch $RH_ROOT/dir.$i/file.$i || error "$? creating $RH_ROOT/file.$i"
        else
            dd if=/dev/zero of=$RH_ROOT/dir.$i/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "$? writing $RH_ROOT/file.$i"
        fi
    done

    echo "1.2-creating symlinks..."
    for i in `seq 1 $(( $total - $total_empty))`; do
        ln -s "symlink_$i" $RH_ROOT/dir.$i/link.$i  >/dev/null 2>/dev/null || error "$? creating symlink $RH_ROOT/dir.$i/link.$"
    done

    # read changelogs
    if (( $no_log )); then
        echo "1.3-scan..."
        $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "scanning"
    else
        echo "1.3-read changelog..."
        $RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading log"
    fi

    sleep 2

    # all files are new
    new_cnt=`$REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -i | grep file | grep new | cut -d ',' -f 3`
    echo "$new_cnt files are new"
    (( $new_cnt == $total )) || error "20 new files expected"

    na_link=`$REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -i | grep symlink | grep "n/a" | cut -d ',' -f 3`
    new_link=`$REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -i | grep symlink | grep new | cut -d ',' -f 3`
    [[ -z $new_link ]] && new_link=0
    [[ -z $na_link ]] && na_link=0
    echo "$new_link symlinks are new, $na_link are n/a"
    if (( $arch_slink == 0 )); then
        (( $na_link == $total - $total_empty )) || error "$total n/a symlinks expected"
    else
        (( $new_link == $total - $total_empty )) || error "$total new symlinks expected"
    fi

    echo "2.1-archiving objects..."
    # archive and modify files
    for i in `seq 1 $total`; do
        if (( $i <= $nb_full )); then
            $RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:"$RH_ROOT/dir.$i/file.$i" --ignore-conditions -l DEBUG -L rh_migr.log 2>/dev/null \
                || error "archiving $RH_ROOT/dir.$i/file.$i"
            $RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:"$RH_ROOT/dir.$i/link.$i" --ignore-conditions -l DEBUG -L rh_migr.log 2>/dev/null \
                || error "archiving $RH_ROOT/dir.$i/link.$i"
            if (( $arch_slink == 0 )); then
                grep "$RH_ROOT/dir.$i/link.$i" rh_migr.log | grep "bad type for migration" > /dev/null 2> /dev/null \
                    || error "$RH_ROOT/dir.$i/link.$i should not have been migrated"
            fi
        elif (( $i <= $(($nb_full+$nb_rename)) )); then
            $RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:"$RH_ROOT/dir.$i/file.$i" --ignore-conditions -l DEBUG -L rh_migr.log 2>/dev/null \
                || error "archiving $RH_ROOT/dir.$i/file.$i"
            $RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:"$RH_ROOT/dir.$i/link.$i" --ignore-conditions -l DEBUG -L rh_migr.log 2>/dev/null \
                || error "archiving $RH_ROOT/dir.$i/link.$i"
            if (( $arch_slink == 0 )); then
                grep "$RH_ROOT/dir.$i/link.$i" rh_migr.log | grep "bad type for migration" > /dev/null 2> /dev/null \
                    || error "$RH_ROOT/dir.$i/link.$i should not have been migrated"
            fi
            mv "$RH_ROOT/dir.$i/file.$i" "$RH_ROOT/dir.$i/file_new.$i" || error "renaming file"
            mv "$RH_ROOT/dir.$i/link.$i" "$RH_ROOT/dir.$i/link_new.$i" || error "renaming link"
            mv "$RH_ROOT/dir.$i" "$RH_ROOT/dir.new_$i" || error "renaming dir"
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta)) )); then
            $RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:"$RH_ROOT/dir.$i/file.$i" --ignore-conditions -l DEBUG -L rh_migr.log 2>/dev/null \
                || error "archiving $RH_ROOT/dir.$i/file.$i"
            touch "$RH_ROOT/dir.$i/file.$i"
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp)) )); then
            # no backup
            :
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp+$nb_empty)) )); then
            # no backup
            :
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp+$nb_empty+$nb_empty_rename)) )); then
            # no backup, just rename
            mv "$RH_ROOT/dir.$i/file.$i" "$RH_ROOT/dir.$i/file_new.$i" || error "renaming file"
            mv "$RH_ROOT/dir.$i" "$RH_ROOT/dir.new_$i" || error "renaming dir"
        fi
    done

    if (( $no_log )); then
        echo "2.2-scan..."
        $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "scanning"
    else
        echo "2.2-read changelog..."
        $RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading log"
    fi

    $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -i > /tmp/report.$$
    [ "$DEBUG" = "1" ] && cat  /tmp/report.$$

    new_cnt=`grep "new" /tmp/report.$$ | grep file | cut -d ',' -f 3`
    mod_cnt=`grep "modified" /tmp/report.$$ | grep file | cut -d ',' -f 3`
    sync_cnt=`grep "synchro" /tmp/report.$$ | grep file | cut -d ',' -f 3`
    [[ -z $new_cnt ]] && new_cnt=0
    [[ -z $mod_cnt ]] && mod_cnt=0
    [[ -z $sync_cnt ]] && sync_cnt=0

    echo "files: new: $new_cnt, modified: $mod_cnt, synchro: $sync_cnt"
    (( $sync_cnt == $nb_full+$nb_rename )) || error "Nbr of synchro files doesn't match: $sync_cnt != $nb_full + $nb_rename"
    (( $mod_cnt == $nb_delta )) || error "Nbr of modified files doesn't match: $mod_cnt != $nb_delta"
    (( $new_cnt == $nb_nobkp + $nb_empty + $nb_empty_rename )) || error "Nbr of new files doesn't match: $new_cnt != $nb_nobkp + $nb_empty + $nb_empty_rename"

    new_cnt=`grep "new" /tmp/report.$$ | grep symlink | cut -d ',' -f 3`
    na_cnt=`grep "n/a" /tmp/report.$$ | grep symlink | cut -d ',' -f 3`
    sync_cnt=`grep "synchro" /tmp/report.$$ | grep symlink | cut -d ',' -f 3`
    [[ -z $new_cnt ]] && new_cnt=0
    [[ -z $na_cnt ]] && na_cnt=0
    [[ -z $sync_cnt ]] && sync_cnt=0

    echo "symlink: new: $new_cnt, synchro: $sync_cnt, n/a: $na_cnt"
    if (( $arch_slink == 0 )); then
        (( $na_cnt == $total - $total_empty )) || error "Nbr of links with no status doesn't match: $na_cnt != $total - $total_empty"
    else
        (( $sync_cnt == $nb_full+$nb_rename )) || error "Nbr of synchro links doesn't match: $sync_cnt != $nb_full + $nb_rename"
        (( $new_cnt == $nb_nobkp+$nb_delta )) || error "Nbr of new links doesn't match: $new_cnt != $(($nb_nobkp+$nb_delta))"
    fi


    # shots before disaster (time is only significant for files)
    find $RH_ROOT -type f -printf "%n %m %T@ %g %u %s %p %l\n" > /tmp/before.$$
    find $RH_ROOT -type d -printf "%n %m %g %u %s %p %l\n" >> /tmp/before.$$
    find $RH_ROOT -type l -printf "%n %m %g %u %s %p %l\n" >> /tmp/before.$$

    # FS disaster
    if [[ -n "$RH_ROOT" ]]; then
        echo "3-Disaster: all FS content is lost"
        rm  -rf $RH_ROOT/*
    fi

    # perform the recovery
    echo "4-Performing recovery..."
    cp /dev/null recov.log
    $RECOV -f $RBH_CFG_DIR/$config_file --start -l DEBUG >> recov.log 2>&1 || error "Error starting recovery"

    $RECOV -f $RBH_CFG_DIR/$config_file --resume -l DEBUG >> recov.log 2>&1 || error "Error performing recovery"

    $RECOV -f $RBH_CFG_DIR/$config_file --complete -l DEBUG >> recov.log 2>&1 || error "Error completing recovery"

    find $RH_ROOT -type f -printf "%n %m %T@ %g %u %s %p %l\n" > /tmp/after.$$
    find $RH_ROOT -type d -printf "%n %m %g %u %s %p %l\n" >> /tmp/after.$$
    find $RH_ROOT -type l -printf "%n %m %g %u %s %p %l\n" >> /tmp/after.$$

    diff  /tmp/before.$$ /tmp/after.$$ > /tmp/diff.$$
    [ "$DEBUG" = "1" ] && cat  /tmp/diff.$$

    # checking status and diff result
    for i in `seq 1 $total`; do
        if (( $i <= $nb_full )); then
            check_recov_status recov.log "$RH_ROOT/dir.$i/file.$i" "OK\$"
            grep "$RH_ROOT/dir.$i/file.$i" /tmp/diff.$$ && error "$RH_ROOT/dir.$i/file.$i NOT expected to differ"
            check_recov_status recov.log "$RH_ROOT/dir.$i/link.$i" "OK \(non-file\)"
        elif (( $i <= $(($nb_full+$nb_rename)) )); then
            check_recov_status recov.log "$RH_ROOT/dir.new_$i/file_new.$i" "OK\$"
            grep "$RH_ROOT/dir.new$i/link_new.$i" /tmp/diff.$$ && error "$RH_ROOT/dir_new.$i/link_new.$i NOT expected to differ"
            check_recov_status recov.log "$RH_ROOT/dir.new_$i/link_new.$i" "OK \(non-file\)"
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta)) )); then
            check_recov_status recov.log "$RH_ROOT/dir.$i/file.$i" "OK \(old version\)"
            grep "$RH_ROOT/dir.$i/file.$i" /tmp/diff.$$ >/dev/null || error "$RH_ROOT/dir.$i/file.$i is expected to differ"
            # links are never expected to differ as they are stored in the database
            grep "$RH_ROOT/dir.$i/link.$i" /tmp/diff.$$ >/dev/null && error "$RH_ROOT/dir.$i/link.$i NOT expected to differ"
            check_recov_status recov.log "$RH_ROOT/dir.$i/link.$i" "OK \(non-file\)"
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp)) )); then
            check_recov_status recov.log "$RH_ROOT/dir.$i/file.$i" "No backup"
            grep "$RH_ROOT/dir.$i/file.$i" /tmp/diff.$$ >/dev/null || error "$RH_ROOT/dir.$i/file.$i is expected to differ"
            # links are never expected to differ as they are stored in the database
            grep "$RH_ROOT/dir.$i/link.$i" /tmp/diff.$$ >/dev/null && error "$RH_ROOT/dir.$i/link.$i NOT expected to differ"
            check_recov_status recov.log "$RH_ROOT/dir.$i/link.$i" "OK \(non-file\)"
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp+$nb_empty)) )); then
            check_recov_status recov.log "$RH_ROOT/dir.$i/file.$i" "OK \(empty file\)"
            grep "$RH_ROOT/dir.$i/file.$i" /tmp/diff.$$ >/dev/null && error "$RH_ROOT/dir.$i/file.$i is NOT expected to differ"
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp+$nb_empty+$nb_empty_rename)) )); then
            check_recov_status recov.log "$RH_ROOT/dir.new_$i/file_new.$i" "OK \(empty file\)"
            grep "$RH_ROOT/dir.new_$i/file_new.$i" /tmp/diff.$$ >/dev/null && error "$RH_ROOT/dir.$i/file.$i is NOT expected to differ"
        fi
    done

    rm -f /tmp/before.$$ /tmp/after.$$ /tmp/diff.$$
}

function recov_filters
{
    config_file=$1
    flavor=$2

    if [[ $flavor == since ]] && [[ $nolog == 1 ]]; then
        echo "'since' can only be used with changelogs"
        set_skipped
        return 1
    fi
    if (( $is_hsmlite == 0 )); then
        echo "Backup test only: skipped"
        set_skipped
        return 1
    fi

    # start filters: --ost and --since
    # resume filters: --dir

    echo "populating filesystem"
    # create one of each recov status matching or not matching the filter
    # (full, delta, empty, rename, empty_new, empty_rename, nobkp, slink, slink_new)
    mkdir $RH_ROOT/dir.match $RH_ROOT/dir.nomatch || error "mkdir failed"

    for f in full delta rename empty empty_rnm; do
        if [[ $flavor != since ]]; then
            $LFS setstripe -c 1 -i 0 $RH_ROOT/dir.match/$f || error "setstripe failed"
        fi
        $LFS setstripe -c 1 -i 1 $RH_ROOT/dir.nomatch/$f || error "setstripe failed"
    done
    # write data to full and delta
    for f in full delta rename; do
        if [[ $flavor != since ]]; then
            dd if=/dev/zero of=$RH_ROOT/dir.match/$f bs=1M count=5  || error "writing data to $f"
        fi
        dd if=/dev/zero of=$RH_ROOT/dir.nomatch/$f bs=1M count=5  || error "writing data to $f"
    done
    ln -s "this is an initial symlink" $RH_ROOT/dir.nomatch/slink || error "creating symlink"
    if [[ $flavor != ost ]] && [[ $flavor != since ]]; then
        ln -s "this is an initial symlink" $RH_ROOT/dir.match/slink || error "creating symlink slink_new"
    fi

    echo "scan and archive"
    # scan and archive
    $RH -f $RBH_CFG_DIR/$config_file --scan $SYNC_OPT -l DEBUG -L rh_scan.log  --once 2>/dev/null || error "scanning or migrating"

    if [[ $flavor == since ]]; then
	    $LFS changelog_clear lustre-MDT0000 cl1 0
        sleep 1
        # only consider entries modifed from now
        since=$(date +'%Y%m%d%H%M%S')

        for f in full delta rename empty empty_rnm; do
            $LFS setstripe -c 1 -i 0 $RH_ROOT/dir.match/$f || error "setstripe failed"
        done
        # write data to full and delta
        for f in full delta rename; do
            dd if=/dev/zero of=$RH_ROOT/dir.match/$f bs=1M count=5  || error "writing data to $f"
        done
        ln -s "this is an initial symlink" $RH_ROOT/dir.match/slink || error "creating symlink slink_new"

        # don't update non-modified objects, migrate other candidates
        $RH -f $RBH_CFG_DIR/$config_file --readlog $SYNC_OPT -l DEBUG -L rh_scan.log  --once 2>/dev/null || error "reading changelogs"
    fi

    echo "making deltas"
    for f in empty_new nobkp; do
        $LFS setstripe -c 1 -i 0 $RH_ROOT/dir.match/$f
        [[ $flavor != since ]] && $LFS setstripe -c 1 -i 1 $RH_ROOT/dir.nomatch/$f
    done
    for d in match nomatch ; do
        # skip no match if flavor is 'since'
        [[ $flavor == since ]] && [[ $d == nomatch ]] && continue
        echo "sqdlqsldsqmdl" >> $RH_ROOT/dir.$d/delta || error "appending dir.$d/delta"
        # force modification (in case Lustre don't report small data changes)
        touch $RH_ROOT/dir.$d/delta || error "touching dir.$d/delta"
        echo "qsldjkqlsdkqs" >> $RH_ROOT/dir.$d/nobkp || error "writing to dir.$d/nobkp"
        mv $RH_ROOT/dir.$d/rename $RH_ROOT/dir.$d/rename.mv || error "renaming 'rename'"
        mv $RH_ROOT/dir.$d/empty_rnm $RH_ROOT/dir.$d/empty_rnm.mv || error "renaming 'empty_rnm'"
    done
    if [[ $flavor != since ]]; then
        ln -s "this is a new symlink" $RH_ROOT/dir.nomatch/slink_new || error "creating symlink"
    fi
    if [[ $flavor != ost ]]; then
        ln -s "this is a new symlink" $RH_ROOT/dir.match/slink_new || error "creating symlink"
    fi

    if [[ $flavor == since ]]; then
        [ "$DEBUG" = "1" ] && $LFS changelog lustre
        # don't update non-modified objects
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_scan.log  --once 2>/dev/null || error "reading changelogs"
    else
        echo "rescan (no archive)"
        $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log  --once 2>/dev/null || error "scanning"
    fi

    $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -i > report.out
    [ "$DEBUG" = "1" ] && cat report.out

    new_cnt=`grep "new" report.out | grep file | cut -d ',' -f 3`
    mod_cnt=`grep "modified" report.out | grep file | cut -d ',' -f 3`
    sync_cnt=`grep "synchro" report.out | grep file | cut -d ',' -f 3`
    [[ -z $new_cnt ]] && new_cnt=0
    [[ -z $mod_cnt ]] && mod_cnt=0
    [[ -z $sync_cnt ]] && sync_cnt=0

    #          full, delta, empty, rename, empty_new, empty_rename, nobkp
    # synchro:    2             2       2                        2
    # modified:          2
    # new:                                         2                    2
    echo "files: new: $new_cnt, modified: $mod_cnt, synchro: $sync_cnt"
    if [[ $flavor != since ]]; then
        (( $sync_cnt == 8 )) || error "Nbr of synchro files doesn't match: $sync_cnt != 8"
        (( $mod_cnt  == 2 )) || error "Nbr of modified files doesn't match: $mod_cnt != 2"
        (( $new_cnt  == 4 )) || error "Nbr of new files doesn't match: $new_cnt != 4"
    else
        (( $sync_cnt == 9 )) || error "Nbr of synchro files doesn't match: $sync_cnt != 9"
        (( $mod_cnt  == 1 )) || error "Nbr of modified files doesn't match: $mod_cnt != 1"
        (( $new_cnt  == 2 )) || error "Nbr of new files doesn't match: $new_cnt != 2"
    fi
    # FS disaster
    if [[ -n "$RH_ROOT" ]]; then
        echo "3-Disaster: all FS content is lost"
        rm  -rf $RH_ROOT/*
    fi

    # perform the recovery
    echo "4-Performing recovery..."
    cp /dev/null recov.log

    case "$flavor" in
        ost)
            start_option="--ost 0"
            resume_option=""
            matching=(full delta empty empty_rnm.mv empty_new rename.mv nobkp)
            status=("OK" "OK \(old version\)" "OK" "OK" "OK \(empty file\)" "OK" "No backup")
            ;;
        since)
            start_option="--since=$since"
            resume_option=""
            matching=(full delta empty empty_rnm.mv empty_new rename.mv nobkp slink slink_new)
            status=("OK" "OK \(old version\)" "OK" "OK" "OK \(empty file\)" "OK" "No backup")
            ;;
        dir)
            start_option=""
            resume_option="--dir=$RH_ROOT/dir.match"
            matching=(full delta empty empty_rnm.mv empty_new rename.mv nobkp slink slink_new)
            status=("OK" "OK \(old version\)" "OK" "OK" "OK \(empty file\)" "OK" "No backup" "OK \(non-file \)" "OK \(non-file \)")
            ;;
    esac

    $RECOV -f $RBH_CFG_DIR/$config_file --start $start_option -l FULL >> recov.log 2>&1 || error "Error starting recovery"
    $RECOV -f $RBH_CFG_DIR/$config_file --resume $resume_option -l DEBUG >> recov.log 2>&1 || error "Error performing recovery"
    if [[ $flavor != dir ]]; then # for dirs, cannot complete as long as it is only for parallelizing the recovery
        $RECOV -f $RBH_CFG_DIR/$config_file --complete -l DEBUG >> recov.log 2>&1 || error "Error completing recovery"
    fi

    # check that all matching entries are recovered with the appropriate status
    [ "$DEBUG" = "1" ] && grep Restoring recov.log
    for i in $(seq 1 ${#matching[@]}); do
        f=${matching[$i]}
        s=${status[$i]}
        check_recov_status recov.log $RH_ROOT/dir.match/$f $s
    done
    (( $(grep Restoring recov.log | wc -l) == ${#matching[@]} )) || error "Too many files restored"

    (( $NB_ERROR == 0 )) && echo OK
}

function test_tokudb
{
    # Check we are using MariaDB
    rpm -qi MariaDB-common || {
        echo "MariaDB not installed: skipped"
        set_skipped
        return 1
    }

    # TokuDB must be available too
    mysql $RH_DB -e "show engines" | grep TokuDB || {
        echo "TokuDB not enabled: skipped"
        set_skipped
        return 1
    }

    clean_logs

    # Create the tables with various compression schemes.

    echo "Test without default compression, i.e. none"
    $CFG_SCRIPT empty_db $RH_DB > /dev/null
    $RH -f $RBH_CFG_DIR/tokudb1.conf --scan -l DEBUG -L rh_scan.log --once || error ""
    mysql $RH_DB -e "show create table ENTRIES;" |
        grep "ENGINE=TokuDB .*\`COMPRESSION\`=tokudb_uncompressed" ||
        error "invalid engine/compression"

    echo "Tests with valid compression names"
    for COMPRESS in tokudb_uncompressed tokudb_zlib tokudb_lzma ; do
        $CFG_SCRIPT empty_db $RH_DB > /dev/null
        RBH_TOKU_COMPRESS=$COMPRESS $RH -f $RBH_CFG_DIR/tokudb2.conf --scan -l DEBUG -L rh_scan.log --once || error ""
        mysql $RH_DB -e "show create table ENTRIES;" |
            grep "ENGINE=TokuDB .*\`COMPRESSION\`=${COMPRESS}" ||
            error "invalid engine/compression"
    done

    echo "Test with invalid compression name"
    $CFG_SCRIPT empty_db $RH_DB > /dev/null
    RBH_TOKU_COMPRESS=some_non_existent_compression $RH -f $RBH_CFG_DIR/tokudb2.conf --scan -l DEBUG -L rh_scan.log --once &&
        error "should have failed"
    grep "Error: Incorrect value 'some_non_existent_compression' for option 'compression'" rh_scan.log ||
        error "expected error not found"
}

function test_cfg_overflow
{
    clean_logs

    # fs_key is harcoded as 128 bytes max. Try various lengths.

    echo "Test with a valid key"
    FS_KEY="fsname" $RH -f $RBH_CFG_DIR/overflow.conf --test-syntax |
        grep "has been read successfully" || error "valid config failed"

    echo "Test with an invalid key, at the size limit"
    FS_KEY="ghfkfkjghsdfklhgjklsdfhgkdjfhgkljfhgkljdfghlkfjghkjfhgjklhkljdfhsglkjfhlkgjhflkjghdflkjhgldfksjhglkdfjhglkjdfhglkjdfhglkjfdhglk" $RH -f $RBH_CFG_DIR/overflow.conf --test-syntax |&
        grep "Invalid type for fs_key" || error "unexpected result for invalid key"

    echo "Test with a key 1 character too long"
    FS_KEY="ghfkfkjghsdfklhgjklsdfhgkdjfhgkljfhgkljdfghlkfjghkjfhgjklhkljdfhsglkjfhlkgjhflkjghdflkjhgldfksjhglkdfjhglkjdfhglkjdfhglkjfdhglkq" $RH -f $RBH_CFG_DIR/overflow.conf --test-syntax |&
        grep "Option too long for parameter 'General::fs_key'" || error "unexpected result for 127 chars key"

    echo "Test with a key several characters too long"
    FS_KEY="ghfkfkjghsdfklhgjklsdfhgkdjfhgkllkhfglkhjgyugfhlgfghfhfhhfkdhliutylkrhgkjdfshgskjjfhgkljdfghlkfjghkjfhgjklhkljdfhsglkjfhlkgjhflkjghdflkjhgldfksjhglkdfjhglkjdfhglkjdfhglkjfdhglkq" $RH -f $RBH_CFG_DIR/overflow.conf --test-syntax |&
        grep "Option too long for parameter 'General::fs_key'" || error "unexpected result for too long key"


}

# Test various aspects of rbh-find -printf
function test_rbh_find_printf
{
    # Populate the database with a single file
    config_file=$1

    clean_logs

    # use rh_cksum.sh from scripts directory
    if [ -d "../../src/robinhood" ]; then
        export PATH="$PATH:../../scripts/"
    # else use the installed one
    fi

    echo "Initial scan of empty filesystem"
    $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""

    # create a file
    echo "1-Creating file..."
    local srcfile=$RH_ROOT/test_printf/testf
    rm -f $srcfile
    mkdir -p $RH_ROOT/test_printf/
    dd if=/dev/zero of=$srcfile bs=1k count=1 >/dev/null 2>/dev/null || error "writing file"

    local fid=$(get_id "$srcfile")

    if (( $no_log )); then
        echo "2-Scanning..."
        $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
    else
        echo "2-Reading changelogs..."
        $RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
    fi
    check_db_error rh_chglogs.log

    if (( $is_lhsm != 0 )); then
        echo "3-Archiving the files"
        $LFS hsm_archive $srcfile || error "executing lfs hsm_archive"

        wait_hsm_state $srcfile 0x00000009

	echo "4-Reading changelogs..."
	$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	check_db_error rh_chglogs.log
    fi

    echo "5-Run checksum policy"
    local before_run=$(date +%s)
    $RH -f $RBH_CFG_DIR/$config_file --run=checksum --target=all -I -l DEBUG -L stdout | grep "Policy run summary"
    local after_run=$(date +%s)

    echo "6-rbh-find checks"

    # Basic functionality
    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "")
    [[ $STR == "" ]] || error "unexpected rbh-find result (001): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "some string")
    [[ $STR == "some string" ]] || error "unexpected rbh-find result (002): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "some string %p")
    [[ $STR == "some string $srcfile" ]] || error "unexpected rbh-find result (003): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "some string %p after")
    [[ $STR == "some string $srcfile after" ]] || error "unexpected rbh-find result (004): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "X%%Y")
    [[ $STR == "X%Y" ]] || error "unexpected rbh-find result (005): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "some string %%%p after")
    [[ $STR == "some string %$srcfile after" ]] || error "unexpected rbh-find result (006): $STR"

    # Test each directive
    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "blocks=%b")
    #[[ $STR == "blocks=8" ]] || error "unexpected rbh-find result (100): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%f bar")
    [[ $STR == "testf bar" ]] || error "unexpected rbh-find result (101): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "group is %g")
    [[ $STR == "group is $root_str" ]] || error "unexpected rbh-find result (102): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "hi   %M is mask")
    [[ $STR == "hi   rw-r--r-- is mask" ]] || error "unexpected rbh-find result (103): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "octal mask twice : %m %m")
    [[ $STR == "octal mask twice : 644 644" ]] || error "unexpected rbh-find result (104): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "nlinks: %n")
    [[ $STR == "nlinks: 1" ]] || error "unexpected rbh-find result (105): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%p\n")
    [[ $STR == "$srcfile" ]] || error "unexpected rbh-find result (106): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "size %s\n")
    [[ $STR == "size 1024" ]] || error "unexpected rbh-find result (107): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "owner %u\n")
    [[ $STR == "owner $root_str" ]] || error "unexpected rbh-find result (108): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "type %Y\n")
    [[ $STR == "type file" ]] || error "unexpected rbh-find result (109): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "short type %y\n")
    [[ $STR == "short type f" ]] || error "unexpected rbh-find result (110): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "short type %y\n")
    [[ $STR == "short type f" ]] || error "unexpected rbh-find result (111): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%p\n")
    [[ $STR == "$srcfile" ]] || error "unexpected rbh-find result (112): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%f\n")
    [[ $STR == "testf" ]] || error "unexpected rbh-find result (113): $STR"

    STR=$($FIND $RH_ROOT/ -nobulk -type f -f $RBH_CFG_DIR/$config_file -printf "%p\n")
    [[ $STR == "$srcfile" ]] || error "unexpected rbh-find result (114): $STR"

    STR=$($FIND $RH_ROOT/ -nobulk -type f -f $RBH_CFG_DIR/$config_file -printf "%f\n")
    [[ $STR == "testf" ]] || error "unexpected rbh-find result (115): $STR"

    STR=$($FIND $RH_ROOT/test_printf -type f -f $RBH_CFG_DIR/$config_file -printf "%p\n")
    [[ $STR == "$srcfile" ]] || error "unexpected rbh-find result (116): $STR"

    STR=$($FIND $RH_ROOT/test_printf -type f -f $RBH_CFG_DIR/$config_file -printf "%f\n")
    [[ $STR == "testf" ]] || error "unexpected rbh-find result (117): $STR"

    # Test each Robinhood sub-directive
    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf " %Rc rh class\n")
    [[ $STR == " [none] rh class" ]] || error "unexpected rbh-find result (200): $STR"

    if (( $lustre_major >= 2 )); then
	# exact match
        STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf " %Rf fid\n")
        [[ $STR == " $fid fid" ]] || error "unexpected rbh-find result (201): $STR ($fid expected)"
    else
	# get_id returns '/<inode>' so we must get <something>/<inode>
        STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%Rf fid\n")
        [[ $STR == *"$fid fid" ]] || error "unexpected rbh-find result (201): $STR ($fid expected)"
    fi

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf " %Ro osts\n")
    #[[ $STR == "ost#0:1044 osts" ]] || error "unexpected rbh-find result (202): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "parent fid=%Rp\n")
    #[[ $STR == "parent fid=0x200000007:0x1:0x0" ]] || error "unexpected rbh-find result (203): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%RCF")
    [[ $STR == "$(date +%F)" ]] || error "unexpected rbh-find result (204): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%AF")
    [[ $STR == "$(date +%F)" ]] || error "unexpected rbh-find result (205): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%TF")
    [[ $STR == "$(date +%F)" ]] || error "unexpected rbh-find result (206): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%CF")
    [[ $STR == "$(date +%F)" ]] || error "unexpected rbh-find result (206b): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%A-" 2>&1)
    [[ $STR == *"%-"* ]] || error "unexpected rbh-find result (207): $STR" # NB: invalid format

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%TG")
    [[ $STR == "$(date +%G)" ]] || error "unexpected rbh-find result (208): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%CG")
    [[ $STR == "$(date +%G)" ]] || error "unexpected rbh-find result (209): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "QWERTY %RCc %TA %CA %Ap %AT" 2>&1)
    [[ $STR == *"QWERTY"* ]] || error "unexpected rbh-find result (210): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "QWERTY %RCOe %TOS %COS %AEx %AEY" 2>&1)
    [[ $STR == *"QWERTY"* ]] || error "unexpected rbh-find result (211): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "QWERTY %RCOA" 2>&1)
    [[ $STR == *"QWERTY %OA"* ]] || error "unexpected rbh-find result (212): $STR" # NB: invalid format

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "QWERTY %RCEB" 2>&1)
    [[ $STR == *"QWERTY %EB"* ]] || error "unexpected rbh-find result (213): $STR" # NB: invalid format

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%RC{%A, %B %dth, %Y %F}" 2>&1)
    [[ $STR == *"Error:"* ]] && error "unexpected rbh-find result (214): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%RC{%A, " 2>&1)
    [[ $STR == *"Error: invalid string format"* ]] || error "unexpected rbh-find result (215): $STR"

    # Test various combinations
    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "FILE %p %s %Y %y %Rc %u %n and stop\n")
    [[ $STR == "FILE $srcfile 1024 file f [none] $root_str 1 and stop" ]] || error "unexpected rbh-find result (300): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%s\t%d\t%y")
    [[ $STR == "1024	1	f" ]] || error "unexpected rbh-find result (301): $STR"

    # Test module attributes
    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%Rm{}" 2>&1)
    [[ $STR == *"Error: cannot extract module attribute name"* ]] || error "unexpected rbh-find result (400): $STR"

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%Rm{nonexistentmod}" 2>&1)
    [[ $STR == *"Error: cannot extract module attribute name"* ]] || error "unexpected rbh-find result (401): $STR"

    if (( $is_lhsm != 0 )); then
        STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%Rm{lhsm.no_such_sym}" 2>&1)
        [[ $STR == *"Error: cannot extract module attribute name"* ]] || error "unexpected rbh-find result (402): $STR"

        STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%Rm{lhsm.archive_id}")
        [[ $STR == "1" ]] || error "unexpected rbh-find result (403): $STR"

        STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%Rm{lhsm.no_release}")
        [[ $STR == "0" ]] || error "unexpected rbh-find result (404): $STR"

        STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%Rm{lhsm.no_archive}")
        [[ $STR == "0" ]] || error "unexpected rbh-find result (405): $STR"

        STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%Rm{lhsm.status}")
        [[ $STR == "synchro" ]] || error "unexpected rbh-find result (406): $STR"
    fi

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%Rm{checksum.last_check}")
    # last check must be between before_run and after_run
    if [[ -z "$STR" ]] || (( $STR < $before_run )) || (( $STR > $after_run )); then
        error "Unexpected checksum timestamp (407): $STR"
    fi

    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%Rm{checksum.status}")
    [[ $STR == "ok" ]] || error "unexpected rbh-find result (408): $STR"

    # With some formatting options
    STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "file='%15f' size=%09s")
    [[ $STR == "file='          testf' size=000001024" ]] || error "unexpected rbh-find result (500): $STR"

    if (( $is_lhsm != 0 )); then
        STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%7Rm{lhsm.archive_id}")
        [[ $STR == "      1" ]] || error "unexpected rbh-find result (501): $STR"

        STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%07Rm{lhsm.archive_id}")
        [[ $STR == "0000001" ]] || error "unexpected rbh-find result (502): $STR"

        STR=$($FIND $RH_ROOT/ -type f -f $RBH_CFG_DIR/$config_file -printf "%-Rm{lhsm.archive_id}")
        [[ $STR == "1" ]] || error "unexpected rbh-find result (503): $STR"
    fi

    rm -f report.out
}

function import_test
{
	config_file=$1
	flavor=$2
	policy_str="$3"

	if (( $is_hsmlite == 0 )); then
		echo "Backup test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

    ensure_init_backend || error "Error initializing backend $BKROOT"

    # initial scan
    echo "0- initial scan..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --once -l DEBUG -L rh_chglogs.log 2>/dev/null || error "scanning"


    # create files in backend
    echo "1- populating backend (import dir)..."

    # empty dir1
    mkdir -p $BKROOT/import/dir1
    # dir2 with files and subdir
    mkdir -p $BKROOT/import/dir2/sub1 #subdir with files
    mkdir -p $BKROOT/import/dir2/sub2 #empty subdir
    # files
    dd if=/dev/zero of=$BKROOT/import/dir2/file1 bs=1k count=5 2>/dev/null || error "creating file"
    dd if=/dev/zero of=$BKROOT/import/dir2/file2 bs=1k count=10 2>/dev/null || error "creating file"
    dd if=/dev/zero of=$BKROOT/import/dir2/sub1/file3 bs=1k count=15 2>/dev/null || error "creating file"
    dd if=/dev/zero of=$BKROOT/import/dir2/sub1/file4 bs=1k count=20 2>/dev/null || error "creating file"
    ln -s "dummy symlink content" $BKROOT/import/dir2/sub1/link.1 || error "creating symlink"
    ln -s "file4" $BKROOT/import/dir2/sub1/link.2 || error "creating symlink"

    chown -hR testuser:testgroup $BKROOT/import || error "setting user/group in $BKROOT/import"
    # different times
    touch -t 201012011234 $BKROOT/import/dir2/file1
    touch -t 201012021234 $BKROOT/import/dir2/file2
    touch -t 201012031234 $BKROOT/import/dir2/sub1/file3
    touch -t 201012041234 $BKROOT/import/dir2/sub1/file4
    # different rights
    chmod 755 $BKROOT/import/dir1
    chmod 750 $BKROOT/import/dir2
    chmod 700 $BKROOT/import/dir2/sub1
    chmod 644 $BKROOT/import/dir2/file1
    chmod 640 $BKROOT/import/dir2/file2
    chmod 600 $BKROOT/import/dir2/sub1/file3
    chmod 755 $BKROOT/import/dir2/sub1/file4

    find $BKROOT -printf "%i %M %u %g %s %T@ %p\n" | sort -k 11 > bk.1

    # 5 directories (imported, dir1, dir2, sub1, sub2) , 4 files, 2 links
    expect_cnt=11

	# perform the import
    echo "2- import to $RH_ROOT..."
    $IMPORT -l DEBUG -f $RBH_CFG_DIR/$config_file $BKROOT/import  $RH_ROOT/dest > recov.log 2>&1 || error "importing data from backend"

    [ "$DEBUG" = "1" ] && cat recov.log

    # "Import summary: 9 entries imported, 0 errors"
    info=$(grep "Import summary: " recov.log | awk '{print $3"-"$6}')
    [ "$info" = "$expect_cnt-0" ] || error "unexpected count of imported entries or errors: expected $expect_cnt-0, got $info"

    rm -f recov.log

    # check that every dir has been imported to Lustre
    echo "3.1-checking dirs..."
    while read i m u g s t p; do
        newp=$(echo $p | sed -e "s#$BKROOT/import#$RH_ROOT/dest#")
	[[ -d $newp ]] || error "Missing dir $newp"
        read pi pm pu pg ps pt < <(stat --format "%i %A %U %G %s %Y" $newp || error "Missing dir $newp")
        [[ $pm == $m ]] || error "$newp has bad rights $pm<>$m"
        [[ $pu == $u ]] || error "$newp has bad user $pu<>$u"
        [[ $pg == $g ]] || error "$newp has bad group $pg<>$g"
    done < <(egrep "^[0-9]+ d" bk.1 | grep import)

    # check that every file has been imported to Lustre with the same size, owner, rights, time
    # TODO and it has been moved in backed
    echo "3.2-checking files..."
    while read i m u g s t p; do
        newp=$(echo $p | sed -e "s#$BKROOT/import#$RH_ROOT/dest#")
	[[ -f $newp ]] || error "Missing file $newp"
        read pi pm pu pg ps pt < <(stat --format "%i %A %U %G %s %Y" $newp || error "Missing file $newp")
        # /!\ on some OS, mtime is retruned as "<epoch>.0000000"
        t=$(echo "$t" | sed -e "s/\.0000000000//")
        [[ $ps == $s ]] || error "$newp has bad size $ps<>$s"
        [[ $pm == $m ]] || error "$newp has bad rights $pm<>$m"
        [[ $pu == $u ]] || error "$newp has bad user $pu<>$u"
        [[ $pg == $g ]] || error "$newp has bad group $pg<>$g"
        [[ $pt == $t ]] || error "$newp has bad mtime $pt<>$t"

        newb=$(echo $p | sed -e "s#$BKROOT/import#$BKROOT/dest#")
        ls -d ${newb}__* || error "${newb}__* not found in backend"

    done < <(egrep "^[0-9]+ -" bk.1 | grep import)

    # check that every link  has been imported to Lustre with the same content, owner, rights
    # TODO and it has been moved in backed
    echo "3.3-checking symlinks..."
    while read i m u g s t p; do
        newp=$(echo $p | sed -e "s#$BKROOT/import#$RH_ROOT/dest#")
	[[ -L $newp ]] || error "Missing symlink $newp"
        read pi pm pu pg ps pt < <(stat --format "%i %A %U %G %s %Y" $newp || error "Missing symlink $newp")
        t=$(echo "$t" | sed -e "s/\.0000000000//")
        [[ $ps == $s ]] || error "$newp has bad size $ps<>$s"
        [[ $pm == $m ]] || error "$newp has bad rights $pm<>$m"
        [[ $pu == $u ]] || error "$newp has bad user $pu<>$u"
        [[ $pg == $g ]] || error "$newp has bad group $pg<>$g"

        newb=$(echo $p | sed -e "s#$BKROOT/import#$BKROOT/dest#")
        ls -d ${newb}__* || error "${newb}__* not found in backend"
    done < <(egrep "^[0-9]+ l" bk.1 | grep import)


    rm -f bk.1

    return 0


	# read changelogs to check there is no side effect
	if (( $no_log )); then
		echo "1.2-scan..."
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "scanning"
	else
		echo "1.2-read changelog..."
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading log"
	fi

	sleep 2

	# all files are new
	new_cnt=`$REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -i | grep new | cut -d ',' -f 3`
	echo "$new_cnt files are new"
	(( $new_cnt == $total )) || error "20 new files expected"

	echo "2.1-archiving files..."
	# archive and modify files
	for i in `seq 1 $total`; do
		if (( $i <= $nb_full )); then
			$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:"$RH_ROOT/dir.$i/file.$i" --ignore-conditions -l DEBUG -L rh_migr.log 2>/dev/null \
				|| error "archiving $RH_ROOT/dir.$i/file.$i"
		elif (( $i <= $(($nb_full+$nb_rename)) )); then
			$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:"$RH_ROOT/dir.$i/file.$i" --ignore-conditions -l DEBUG -L rh_migr.log 2>/dev/null \
				|| error "archiving $RH_ROOT/dir.$i/file.$i"
			mv "$RH_ROOT/dir.$i/file.$i" "$RH_ROOT/dir.$i/file_new.$i" || error "renaming file"
			mv "$RH_ROOT/dir.$i" "$RH_ROOT/dir.new_$i" || error "renaming dir"
		elif (( $i <= $(($nb_full+$nb_rename+$nb_delta)) )); then
			$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:"$RH_ROOT/dir.$i/file.$i" --ignore-conditions -l DEBUG -L rh_migr.log 2>/dev/null \
				|| error "archiving $RH_ROOT/dir.$i/file.$i"
			touch "$RH_ROOT/dir.$i/file.$i"
		elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp)) )); then
			# no backup
			:
		fi
	done

	if (( $no_log )); then
		echo "2.2-scan..."
		$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "scanning"
	else
		echo "2.2-read changelog..."
		$RH -f $RBH_CFG_DIR/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading log"
	fi

	$REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR --csv -i > /tmp/report.$$
	new_cnt=`grep "new" /tmp/report.$$ | cut -d ',' -f 3`
	mod_cnt=`grep "modified" /tmp/report.$$ | cut -d ',' -f 3`
	sync_cnt=`grep "synchro" /tmp/report.$$ | cut -d ',' -f 3`
	[[ -z $new_cnt ]] && new_cnt=0
	[[ -z $mod_cnt ]] && mod_cnt=0
	[[ -z $sync_cnt ]] && sync_cnt=0

	echo "new: $new_cnt, modified: $mod_cnt, synchro: $sync_cnt"
	(( $sync_cnt == $nb_full+$nb_rename )) || error "Nbr of synchro files doesn't match: $sync_cnt != $nb_full + $nb_rename"
	(( $mod_cnt == $nb_delta )) || error "Nbr of modified files doesn't match: $mod_cnt != $nb_delta"
	(( $new_cnt == $nb_nobkp )) || error "Nbr of new files doesn't match: $new_cnt != $nb_nobkp"

	# shots before disaster (time is only significant for files)
	find $RH_ROOT -type f -printf "%n %m %T@ %g %u %s %p %l\n" > /tmp/before.$$
	find $RH_ROOT -type d -printf "%n %m %g %u %s %p %l\n" >> /tmp/before.$$
	find $RH_ROOT -type l -printf "%n %m %g %u %s %p %l\n" >> /tmp/before.$$

	# FS disaster
	if [[ -n "$RH_ROOT" ]]; then
		echo "3-Disaster: all FS content is lost"
		rm  -rf $RH_ROOT/*
	fi

	# perform the recovery
	echo "4-Performing recovery..."
	cp /dev/null recov.log
	$RECOV -f $RBH_CFG_DIR/$config_file --start -l DEBUG >> recov.log 2>&1 || error "Error starting recovery"

	$RECOV -f $RBH_CFG_DIR/$config_file --resume -l DEBUG >> recov.log 2>&1 || error "Error performing recovery"

	$RECOV -f $RBH_CFG_DIR/$config_file --complete -l DEBUG >> recov.log 2>&1 || error "Error completing recovery"

	find $RH_ROOT -type f -printf "%n %m %T@ %g %u %s %p %l\n" > /tmp/after.$$
	find $RH_ROOT -type d -printf "%n %m %g %u %s %p %l\n" >> /tmp/after.$$
	find $RH_ROOT -type l -printf "%n %m %g %u %s %p %l\n" >> /tmp/after.$$

	diff  /tmp/before.$$ /tmp/after.$$ > /tmp/diff.$$

	# checking status and diff result
	for i in `seq 1 $total`; do
		if (( $i <= $nb_full )); then
			grep "Restoring $RH_ROOT/dir.$i/file.$i" recov.log | egrep -e "OK\$" >/dev/null || error "Bad status (OK expected)"
			grep "$RH_ROOT/dir.$i/file.$i" /tmp/diff.$$ && error "$RH_ROOT/dir.$i/file.$i NOT expected to differ"
		elif (( $i <= $(($nb_full+$nb_rename)) )); then
			grep "Restoring $RH_ROOT/dir.new_$i/file_new.$i" recov.log	| egrep -e "OK\$" >/dev/null || error "Bad status (OK expected)"
			grep "$RH_ROOT/dir.new_$i/file_new.$i" /tmp/diff.$$ && error "$RH_ROOT/dir.new_$i/file_new.$i NOT expected to differ"
		elif (( $i <= $(($nb_full+$nb_rename+$nb_delta)) )); then
			grep "Restoring $RH_ROOT/dir.$i/file.$i" recov.log	| grep "OK (old version)" >/dev/null || error "Bad status (old version expected)"
			grep "$RH_ROOT/dir.$i/file.$i" /tmp/diff.$$ >/dev/null || error "$RH_ROOT/dir.$i/file.$i is expected to differ"
		elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp)) )); then
			grep -A 1 "Restoring $RH_ROOT/dir.$i/file.$i" recov.log | grep "No backup" >/dev/null || error "Bad status (no backup expected)"
			grep "$RH_ROOT/dir.$i/file.$i" /tmp/diff.$$ >/dev/null || error "$RH_ROOT/dir.$i/file.$i is expected to differ"
		fi
	done

	rm -f /tmp/before.$$ /tmp/after.$$ /tmp/diff.$$
}


function check_find
{
    dir=$1
    args=$2
    count=$3

    [ "$DEBUG" = "1" ] && echo "==================="
    [ "$DEBUG" = "1" ] && echo $FIND $args $dir
    [ "$DEBUG" = "1" ] && $FIND -d VERB $args $dir -l

    c=`$FIND $args $dir | wc -l`
    (( $c == $count )) || error "find: $count entries expected in $dir, got: $c"

    # same test with '-ls option'
    c=`$FIND $args $dir -ls | wc -l`
    (( $c == $count )) || error "find -ls: $count entries expected in $dir, got: $c"
}

function test_find
{
	cfg=$RBH_CFG_DIR/$1
	opt=$2
	policy_str="$3"

	clean_logs

    # by default stripe all files on 0 and 1
    if [ -z "$POSIX_MODE" ]; then
	    $LFS setstripe -c 2 -i 0 $RH_ROOT || echo "error setting stripe on root"
    fi
    # 1) create a FS tree with several levels:
    #   root
    #       file.1
    #       file.2
    #       dir.1
    #       dir.2
    #           file.1
    #           file.2
    #           dir.1
    #           dir.2
    #               file.1
    #               file.2
    #               dir.1
    touch $RH_ROOT/file.1 || error "creating file"
    chown daemon:bin $RH_ROOT/file.1
    touch $RH_ROOT/file.2 || error "creating file"
    chown bin:wheel $RH_ROOT/file.2
    mkdir $RH_ROOT/dir.1 || error "creating dir"
    mkdir $RH_ROOT/dir.2 || error "creating dir"
    dd if=/dev/zero of=$RH_ROOT/dir.2/file.1 bs=1k count=10 2>/dev/null || error "creating file"
    if [ -z "$POSIX_MODE" ]; then
	    $LFS setstripe -c 1 -i 1 $RH_ROOT/dir.2/file.2 || error "creating file with stripe"
    else
        touch $RH_ROOT/dir.2/file.2 || error "creating file"
    fi
    mkdir $RH_ROOT/dir.2/dir.1 || error "creating dir"
    mkdir $RH_ROOT/dir.2/dir.2 || error "creating dir"
    dd if=/dev/zero of=$RH_ROOT/dir.2/dir.2/file.1 bs=1M count=1 2>/dev/null || error "creating file"
    if [ -z "$POSIX_MODE" ]; then
	    $LFS setstripe -c 1 -i 0 $RH_ROOT/dir.2/dir.2/file.2 || error "creating file with stripe"
    else
        touch $RH_ROOT/dir.2/dir.2/file.2 || error "creating file"
    fi
    mkdir $RH_ROOT/dir.2/dir.2/dir.1 || error "creating dir"

    # scan FS content
    $RH -f $cfg --scan -l DEBUG -L rh_scan.log --once 2>/dev/null || error "scanning"

    # 2) test find at several levels
    echo "checking find list at all levels..."
    check_find "" "-f $cfg" 12 # should return all (including root)
    check_find "" "-f $cfg -b" 12 # should return all (including root)
    check_find $RH_ROOT "-f $cfg" 12 # should return all (including root)
    check_find $RH_ROOT/file.1 "-f $cfg" 1 # should return only the file
    check_find $RH_ROOT/dir.1 "-f $cfg" 1  # should return dir.1
    check_find $RH_ROOT/dir.2 "-f $cfg" 8  # should return dir.2 + its content
    check_find $RH_ROOT/dir.2/file.2 "-f $cfg" 1  # should return dir.2/file.2
    check_find $RH_ROOT/dir.2/dir.1 "-f $cfg" 1  # should return dir2/dir.1
    check_find $RH_ROOT/dir.2/dir.2 "-f $cfg" 4  # should return dir.2/dir.2 + its content
    check_find $RH_ROOT/dir.2/dir.2/file.1 "-f $cfg" 1  # should return dir.2/dir.2/file.1
    check_find $RH_ROOT/dir.2/dir.2/dir.1 "-f $cfg" 1 # should return dir.2/dir.2/dir.1

    # 3) test -td / -tf
    echo "testing type filter (-type d)..."
    check_find "" "-f $cfg -type d" 6 # should return all (including root)
    check_find "" "-f $cfg -type d -b" 6 # should return all (including root)
    check_find $RH_ROOT "-f $cfg -type d" 6 # 6 including root
    check_find $RH_ROOT/dir.2 "-f $cfg -type d" 4 # 4 including dir.2
    check_find $RH_ROOT/dir.2/dir.2 "-f $cfg -type d" 2 # 2 including dir.2/dir.2
    check_find $RH_ROOT/dir.1 "-f $cfg -type d" 1
    check_find $RH_ROOT/dir.2/dir.1 "-f $cfg -type d" 1
    check_find $RH_ROOT/dir.2/dir.2/dir.1 "-f $cfg -type d" 1

    echo "testing type filter (-type f)..."
    check_find "" "-f $cfg -type f" 6
    check_find "" "-f $cfg -type f -b" 6
    check_find $RH_ROOT "-f $cfg -type f" 6
    check_find $RH_ROOT/dir.2 "-f $cfg -type f" 4
    check_find $RH_ROOT/dir.2/dir.2 "-f $cfg -type f" 2
    check_find $RH_ROOT/dir.1 "-f $cfg -type f" 0
    check_find $RH_ROOT/dir.2/dir.1 "-f $cfg -type f" 0
    check_find $RH_ROOT/dir.2/dir.2/dir.1 "-f $cfg -type f" 0
    check_find $RH_ROOT/file.1 "-f $cfg -type f" 1
    check_find $RH_ROOT/dir.2/file.1 "-f $cfg -type f" 1

    echo "testing name filter..."
    check_find "" "-f $cfg -name dir.*" 5 # 5
    check_find "" "-f $cfg -name dir.* -b" 5 # 5
    check_find $RH_ROOT "-f $cfg -name dir.*" 5 # 5
    check_find $RH_ROOT "-f $cfg -not -name dir.*" 7 # all except 5
    check_find $RH_ROOT/dir.2 "-f $cfg -name dir.*" 4 # 4 including dir.2
    check_find $RH_ROOT/dir.2/dir.2 "-f $cfg -name dir.*" 2 # 2 including dir.2/dir.2
    check_find $RH_ROOT/dir.1 "-f $cfg -name dir.*" 1
    check_find $RH_ROOT/dir.2/dir.1 "-f $cfg -name dir.*" 1
    check_find $RH_ROOT/dir.2/dir.2/dir.1 "-f $cfg -name dir.*" 1

    echo "testing name filter (case insensitive)..."
    check_find "" "-f $cfg -iname Dir.*" 5 # match all "dir.*"
    check_find "" "-f $cfg -b -iname Dir.*" 5
    check_find "" "-f $cfg -iname dir.*" 5 # match all "dir.*"
    check_find "" "-f $cfg -b -iname dir.*" 5
    check_find $RH_ROOT "-f $cfg -name Dir.*" 0 # no match expected
    check_find $RH_ROOT "-f $cfg -b -name Dir.*" 0
    check_find $RH_ROOT "-f $cfg -iname Dir.*" 5 # match all "dir.*"
    check_find $RH_ROOT "-f $cfg -b -iname Dir.*" 5
    check_find $RH_ROOT "-f $cfg -iname dir.*" 5 # match all "dir.*"
    check_find $RH_ROOT "-f $cfg -b -iname dir.*" 5
    check_find $RH_ROOT "-f $cfg -not -iname Dir.*" 7 # all (12) except 5
    check_find $RH_ROOT "-f $cfg -b -not -iname Dir.*" 7
    check_find $RH_ROOT "-f $cfg -not -iname dir.*" 7 # all (12) except 5
    check_find $RH_ROOT "-f $cfg -b -not -iname dir.*" 7

    echo "testing size filter..."
    check_find "" "-f $cfg -type f -size +2k" 2
    check_find "" "-f $cfg -type f -size +2k -b" 2
    check_find $RH_ROOT "-f $cfg -type f -size +2k" 2
    check_find $RH_ROOT "-f $cfg -type f -size +11k" 1
    check_find $RH_ROOT "-f $cfg -type f -size +1M" 0
    check_find $RH_ROOT "-f $cfg -type f -size 1M" 1
    check_find $RH_ROOT "-f $cfg -type f -size 10k" 1
    check_find $RH_ROOT "-f $cfg -type f -size -1M" 5
    check_find $RH_ROOT "-f $cfg -type f -size -10k" 4

    echo "testing user/group filter..."
    check_find $RH_ROOT "-f $cfg -user daemon" 1
    check_find $RH_ROOT "-f $cfg -user bin" 1
    check_find $RH_ROOT "-f $cfg -user adm" 0
    check_find $RH_ROOT "-f $cfg -not -user adm" 12
    check_find $RH_ROOT "-f $cfg -not -user daemon" 11
    check_find $RH_ROOT "-f $cfg -not -user bin" 11

    check_find $RH_ROOT "-f $cfg -group bin" 1
    check_find $RH_ROOT "-f $cfg -group wheel" 1
    check_find $RH_ROOT "-f $cfg -group sys" 0
    check_find $RH_ROOT "-f $cfg -not -group sys" 12
    check_find $RH_ROOT "-f $cfg -not -group bin" 11
    check_find $RH_ROOT "-f $cfg -not -group wheel" 11

    check_find $RH_ROOT "-f $cfg -user daemon -group bin" 1
    check_find $RH_ROOT "-f $cfg -user daemon -group wheel" 0
    check_find $RH_ROOT "-f $cfg -user daemon -not -group wheel" 1
    check_find $RH_ROOT "-f $cfg -not -user daemon -not -group wheel" 10
    check_find $RH_ROOT "-f $cfg -not -user daemon -not -group wheel -type f" 4

    if [ -z "$POSIX_MODE" ]; then
        echo "testing ost filter..."
        check_find "" "-f $cfg -ost 0" 5 # all files but 1
        check_find "" "-f $cfg -ost 0 -b" 5 # all files but 1
        check_find $RH_ROOT "-f $cfg -ost 0" 5 # all files but 1
        check_find $RH_ROOT "-f $cfg -ost 1" 5 # all files but 1
        check_find $RH_ROOT/dir.2/dir.2 "-f $cfg -ost 1" 1  # all files in dir.2 but 1
    fi

    echo "testing mtime filter..."
    check_find "" "-f $cfg -mtime +1d" 0  #none
    check_find "" "-f $cfg -mtime -1d" 12 #all
    check_find "" "-f $cfg -mtime +1d -b" 0  #none
    check_find "" "-f $cfg -mtime -1d -b" 12 #all
    # change last day
    check_find $RH_ROOT "-f $cfg -mtime +1d" 0  #none
    check_find $RH_ROOT "-f $cfg -mtime -1d" 12 #all
    # the same with another syntax
    check_find $RH_ROOT "-f $cfg -mtime +1" 0  #none
    check_find $RH_ROOT "-f $cfg -mtime -1" 12 #all
    # without 2 hour
    check_find $RH_ROOT "-f $cfg -mtime +2h" 0  #none
    check_find $RH_ROOT "-f $cfg -mtime -2h" 12 #all
    # the same with another syntax
    check_find $RH_ROOT "-f $cfg -mtime +120m" 0  #none
    check_find $RH_ROOT "-f $cfg -mtime -120m" 12 #all
    # the same with another syntax
    check_find $RH_ROOT "-f $cfg -mmin +120" 0  #none
    check_find $RH_ROOT "-f $cfg -mmin -120" 12 #all

    # restore default striping
    if [ -z "$POSIX_MODE" ]; then
        $LFS setstripe -c 2 -i -1 $RH_ROOT
    fi
}

function test_du
{
	cfg=$RBH_CFG_DIR/$1
	opt=$2
	policy_str="$3"

	clean_logs

    # 1) create a FS tree with several levels and sizes:
    #   root
    #       file.1          1M
    #       file.2          1k
    #       dir.1
    #           file.1      2k
    #           file.2      10k
    #           link.1
    #       dir.2
    #           file.1      1M
    #           file.2      1
    #           link.1
    #           dir.1
    #           dir.2
    #               file.1 0
    #               file.2 0
    #               dir.1
    dd if=/dev/zero of=$RH_ROOT/file.1 bs=1M count=1 2>/dev/null || error "creating file"
    dd if=/dev/zero of=$RH_ROOT/file.2 bs=1k count=1 2>/dev/null || error "creating file"

    mkdir $RH_ROOT/dir.1 || error "creating dir"
    dd if=/dev/zero of=$RH_ROOT/dir.1/file.1 bs=1k count=2 2>/dev/null || error "creating file"
    dd if=/dev/zero of=$RH_ROOT/dir.1/file.2 bs=10k count=1 2>/dev/null || error "creating file"
    ln -s "content1" $RH_ROOT/dir.1/link.1 || error "creating symlink"

    mkdir $RH_ROOT/dir.2 || error "creating dir"
    dd if=/dev/zero of=$RH_ROOT/dir.2/file.1 bs=1M count=1 2>/dev/null || error "creating file"
	dd if=/dev/zero of=$RH_ROOT/dir.2/file.2 bs=1 count=1 2>/dev/null || error "creating file"
    ln -s "content2" $RH_ROOT/dir.2/link.1 || error "creating symlink"
    mkdir $RH_ROOT/dir.2/dir.1 || error "creating dir"
    mkdir $RH_ROOT/dir.2/dir.2 || error "creating dir"
    touch $RH_ROOT/dir.2/dir.2/file.1 || error "creating file"
	touch $RH_ROOT/dir.2/dir.2/file.2 || error "creating file"
    mkdir $RH_ROOT/dir.2/dir.2/dir.1 || error "creating dir"

    # write blocks to disk
    sync

    # scan FS content
    $RH -f $cfg --scan -l DEBUG -L rh_scan.log --once 2>/dev/null || error "scanning"

    # test byte display on root
    size=$($DU -f $cfg -t f -b $RH_ROOT | awk '{print $1}')
    [ $size = "2110465" ] || error "bad returned size $size: 2110465 expected"

    # test on subdirs
    size=$($DU -f $cfg -t f -b $RH_ROOT/dir.1 | awk '{print $1}')
    [ $size = "12288" ] || error "bad returned size $size: 12288 expected"

    # block count is hard to predict (due to ext4 prealloc)
    # only test 1st digit
    kb=$($DU -f $cfg -t f -k $RH_ROOT | awk '{print $1}')
    [[ $kb = 2??? ]] || error "nb 1K block should be about 2k+smthg (got $kb)"

    # 2 (for 2MB) + 1 for small files
    mb=$($DU -f $cfg -t f -m $RH_ROOT | awk '{print $1}')
    [[ $mb = 3 ]] || error "nb 1M block should be 3 (got $mb)"

    # count are real
    nb_file=$($DU -f $cfg -t f -c $RH_ROOT | awk '{print $1}')
    nb_link=$($DU -f $cfg -t l -c $RH_ROOT | awk '{print $1}')
    nb_dir=$($DU -f $cfg -t d -c $RH_ROOT | awk '{print $1}')
    [[ $nb_file = 8 ]] || error "found $nb_file files/8"
    [[ $nb_dir = 6 ]] || error "found $nb_dir dirs/6"
    [[ $nb_link = 2 ]] || error "found $nb_link links/2"

}

function check_disabled
{
       config_file=$1
       flavor=$2
       policy_str="$3"

       clean_logs

       case "$flavor" in
               purge)
                       cmd='--run=purge'
                       match='Policy purge is disabled'
                       ;;
               migration)
                       if (( $is_hsmlite + $is_lhsm == 0 )); then
                               echo "hsmlite or HSM test only: skipped"
                               set_skipped
                               return 1
                       fi
                       cmd='--run=migration'
                       match='Policy migration is disabled'
                       ;;
               hsm_remove)
                       if (( $is_hsmlite + $is_lhsm == 0 )); then
                               echo "hsmlite or HSM test only: skipped"
                               set_skipped
                               return 1
                       fi
                       cmd='--run=hsm_remove'
                       match='Policy hsm_remove is disabled'
                       ;;
               rmdir)
                       cmd='--run=rmdir_empty'
                       match='Policy rmdir_empty is disabled'
                       ;;
               class)
                       cmd='--scan'
                       match='disabling fileclass matching'
                       ;;
               *)
                       error "unexpected flavor $flavor"
                       return 1 ;;
       esac

       echo "1.1. Performing action $cmd (daemon mode)..."
       # run with --scan, to keep the daemon alive (else, it would have nothing to do)
       $RH -f $RBH_CFG_DIR/$config_file --scan $cmd -l DEBUG -L rh_scan.log -p rh.pid &

       sleep 2
       echo "1.2. Checking that kill -HUP does not terminate the process..."
       kill -HUP $(cat rh.pid)
       sleep 2
       [[ -f /proc/$(cat rh.pid)/status ]] || error "process terminated on kill -HUP"

       kill $(cat rh.pid)
       sleep 2
       rm -f rh.pid

       grep "$match" rh_scan.log || error "log should contain \"$match\""

       cp /dev/null rh_scan.log
       echo "2. Performing action $cmd (one shot)..."
        $RH -f $RBH_CFG_DIR/$config_file $cmd --once -l DEBUG -L rh_scan.log

       grep "$match" rh_scan.log || error "log should contain \"$match\""

}

function test_reload
{
    config_file=$1

    clean_logs

    # create test cases
    $RBH_TESTS_DIR/fill_fs.sh $RH_ROOT 100 >/dev/null

    # create a tmp copy of the config to modify it
    cfg=$RBH_CFG_DIR/${config_file}.COPY.conf
    cp -f $RBH_CFG_DIR/$config_file $cfg

    # run regular scan + alerts
    export ALERT_CLASS=size10k
    $RH -f $cfg --scan --run=alert -L rh_scan.log -l DEBUG -p rh.pid &

    # check the effect of reload
    sleep 2 # wait for full rbh initialization
    # change config file
    echo "EntryProcessor { nb_threads = 2; }" >> $cfg
    kill -HUP $(cat rh.pid)
    sleep 2 # signal processing loop awakes every second

    # check the signal is properly received
    grep "SIGHUP received" rh_scan.log || error "Signal not received"
    # check the reload operation is properly triggered
    grep "Reloading configuration" rh_scan.log ||
        error "config reload not triggered"

    grep "Failure reloading" rh_scan.log && error "failed to parse cfg"

    # check config parsing is triggered for submodules
    grep "Loading policies config"  rh_scan.log ||
        error "Config parsing not triggered for submodules"

    # check the config change is taken into account (to be completed)
    grep "EntryProcessor::nb_threads changed" rh_scan.log ||
        error "Parameter change not taken into account"

    kill $(cat rh.pid)
    rm -f $cfg
    return 0
}

function test_lhsm_archive
{
    # test_lhsm1.conf "check sql query string in case of multiple AND/OR"

    if (( $is_lhsm == 0 )); then
        echo "Lustre/HSM test only: skipped"
        set_skipped
        return 1
    fi

    config_file=$1
    rm -f rh_archive.log

    # run one pass lhsm_archive - need full scan first
    $RH -f $RBH_CFG_DIR/$config_file --scan --once 2>&1 > /dev/null
    $RH -f $RBH_CFG_DIR/$config_file --run=lhsm_archive -L rh_archive.log -l FULL -O

    # check
    grep "AS id FROM ENTRIES" rh_archive.log |
      grep "AND (((ENTRIES.lhsm_lstarc=0" |
      grep -q "ENTRIES.last_mod IS NULL))) AND (ENTRIES.last_access" ||
      error "lhsm_archive query begin blocks incorrect"

    grep "Error 7 executing query" rh_archive.log > /dev/null &&
      error "lhsm_archive DB query failure"

    return 0
}

function test_multirule_select
{
    # test_multirule.conf "check sql query string in case of multiple rules"

    config_file=$1
    logfile=rh_multirule.log
    rm -f $logfile

    # run one pass lhsm_archive - need full scan first
    $RH -f $RBH_CFG_DIR/$config_file --scan --once 2>&1 > /dev/null
    $RH -f $RBH_CFG_DIR/$config_file --run=cleanup -L $logfile -l FULL -O

    # check
    grep "AS id FROM ENTRIES" $logfile |
      grep "OR ENTRIES.invalid IS NULL) AND ((((ENTRIES.last_access" |
      grep "OR ENTRIES.last_mod IS NULL" |
      grep -q "AND NOT (ENTRIES.fileclass LIKE BINARY '%+foo_files+%')" ||
      error "multirule_select query block incorrect"

    grep "Error 7 executing query" $logfile > /dev/null &&
      error "multirule_select DB query failure"

    return 0
}


function test_rmdir_depth
{
    # test_rmdir_depth.conf "check sql query for rmdir with depth condition"

    config_file=$1
    logfile=rh_rmdir.log
    rm -f $logfile

    export MATCH_PATH="$RH_ROOT"

    # run one pass lhsm_archive - need full scan first
    $RH -f $RBH_CFG_DIR/$config_file --scan --once 2>&1 > /dev/null
    $RH -f $RBH_CFG_DIR/$config_file --run=rmdir_empty -L $logfile -l FULL -O \
        2>/dev/null

    # make sure query succeeds

    grep -q "SELECT ENTRIES.id AS id FROM ENTRIES WHERE ENTRIES.type='dir'" $logfile ||
      error "rmdir depth check DB query failure"

    grep "Error 7 executing query" $logfile > /dev/null &&
      error "rmdir depth check DB query failure"

    return 0
}

function test_prepost_cmd
{
    local config_file=$1
    local output

    clean_logs

    # genrate and export command line to the robinhood configuration
    local testfile=$(mktemp)
    export pre_command="./prepost_cmd.sh init $testfile ABC"
    export post_command="./prepost_cmd.sh append $testfile DEF"

    # scan and apply a purge policy
    $RH -f $RBH_CFG_DIR/$config_file --scan --run=purge --target=all -l DEBUG \
        -L rh_purge.log --once  2>/dev/null

    grep "Executing pre_run_command" rh_purge.log ||
        error "pre_run_command not executed"
    grep "Executing post_run_command" rh_purge.log ||
        error "post_run_command not executed"

    # check that pre/post run cmd have been run
    output="$(head -n 1 $testfile)"
    [[ "$output" == "ABC" ]] ||
        error "Unexpected contents in test file: '$output' ('ABC' expected)"
    output="$(tail -n 1 $testfile)"
    [[ "$output" == "DEF" ]] ||
        error "Unexpected contents in test file: '$output' ('DEF' expected)"

    # check that a faulty precommand aborts the run
    export pre_command="FOO BAR"

    :> rh_purge.log
    $RH -f $RBH_CFG_DIR/$config_file --run=purge --target=all -l DEBUG \
        -L rh_purge.log --once  2>/dev/null

    grep "Aborting policy run because pre_run_commmand failed" rh_purge.log ||
        error "Policy run should have been aborted"
    grep " 0 successful actions" rh_purge.log ||
        error "No successful action expected"

    rm -f $testfile
}


#############################################################################


only_test=""
quiet=0
junit=0

while getopts qj o
do	case "$o" in
	q)	quiet=1;;
	j)	junit=1;;
	[?])	print >&2 "Usage: $0 [-q] [-j] test_nbr ..."
		exit 1;;
	esac
done
shift $(($OPTIND-1))

if [[ -n "$1" ]]; then
	only_test=$1

    # prepare only_test variable
    # 1,2 => ,1,2,
    only_test=",$only_test,"
fi


# initialize tmp files for XML report
function junit_init
{
	cp /dev/null $TMPXML_PREFIX.stderr
	cp /dev/null $TMPXML_PREFIX.stdout
	cp /dev/null $TMPXML_PREFIX.tc
}

# report a success for a test
function junit_report_success # (class, test_name, time)
{
	class="$1"
	name="$2"
	time="$3"

	# remove quotes in name
	name=`echo "$name" | sed -e 's/"//g'`

	echo "<testcase classname=\"$class\" name=\"$name\" time=\"$time\" />" >> $TMPXML_PREFIX.tc
}

# report a failure for a test
function junit_report_failure # (class, test_name, time, err_type)
{
	class="$1"
	name="$2"
	time="$3"
	err_type="$4"

	# remove quotes in name
	name=`echo "$name" | sed -e 's/"//g'`

	echo "<testcase classname=\"$class\" name=\"$name\" time=\"$time\">" >> $TMPXML_PREFIX.tc
	echo -n "<failure type=\"$err_type\"><![CDATA[" >> $TMPXML_PREFIX.tc
	cat $TMPERR_FILE	>> $TMPXML_PREFIX.tc
	echo "]]></failure>" 	>> $TMPXML_PREFIX.tc
	echo "</testcase>" 	>> $TMPXML_PREFIX.tc
}

function junit_write_xml # (time, nb_failure, tests)
{
	time=$1
	failure=$2
	tests=$3

	cp /dev/null $XML

	echo "<?xml version=\"1.0\" encoding=\"ISO8859-2\" ?>" > $XML
	echo "<testsuite name=\"robinhood.LustreTests\" errors=\"0\" failures=\"$failure\" tests=\"$tests\" time=\"$time\">" >> $XML
	sed 's/[^[:print:]]//g' $TMPXML_PREFIX.tc >> $XML
	echo -n "<system-out><![CDATA[" >> $XML
	sed 's/[^[:print:]]//g' $TMPXML_PREFIX.stdout >> $XML
	echo "]]></system-out>"		>> $XML
	echo -n "<system-err><![CDATA[" >> $XML
	sed 's/[^[:print:]]//g' $TMPXML_PREFIX.stderr >> $XML
	echo "]]></system-err>" 	>> $XML
	echo "</testsuite>"		>> $XML
}


function cleanup
{
	echo "Filesystem cleanup..."
    if (( $quiet == 1 )); then
            clean_fs | tee "rh_test.log" | egrep -i -e "OK|ERR|Fail|skip|pass"
    else
            clean_fs
    fi
}

function run_test
{
    export index=$1
    # last argument
    title=${!#}

	shift

	index_clean=`echo $index | sed -e 's/[a-z]//'`

    if [[ -z "$only_test" || $only_test = *",$index_clean,"* || $only_test = *",$index,"* ]]; then
		cleanup
		echo
		echo "==== TEST #$index $2 ($title) ===="

		error_reset

		t0=`date "+%s.%N"`
        echo "Test start: `date +'%F %H:%M:%S.%N'`"

		if (($junit == 1)); then
			# markup in log
			echo "==== TEST #$index $2 ($title) ====" >> $TMPXML_PREFIX.stdout
			echo "==== TEST #$index $2 ($title) ====" >> $TMPXML_PREFIX.stderr
			"$@" 2>> $TMPXML_PREFIX.stderr >> $TMPXML_PREFIX.stdout
		elif (( $quiet == 1 )); then
			"$@" 2>&1 > rh_test.log
			egrep -i -e "OK|ERR|Fail|skip|pass" rh_test.log
		else
			"$@"
		fi

		t1=`date "+%s.%N"`
		dur=`echo "($t1-$t0)" | bc -l`
        echo "Test end: `date +'%F %H:%M:%S.%N'`"
		echo "duration: $dur sec"

		if (( $DO_SKIP )); then
			echo "(TEST #$index : skipped)" >> $SUMMARY
			SKIP=$(($SKIP+1))
		elif (( $NB_ERROR > 0 )); then
			grep "Failed" ${LOGS[*]} 2>/dev/null
			echo "TEST #$index : *FAILED*" >> $SUMMARY
			RC=$(($RC+1))
			if (( $junit )); then
				junit_report_failure "robinhood.$PURPOSE.Lustre" "Test #$index: $title" "$dur" "ERROR"
			fi
		else
			grep "Failed" ${LOGS[*]} 2>/dev/null
			echo "TEST #$index : OK" >> $SUMMARY
			SUCCESS=$(($SUCCESS+1))
			if (( $junit )); then
				junit_report_success "robinhood.$PURPOSE.Lustre" "Test #$index: $title" "$dur"
			fi
		fi
	fi
}



###############################################
############### Alert Functions ###############
###############################################

function test_alerts
{
	# send an alert in accordance to the input file and configuration
	# 	test_alerts config_file testKey sleepTime
	#=>
	# config_file == config file name
	# testKey == 'extAttributes' for testing extended attributes
	# 	     'lastAccess' for testing last access
	# 	     'lastModif' for testing last modification
	# sleepTime == expected time in second to sleep for the test, if=0 no sleep

	# get input parameters ....................
	config_file=$1
	testKey=$2  #== key word for specific tests
	sleepTime=$3

	clean_logs

	test -f "/tmp/rh_alert.log" || touch "/tmp/rh_alert.log"

	echo "1-Preparing Filesystem..."
	if [ $testKey == "extended_attribute" ]; then
		echo " is for extended attributes"
		echo "data" > $RH_ROOT/file.1
		echo "data" > $RH_ROOT/file.2
		echo "data" > $RH_ROOT/file.3
		echo "data" > $RH_ROOT/file.4
		setfattr -n user.foo -v "abc.1.log" $RH_ROOT/file.1
		setfattr -n user.foo -v "abc.6.log" $RH_ROOT/file.3
		setfattr -n user.bar -v "abc.3.log" $RH_ROOT/file.4
	else
		mkdir -p $RH_ROOT/dir1
		dd if=/dev/zero of=$RH_ROOT/dir1/file.1 bs=1k count=11 >/dev/null 2>/dev/null || error "writing file.1"

		mkdir -p $RH_ROOT/dir2
		dd if=/dev/zero of=$RH_ROOT/dir2/file.2 bs=1k count=10 >/dev/null 2>/dev/null || error "writing file.2"
  		chown testuser $RH_ROOT/dir2/file.2 || error "invalid chown on user 'testuser' for $RH_ROOT/dir2/file.2"
		dd if=/dev/zero of=$RH_ROOT/dir2/file.3 bs=1k count=1 >/dev/null 2>/dev/null || error "writing file.3"
		ln -s $RH_ROOT/dir1/file.1 $RH_ROOT/dir1/link.1 || error "creating hardlink $RH_ROOT/dir1/link.1"

		if  [ $testKey == "nonempty_dir" ]; then
			# add a folder with one file
			mkdir -p $RH_ROOT/dir3
		    dd if=/dev/zero of=$RH_ROOT/dir3/file.4 bs=1k count=1 >/dev/null 2>/dev/null || error "writing file.4"
		fi
	fi
	# optional sleep process ......................
	if [ $sleepTime != 0 ]; then
		echo "wait $sleepTime seconds ..."
		sleep $sleepTime
	fi
	# specific optional action after sleep process ..........
	if [ $testKey == "last_access_1min" ]; then
		head $RH_ROOT/dir1/file.1 > /dev/null || error "opening $RH_ROOT/dir1/file.1"
	elif [ $testKey == "last_mod_1min" ]; then
		echo "data" > $RH_ROOT/dir1/file.1 || error "writing in $RH_ROOT/dir1/file.1"
	fi

    export ALERT_CLASS=$testKey

	echo "2-Scanning filesystem..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=alert -l MAJOR -I --once || error "scan+alert error"

	echo "3-Checking results..."
	logFile=/tmp/rh_alert.log
	case "$testKey" in
		file1)
			expectedEntry="file.1 "
			occur=1
			;;
		type_file)
			expectedEntry="file.1;file.2;file.3"
			occur=3
			;;
		root_owner)
			expectedEntry="file.1;file.3"
			occur=2
			;;
		size10k)
			expectedEntry="file.1;file.2"
			occur=2
			;;
		last_access_1min)
			expectedEntry="file.1 "
			occur=1
			;;
		last_mod_1min)
			expectedEntry="file.1 "
			occur=1
			;;
		nonempty_dir)
			expectedEntry="dir1;dir2"
			occur=2
			;;
		extended_attribute)
			expectedEntry="file.1"
			occur=1
			;;
		*)
			error "unexpected testKey $testKey"
			return 1 ;;
	esac

	# launch the validation for all alerts
	check_alert $testKey $expectedEntry $occur $logFile
	res=$?

	if (( $res == 1 )); then
		error "Test for $testKey failed"
	fi

	echo "end...."
}

function test_alerts_OST
{
	# send an alert in accordance to the input file and configuration
	# 	test_alerts_OST config_file
	#=>
	# config_file == config file name

	# get input parameters ....................
	config_file=$1
	testKey=$2  #== key word for specific tests

    if [ -n "$POSIX_MODE" ]; then
        echo "No OST support for POSIX mode"
        set_skipped
        return 1
    fi

	clean_logs

	echo "1-Create Pools ..."
	create_pools

	echo "2-Create Files ..."
    for i in `seq 1 2`; do
		$LFS setstripe  -p lustre.$POOL1 $RH_ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

    for i in `seq 3 5`; do
		$LFS setstripe  -p lustre.$POOL2 $RH_ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

    export ALERT_CLASS=$testKey

	echo "2-Scanning filesystem..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=alert -l MAJOR -I --once || error "scan+alert error"

	echo "3-Checking results..."
	logFile=/tmp/rh_alert.log
	expectedEntry="file.3;file.4;file.5"
	occur=3

	# launch the validation for all alerts
	check_alert $testKey $expectedEntry $occur $logFile
	res=$?

	if (( $res == 1 )); then
		error "Test for $testKey failed"
	fi
}

function check_alert
{
# return 0 if the $alertKey is found $occur times in the log $logFile; and if each entry of
# $expectedEntries is found at least one time
# return 1 otherwise and print an error message
#    check_alert $alertKey $expectedEntry $occur $logFile
# =>
#	alertKey = alert name which is the string to find $occur times
#	expectedEntries = list of word to find at least one time if alertKey is found
#		ex: expectedEntry="file.1;file.2;file.3", expectedEntry="file.1" ...
#	occur = expected nb of occurrences for alertKey
#	logFile = name of the file to scan

	# get input parameters ......................
	alertKey=$1
	expectedEntries=$2
	occur=$3
	logFile=$4

	# set default output value .................
	out=1
	# get all entries separated by ';' ..........
	splitEntries=$(echo $expectedEntries | tr ";" "\n")

	# get the nb of alertKey found in log ........
	nbOccur=`grep -c $alertKey $logFile`
	if [ $nbOccur == $occur ]; then
		# search the appropriated filename ...
		for entry in $splitEntries
    		do
			#  get the nb of filename found in log
       			nbOccur=`grep -c $entry $logFile`
			if [ $nbOccur != 0 ]; then
				out=0
			else
				# the entry has been not found
				echo "ERROR in check_alert: Entry $entry not found"
				return 1
			fi
    		done

	else
		# the alertKey has been not found as expected
		echo "ERROR in check_alert: Bad number of occurrences for $alertKey: expected=$occur & found=$nbOccur"
		return 1
	fi

	return $out
}

###################################################
############### End Alert Functions ###############
###################################################

###################################################
############### Migration Functions ###############
###################################################

function create_files_migration
{
	# create all directory and files for migration tests
	#  create_files_migration

	mkdir $RH_ROOT/dir1
	mkdir $RH_ROOT/dir2

	for i in `seq 1 5` ; do
		dd if=/dev/zero of=$RH_ROOT/dir1/file.$i bs=1K count=1 >/dev/null 2>/dev/null || error "writing dir1/file.$i"
	done

	ln -s $RH_ROOT/dir1/file.1 $RH_ROOT/dir1/link.1
	ln -s $RH_ROOT/dir1/file.1 $RH_ROOT/dir1/link.2

	chown root:testgroup $RH_ROOT/dir1/file.2
	chown testuser:testgroup $RH_ROOT/dir1/file.3

	setfattr -n user.foo -v 1 $RH_ROOT/dir1/file.4
	setfattr -n user.bar -v 1 $RH_ROOT/dir1/file.5

	dd if=/dev/zero of=$RH_ROOT/dir2/file.6 bs=1K count=10 >/dev/null 2>/dev/null || error "writing dir2/file.6"
	dd if=/dev/zero of=$RH_ROOT/dir2/file.7 bs=1K count=11 >/dev/null 2>/dev/null || error "writing dir2/file.7"
	dd if=/dev/zero of=$RH_ROOT/dir2/file.8 bs=1K count=1 >/dev/null 2>/dev/null || error "writing dir2/file.8"
}

function update_files_migration
{
	# Update several files for migration tests
	# 	update_files_migration

    for i in `seq 1 500`; do
		echo "aaaaaaaaaaaaaaaaaaaa" >> $RH_ROOT/dir2/file.8
	done
    dd if=/dev/zero of=$RH_ROOT/dir2/file.9 bs=1K count=1 >/dev/null 2>/dev/null || error "writing dir2/file.9"
}

# return an error count
function check_migrate_arr
{
    errors=0

    for n in $*
    do
        (( $is_lhsm > 0 )) && [[ $n = *"link"* ]] && continue

        # lustre/HSM: search in backend by fid
        if (( $is_lhsm > 0 )); then
            x=$(find $RH_ROOT -name "$n" | xargs -n 1 -r $LFS path2fid | tr -d '[]')
        else
            x="$n"
        fi

        [ "$DEBUG" = "1" ] && ls -R $BKROOT | grep $x

        countMigrFile=`ls -R $BKROOT | grep $x | wc -l`
        if (($countMigrFile == 0)); then
            error "********** TEST FAILED (File System): $x is not archived"
            ((errors++))
	    fi

        [ "$DEBUG" = "1" ] && grep "$ARCH_STR" rh_migr.log | grep "$n"

        countMigrLog=`grep "$ARCH_STR" rh_migr.log | grep "$n" | wc -l`
        if (($countMigrLog == 0)); then
            error "********** TEST FAILED (Log): $n is not archived"
            ((errors++))
	    fi
    done
    return $errors
}


function test_migration
{
	# Realise a unit test for migration functionalities
	# 	test_migration config_file sleepTime countFinal migrate_list migrOpt
	#=>
	# config_file == config file name
	# sleepTime == expected time in second to sleep for the test, if=0 no sleep and no update
	# countFinal == number of files migrated at the end
	# migrate_list == list of migrated files at the end : "file.1;file.2;link.2"
	# migrOpt == an migrate option of robinhood : "--migrate" "--migrate-ost=1"

    config_file=$1
    sleep_time=$2
    countFinal=$3
	migrate_list=$4
    migrate_arr=$(echo $migrate_list | tr ";" "\n")
    migrOpt=$5

    if (( ($is_hsmlite == 0) && ($is_lhsm == 0) )); then
		echo "No Migration for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "Create Files ..."
	create_files_migration

	if(($sleep_time != 0)); then
	    echo "Sleep $sleep_time"
        sleep $sleep_time

	    echo "update Files"
        update_files_migration
    fi

	echo "Reading changelogs and Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan $migrOpt -l DEBUG -L rh_migr.log --once
    (( $is_lhsm > 0 )) && wait_done 60

    # no symlink archiving for Lustre/HSM
    if (( $is_lhsm > 0 )); then
        for x in $migrate_arr; do
            if [[ $x = *"link"* ]]; then
                ((countFinal=$countFinal-1))
            fi
        done
    fi

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
    fi

    check_migrate_arr $migrate_arr
    nbError=$?

    if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

function migration_file_type
{
	# Realise a unit test for migration functionalities based on file type
	# 	migration_file_type config_file sleepTime countFinal migrate_list
	#=>
	# config_file == config file name
	# sleepTime == expected time in second to sleep for the test, if=0 no sleep and no update
	# countFinal == number of files migrated at the end
	# migrate_list == list of migrated files at the end : "file.1;file.2;link.2"

    config_file=$1
    sleep_time=$2
    countFinal=$3
	migrate_list=$4
    migrate_arr=$(echo $migrate_list | tr ";" "\n")

    if (( $is_hsmlite == 0 )); then
		echo "No symlink migration for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "Create Files ..."
	create_files_migration

	if(($sleep_time != 0)); then
	    echo "Sleep $sleep_time"
        sleep $sleep_time

	    echo "update Files"
        update_files_migration
    fi

	echo "Reading changelogs and Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=file:$RH_ROOT/dir1/link.1 -l DEBUG -L rh_migr.log

    nbError=0
    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
            ((nbError++))
    fi

    check_migrate_arr $migrate_arr
    ((nbError+=$?))

	echo "Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:$RH_ROOT/dir1/file.1 -l DEBUG -L rh_migr.log
    (( $is_lhsm > 0 )) && wait_done 10

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
            ((nbError++))
    fi

    check_migrate_arr $migrate_arr
    ((nbError+=$?))

    if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

function migration_file_owner
{
	# Realise a unit test for migration functionalities based on file owner
	# 	migration_file_owner config_file sleepTime countFinal migrate_list
	#=>
	# config_file == config file name
	# sleepTime == expected time in second to sleep for the test, if=0 no sleep and no update
	# countFinal == number of files migrated at the end
	# migrate_list == list of migrated files at the end : "file.1;file.2;link.2"

    config_file=$1
    sleep_time=$2
    countFinal=$3
	migrate_list=$4
    migrate_arr=$(echo $migrate_list | tr ";" "\n")

    if (( ($is_hsmlite == 0) && ($is_lhsm == 0) )); then
		echo "No Migration for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "Create Files ..."
	create_files_migration

	if(($sleep_time != 0)); then
	    echo "Sleep $sleep_time"
        sleep $sleep_time

	    echo "update Files"
        update_files_migration
    fi

	echo "Reading changelogs and Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=file:$RH_ROOT/dir1/file.1 -l DEBUG -L rh_migr.log
    (( $is_lhsm > 0 )) && wait_done 10

    nbError=0
    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != 0)); then
        error "********** TEST FAILED (File System): $count files migrated, but 0 expected"
            ((nbError++))
    fi

	echo "Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:$RH_ROOT/dir1/file.3 -l DEBUG -L rh_migr.log
    (( $is_lhsm > 0 )) && wait_done 10

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
            ((nbError++))
    fi

    check_migrate_arr $migrate_arr
    ((nbError+=$?))

    if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

function migration_file_Last
{
	# Realise a unit test for migration functionalities based on file last acces/modification
	# 	migration_file_Last config_file sleepTime countFinal migrate_list
	#=>
	# config_file == config file name
	# sleepTime == expected time in second to sleep for the test, if=0 no sleep and no update
	# countFinal == number of files migrated at the end
	# migrate_list == list of migrated files at the end : "file.1;file.2;link.2"

    config_file=$1
    sleep_time=$2
    countFinal=$3
	migrate_list=$4
    migrate_arr=$(echo $migrate_list | tr ";" "\n")

    if (( ($is_hsmlite == 0) && ($is_lhsm == 0) )); then
		echo "No Migration for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "Create Files ..."
	create_files_migration

	echo "Reading changelogs and Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=file:$RH_ROOT/dir1/file.1 -l DEBUG -L rh_migr.log
    (( $is_lhsm > 0 )) && wait_done 10

	nbError=0
    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != 0)); then
        error "********** TEST FAILED (File System): $count files migrated, but 0 expected"
            ((nbError++))
    fi

	if(($sleep_time != 0)); then
	    echo "Sleep $sleep_time"
        sleep $sleep_time

	    echo "update Files"
        update_files_migration
    fi

	echo "Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --run=migration --target=file:$RH_ROOT/dir1/file.1 -l DEBUG -L rh_migr.log
    (( $is_lhsm > 0 )) && wait_done 10

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
            ((nbError++))
    fi

    check_migrate_arr $migrate_arr
    ((nbError+=$?))

    if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

function migration_file_ExtendedAttribut
{
	# Realise a unit test for migration functionalities based on Extended Attribut
	# 	migration_file_ExtendedAttribut config_file sleepTime countFinal migrate_list
	#=>
	# config_file == config file name
	# sleepTime == expected time in second to sleep for the test, if=0 no sleep and no update
	# countFinal == number of files migrated at the end
	# migrate_list == list of migrated files at the end : "file.1;file.2;link.2"

    config_file=$1
    sleep_time=$2
    countFinal=$3
	migrate_list=$4
    migrate_arr=$(echo $migrate_list | tr ";" "\n")

    if (( ($is_hsmlite == 0) && ($is_lhsm == 0) )); then
		echo "No Migration for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "Create Files ..."
	create_files_migration

	echo "Reading changelogs and Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=file:$RH_ROOT/dir1/file.4 -l DEBUG -L rh_migr.log
    (( $is_lhsm > 0 )) && wait_done 10

	nbError=0
    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
       ((nbError++))
    fi

	echo "Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=file:$RH_ROOT/dir1/file.5 -l DEBUG -L rh_migr.log
    (( $is_lhsm > 0 )) && wait_done 10

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
       ((nbError++))
    fi

	echo "Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=file:$RH_ROOT/dir1/file.1 -l DEBUG -L rh_migr.log
    (( $is_lhsm > 0 )) && wait_done 10

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
       ((nbError++))
    fi

    check_migrate_arr $migrate_arr
    ((nbError+=$?))

    if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

function migration_OST
{
	# Realise a unit test for migration functionalities for OST filesystem (Lustre)
	# 	migration_OST config_file countFinal migrate_list migrOpt
	#=>
	# config_file == config file name
	# countFinal == number of files migrated at the end
	# migrate_list == list of migrated files at the end : "file.1;file.2;link.2"
	# migrOpt == an migrate option of robinhood : "--migrate" "--migrate-ost=1"

	config_file=$1
    countFinal=$2
	migrate_list=$3
    migrate_arr=$(echo $migrate_list | tr ";" "\n")
    migrOpt=$4

    if (( ($is_hsmlite == 0) && ($is_lhsm == 0) )); then
		echo "No Migration for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "1-Create Pools ..."
	create_pools

	echo "2-Create Files ..."
    for i in `seq 1 2`; do
		$LFS setstripe  -p lustre.$POOL1 $RH_ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

    for i in `seq 3 4`; do
		$LFS setstripe  -p lustre.$POOL2 $RH_ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

	echo "Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan $migrOpt -l DEBUG -L rh_migr.log --once
    (( $is_lhsm > 0 )) && wait_done 60
    nbError=0
    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
        ((nbError++))
    fi

    check_migrate_arr $migrate_arr
    ((nbError+=$?))

	echo $nbError
    if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

function migration_file_OST
{
	# Realise a unit test for migration functionalities for OST filesystem (Lustre) based on file policy
	# 	migration_file_OST config_file countFinal migrate_list
	#=>
	# config_file == config file name
	# countFinal == number of files migrated at the end
	# migrate_list == list of migrated files at the end : "file.1;file.2;link.2"

	config_file=$1
    countFinal=$2
	migrate_list=$3
    migrate_arr=$(echo $migrate_list | tr ";" "\n")

    if (( ($is_hsmlite == 0) && ($is_lhsm == 0) )); then
		echo "No Migration for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "1-Create Pools ..."
	create_pools

	echo "2-Create Files ..."
    for i in `seq 1 2`; do
		$LFS setstripe  -p lustre.$POOL1 $RH_ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

    for i in `seq 3 4`; do
		$LFS setstripe  -p lustre.$POOL2 $RH_ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

	echo "3-Reading changelogs and Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=file:$RH_ROOT/file.2 -l DEBUG -L rh_migr.log
    (( $is_lhsm > 0 )) && wait_done 10

    nbError=0
    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != 0)); then
        error "********** TEST FAILED (File System): $count files migrated, but 0 expected"
        ((nbError++))
    fi

	echo "Applying migration policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=file:$RH_ROOT/file.3 -l DEBUG -L rh_migr.log
    (( $is_lhsm > 0 )) && wait_done 10

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
        ((nbError++))
    fi

    check_migrate_arr $migrate_arr
    ((nbError+=$?))

    if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

###################################################
############# End Migration Functions #############
###################################################

function fs_usage
{
    df -P "$RH_ROOT" | tail -n 1 | awk '{ print $(NF-1) }' | tr -d '%'
}

###########################################################
############### Purge Trigger Functions ###################
###########################################################

function trigger_purge_QUOTA_EXCEEDED
{
	# Function to test the trigger system when a quota is exceeded
	# 	trigger_purge_QUOTA_EXCEEDED config_file
	#=>
	# config_file == config file name

	config_file=$1

	clean_logs

    if [ -z "$POSIX_MODE" ]; then
        $LFS setstripe -c 2 $RH_ROOT || echo "error setting stripe count=2"
    fi
    elem=$(fs_usage)

	echo "1-Create Files ..."
	limit=80
    limit_init=$limit
	indice=1
    while [ $elem -lt $limit ]
    do
        # write 2M to fulfill 2 stripes
        dd if=/dev/zero of=$RH_ROOT/file.$indice bs=2M count=1 conv=sync >/dev/null 2>/dev/null
        if (( $? != 0 )); then
            echo "WARNING: failed to write $RH_ROOT/file.$indice"
            # give it a chance to end the loop
            ((limit=$limit-1))
        else
            # reinitialize the limit on success
            limit=$limit_init
        fi

        elem=$(fs_usage)
        ((indice++))
    done

    echo "2-Reading changelogs and Applying purge trigger policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --check-thresholds=purge -l DEBUG -L rh_purge.log --once

    countMigrLog=`grep "High threshold reached on Filesystem" rh_purge.log | wc -l`
    if (($countMigrLog == 0)); then
        error "********** TEST FAILED **********"
    else
        echo "OK: test successful"
    fi
}

function trigger_purge_OST_QUOTA_EXCEEDED
{
	# Function to test the trigger system when a quota is exceeded in OST filesytem (Lustre)
	# 	trigger_purge_OST_QUOTA_EXCEEDED config_file
	#=>
	# config_file == config file name

	config_file=$1

    if [ -n "$POSIX_MODE" ]; then
        echo "No OST support for POSIX mode"
        set_skipped
        return 1
    fi

	clean_logs
    # make sure df is up to date
    wait_stable_df

	echo "1-Fullfilling OST#0 up to 80%..."
	elem=`$LFS df $RH_ROOT | grep "OST:0" | awk '{ print $5 }' | sed 's/%//'`
	limit=80
	indice=1
    while [ $elem -lt $limit ]
    do
        $LFS setstripe -o 0 $RH_ROOT/file.$indice -c 1 >/dev/null 2>/dev/null
        dd if=/dev/zero of=$RH_ROOT/file.$indice bs=10M count=1 \
            conv=sync >/dev/null 2>/dev/null
        unset elem
	    elem=`$LFS df $RH_ROOT | grep "OST:0" | awk '{ print $5 }' | sed 's/%//'`
        [ "$DEBUG" = "1" ] && echo "used: $elem, target: $limit"
        ((indice++))
    done

    echo "2-Applying purge trigger policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --check-thresholds=purge -l DEBUG \
        -L rh_purge.log --once 2>/dev/null

    countMigrLog=`grep "High threshold reached on OST #0" rh_purge.log | wc -l`
    if (($countMigrLog == 0)); then
        error "********** TEST FAILED **********"
    else
        echo "OK: test successful"
    fi
}

function trigger_purge_USER_GROUP_QUOTA_EXCEEDED
{
	# Function to test the trigger system when a quota is exceeded for a group or an user
	# 	trigger_purge_USER_GROUP_QUOTA_EXCEEDED config_file usage
	#=>
	# config_file == config file name
	# usage == "User" or "Group"

	config_file=$1
	usage=$2

	clean_logs

        # force df update
        while (( 1 )); do
                elem=$(fs_usage)

                if (( $elem > 20 )); then
                        echo "filesystem is still ${elem}% full. waiting for df update..."
        		clean_caches
                        sleep 1
                else
                        break
                fi
        done

	echo "1-Create Files ..."
	limit=80
    limit_init=$limit
	indice=1
    last=1
    dd_out=/tmp/dd.out.$$
    one_error=""
    dd_err_count=0
    elem=$(fs_usage)
    while [ $elem -lt $limit ]
    do
        # write 2M to fulfill 2 stripes
        dd if=/dev/zero of=$RH_ROOT/file.$indice bs=2M count=1 conv=sync >/dev/null 2>$dd_out
        if (( $? != 0 )); then
            [[ -z "$one_error" ]] && one_error="failed to write $RH_ROOT/file.$indice: $(cat $dd_out)"
            ((dd_err_count++))
            ((limit=$limit-1))
        else
            # on success, reinitialize limit
            limit=$limit_init
        fi

        if [[ -s $RH_ROOT/file.$indice ]]; then
            ((last++))
        fi

    	# force df update
        clean_caches
        elem=$(fs_usage)
        ((indice++))
    done
    (($dd_err_count > 0)) && echo "WARNING: $dd_err_count errors writing $RH_ROOT/file.*: first error: $one_error"

    rm -f $dd_out

    # limit is 25% => leave half of files with owner root
    ((limit=$last/2))
    ((limit=$limit-1))
    echo "$last files created, changing $limit files to testuser:testgroup"
    df -h $RH_ROOT
    ((indice=1))
    while [ $indice -lt $limit ]
    do
        chown testuser:testgroup $RH_ROOT/file.$indice
        ((indice++))
    done


    echo "2-Reading changelogs and Applying purge trigger policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --check-thresholds=purge -l DEBUG -L rh_purge.log --once

    countMigrLog=`grep "$usage exceeds high threshold" rh_purge.log | wc -l`
    if (($countMigrLog == 0)); then
        error "********** TEST FAILED **********"
    else
        echo "OK: test successful"
    fi
}

###########################################################
############# End Purge Trigger Functions #################
###########################################################

###################################################
############### Purge Functions ###################
###################################################

function create_files_Purge
{
	# create all directory and files for purge tests
	#  create_files_Purge

    mkdir $RH_ROOT/dir1
    mkdir $RH_ROOT/dir2

    for i in `seq 1 5` ; do
    	dd if=/dev/zero of=$RH_ROOT/dir1/file.$i bs=1K count=1 >/dev/null 2>/dev/null || error "writing dir1/file.$i"
	done

	ln -s $RH_ROOT/dir1/file.1 $RH_ROOT/dir1/link.1
	ln -s $RH_ROOT/dir1/file.1 $RH_ROOT/dir1/link.2

	chown root:testgroup $RH_ROOT/dir1/file.2
    chown testuser:testgroup $RH_ROOT/dir1/file.3

	setfattr -n user.foo -v 1 $RH_ROOT/dir1/file.4
	setfattr -n user.bar -v 1 $RH_ROOT/dir1/file.5

    dd if=/dev/zero of=$RH_ROOT/dir2/file.6 bs=1K count=10 >/dev/null 2>/dev/null || error "writing dir2/file.6"
    dd if=/dev/zero of=$RH_ROOT/dir2/file.7 bs=1K count=11 >/dev/null 2>/dev/null || error "writing dir2/file.7"
    dd if=/dev/zero of=$RH_ROOT/dir2/file.8 bs=1K count=1 >/dev/null 2>/dev/null || error "writing dir2/file.8"
}

function update_files_Purge
{
	# update files for Purge tests
	#  update_files_migration

    for i in `seq 1 500`; do
		echo "aaaaaaaaaaaaaaaaaaaa" >> $RH_ROOT/dir2/file.8
    done
	cat $RH_ROOT/dir2/file.8 >/dev/null 2>/dev/null
}

function test_purge
{
	# Realise a unit test for purge functionalities
	# 	test_migration config_file sleep_time countFinal purge_list purgeOpt
	#=>
	# config_file == config file name
	# sleep_time == expected time in second to sleep for the test, if=0 no sleep and no update
	# countFinal == number of files not purged at the end
	# purge_list == list of purged files at the end : "file.1;file.2;link.2"
	# purgeOpt == an migrate option of robinhood : "--purge" "--purge-ost=1"

    config_file=$1
    sleep_time=$2
    countFinal=$3
	purge_list=$4
    purge_arr=$(echo $purge_list | tr ";" "\n")
    purgeOpt=$5

    # adapt the test for any root
    export MATCH_PATH2="$RH_ROOT/dir2/*"
    export MATCH_PATH1="$RH_ROOT/dir1/*"

	needPurge=0
	((needPurge=10-countFinal))

	clean_logs

	echo "Create Files ..."
	create_files_Purge

	sleep 1
	$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log --once

	# use robinhood for flushing
    if (( ($is_hsmlite != 0) || ($is_lhsm != 0) )); then
		echo "Archiving files"
		$RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG  -L rh_migr.log || error "executing Archiving files"
	fi

	if(($sleep_time != 0)); then
	    echo "Sleep $sleep_time"
        sleep $sleep_time

	    echo "update Files"
        update_files_Purge

        if (( ($is_hsmlite != 0) || ($is_lhsm != 0) )); then
	        echo "Update and archiving files"
	        $RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=all --once -l DEBUG  -L rh_migr.log
            (( $is_lhsm > 0 )) && wait_done 60
	    fi
    fi

	echo "Scan and apply purge policy ($purgeOpt)..."
	$RH -f $RBH_CFG_DIR/$config_file --scan  $purgeOpt --once -l DEBUG -L rh_purge.log

	nbError=0
	nb_purge=`grep "$REL_STR" rh_purge.log | wc -l`
	if (( $nb_purge != $needPurge )); then
	    error "********** TEST FAILED (Log): $nb_purge files purged, but $needPurge expected"
        ((nbError++))
	fi

    if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

function test_purge_tmp_fs_mgr
{
	# Realise a unit test for purge functionalities for TMP_FS_MGR mod
	# 	test_migration_tmp_fs_mgr config_file sleep_time countFinal purge_list purgeOpt
	#=>
	# config_file == config file name
	# sleep_time == expected time in second to sleep for the test, if=0 no sleep and no update
	# countFinal == number of files not purged at the end
	# purge_list == list of purged files at the end : "file.1;file.2;link.2"
	# purgeOpt == an migrate option of robinhood : "--purge" "--purge-ost=1"

    config_file=$1
    sleep_time=$2
    countFinal=$3
	purge_list=$4
    purge_arr=$(echo $purge_list | tr ";" "\n")
    purgeOpt=$5


	if (( $is_lhsm > 0 )); then
		echo "No link purge for Lustre/HSM"
		set_skipped
		return 1
	fi

	needPurge=0
	((needPurge=10-countFinal))

	clean_logs

	echo "Create Files ..."
	create_files_Purge

	sleep 1
	$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log --once

	if(($sleep_time != 0)); then
	    echo "Sleep $sleep_time"
        sleep $sleep_time

	    echo "update Files"
        update_files_Purge
    fi

	echo "Reading changelogs and Applying purge policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan  $purgeOpt --once -l DEBUG -L rh_purge.log

	nbError=0
	nb_purge=`grep "$REL_STR" rh_purge.log | wc -l`
	if (( $nb_purge != $needPurge )); then
	    error "********** TEST FAILED (Log): $nb_purge files purged, but $needPurge expected"
        ((nbError++))
	fi

    countFileDir1=`find $RH_ROOT/dir1 -type f | wc -l`
    countFileDir2=`find $RH_ROOT/dir2 -type f | wc -l`
    countLink=`find $RH_ROOT/dir1 -type l | wc -l`
    count=$(($countFileDir1+$countFileDir2+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files stayed in filesystem, but $countFinal expected"
        ((nbError++))
    fi

    for x in $purge_arr
    do
        if [ -e "$RH_ROOT/dir1/$x" -o -e "$RH_ROOT/dir2/$x" ]; then
	        error "********** TEST FAILED (File System): $x is not purged"
            ((nbError++))
        fi
    done

    if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

function purge_OST
{
	# Realise a unit test for purge functionalities for OST fileSystem (Lustre)
	# 	migration_OST config_file countFinal purge_list purgeOpt
	#=>
	# config_file == config file name
	# countFinal == number of files not purged at the end
	# purge_list == list of purged files at the end : "file.1;file.2;link.2"
	# purgeOpt == an migrate option of robinhood : "--purge" "--purge-ost=1"

	config_file=$1
    countFinal=$2
	purge_list=$3
    purge_arr=$(echo $purge_list | tr ";" "\n")
    purgeOpt=$4

    if [ -n "$POSIX_MODE" ]; then
        echo "No OST support for POSIX mode"
        set_skipped
        return 1
    fi

	needPurge=0
	((needPurge=4-countFinal))

	clean_logs

	echo "1-Create Pools ..."
	create_pools

	echo "2-Create Files ..."
    for i in `seq 1 2`; do
		$LFS setstripe  -p lustre.$POOL1 $RH_ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

    for i in `seq 3 4`; do
		$LFS setstripe  -p lustre.$POOL2 $RH_ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

	sleep 1
	$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log --once

	# use robinhood for flushing
	if (( $is_hsmlite + $is_lhsm > 0 )); then
		echo "2bis-Archiving files"
		$RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG  -L rh_migr.log || error "executing Archiving files"
        (( $is_lhsm > 0 )) && wait_done 60
	fi

	echo "Reading changelogs and Applying purge policy..."
	$RH -f $RBH_CFG_DIR/$config_file --scan $purgeOpt -l DEBUG -L rh_purge.log --once

	nbError=0
	nb_purge=`grep "$REL_STR" rh_purge.log | wc -l`
	if (( $nb_purge != $needPurge )); then
	    error "********** TEST FAILED (Log): $nb_purge files purged, but $needPurge expected"
        ((nbError++))
	fi

	if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

###################################################
############# End Purge Functions #################
###################################################

##################################################
############# Removing Functions #################
##################################################

function test_removing
{
	# remove directory/ies in accordance to the input file and configuration
	# 	test_removing config_file forExtAttributes sleepTime mode_list
	#=>
	# config_file == config file name
	# testKey == 'emptyDir' for testing extended attributes
	# 	     'lastAction' for testing last access or modification
	# sleepTime == expected time in second to sleep for the test, if=0 no sleep

	# get input parameters ....................
	config_file=$1
	testKey=$2  #== key word for specific tests
	sleepTime=$3

	#  clean logs ..............................
	clean_logs

	# prepare data..............................
	echo "1-Preparing Filesystem..."
	mkdir -p $RH_ROOT/dir1
	mkdir -p $RH_ROOT/dir5
	echo "data" > $RH_ROOT/dir5/file.5

	if [ $testKey == "emptyDir" ]; then
		# wait and write more data
		if [ $sleepTime != 0 ]; then
			echo "Please wait $sleepTime seconds ..."
			sleep $sleepTime || error "sleep time"
		fi
		sleepTime=0
		mkdir -p $RH_ROOT/dir6
		mkdir -p $RH_ROOT/dir7
		echo "data" > $RH_ROOT/dir7/file.7

	else
		# in dir1: manage folder owner and attributes
		chown testuser $RH_ROOT/dir1 || error "invalid chown on user 'testuser' for $RH_ROOT/dir1 "  #change owner
		setfattr -n user.foo -v "abc.1.test" $RH_ROOT/dir1
		echo "data" > $RH_ROOT/dir1/file.1
		mkdir -p $RH_ROOT/dir1/dir2
		echo "data" > $RH_ROOT/dir1/dir2/file.2
		mkdir -p $RH_ROOT/dir1/dir3
		echo "data" > $RH_ROOT/dir1/dir3/file.3
	 	mkdir -p $RH_ROOT/dir1/dir4
		chown testuser $RH_ROOT/dir1/dir4 || error "invalid chown on user 'testuser' for $RH_ROOT/dir4" #change owner
		echo "data" > $RH_ROOT/dir1/dir4/file.41
		echo "data" > $RH_ROOT/dir1/dir4/file.42

		# in dir5:
		setfattr -n user.bar -v "abc.1.test" $RH_ROOT/dir5
		echo "data" > $RH_ROOT/dir5/file.5

		# in dir6:
		mkdir -p $RH_ROOT/dir6
		chown testuser $RH_ROOT/dir6 || error "invalid chown on user 'testuser' for $RH_ROOT/dir6" #change owner
	fi

	# launch the scan ..........................
	echo "2-Scanning directories in filesystem ..."
	$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log --once || error "scanning filesystem"

	# optional sleep process ......................
	if [ $sleepTime != 0 ]; then
		echo "Please wait $sleepTime seconds ..."
		sleep $sleepTime
	fi
	# specific optional action after sleep process ..........
	if [ $testKey == "lastAccess" ]; then
	#	ls -R $RH_ROOT/dir1 || error "scaning $RH_ROOT/dir1"
		touch $RH_ROOT/dir1/file.touched || error "touching file in $RH_ROOT/dir1"
	elif [ $testKey == "lastModif" ]; then
		echo "data" > $RH_ROOT/dir1/file.12 || error "writing in $RH_ROOT/dir1/file.12"
	fi

	# launch the rmdir ..........................
	echo "3-Removing directories in filesystem ..."
	if [ $testKey == "lastAccess" ]; then
	$RH -f $RBH_CFG_DIR/$config_file --run=rmdir_recurse --target=all -l DEBUG -L rh_rmdir.log --once || error "performing FS removing"
	else
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=rmdir_recurse --target=all -l DEBUG -L rh_rmdir.log --once || error "performing FS removing"
	fi

	# launch the validation ..........................
	echo "4-Checking results ..."
	logFile=/tmp/rh_alert.log
	case "$testKey" in
		pathName)
			existedDirs="$RH_ROOT/dir5;$RH_ROOT/dir6"
			notExistedDirs="$RH_ROOT/dir1"
			;;
		emptyDir)
			existedDirs="$RH_ROOT/dir6;$RH_ROOT/dir5;$RH_ROOT/dir7"
			notExistedDirs="$RH_ROOT/dir1"
			;;
		owner)
			existedDirs="$RH_ROOT/dir5"
			notExistedDirs="$RH_ROOT/dir1;$RH_ROOT/dir6"
			;;
		lastAccess)
			existedDirs="$RH_ROOT/dir1"
			notExistedDirs="$RH_ROOT/dir5;$RH_ROOT/dir6"
			;;
		lastModif)
			existedDirs="$RH_ROOT/dir1"
			notExistedDirs="$RH_ROOT/dir5;$RH_ROOT/dir6"
			;;
		dircount)
			existedDirs="$RH_ROOT/dir5;$RH_ROOT/dir6"
			notExistedDirs="$RH_ROOT/dir1"
			;;
		extAttributes)
			existedDirs="$RH_ROOT/dir5;$RH_ROOT/dir6"
			notExistedDirs="$RH_ROOT/dir1"
			;;
		*)
			error "unexpected testKey $testKey"
			return 1 ;;
	esac
	# launch the validation for all remove process
	exist_dirs_or_not $existedDirs $notExistedDirs
	res=$?

	if (( $res == 1 )); then
		error "Test for RemovingDir_$testKey failed"
    else
        echo "OK: Test successful"
	fi
}

function test_rmdir_mix
{
    config_file=$1
    sleepTime=$2 # for age_rm_empty_dirs

    #  clean logs
    clean_logs

    export NO_RM_TREE="$RH_ROOT/no_rm"

    # prepare data
    echo "1-Preparing Filesystem..."
    # old dirempty
    mkdir -p $RH_ROOT/no_rm/dirempty
    mkdir -p $RH_ROOT/dirempty
    sleep $sleepTime

    # new dirs
    mkdir -p $RH_ROOT/no_rm/dir1
    mkdir -p $RH_ROOT/no_rm/dir2
    mkdir -p $RH_ROOT/no_rm/dirempty_new
    mkdir -p $RH_ROOT/dir1
    mkdir -p $RH_ROOT/dir2
    mkdir -p $RH_ROOT/dirempty_new
    echo "data" >  $RH_ROOT/no_rm/dir1/file
    echo "data" >  $RH_ROOT/no_rm/dir2/file
    echo "data" >  $RH_ROOT/dir1/file
    echo "data" >  $RH_ROOT/dir2/file

    # launch the scan ..........................
    echo "2-Scanning directories in filesystem ..."
    $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log --once || error "scanning filesystem"

    echo "3-Checking old dirs report"
    $REPORT -f $RBH_CFG_DIR/$config_file -l MAJOR -cq --oldest-empty-dirs > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    # must report empty dirs
    grep "no_rm/dirempty," report.out || error "no_rm/dirempty not in empty dir report"
    grep "no_rm/dirempty_new," report.out || error "no_rm/dirempty_new not in empty dir report"
    grep "$RH_ROOT/dirempty," report.out || error "$RH_ROOT/dirempty not in empty dir report"
    grep "$RH_ROOT/dirempty_new," report.out || error "$RH_ROOT/dirempty_new not in empty dir report"
    # must no report other dirs
    grep "no_rm/dir1," report.out && error "no_rm/dir1 in empty dir report"
    grep "no_rm/dir1," report.out && error "no_rm/dir2 in empty dir report"
    grep "$RH_ROOT/dir2," report.out && error "$RH_ROOT/dir1 in empty dir report"
    grep "$RH_ROOT/dir2," report.out && error "$RH_ROOT/dir2 in empty dir report"

    # launch the rmdir ..........................
    echo "4-Removing directories in filesystem ..."
    $RH -f $RBH_CFG_DIR/$config_file --run=rmdir --target=all -l DEBUG -L rh_rmdir.log --once || error "performing rmdir"

    echo "5-Checking results ..."
    exist="$RH_ROOT/no_rm/dirempty;$RH_ROOT/no_rm/dir1;$RH_ROOT/no_rm/dir2;$RH_ROOT/no_rm/dirempty_new;$RH_ROOT/dir2;$RH_ROOT/dirempty_new"
    noexist="$RH_ROOT/dir1;$RH_ROOT/dirempty"

    # launch the validation for all remove process
    exist_dirs_or_not $exist $noexist
    res=$?

    if (( $res == 1 )); then
        error "Test for RemovingDir_mixed failed"
    else
        echo "OK: Test successful"
    fi
}


function test_removing_ost
{
	# remove directory/ies in accordance to the input file and configuration
	# 	test_removing config_file mode_list
	#=>
	# config_file == config file name

	# get input parameters ....................
	config_file=$1

    echo "Directory stripe is not taken into account for rmdir policies: skipped"
	set_skipped
	return 1

	clean_logs

	echo "Create Pools ..."
	create_pools

	echo "Create Files ..."
	mkdir $RH_ROOT/dir1

	$LFS setstripe  -p lustre.$POOL1 $RH_ROOT/dir1 >/dev/null 2>/dev/null

	$LFS setstripe  -p lustre.$POOL1 $RH_ROOT/dir1/file.1 -c 1 >/dev/null 2>/dev/null
	$LFS setstripe  -p lustre.$POOL1 $RH_ROOT/dir1/file.2 -c 1 >/dev/null 2>/dev/null
	$LFS setstripe  -p lustre.$POOL1 $RH_ROOT/dir1/file.3 -c 1 >/dev/null 2>/dev/null

	mkdir $RH_ROOT/dir2
	$LFS setstripe  -p lustre.$POOL2 $RH_ROOT/dir2 >/dev/null 2>/dev/null

    $LFS setstripe  -p lustre.$POOL2 $RH_ROOT/file.1 -c 1 >/dev/null 2>/dev/null
	$LFS setstripe  -p lustre.$POOL2 $RH_ROOT/dir2/file.2 -c 1 >/dev/null 2>/dev/null
	$LFS setstripe  -p lustre.$POOL2 $RH_ROOT/dir2/file.3 -c 1 >/dev/null 2>/dev/null

	echo "Removing directories in filesystem ..."
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=rmdir_recurse --target=all -l DEBUG -L rh_rmdir.log --once || error "performing FS removing"

	# launch the validation ..........................
	echo "Checking results ..."
	logFile=/tmp/rh_alert.log
	existedDirs="$RH_ROOT/dir1"
	notExistedDirs="$RH_ROOT/dir2"
	# launch the validation for all remove process
	exist_dirs_or_not $existedDirs $notExistedDirs
	res=$?

	if (( $res == 1 )); then
		error "Test for RemovingDir_ost failed"
	fi

	test -f $RH_ROOT/file.1
	res=$?

	if (( $res == 1 )); then
		error "Test for RemovingDir_ost failed"
	fi
}

function exist_dirs_or_not
{
    # read two lists of folders and check:
    # 1- the first list must contain existed dirs
    # 2- the first list must contain not existed dirs
    #If the both conditions are realized, then the function returns 0, otherwise 1.
    # 	exist_dirs_or_not $existedDirs $notExistedDirs
    #=> existedDirs & notExistedDirs list of dirs to check separated by ';'
    # ex: "$RH_ROOT/dir1;$RH_ROOT/dir5"
    # ex: Use "/" for giving an empty list

    existedDirs=$1
    notExistedDirs=$2

    echo "[$existedDirs] & [$notExistedDirs]"
    # launch the command which return 1 if one dir is not "! -d" (== does not exist)
    check_cmd $existedDirs "! -d"
    if [  $? -eq 1 ] ; then
	    echo "error for $existedDirs"
	    return 1
    else
    # launch the command which return 1 if one dir is not "-d" (== does exist)
	    check_cmd $notExistedDirs "-d"
	    if [  $? -eq 1 ] ; then
		    echo "error for $notExistedDirs"
		    return 1
	    fi
    fi
}

function check_cmd
{
    # check if each dir respects the reverse of the given command.
    # return 0 if it repects, 1 otherwise
    # check_cmd $listDirs $commande
    # =>
    # 	$listDirs = list of dirs separated by ';'
    #	ex: "$RH_ROOT/dir1;$RH_ROOT/dir5"  or "/" to no check command
    #	$commande = "-d" or "! -d"
    #	ex: check_cmd $notExistedDirs "-d": checks that all dirs does not exist

    existedDirs=$1
    cmd=$2
    # set default output value
    out=1
    #get the dirs which must exist
    if [ $existedDirs != "/" ]; then
	    splitExDirs=$(echo $existedDirs | tr ";" "\n")
	    for entry in $splitExDirs
        	do
		    # for each dir check the existence, otherwise return 1
		    if [ $cmd $entry ]; then
			    return 1
		    fi
	    done
    fi
}

######################################################
############# End Removing Functions #################
######################################################

###############################################################
############### Report generation Functions ###################
###############################################################

function test_report_generation_1
{
	# report many statistics in accordance to the input file and configuration
	# 	test_report_generation_1 config_file
	#=>
	# config_file == config file name

	# get input parameters ....................
	config_file=$1

    if [[ $RBH_NUM_UIDGID = "yes" ]]; then
        echo "Test needs adaptation for numerical UID/GID: skipped"
        set_skipped
        return 1
    fi

	#  clean logs ..............................
	clean_logs

	# prepare data..............................
	echo -e "\n 1-Preparing Filesystem..."
	# dir1:
	mkdir -p $RH_ROOT/dir1/dir2
	printf "." ; sleep 1
	dd if=/dev/zero of=$RH_ROOT/dir1/file.1 bs=1k count=5 >/dev/null 2>/dev/null || error "writing file.1"
	printf "." ; sleep 1
	dd if=/dev/zero of=$RH_ROOT/dir1/file.2 bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.2"
	printf "." ; sleep 1
	dd if=/dev/zero of=$RH_ROOT/dir1/file.3 bs=1k count=15 >/dev/null 2>/dev/null || error "writing file.3"
	printf "." ; sleep 1
	# link from dir1:
	ln -s $RH_ROOT/dir1/file.1 $RH_ROOT/link.1 || error "creating symbolic link $RH_ROOT/link.1"
	printf "." ; sleep 1
	# dir2 inside dir1:
	ln -s $RH_ROOT/dir1/file.3 $RH_ROOT/dir1/dir2/link.2 || error "creating symbolic link $RH_ROOT/dir1/dir2/link.2"
	printf "." ; sleep 1
	# dir3 inside dir1:
	mkdir -p $RH_ROOT/dir1/dir3
	printf "." ; sleep 1
	#dir4:
	mkdir -p $RH_ROOT/dir4
	printf "." ; sleep 1
	#dir5:
	mkdir -p $RH_ROOT/dir5
	printf "." ; sleep 1
	dd if=/dev/zero of=$RH_ROOT/dir5/file.4 bs=1k count=10 >/dev/null 2>/dev/null || error "writing file.4"
	printf "." ; sleep 1
	dd if=/dev/zero of=$RH_ROOT/dir5/file.5 bs=1k count=20 >/dev/null 2>/dev/null || error "writing file.5"
	printf "." ; sleep 1
	dd if=/dev/zero of=$RH_ROOT/dir5/file.6 bs=1k count=21 >/dev/null 2>/dev/null || error "writing file.6"
	printf "." ; sleep 1
	ln -s $RH_ROOT/dir1/file.2 $RH_ROOT/dir5/link.3 || error "creating symbolic link $RH_ROOT/dir5/link.3"
	printf "." ; sleep 1
	#dir6 and dir8 inside dir5:
	mkdir -p $RH_ROOT/dir5/dir6
	printf "." ; sleep 1
	mkdir -p $RH_ROOT/dir5/dir8
	printf "." ; sleep 1
	# dir7:
	mkdir -p $RH_ROOT/dir7
	printf "." ; sleep 1
    #2links in dir.1
    ln -s $RH_ROOT/dir1 $RH_ROOT/dir1/link.0 || error "creating symbolic link $RH_ROOT/dir1/link.0"
    printf "." ; sleep 1
    ln -s $RH_ROOT/dir1 $RH_ROOT/dir1/link.1 || error "creating symbolic link $RH_ROOT/dir1/link.1"
    printf "." ; sleep 1

    # make sure all data is on disk
    sync

	# manage owner and group
	filesList="$RH_ROOT/link.1 $RH_ROOT/dir1/dir2/link.2"
	chgrp -h testgroup $filesList || error "invalid chgrp on group 'testgroup' for $filesList "
	chown -h testuser $filesList || error "invalid chown on user 'testuser' for $filesList "
	filesList="$RH_ROOT/dir1/file.2 $RH_ROOT/dir1/dir2 $RH_ROOT/dir1/dir3 $RH_ROOT/dir5 $RH_ROOT/dir7 $RH_ROOT/dir5/dir6 $RH_ROOT/dir5/dir8"
	chown testuser:testgroup $filesList || error "invalid chown on user 'testuser' for $filesList "
	filesList="$RH_ROOT/dir1/file.1 $RH_ROOT/dir5/file.6"
	chgrp testgroup $filesList || error "invalid chgrp on group 'testgroup' for $filesList "

	# launch the scan ..........................
	echo -e "\n 2-Scanning Filesystem..."
	$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "performing FS scan"

	# launch another scan ..........................
	echo -e "\n 3-Filesystem content statistics..."
	#$REPORT -f $RBH_CFG_DIR/$config_file --fs-info -c || error "performing FS statistics (--fs-info)"
	$REPORT -f $RBH_CFG_DIR/$config_file --fs-info --csv > report.out || error "performing FS statistics (--fs-info)"
	logFile=report.out

    typeValues="dir;file;symlink"
    countValues="8;6;5"
    colSearch=2
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating FS statistics (--fs-info)"


	# launch another scan ..........................
	echo -e "\n 4-FileClasses summary..."
	$REPORT -f $RBH_CFG_DIR/$config_file --class-info --csv > report.out || error "performing FileClasses summary (--class)"
    if (( $is_lhsm == 0 )); then
        typeValues="test_file_type;test_link_type"
        countValues="6;5"
    else
        # Lustre/HSM: no fileclass for symlinks
        typeValues="test_file_type"
        countValues="6"
    fi

    colSearch=2
	#echo "arguments= $logFile $typeValues $countValues $colSearch**"
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating FileClasses summary (--class)"
	# launch another scan ..........................
	echo -e "\n 5-User statistics of root..."
	$REPORT -f $RBH_CFG_DIR/$config_file --user-info -u root --csv > report.out || error "performing User statistics (--user)"
    typeValues="root.*dir;root.*file;root.*symlink"
    countValues="2;5;3"
	colSearch=3
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating FS User statistics (--user)"

	# launch another scan ..........................
	echo -e "\n 6-Group statistics of testgroup..."
	$REPORT -f $RBH_CFG_DIR/$config_file --group-info -g testgroup --csv > report.out || error "performing Group statistics (--group)"
	typeValues="testgroup.*dir;testgroup.*file;testgroup.*symlink"
	countValues="6;3;2"
	colSearch=3
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Group statistics (--group)"

	# launch another scan ..........................
	echo -e "\n 7-Four largest files of Filesystem..."
	$REPORT -f $RBH_CFG_DIR/$config_file --top-size=4 --csv > report.out || error "performing Largest files list (--top-size)"
	typeValues="file\.2;file\.6;file\.5;file\.3"
	countValues="1;2;3;4"
	colSearch=1
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Largest files list (--top-size)"

	echo -e "\n 8-Largest directories of Filesystem..."
	$REPORT -f $RBH_CFG_DIR/$config_file --top-dirs=3 --csv > report.out || error "performing Largest folders list (--top-dirs)"
	# 2 possible orders
	typeValues="$RH_ROOT/dir1;$RH_ROOT/dir5;$RH_ROOT,"
	typeValuesAlt="$RH_ROOT/dir1;$RH_ROOT,;$RH_ROOT/dir5"
	countValues="1;2;3"
	colSearch=1
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || \
	find_allValuesinCSVreport $logFile $typeValuesAlt $countValues $colSearch || \
	error "validating Largest folders list (--top-dirs)"


	# /!\ scan/backup modifies files and symlink atime!
	echo -e "\n 9-Four oldest purgeable entries of Filesystem..."
    echo "FIXME: test is disturbed by file and symlink reading"
    if (( 0 )); then
        if (( $is_hsmlite + $is_lhsm != 0 )); then
        $RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG -L rh_migr.log  --once || error "performing migration"
        $REPORT -f $RBH_CFG_DIR/$config_file --oldest-files=4 --csv > report.out || error "performing Oldest entries list (--oldest-files)"
        typeValues="link\.3;link\.1;link\.2;file\.1"
        countValues="1;2;3;4"
        else
        $REPORT -f $RBH_CFG_DIR/$config_file --oldest-files=4 --csv > report.out || error "performing Oldest entries list (--oldest-files)"
        typeValues="file\.3;file\.4;file\.5;link\.3"
        countValues="1;2;3;4"
        fi
        colSearch=1
        find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Oldest entries list (--oldest-files)"
    fi

    echo -e "\n 10-Oldest empty directories of Filesystem..."
    $REPORT -f $RBH_CFG_DIR/$config_file --oldest-empty-dirs --csv > report.out || error "performing oldest empty folders list (--oldest-empty-dirs)"
    [ "$DEBUG" = "1" ] && cat report.out
    nb_dir3=`grep "dir3" $logFile | wc -l`
    if (( nb_dir3==0 )); then
        error "validating Oldest and empty folders list (--oldest-empty-dirs) : dir3 not found"
    fi
    nb_dir4=`grep "dir4" $logFile | wc -l`
    if (( nb_dir4==0 )); then
        error "validating Oldest and empty folders list (--oldest-empty-dirs) : dir4 not found"
    fi
    nb_dir6=`grep "dir6" $logFile | wc -l`
    if (( nb_dir6==0 )); then
        error "validating Oldest and empty folders list (--oldest-empty-dirs) : dir6 not found"
    fi
    nb_dir7=`grep "dir7" $logFile | wc -l`
    if (( nb_dir7==0 )); then
        error "validating Oldest and empty folders list (--oldest-empty-dirs) : dir7 not found"
    fi

	# launch another scan ..........................
	echo -e "\n 11-Top disk space consumers of Filesystem..."
	$REPORT -f $RBH_CFG_DIR/$config_file --top-users --csv > report.out || error "performing disk space consumers (--top-users)"
	typeValues="testuser;root"
	countValues="1;2"
	colSearch=1
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating disk space consumers (--top-users)"

	# launch another scan ..........................
	echo -e "\n 12-Dump entries for one user of Filesystem..."
	$REPORT -f $RBH_CFG_DIR/$config_file --dump-user root --csv > report.out || error "dumping entries for one user 'root'(--dump-user)"
	typeValues="root.*[root|testgroup].*dir1$;root.*[root|testgroup].*file\.1;root.*[root|testgroup].*file\.3;root.*[root|testgroup].*dir4$;"
	countValues="dir;file;file;dir"
	colSearch=1
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating entries for one user 'root'(--dump-user)"
	typeValues="root.*[root|testgroup].*file\.4;root.*[root|testgroup].*file\.5;root.*[root|testgroup].*file\.6;root.*[root|testgroup].*link\.3;"
	countValues="file;file;file;symlink"
	colSearch=1
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating entries for one user 'root'(--dump-user)"
	typeValue="root.*[root|testgroup]"
	if (( $(grep $typeValue $logFile | wc -l) != 10 )) ; then
		 error "validating entries for one user 'root'(--dump-user)"
	fi
	# launch another scan ..........................
	echo -e "\n 13-Dump entries for one group of Filesystem..."
	$REPORT -f $RBH_CFG_DIR/$config_file --dump-group testgroup --csv > report.out || error "dumping entries for one group 'testgroup'(--dump-group)"
	#$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "performing FS scan"
	typeValues="testgroup.*link\.1;testgroup.*file\.1;testgroup.*file\.2;testgroup.*link\.2;testgroup.*file\.6"
	countValues="symlink;file;file;symlink;file"
	colSearch=1
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Group entries for one group 'testgroup'(--dump-group)"
	typeValues="testgroup.*dir2$;testgroup.*dir3$;testgroup.*dir5$;testgroup.*dir6$;testgroup.*dir7$"
	countValues="dir;dir;dir;dir;dir"
	colSearch=1
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Group entries for one group 'testgroup'(--dump-group)"
	typeValue="testgroup"
	if (( $(grep $typeValue $logFile | wc -l) != 11 )) ; then
		 error "validating Group entries for one group 'testgroup'(--dump-group)"
	fi
}

function find_allValuesinCSVreport
{
    # The research is based on file CSV format generated by the report Robinhood method (--csv):
    # one line per information; informations separeted by ','
    # Search in the file logFile the given series (typeValue & countValue) in the column
    # colSearch.
    # return 0 if all is found, 0 otherwise
    # 	find_valueInCSVreport $logFile $typeValues $countValues $colSearch
    # logFile = name of file to scan
    # typeValues = list of words to extract the line. Each word must be separeted by ';'
    # countValues = list of associated values (to typeValues) in the extracted line. Each word must be separeted by ';'
    # colSearch =  column index to find the countValues (each column is separated by ',' in the file)

    # get input parameters
    logFile=$1
    typeValues=$2
    countValues=$3
    colSearch=$4

    # get typeValue and associated countvalue
    splitTypes=$(echo $typeValues | tr ";" "\n")
    tabTypes=""
    j=1
    for entry in $splitTypes
       do
       	tabTypes[$j]=$entry
	    j=$(($j+1))
    done
    iDataMax=$j

    splitValues=$(echo $countValues | tr ";" "\n")
    tabValues=""
    j=1
    for entry in $splitValues
       do
       	tabValues[$j]=$entry
	    j=$(($j+1))
    done
    if [ ${#tabValues[*]} != ${#tabTypes[*]} ]; then
	    echo "Error: The given conditions have different length!!"
	    return 1
    fi
    # treatement for each typeValue & countvalue
    iData=1
    #iDataMax=${#tabValues[*]}
    #echo "... length of conditions = $iDataMax"
    while (( $iData < $iDataMax ))
    do
	    # get current typeValue & countvalue
	    typeValue=${tabTypes[$iData]}
	    countValue=${tabValues[$iData]}

	    find_valueInCSVreport $logFile "$typeValue" "$countValue" $colSearch
	    res=$?
	    if (( $res == 1 )); then
		    #error "Test for $alertKey failed"
		    iData=$iDataMax
		    return 1
	    fi
	    # go to next serie
	    iData=$(($iData+1))
    done
}

function find_valueInCSVreport
{
    # The research is based on file CSV format generated by the report Robinhood method (--csv):
    # one line per information; informations separated by ','
    # Search in the same line the given words typeValue & countValue in the column
    # colSearch in the file logFile.
    # return 0 if all is found, 0 otherwise
    # 	find_valueInCSVreport $logFile $typeValues $countValues $colSearch
    # logFile = name of file to scan
    # typeValue = word to extract the line
    # countValue = associated value to typeValue in the extracted line
    # colSearch =  column index to find the countValue (each column is separated by ',')


    # get input parameters
    logFile=$1
    typeValue=$2
    countValue=$3
    colSearch=$4
    #echo '-------------------------------------'
    #more $logFile
    #echo "colSearch=$colSearch"
    #echo '-------------------------------------'
    # find line contains expected value type
    line=$(grep "$typeValue" $logFile)
    #echo $line
    if (( ${#line} == 0 )); then
	    [ "$DEBUG" = "1" ] && echo "=====> NOT found for $typeValue" >&2
	    return 1
    fi

    # get found value count for this value type
    foundCount=$(grep "$typeValue" $logFile | cut -d ',' -f $colSearch | tr -d ' ')
    #echo "foundCount=$foundCount**"
    if [[ "$foundCount" != "$countValue" ]]; then
	    [ "$DEBUG" = "1" ] && echo "=====> NOT found for $typeValue : $countValue != $foundCount" >&2
	    return 1
    else
	    [ "$DEBUG" = "1" ] && echo "=====> found for $typeValue (col $colSearch): $countValue " >&2
	    return 0
    fi
}

function report_generation2
{
	# report many statistics for OST fileSystem
	# 	report_generation_2

    if (( ($is_hsmlite == 0) && ($is_lhsm == 0) )); then
		echo "No report generation for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "1-Create Pools ..."
	create_pools

	echo "2-Create Files ..."
    for i in `seq 1 2`; do
		$LFS setstripe  -p lustre.$POOL1 $RH_ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

    for i in `seq 3 4`; do
		$LFS setstripe  -p lustre.$POOL2 $RH_ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

	sleep 1
	$RH -f $RBH_CFG_DIR/common.conf --scan -l DEBUG -L rh_scan.log --once


	echo "Generate report..."
	$REPORT -f $RBH_CFG_DIR/common.conf --dump-ost 1 >> report.out

	nbError=0
	nb_report=`grep "$RH_ROOT/file." report.out | wc -l`
	if (( $nb_report != 2 )); then
	    error "********** TEST FAILED (Log): $nb_report files purged, but 2 expected"
        ((nbError++))
	fi

	nb_report=`grep "$RH_ROOT/file.3" report.out | wc -l`
	if (( $nb_report != 1 )); then
	    error "********** TEST FAILED (Log): No report for file.3"
        ((nbError++))
	fi

	nb_report=`grep "$RH_ROOT/file.4" report.out | wc -l`
	if (( $nb_report != 1 )); then
	    error "********** TEST FAILED (Log): No report for file.4"
        ((nbError++))
	fi

	if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

###################################################################
############### End report generation Functions ###################
###################################################################

#######################################################
############### Changelog functions ###################
#######################################################

function test_changelog
{
    config_file=$1

    clean_logs

	if (( $no_log )); then
            echo "Changelogs not supported on this config: skipped"
            set_skipped
            return 1
    fi

    # create a single file and do several operations on it
    # This will generate a CREATE+CLOSE+CLOSE+SATTR records
    echo "1. Creating initial objects..."
    touch $RH_ROOT/file.1 || error "touch file.1"
	touch $RH_ROOT/file.1 || error "touch file.1"
	chmod +x $RH_ROOT/file.1 || error "chmod file.1"

    # Reading changelogs
    echo "2. Scanning ..."
   	$RH -f $RBH_CFG_DIR/$config_file --readlog --once -l FULL -L rh_scan.log || error "reading changelog"
	grep ChangeLog rh_scan.log

    # check that the MARK, CLOSE and SATTR have been ignored, but
    # CREAT was processed. Some versions of Lustre (2.1) do not issue
    # a close, so we check whether all the close seen have been ignored.
    echo "3. Checking ignored records..."
    ignore_mark=$(grep -E "Ignoring event MARK" rh_scan.log | wc -l)
    seen_mark=$(grep -E "ChangeLog.*00MARK" rh_scan.log | wc -l)
    ignore_creat=$(grep -E "Ignoring event CREAT" rh_scan.log | wc -l)
    seen_close=$(grep -E "ChangeLog.*11CLOSE" rh_scan.log | wc -l)
    ignore_close=$(grep -E "Ignoring event CLOSE" rh_scan.log | wc -l)
    ignore_sattr=$(grep -E "Ignoring event SATTR" rh_scan.log | wc -l)

    (( $seen_mark == $ignore_mark ))  || error "MARK record not ignored"
    (( $ignore_creat == 0 )) || error "CREATE record ignored"
    (( $seen_close == $ignore_close )) || error "CLOSE record not ignored"
    (( $ignore_sattr == 1 )) || error "SATTR record not ignored"

    rm -f report.out find.out
}

# wait for changelog_clear until a given timeout
# return 0 if change_clear occurs before the timeout
# return 1 else
function wait_changelog_clear
{
    local log=$1
    local timeout=$2
    local i=0

    # changelog_clear indicate the changelog processing is done
    while [ $i -lt $timeout ]; do
        grep llapi_changelog_clear $log && return 0
        sleep 1
        ((i++))
    done
    # timeout
    return 1
}

function test_commit_update
{
    local config_file=$1

    clean_logs

    if (( $no_log )); then
            echo "Changelogs not supported on this config: skipped"
            set_skipped
            return 1
    fi

    # fill the changelog with 15 records
    # as the max_delta is 5, we should have about 3 updates
    echo "1. Creating initial objects..."
    mkdir $RH_ROOT/dir.{1..15}

    # count changelogs
    local nb_log=$($LFS changelog lustre | wc -l)
    echo "$nb_log pending changelogs"

    # read the log and check last commit is updated every n records
    echo "2. Reading changelogs..."
        $RH -f $RBH_CFG_DIR/$config_file --readlog --once -l FULL \
            -L rh_chglogs.log 2>/dev/null || error "reading changelog"

    # count the number of updates of last commit
    local commit_count=$(grep CL_LastCommit rh_chglogs.log | \
                         grep "INSERT INTO" | wc -l)

    # expected: nb change_log/5 (+ 1)
    ((expect=$nb_log/5))
    if (($commit_count != $expect)) && (($commit_count != $expect + 1)); then
        error "Unexpected count of commit id update in DB ($commit_count vs. $expect (+1))"
    else
        echo "OK: commit id updated $commit_count times"
    fi

    :>rh_chglogs.log
    # now start in daemon mode (queue 1 changelog to init the last commit time)
    mkdir $RH_ROOT/dir.16
    $RH -f $RBH_CFG_DIR/$config_file --readlog -l FULL -L rh_chglogs.log \
        -p rh.pid -d 2>/dev/null

    # changelog_clear indicate the changelog processing is done
    wait_changelog_clear rh_chglogs.log 5 ||
        error "No changelog_clear after 5s"

    :>rh_chglogs.log
    # wait for the timeout delay and check the commit id is updated
    # when a new changelog is read
    sleep 3
    touch $RH_ROOT/dir.17

    wait_changelog_clear rh_chglogs.log 10 ||
        error "No changelog_clear after 10s"

    # 1 update expected
    commit_count=$(grep CL_LastCommit_ rh_chglogs.log | \
                   grep "INSERT INTO" | wc -l)

    if (($commit_count != 1)); then
        error "Unexpected count of commit id update in DB ($commit_count vs. 1)"
    else
        echo "OK: commit id updated"
    fi

    kill_from_pidfile
}

function test_path_gc1
{
    local cfg=$RBH_CFG_DIR/$1

    if [ -n "$POSIX_MODE" ]; then
		echo "Cannot fully determine id for POSIX"
		set_skipped
		return 1
    fi

    mkdir $RH_ROOT/dir.1
    mkdir $RH_ROOT/dir.2
    touch $RH_ROOT/dir.1/file.1

    local fid=$(get_id $RH_ROOT/dir.1/file.1)

    # make robinhood discover this file
    $RH -f $cfg --scan --once -l DEBUG -L rh_scan.log 2>/dev/null ||
        error "scanning"
    check_db_error rh_scan.log

    # entry path is known
    $REPORT -f $cfg -e $fid --csv | grep "^path," ||
        error "unknown path for $fid"

    # create a hardlink of it and run a partial scan
    ln $RH_ROOT/dir.1/file.1 $RH_ROOT/dir.2/file.1 || error "hardlink failed"
    $RH -f $cfg --scan=$RH_ROOT/dir.2 --once --no-gc -l DEBUG -L rh_scan.log \
        2>/dev/null || error "scanning"
    check_db_error rh_scan.log

    # the following request did fails without LIMIT 1"
    mysql $RH_DB -e "select one_path(id) from  NAMES;" ||
        error "one_path function fails"

    # query the DB to get all known entry paths
    local paths=($(mysql $RH_DB -Bse "SELECT this_path(parent_id,name) FROM NAMES WHERE id='$fid'" | sort))

    [[ ${paths[0]} == *"dir.1/file.1" ]] || error "Missing path dir.1/file.1, found: ${paths[0]}"
    [[ ${paths[1]} == *"dir.2/file.1" ]] || error "Missing path dir.2/file.1, found: ${paths[1]}"

    # partial GC is based on the timestamp of last path update
    # this ensures the GC is not done the same second as entry discovery
    # during first scan
    sleep 1

    # remove the first path and run a partial scan with GC
    rm -f $RH_ROOT/dir.1/file.1
    $RH -f $cfg --scan=$RH_ROOT/dir.1 --once -l DEBUG -L rh_scan.log \
        2>/dev/null || error "scanning"
    check_db_error rh_scan.log

    # a single path is expected now
    local cnt=$(mysql $RH_DB -Bse "SELECT count(*) FROM NAMES WHERE id='$fid'")
    [[ $cnt == 1 ]] || error "unexpected path count: $cnt"

    # check this path
    local path=$($REPORT -f $cfg -e $fid --csv | grep "^path," | cut -d ',' -f 2 | tr -d " ")
    [[ $path == "$RH_ROOT/dir.2/file.1" ]] || error "invalid remaining path $path"
}

function test_path_gc2
{
    local cfg=$RBH_CFG_DIR/$1

    mkdir $RH_ROOT/dir.1
    touch $RH_ROOT/dir.1/file.1
    touch $RH_ROOT/dir.1/file.2

    # make robinhood discover this file
    $RH -f $cfg --scan --once -l DEBUG -L rh_scan.log 2>/dev/null ||
        error "scanning"
    check_db_error rh_scan.log

    # remove one file and rename directory
    rm $RH_ROOT/dir.1/file.2
    mv $RH_ROOT/dir.1 $RH_ROOT/dir.2

    # make sure the moved entry is eligible for GC (path update < scan time)
    sleep 1

    # GC fails if stored function doesn't support multiple paths
    $RH -f $cfg --scan --once -l DEBUG -L rh_scan.log 2>/dev/null ||
            error "scanning"
    check_db_error rh_scan.log
}

###########################################################
############### End changelog functions ###################
###########################################################

##############################################################
############### Other Parameters Functions ###################
##############################################################

function TEST_OTHER_PARAMETERS_1
{
	# Test for many parameters
	# 	TEST_OTHER_PARAMETERS_1 config_file
	#=>
	# config_file == config file name

	config_file=$1

	clean_logs

	echo "Create Files ..."
    for i in `seq 1 10` ; do
    	dd if=/dev/zero of=$RH_ROOT/file.$i bs=1K count=1 >/dev/null 2>/dev/null || error "writing file.$i"
	    setfattr -n user.foo -v $i $RH_ROOT/file.$i
	done

	echo "Scan Filesystem"
	sleep 1
	$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log --once

	echo "Report : --dump --filter-class test_purge"
	$REPORT -f $RBH_CFG_DIR/$config_file --dump --filter-class test_purge > report.out

    [ "$DEBUG" = "1" ] && cat report.out
	nbError=0
    # match classes is "no"
	nb_entries=`grep "0 entries" report.out | wc -l`
	if (( $nb_entries != 1 )); then
	    error "********** TEST FAILED (Log): not found line \" $nb_entries \" "
        ((nbError++))
	fi


	# use robinhood for flushing
	if (( ($is_hsmlite == 0 && $is_lhsm == 1 && $shook == 0) || ($is_hsmlite == 1 && $is_lhsm == 0 && $shook == 1) )); then
		echo "Archiving files"
		$RH -f $RBH_CFG_DIR/$config_file $SYNC_OPT -l DEBUG  -L rh_migr.log || error "executing Archiving files"
	fi

	if (( $is_hsmlite == 0 || $shook != 0 || $is_lhsm != 0 )); then
	    echo "Reading changelogs and Applying purge policy..."
	    $RH -f $RBH_CFG_DIR/$config_file --scan --run=purge -l DEBUG -L rh_purge.log --once  &

	    sleep 1

	    echo "wait robinhood"
	    wait

	    nb_purge=`grep "$REL_STR" rh_purge.log | wc -l`
	    if (( $nb_purge != 10 )); then
	        error "********** TEST FAILED (Log): $nb_purge files purged, but 10 expected"
            ((nbError++))
	    fi
    else #backup mod
	    echo "Launch Migration in background"
	    $RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=all -l DEBUG -L rh_migr.log --once &

	    sleep 1

	    echo "wait robinhood"
	    wait

        count=`find $BKROOT -type f  -not -name "*.lov" | wc -l`
        if (($count != 10)); then
            error "********** TEST FAILED (File System): $count files migrated, but 10 expected"
            ((nbError++))
        fi
    fi

	if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

function get_nb_stat
{
    grep "STATS" $1 | grep "Dumping stats at" | wc -l
}

function TEST_OTHER_PARAMETERS_2
{
	# Test for many parameters
	# 	TEST_OTHER_PARAMETERS_2 config_file
	#=>
	# config_file == config file name

	config_file=$1

    if (( ($is_hsmlite == 0) && ($is_lhsm == 0) )); then
		echo "No TEST_OTHER_PARAMETERS_2 for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "Create Files ..."
    for i in `seq 1 5` ; do
    	dd if=/dev/zero of=$RH_ROOT/file.$i bs=10M count=1 >/dev/null \
            2>/dev/null || error "writing file.$i"
	done
    for i in `seq 6 10` ; do
    	touch $RH_ROOT/file.$i
	done

	sleep 1
	$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log --once \
        2>/dev/null

	echo "Migrate files"
	$RH -f $RBH_CFG_DIR/$config_file --run=migration -l DEBUG -L rh_migr.log \
        2>/dev/null &
	pid=$!

    if (( $is_lhsm > 0 )); then
        sleep 2
        wait_done 60
    else
	    sleep 5
    fi

    nbError=0
	count=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    if (( $count != 10 )); then
        error "********** TEST FAILED (File System): $count files migrated,"\
              "but 10 expected"
        ((nbError++))
    fi

    # Migration dans fs
    countMigrLog=`grep "$ARCH_STR" rh_migr.log | wc -l`
    if (( $countMigrLog != 10 )); then
        error "********** TEST FAILED (Log): $countMigrLog files migrated,"\
              "but 10 expected"
        ((nbError++))
    fi

    #comptage du nombre de "STATS"
    nb_Stats=$(get_nb_stat rh_migr.log)

	echo "Sleep 5.5 seconds"
	usleep 5500000

    #comptage du nombre de "STATS"
    nb_Stats2=$(get_nb_stat rh_migr.log)
	if (( $nb_Stats2 != $nb_Stats + 1 )); then
        error "********** TEST FAILED (Stats): $nb_Stats2 \"STATS\" detected,"\
              "but $nb_Stats + 1 \"STATS\" expected"
        ((nbError++))
    fi

	count=`find $BKROOT -type f  -not -name "*.lov" | wc -l`
    if (( $count != 10 )); then
        error "********** TEST FAILED (File System): $count files migrated, "\
              "but 10 expected"
        ((nbError++))
    fi

	if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi

    kill -9 $pid
}

function TEST_OTHER_PARAMETERS_3
{
	# Test for many parameters
	# 	TEST_OTHER_PARAMETERS_3 config_file
	#=>
	# config_file == config file name

	config_file=$1

    if (( ($is_hsmlite == 0) && ($is_lhsm == 0) )); then
		echo "No TEST_OTHER_PARAMETERS_3 for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "Create Files ..."
    for i in `seq 1 5` ; do
    	dd if=/dev/zero of=$RH_ROOT/file.$i bs=1K count=1 >/dev/null \
            2>/dev/null || error "writing file.$i"
	done

	echo "Archives files"
	$RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=all \
        --once -l DEBUG -L rh_migr.log 2>/dev/null
    (( $is_lhsm > 0 )) && wait_done 60

	nbError=0
	count=`find $BKROOT -type f  -not -name "*.lov" | wc -l`
    if (( $count != 5 )); then
        error "********** TEST FAILED (File System): $count files migrated,"\
              "but 5 expected"
        ((nbError++))
    fi

    local rmd=()
    for i in `seq 1 5` ; do
        local f=$RH_ROOT/file.$i
        (( $is_lhsm > 0 )) && f=$($LFS path2fid $f | tr -d '[]')
    	rm -f $RH_ROOT/file.$i && rmd+=($f)
	done

	$RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log \
        --once 2>/dev/null

    # wait rm_time + 1
	echo "sleep 6 seconds"
	sleep 6

	echo "HSM Remove"
	$RH -f $RBH_CFG_DIR/$config_file --run=hsm_remove -l DEBUG \
        -L rh_purge.log 2>/dev/null &
	pid=$!

    # make sure hsm remove pass finished
	sleep 2

	nb_Remove=`grep "$HSMRM_STR" rh_purge.log | wc -l`
	if (( $nb_Remove != 4 )); then
        error "********** TEST FAILED (LOG): $nb_Remove remove detected,"\
              "but 4 expected"
        ((nbError++))
    fi

    # 1 file out of 5 must remain in the backend
    local countRemainFile=0
	for i in ${rmd[*]}; do
        bi=$(basename $i)
        if (( $is_lhsm > 0 )); then # <fid>
            [ "$DEBUG" = "1" ] && find $BKROOT -type f -name "${bi}"
	        local found=`find $BKROOT -type f -name "${bi}" | wc -l`
        else # <name>__<fid>
            [ "$DEBUG" = "1" ] && find $BKROOT -type f -name "${bi}__*"
	        local found=`find $BKROOT -type f -name "${bi}__*" | wc -l`
        fi
        (( $found != 0 )) && echo "$i remains in backend"
        ((countRemainFile+=$found))
	done
    if (($countRemainFile != 1)); then
        error "********** TEST FAILED (File System): Wrong count of "\
              "remaining files: $countRemainFile (1 expected)"
        ((nbError++))
    fi

    # wait check_interval +1
	echo "sleep 11 seconds"
	sleep 11

	nb_Remove=`grep "$HSMRM_STR" rh_purge.log | wc -l`
	if (( $nb_Remove != 5 )); then
        error "********** TEST FAILED (LOG): $nb_Remove remove detected,"\
              "but 5 expected"
        ((nbError++))
    fi

    # no file must remain in the backend
    countRemainFile=0
	for i in ${rmd[*]}; do
        bi=$(basename $i)
        [ "$DEBUG" = "1" ] && find $BKROOT -type f -name "${bi}__*"
	    local found=`find $BKROOT -type f -name "${bi}__*" | wc -l`
        (( $found != 0 )) && echo "$i remains in backend"
        ((countRemainFile+=$found))
	done
    if (($countRemainFile != 0)); then
        error "********** TEST FAILED (File System): Wrong count of "\
              "remaining files: $countRemainFile (0 expected)"
        ((nbError++))
    fi

	if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi

    kill -9 $pid
}

function TEST_OTHER_PARAMETERS_4
{
    # Test for many parameters
    #     TEST_OTHER_PARAMETERS_4 config_file
    #=>
    # config_file == config file name

    config_file=$1

    if (( $is_hsmlite == 0 )); then
        echo "No TEST_OTHER_PARAMETERS_4 for this purpose: skipped"
        set_skipped
        return 1
    fi

    clean_logs

    # test initial condition: backend must not be mounted
    umount $BKROOT || umount -f $BKROOT

    echo "Create Files ..."
    for i in `seq 1 11` ; do
        dd if=/dev/zero of=$RH_ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"
    done

    echo "Migrate files (must fail)"
    $RH -f $RBH_CFG_DIR/$config_file --scan --run=migration --target=all --once -l DEBUG -L rh_migr.log
    (( $is_lhsm > 0 )) && wait_done 60

    nbError=0
    count=`find $BKROOT -type f  -not -name "*.lov" | wc -l`
    if (( $count != 0 )); then
        error "********** TEST FAILED (File System): $count files migrated, but 0 expected"
        ((nbError++))
    elif grep "Failed to initialize status manager backup" rh_migr.log > /dev/null; then
        echo "OK: backend not initialized"
    else
        error "Backend initialization SHOULD have FAILED"
    fi
    :> rh_migr.log

    ensure_init_backend || error "Error initializing backend $BKROOT"

    echo "Migrate files (once)"
    $RH -f $RBH_CFG_DIR/$config_file --scan -l DEBUG -L rh_scan.log --once
    $RH -f $RBH_CFG_DIR/$config_file --run="migration(target=all)" -l DEBUG -L rh_migr.log

    nbError=0
    count=`find $BKROOT -type f  -not -name "*.lov" | wc -l`
    if (( $count != 0 )); then
        error "********** TEST FAILED (File System): $count files migrated, but 0 expected"
        ((nbError++))
    fi

    echo "Migrate files (daemon)"
    $RH -f $RBH_CFG_DIR/$config_file --run=migration -l DEBUG -L rh_migr.log &
    pid=$!

    # wait for runtime_interval
    echo "sleep 11 seconds"
    sleep 11
        (( $is_lhsm > 0 )) && wait_done 60

    nbError=0
    count=`find $BKROOT -type f  -not -name "*.lov" | wc -l`
    if (( $count != 10 )); then
        error "********** TEST FAILED (File System): $count files migrated, but 10 expected"
        ((nbError++))
    fi

    if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi

    kill -9 $pid
    rm -rf $BKROOT/*
    umount -f $BKROOT
}

function assert_nb_scan
{
    local log=$1
    local expect=$2

    grep -E "Starting scan|Full scan of" $log >&2

    local nb_scan=`grep "Starting scan of" $log | wc -l`
    if (( $nb_scan != $expect )); then
        error "********** TEST FAILED (LOG): $nb_scan scan detected,"\
              "but $expect expected"
        return 1
    else
        echo "OK: $nb_scan scan started"
    fi
    return 0
}

function get_scan_interval
{
    local log=$1
    local pid=$2
    local interv=""

    # make robinhood dump current scan interval in its log
    kill -USR1 $pid
    while  [ -z "$interv" ]; do
        sleep 1
        interv=$(grep "scan interval" $log | awk '{print $(NF)}' |
                 sed -e "s/s$//" | sed -e "s/^0\([^0]\)/\1/g")
    done
    echo "current scan interval: $interv sec" >&2
    echo $interv
}

function TEST_OTHER_PARAMETERS_5
{
    # Test for many parameters
    #     TEST_OTHER_PARAMETERS_5 config_file
    #=>
    # config_file == config file name

    config_file=$1

    if (( ($shook + $is_lhsm) == 0 )); then
        echo "No TEST_OTHER_PARAMETERS_5 for this purpose: skipped"
        set_skipped
        return 1
    fi

    clean_logs
    # make sure the initial scan interval in based on empty FS
    wait_stable_df

    echo "Launch scan in background..."
    $RH -f $RBH_CFG_DIR/$config_file --scan --check-thresholds=purge -l DEBUG \
        -L rh_scan.log 2>/dev/null &
    local pid=$!

    # wait for scan to actually start
    sleep 1

    nbError=0
    assert_nb_scan rh_scan.log 1 || ((nbError++))

    # make robinhood dump current scan interval in its log
    local interv=$(get_scan_interval rh_scan.log $pid)
    ((interv++))

    echo "sleeping $interv seconds"
    sleep $interv 

    # terminate the process and flush its log
    kill $pid
    sleep 1
    # check there was a second scan
    assert_nb_scan rh_scan.log 2 || ((nbError++))

    kill -9 $pid 2>/dev/null

    # create files to fullfill the FS
    echo "Create files"
    elem=`$LFS df $RH_ROOT | grep "filesystem summary" | awk '{ print $6 }' | sed 's/%//'`
    limit=50
    indice=1
    while (( $elem < $limit ))
    do
        dd if=/dev/zero of=$RH_ROOT/file.$indice bs=10M count=1 >/dev/null 2>/dev/null
        if (( $? != 0 )); then
            echo "WARNING: fail writing file.$indice (usage $elem/$limit)"
            # give it a chance to end the loop
            ((limit=$limit-1))
        fi
        unset elem
        elem=`$LFS df $RH_ROOT | grep "filesystem summary" | awk '{ print $6 }' | sed 's/%//'`
        ((indice++))
    done

    :> rh_scan.log

    echo "Launch scan in background..."
    $RH -f $RBH_CFG_DIR/$config_file --scan --check-thresholds=purge -l DEBUG \
        -L rh_scan.log 2>/dev/null &
    pid=$!

    sleep 2
    local interv2=$(get_scan_interval rh_scan.log $pid)
    ((interv2++))

    # interv2 is expected to be smaller than interv
    # as the FS is more full
    (( $interv2 <= $interv )) || error "2nd scan interval should be smaller"

    echo "sleep $interv2 seconds"
    sleep $interv2

    # should start 2 scans (1 initial + 1 after 3sec)
    assert_nb_scan rh_scan.log 2 || ((nbError++))

    if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi

    kill -9 $pid
}

##################################################################
############### End Other Parameters Functions ###################
##################################################################

# clear summary
cp /dev/null $SUMMARY

#init xml report
if (( $junit )); then
	junit_init
	tinit=`date "+%s.%N"`
fi

######### TEST FAMILIES ########
# 1xx - collecting info and database
# 2xx - policy matching
# 3xx - triggers
# 4xx - reporting
# 5xx - internals, misc.
# 6xx - Tests by Sogeti
################################

##### info collect. + DB tests #####

run_test 100	test_info_collect info_collect.conf 1 1 "escape string in SQL requests"
run_test 101a    test_info_collect2  info_collect2.conf  1 "scan x3"
run_test 101b 	test_info_collect2  info_collect2.conf	2 "readlog/scan x2"
run_test 101c 	test_info_collect2  info_collect2.conf	3 "readlog x2 / scan x2"
run_test 101d 	test_info_collect2  info_collect2.conf	4 "scan x2 / readlog x2"
run_test 101e 	test_info_collect2  info_collect2.conf	5 "diff+apply x2"
run_test 102	update_test test_updt.conf 3 14 "db update policy"
run_test 103a    test_acct_table common.conf 5 "" "Acct table and triggers creation (default)"
run_test 103b    test_acct_table acct.conf 5 "yes" "Acct table and triggers creation (accounting ON)"
run_test 103c    test_acct_table acct.conf 5 "no" "Acct table and triggers creation (accounting OFF)"
run_test 104     test_size_updt test_updt.conf 1 "test size update"
run_test 105     test_enoent test_pipeline.conf "readlog with continuous create/unlink"
run_test 106a    test_diff info_collect2.conf "diff" "rbh-diff"
run_test 106b    test_diff info_collect2.conf "diffapply" "rbh-diff --apply"
run_test 106c    test_diff info_collect2.conf "scan" "robinhood --scan --diff"
run_test 106d    test_diff info_collect2.conf "partdiff" "rbh-diff --scan=subdir"
run_test 107a    test_completion test_completion.conf OK        "scan completion command"
run_test 107b    test_completion test_completion.conf unmatched "wrong completion command (syntax error)"
run_test 107c    test_completion test_completion.conf invalid_ctx_id "wrong completion command (using id)"
run_test 107d    test_completion test_completion.conf invalid_ctx_attr "wrong completion command (using attr)"
run_test 107e    test_completion test_completion.conf invalid_attr "wrong completion command (unknown var)"
run_test 108a    test_rename info_collect.conf scan "rename cases (scan)"
run_test 108b    test_rename info_collect.conf readlog "rename cases (readlog)"
run_test 108c    test_rename info_collect.conf partial "rename cases (partial scans)"
run_test 108d    test_rename info_collect.conf diff "rename cases (diff+apply)"
run_test 108e    test_rename info_collect.conf partdiff "rename cases (partial diffs+apply)"
run_test 109a    test_hardlinks info_collect.conf scan "hardlinks management (scan)"
run_test 109b    test_hardlinks info_collect.conf readlog "hardlinks management (readlog)"
run_test 109c    test_hardlinks info_collect.conf partial "hardlinks management (partial scans)"
run_test 109d    test_hardlinks info_collect.conf diff "hardlinks management (diff+apply)"
run_test 109e    test_hardlinks info_collect.conf partdiff "hardlinks management (partial diffs+apply)"
run_test 110     test_unlink info_collect.conf "unlink (readlog)"
run_test 111     test_layout info_collect.conf "layout changes"
run_test 112     test_hl_count info_collect.conf "reports with hardlinks"
run_test 113     test_diff_apply_fs info_collect2.conf  "diff"  "rbh-diff --apply=fs"
run_test 114     test_root_changelog info_collect.conf "changelog record on root entry"
run_test 115     partial_paths info_collect.conf "test behavior when handling partial paths"
run_test 116     test_mnt_point test_mnt_point.conf "test with mount point != fs_path"

function runtest_117
{
    cfg=common.conf
    char=a
    for flavor in scan scandiff1 scandiff2 scandiff3 diff1 diff2 diff3 cl cldiff1 cldiff2 cldiff3; do
        run_test 117$char stripe_update $cfg $flavor "stripe information update (flavor=$flavor)"
        # increment char
        char=$(echo $char | tr "a-y" "b-z")
    done
}
runtest_117

function runtest_118
{
    cfg=common.conf
    char=a
    for flavor in diffna1 diffna2 diffna3; do
        run_test 118$char stripe_no_update $cfg $flavor "stripe information => update (flavor=$flavor)"
        # increment char
        char=$(echo $char | tr "a-y" "b-z")
    done
}
runtest_118

run_test 119 uid_gid_as_numbers uidgidnum.conf "Store UIDs and GIDs as numbers"
run_test 120 posix_acmtime common.conf "Test for posix ctimes"
run_test 121 db_schema_convert "" "Test DB schema conversion"
run_test 122 random_names common.conf "Test random file names"
run_test 123 test_acct_borderline acct.conf "yes" "Test borderline ACCT cases"
run_test 124 test_commit_update commit_update.conf "Update of last committed changelog"
run_test 125a test_path_gc1 test_rm1.conf "Test namespace garbage collection with partial scans"
run_test 125b test_path_gc2 test_rm1.conf "Test namespace garbage collection after rename"

#### policy matching tests  ####

run_test 200	path_test test_path.conf 2 "path matching policies"
run_test 201	migration_test test1.conf 11 11 "last_mod>10s"
run_test 202	migration_test test2.conf 5  11 "last_mod>10s and name == \"*[0-5]\""
run_test 203	migration_test test3.conf 5  11 "complex policy with filesets"
run_test 204	migration_test test3.conf 10 21 "complex policy with filesets"
run_test 205	xattr_test test_xattr.conf 2 "xattr-based fileclass definition"
run_test 206	purge_test test_purge.conf 11 16 "last_access > 15s"
run_test 207	purge_size_filesets test_purge2.conf 2 3 "purge policies using size-based filesets"
run_test 208a	periodic_class_match_migr test_updt.conf 10 "fileclass matching 1 (migration)"
run_test 208b	policy_check_migr test_check_migr.conf 10 "fileclass matching 2 (migration)"
run_test 209a	periodic_class_match_purge test_updt.conf 10 "fileclass matching 1 (purge)"
run_test 209b	policy_check_purge test_check_purge.conf 10 "fileclass matching 2 (purge)"
run_test 210	fileclass_test test_fileclass.conf 2 "complex policies with unions and intersections of filesets"
run_test 211	test_pools test_pools.conf 1 "class matching with condition on pools"
run_test 212a	link_unlink_remove_test test_rm1.conf 1 11 "deferred hsm_remove"
run_test 212b   test_hsm_remove         test_rm1.conf 2 11 "deciding softrm for removed entries"
run_test 212c   test_lhsm_remove        test_rm1.conf 4 11 "test archive_id parameter for lhsm_remove"
run_test 213	migration_test_single test1.conf 11 11 "simple migration policy"
run_test 214a  check_disabled  common.conf  purge      "no purge if not defined in config"
run_test 214b  check_disabled  common.conf  migration  "no migration if not defined in config"
run_test 214c  check_disabled  common.conf  rmdir      "no rmdir if not defined in config"
run_test 214d  check_disabled  common.conf  hsm_remove "hsm_rm is enabled by default"
run_test 214e  check_disabled  common.conf  class      "no class matching if none defined in config"
run_test 215	mass_softrm    test_rm1.conf 11 1000    "rm are detected between 2 scans"
run_test 216   test_maint_mode test_maintenance.conf 30 45 5 "pre-maintenance mode"
run_test 217	migrate_symlink test1.conf 11 		"symlink migration"
run_test 218	test_rmdir 	rmdir.conf 11 		"rmdir policies"
run_test 219    test_rmdir_mix RemovingDir_Mixed.conf 11 "mixed rmdir policies"
# test sort order by last_archive, last_mod, creation
# check order of application
# check request splitting, optimizations, ...
run_test 220a test_lru_policy lru_sort_creation.conf "" "0 1 2 3" 20 "lru sort on creation"
run_test 220b test_lru_policy lru_sort_mod.conf "" "0 1 5 8 9" 10 "lru sort on last_mod"
run_test 220c test_lru_policy lru_sort_mod_2pass.conf "" "0 1 2 3 4 5 6 7 8 9" 30 "lru sort on last_mod in 2 pass"
run_test 220d test_lru_policy lru_sort_access.conf "" "0 2 3 6 8 9" 20 "lru sort on last_access"
run_test 220e test_lru_policy lru_sort_archive.conf "0 1 2 3 4 5 6 7 8 9" "" 15 "lru sort on last_archive"
run_test 220f test_lru_policy lru_sort_creat_last_arch.conf "0 1 2 3" "4 5 6 7 8 9" 10 "lru sort on creation and last_archive==0"
run_test 221  test_suspend_on_error migr_fail.conf  2 "suspend migration if too many errors"
run_test 222  test_custom_purge test_custom_purge.conf 2 "custom purge command"
run_test 223  test_default test_default_case.conf "ignore entries if no default case is specified"
run_test 224  test_undelete test_rm1.conf   "undelete"
run_test 225  test_compress compress.conf "compressed archived files"
run_test 226a  test_purge_lru lru_purge.conf last_access "test purge order (lru_sort_attr=last_access)"
run_test 226b  test_purge_lru lru_purge.conf none "test purge order (lru_sort_attr=none)"
run_test 227  test_action_params test_action_params.conf "custom policy actions and parameters"
run_test 228a  test_manual_run test_run.conf 5 run "test manual policy runs (run)"
run_test 228b  test_manual_run test_run.conf 5 run_all "test manual policy runs (run all)"
run_test 228c  test_manual_run test_run.conf 5 run_migr "test manual policy runs (run migr.)"
run_test 228d  test_manual_run test_run.conf 5 run_migr_tgt "test manual policy runs (run migr with target)"
run_test 228e  test_manual_run test_run.conf 5 run_migr_usage "test manual policy runs (run migr with target usage)"
run_test 228f  test_manual_run test_run.conf 5 run_both "test manual policy runs (run migr and purge with targets)"
run_test 229a  test_limits test_limits.conf trig_cnt "test trigger limit on count"
run_test 229b  test_limits test_limits.conf trig_vol "test trigger limit on volume"
run_test 229c  test_limits test_limits.conf param_cnt "test parameter limit on count"
run_test 229d  test_limits test_limits.conf param_vol "test parameter limit on volume"
run_test 229e  test_limits test_limits.conf run_cnt "test run limit on count"
run_test 229f  test_limits test_limits.conf run_vol "test run limit on volume"
run_test 229g  test_limits test_limits.conf trig_param "test limit on both trigger and param"
run_test 229h  test_limits test_limits.conf trig_run "test limit on both trigger and run"
run_test 229i  test_limits test_limits.conf param_run "test limit on both param and run"
run_test 230   test_checker test_checker.conf "policies based on 'checker' module"
run_test 231   test_action_check OtherParameters_4.conf "check status of current actions"
run_test 232a  test_sched_limits test_sched1.conf sched_max_cnt "check max count enforced by scheduler"
run_test 232b  test_sched_limits test_sched1.conf sched_max_vol "check max vol enforced by scheduler"
run_test 232c  test_sched_limits test_sched1.conf trigger "check trigger vs. max_per_run scheduler"
run_test 232d  test_sched_limits test_sched1.conf param "check policy parameter vs. max_per_run scheduler"
run_test 232e  test_sched_limits test_sched1.conf cmd "check cmd line vs. max_per_run scheduler"
run_test 233   test_basic_sm     test_basic.conf  "Test basic status manager"
run_test 234   test_modeguard_sm_dir test_modeguard_dir.conf "Test modeguard status manager with directories"
run_test 235   test_modeguard_sm_file test_modeguard_file.conf "Test modeguard status manager with files"
run_test 236a  test_prepost_sched test_prepost_sched.conf none none \
    common.max_per_run "pre/post_sched_match=none"
run_test 236b  test_prepost_sched test_prepost_sched.conf cache_only none \
    common.max_per_run "pre_sched_match=cache_only"
run_test 236c  test_prepost_sched test_prepost_sched.conf auto_update none \
    common.max_per_run "pre_sched_match=auto_update"
run_test 236d  test_prepost_sched test_prepost_sched.conf auto_update none \
    "" "pre_sched_match=auto_update (no scheduler)"
run_test 236e  test_prepost_sched test_prepost_sched.conf force_update none \
    common.max_per_run "pre_sched_match=force_update"
run_test 236f  test_prepost_sched test_prepost_sched.conf none cache_only \
    common.max_per_run "post_sched_match=cache_only"
run_test 236g  test_prepost_sched test_prepost_sched.conf none auto_update \
    common.max_per_run "post_sched_match=auto_update"
run_test 236h  test_prepost_sched test_prepost_sched.conf none auto_update \
    "" "post_sched_match=auto_update (no scheduler)"
run_test 236i  test_prepost_sched test_prepost_sched.conf none force_update \
    common.max_per_run "post_sched_match=force_update"
run_test 237   test_lhsm_archive test_lhsm1.conf "check sql query string in case of multiple AND/OR"
run_test 238   test_multirule_select test_multirule.conf "check sql query string in case of multiple rules"
run_test 239   test_rmdir_depth  test_rmdir_depth.conf "check sql query for rmdir with depth condition"
run_test 240   test_prepost_cmd  test_prepost_cmd.conf "test pre/post_run_command"

#### triggers ####

run_test 300	test_cnt_trigger test_trig.conf 151 21 "trigger on file count"
run_test 301    test_ost_trigger test_trig2.conf 150 110 "trigger on OST usage"
run_test 302	test_trigger_check test_trig3.conf 60 110 "triggers check only" 40 80 5 10 40
run_test 303    test_periodic_trigger test_trig4.conf 5 "periodic trigger"
run_test 304    test_ost_order test_trig2.conf "OST purge order"


#### reporting ####
run_test 400	test_rh_report common.conf 3 1 "reporting tool"
run_test 401a   test_rh_acct_report common.conf 5 "" "reporting tool: config file without acct param"
run_test 401b   test_rh_acct_report acct.conf 5 "yes" "reporting tool: config file with accounting=no"
run_test 401c   test_rh_acct_report acct.conf 5 "no" "reporting tool: config file with accounting=yes"
run_test 402a   test_rh_report_split_user_group common.conf 5 "" "report with split-user-groups option"
run_test 402b   test_rh_report_split_user_group common.conf 5 "--force-no-acct" "report with split-user-groups and force-no-acct option"
run_test 403    test_sort_report common.conf 0 "Sort options of reporting command"
run_test 404   test_dircount_report common.conf 20  "dircount reports"

run_test 405    test_find   common.conf ""  "rbh-find command"
run_test 406    test_du   common.conf ""    "rbh-du command"

#### misc, internals #####
run_test 500a	test_logs log1.conf file_nobatch 	"file logging without alert batching"
run_test 500b	test_logs log2.conf syslog_nobatch 	"syslog without alert batching"
run_test 500c	test_logs log3.conf stdio_nobatch 	"stdout and stderr without alert batching"
run_test 500d	test_logs log1b.conf file_batch 	"file logging with alert batching"
run_test 500e	test_logs log2b.conf syslog_batch 	"syslog with alert batching"
run_test 500f	test_logs log3b.conf stdio_batch 	"stdout and stderr with alert batching"
run_test 501a 	test_cfg_parsing basic none		"parsing of Robinhood v3 basic.conf"
run_test 501b 	test_cfg_parsing example none		"parsing of Robinhood v3 example.conf"
run_test 501c 	test_cfg_parsing generated none		"parsing of generated template"
run_test 502a    recovery_test	test_recov.conf  full    1 "FS recovery"
run_test 502b    recovery_test	test_recov.conf  delta   1 "FS recovery with delta"
run_test 502c    recovery_test	test_recov.conf  rename  1 "FS recovery with renamed entries"
run_test 502d    recovery_test	test_recov.conf  partial 1 "FS recovery with missing backups"
run_test 502e    recovery_test	test_recov.conf  mixed   1 "FS recovery (mixed status)"
run_test 503a    recovery_test	test_recov2.conf  full    0 "FS recovery (archive_symlinks=FALSE)"
run_test 503b    recovery_test	test_recov2.conf  delta   0 "FS recovery with delta (archive_symlinks=FALSE)"
run_test 503c    recovery_test	test_recov2.conf  rename  0 "FS recovery with renamed entries (archive_symlinks=FALSE)"
run_test 503d    recovery_test	test_recov2.conf  partial 0 "FS recovery with missing backups (archive_symlinks=FALSE)"
run_test 503e    recovery_test	test_recov2.conf  mixed   0 "FS recovery (mixed status, archive_symlinks=FALSE)"
run_test 504     import_test    test_recov.conf "Import from backend"
run_test 505a     recov_filters  test_recov.conf  ost    "FS recovery with OST filter"
run_test 505b     recov_filters  test_recov2.conf  ost    "FS recovery with OST filter (archive_symlinks=FALSE)"
run_test 506a     recov_filters  test_recov.conf  since    "FS recovery with time filter"
run_test 506b     recov_filters  test_recov2.conf  since    "FS recovery with time filter (archive_symlinks=FALSE)"
run_test 507a     recov_filters  test_recov.conf  dir    "FS recovery with dir filter"
run_test 507b     recov_filters  test_recov2.conf  dir    "FS recovery with dir filter (archive_symlinks=FALSE)"
run_test 508    test_tokudb "Test TokuDB compression"
run_test 509    test_cfg_overflow "config options too long"
run_test 510    test_rbh_find_printf test_checker.conf "Test rbh-find with -printf option"
run_test 511    archive_uuid1 test_uuid.conf "Test UUID presence while scanning"
run_test 512    archive_uuid2 test_uuid.conf "Archive and undelete file with UUID using changelogs"
run_test 513    test_reload   alert.conf "Reloading configuration (with alert policy)"

#### Tests by Sogeti ####
run_test 600a test_alerts alert.conf "file1" 0 "TEST_ALERT_PATH_NAME"
run_test 600b test_alerts alert.conf "type_file" 0 "TEST_ALERT_TYPE"
run_test 600c test_alerts alert.conf "root_owner" 0 "TEST_ALERT_OWNER"
run_test 600d test_alerts alert.conf "size10k" 0 "TEST_ALERT_SIZE"
run_test 600e test_alerts alert.conf "last_access_1min" 60 "TEST_ALERT_LAST_ACCESS"
run_test 600f test_alerts alert.conf "last_mod_1min" 60 "TEST_ALERT_LAST_MODIFICATION"
run_test 600g test_alerts_OST alert_ost.conf "ost1" "TEST_ALERT_OST"
run_test 600h test_alerts alert.conf "extended_attribute" 0 "TEST_ALERT_EXTENDED_ATTRIBUT"
run_test 600i test_alerts alert.conf "nonempty_dir" 0 "TEST_ALERT_DIRCOUNT"

run_test 601a test_migration MigrationStd_Path_Name.conf 0 3 "file.6;file.7;file.8" "--run=migration --target=all" "TEST_test_migration_PATH_NAME"
run_test 601b test_migration MigrationStd_Type.conf 0 8 "file.1;file.2;file.3;file.4;file.5;file.6;file.7;file.8" "--run=migration --target=all" "TEST_MIGRATION_STD_TYPE"
run_test 601c test_migration MigrationStd_Owner.conf 0 1 "file.3" "--run=migration --target=all" "TEST_MIGRATION_STD_OWNER"
run_test 601d test_migration MigrationStd_Size.conf 0 2 "file.6;file.7" "--run=migration --target=all" "TEST_MIGRATION_STD_SIZE"
run_test 601e test_migration MigrationStd_LastAccess.conf 12 9  "file.1;file.2;file.3;file.4;file.5;file.6;file.7;link.1;link.2" "--run=migration --target=all" "TEST_MIGRATION_STD_LAST_ACCESS"
run_test 601f test_migration MigrationStd_LastModification.conf 11 2 "file.8;file.9" "--run=migration --target=all" "TEST_MIGRATION_STD_LAST_MODIFICATION"
run_test 601g test_migration MigrationStd_ExtendedAttribut.conf 0 1 "file.4" "--run=migration --target=all" "TEST_MIGRATION_STD_EXTENDED_ATTRIBUT"
run_test 601h test_migration MigrationClass_Path_Name.conf 0 3 "file.6;file.7;file.8" "--run=migration --target=all" "TEST_MIGRATION_CLASS_PATH_NAME"
run_test 601i test_migration MigrationClass_Type.conf 0 2 "link.1;link.2" "--run=migration --target=all" "TEST_MIGRATION_CLASS_TYPE"
run_test 601j test_migration MigrationClass_Owner.conf 0 1 "file.3" "--run=migration --target=all" "TEST_MIGRATION_CLASS_OWNER"
run_test 601k test_migration MigrationClass_Size.conf 0 2 "file.6;file.7" "--run=migration --target=all" "TEST_MIGRATION_CLASS_SIZE"
run_test 601l test_migration MigrationClass_LastAccess.conf 11 8 "file.1;file.2;file.4;file.5;file.6;file.7;link.1;link.2" "--run=migration --target=all" "TEST_MIGRATION_CLASS_LAST_ACCESS"
run_test 601m test_migration MigrationClass_LastModification.conf 11 2 "file.8;file.9" "--run=migration --target=all" "TEST_MIGRATION_CLASS_LAST_MODIFICATION"
run_test 601n test_migration MigrationClass_ExtendedAttribut.conf 0 1 "file.4" "--run=migration --target=all" "TEST_MIGRATION_CLASS_EXTENDED_ATTRIBUT"
run_test 601o test_migration MigrationUser.conf 0 1 "file.3" "--run=migration --target=user:testuser" "TEST_MIGRATION_USER"
run_test 601p test_migration MigrationGroup.conf 0 2 "file.2;file.3" "--run=migration --target=group:testgroup" "TEST_MIGRATION_GROUP"
run_test 601q test_migration MigrationFile_Path_Name.conf 0 1 "file.1" "--run=migration --target=file:$RH_ROOT/dir1/file.1" "TEST_MIGRATION_FILE_PATH_NAME"
run_test 601r test_migration MigrationFile_Size.conf 1 1 "file.8" "--run=migration --target=file:$RH_ROOT/dir2/file.8" "TEST_MIGRATION_FILE_SIZE"

run_test 602a migration_OST MigrationStd_OST.conf 2 "file.3;file.4" "--run=migration --target=all" "TEST_MIGRATION_STD_OST"
run_test 602b migration_OST MigrationOST.conf 2 "file.3;file.4" "--run=migration --target=ost:1" "TEST_MIGRATION_OST"
run_test 602c migration_OST MigrationClass_OST.conf 2 "file.3;file.4" "--run=migration --target=all" "TEST_MIGRATION_CLASS_OST"

run_test 603 migration_file_type MigrationFile_Type.conf 0 1 "link.1" "TEST_MIGRATION_FILE_TYPE"
run_test 604 migration_file_owner MigrationFile_Owner.conf 0 1 "file.3" "--run=migration --target=file:$RH_ROOT/dir1/file.3" "TEST_MIGRATION_FILE_OWNER"
run_test 605 migration_file_Last MigrationFile_LastAccess.conf 12 1 "file.1" "TEST_MIGRATION_FILE_LAST_ACCESS"
run_test 606 migration_file_Last MigrationFile_LastModification.conf 12 1 "file.1" "TEST_MIGRATION_FILE_LAST_MODIFICATION"
run_test 607 migration_file_OST MigrationFile_OST.conf 1 "file.3" "TEST_MIGRATION_FILE_OST"
run_test 608 migration_file_ExtendedAttribut MigrationFile_ExtendedAttribut.conf 0 1 "file.4"  "TEST_MIGRATION_FILE_EXTENDED_ATTRIBUT"

run_test 609 trigger_purge_QUOTA_EXCEEDED TriggerPurge_QuotaExceeded.conf "TEST_TRIGGER_PURGE_QUOTA_EXCEEDED"
run_test 610 trigger_purge_OST_QUOTA_EXCEEDED TriggerPurge_OstQuotaExceeded.conf "TEST_TRIGGER_PURGE_OST_QUOTA_EXCEEDED"
if [[ $RBH_NUM_UIDGID = "yes" ]]; then
    run_test 611 trigger_purge_USER_GROUP_QUOTA_EXCEEDED TriggerPurge_UserQuotaExceeded.conf "user '0'" "TEST_TRIGGER_PURGE_USER_QUOTA_EXCEEDED"
    run_test 612 trigger_purge_USER_GROUP_QUOTA_EXCEEDED TriggerPurge_GroupQuotaExceeded.conf "group '0'" "TEST_TRIGGER_PURGE_GROUP_QUOTA_EXCEEDED"
else
    run_test 611 trigger_purge_USER_GROUP_QUOTA_EXCEEDED TriggerPurge_UserQuotaExceeded.conf "user 'root'" "TEST_TRIGGER_PURGE_USER_QUOTA_EXCEEDED"
    run_test 612 trigger_purge_USER_GROUP_QUOTA_EXCEEDED TriggerPurge_GroupQuotaExceeded.conf "group 'root'" "TEST_TRIGGER_PURGE_GROUP_QUOTA_EXCEEDED"
fi

run_test 613a test_purge PurgeStd_Path_Name.conf 0 7 "file.6;file.7;file.8" "--run=purge --target=all" "TEST_PURGE_STD_PATH_NAME"
run_test 613b test_purge_tmp_fs_mgr PurgeStd_Type.conf 0 8 "link.1;link.2" "--run=purge --target=all" "TEST_PURGE_STD_TYPE"
run_test 613c test_purge PurgeStd_Owner.conf 0 9 "file.3" "--run=purge --target=all" "TEST_PURGE_STD_OWNER"
run_test 613d test_purge PurgeStd_Size.conf 0 8 "file.6;file.7" "--run=purge --target=all" "TEST_PURGE_STD_SIZE"
run_test 613e test_purge PurgeStd_LastAccess.conf 10 9 "file.8" "--run=purge --target=all" "TEST_PURGE_STD_LAST_ACCESS"
run_test 613f test_purge PurgeStd_LastModification.conf 30 9 "file.8" "--run=purge --target=all" "TEST_PURGE_STD_LAST_MODIFICATION"
run_test 613g test_purge PurgeStd_ExtendedAttribut.conf 0 9 "file.4" "--run=purge --target=all" "TEST_PURGE_STD_EXTENDED_ATTRIBUT"
run_test 613h test_purge PurgeClass_Path_Name.conf 0 9 "file.1" "--run=purge --target=all" "TEST_PURGE_CLASS_PATH_NAME"
run_test 613i test_purge PurgeClass_Type.conf 0 2 "file.1;file.2;file.3;file.4;file.5;file.6;file.7;file.8" "--run=purge --target=all" "TEST_PURGE_CLASS_TYPE"
run_test 613j test_purge PurgeClass_Owner.conf 0 3 "file.1;file.2;file.4;file.5;file.6;file.7;file.8" "--run=purge --target=all" "TEST_PURGE_CLASS_OWNER"
run_test 613k test_purge PurgeClass_Size.conf 0 8 "file.6;file.7" "--run=purge --target=all" "TEST_PURGE_CLASS_SIZE"
run_test 613l test_purge PurgeClass_LastAccess.conf 20 9 "file.8" "--run=purge --target=all" "TEST_PURGE_CLASS_LAST_ACCESS"
run_test 613m test_purge PurgeClass_LastModification.conf 20 9 "file.8" "--run=purge --target=all" "TEST_PURGE_CLASS_LAST_MODIFICATION"
run_test 613n test_purge PurgeClass_ExtendedAttribut.conf 0 9 "file.4" "--run=purge --target=all" "TEST_PURGE_CLASS_EXTENDED_ATTRIBUT"

run_test 614a purge_OST PurgeStd_OST.conf 2 "file.3;file.4" "--run=purge --target=all" "TEST_PURGE_STD_OST"
run_test 614b purge_OST PurgeOST.conf 2 "file.3;file.4" "--run=purge --target=ost:1 --target-usage=0" "TEST_PURGE_OST"
run_test 614c purge_OST PurgeClass_OST.conf 2 "file.3;file.4" "--run=purge --target=all" "TEST_PURGE_CLASS_OST"

run_test 615a test_removing RemovingEmptyDir.conf "emptyDir" 11 "TEST_REMOVING_EMPTY_DIR"
run_test 615b test_removing RemovingDir_Path_Name.conf "pathName" 0 "TEST_REMOVING_DIR_PATH_NAME"
run_test 615c test_removing RemovingDir_Owner.conf "owner" 0 "TEST_REMOVING_DIR_OWNER"
run_test 615d test_removing RemovingDir_LastAccess.conf "lastAccess" 11 "TEST_REMOVING_DIR_LAST_ACCESS"
run_test 615e test_removing RemovingDir_LastModification.conf "lastModif" 11 "TEST_REMOVING_DIR_LAST_MODIFICATION"
run_test 615f test_removing RemovingDir_ExtendedAttribute.conf "extAttributes" 0 "TEST_REMOVING_DIR_EXTENDED_ATTRIBUT"
run_test 615g test_removing RemovingDir_Dircount.conf "dircount" 0 "TEST_REMOVING_DIR_DIRCOUNT"

run_test 616 test_removing_ost RemovingDir_OST.conf "TEST_REMOVING_DIR_OST"

run_test 617 test_report_generation_1 Generation_Report_1.conf "TEST_REPORT_GENERATION_1"
run_test 618 report_generation2 "TEST_REPORT_GENERATION_2"

run_test 619 TEST_OTHER_PARAMETERS_1 OtherParameters_1.conf "TEST_OTHER_PARAMETERS_1"
run_test 620 TEST_OTHER_PARAMETERS_2 OtherParameters_2.conf "TEST_OTHER_PARAMETERS_2"
run_test 621 TEST_OTHER_PARAMETERS_3 OtherParameters_3.conf "TEST_OTHER_PARAMETERS_3"
run_test 622 TEST_OTHER_PARAMETERS_4 OtherParameters_4.conf "TEST_OTHER_PARAMETERS_4"
run_test 623 TEST_OTHER_PARAMETERS_5 OtherParameters_5.conf "TEST_OTHER_PARAMETERS_5"

run_test 700 test_changelog common.conf "Changelog record suppression"

echo
echo "========== TEST SUMMARY ($PURPOSE) =========="
cat $SUMMARY
echo "============================================="

#init xml report
if (( $junit )); then
	tfinal=`date "+%s.%N"`
	dur=`echo "($tfinal-$tinit)" | bc -l`
	echo "total test duration: $dur sec"
	junit_write_xml "$dur" $RC $(( $RC + $SUCCESS ))
	rm -f $TMPXML_PREFIX.stderr $TMPXML_PREFIX.stdout $TMPXML_PREFIX.tc
fi

rm -f $SUMMARY
if (( $RC > 0 )); then
	echo "$RC tests FAILED, $SUCCESS successful, $SKIP skipped"
else
	echo "All tests passed ($SUCCESS successful, $SKIP skipped)"
fi
rm -f $TMPERR_FILE
exit $RC
