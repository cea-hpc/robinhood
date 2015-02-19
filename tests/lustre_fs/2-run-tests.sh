#!/bin/bash
# -*- mode: shell; sh-basic-offset: 4; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=4:tabstop=4:

[ -z "$LFS" ] && LFS=lfs
[ -z "$TESTOPEN" ] && TESTOPEN=/usr/lib64/lustre/tests/openfile

ROOT="/mnt/lustre"

RBH_BINDIR="../../src/robinhood"
#RBH_BINDIR="/usr/sbin"

BKROOT="/tmp/backend"
RBH_OPT=""

XML="test_report.xml"
TMPXML_PREFIX="/tmp/report.xml.$$"
TMPERR_FILE="/tmp/err_str.$$"

TEMPLATE_DIR='../../doc/templates'

if [[ ! -d $ROOT ]]; then
	echo "Creating directory $ROOT"
	mkdir -p "$ROOT"
else
	echo "Creating directory $ROOT"
fi

#default: TMP_FS_MGR
if [[ -z "$PURPOSE" || $PURPOSE = "TMP_FS_MGR" ]]; then
	is_lhsm=0
	is_hsmlite=0
	shook=0
	RH="$RBH_BINDIR/robinhood $RBH_OPT"
	REPORT="$RBH_BINDIR/rbh-report $RBH_OPT"
	FIND=$RBH_BINDIR/rbh-find
	DU=$RBH_BINDIR/rbh-du
    DIFF=$RBH_BINDIR/rbh-diff
	CMD=robinhood
	REL_STR="Purged"
	PURPOSE="TMP_FS_MGR"
elif [[ $PURPOSE = "LUSTRE_HSM" ]]; then
	is_lhsm=1
	is_hsmlite=0
	shook=0
	RH="$RBH_BINDIR/rbh-lhsm $RBH_OPT"
	REPORT=$RBH_BINDIR/rbh-lhsm-report
	FIND=$RBH_BINDIR/rbh-lhsm-find
	DU=$RBH_BINDIR/rbh-lhsm-du
    DIFF=$RBH_BINDIR/rbh-lhsm-diff
    UNDELETE=$RBH_BINDIR/rbh-lhsm-undo-rm
	CMD=rbh-lhsm
	PURPOSE="LUSTRE_HSM"
	ARCH_STR="Start archiving" # string followed by (fid, hints=...)
	ARCH_STR2="Start archiving" # string followed by 'path', policy_rule ...
	REL_STR="Released"
elif [[ $PURPOSE = "BACKUP" ]]; then
	is_lhsm=0
	shook=0
	is_hsmlite=1

	RH="$RBH_BINDIR/rbh-backup $RBH_OPT"
	REPORT="$RBH_BINDIR/rbh-backup-report $RBH_OPT"
	RECOV="$RBH_BINDIR/rbh-backup-recov $RBH_OPT"
	IMPORT="$RBH_BINDIR/rbh-backup-import $RBH_OPT"
	FIND=$RBH_BINDIR/rbh-backup-find
	DU=$RBH_BINDIR/rbh-backup-du
    DIFF=$RBH_BINDIR/rbh-backup-diff
    UNDELETE=$RBH_BINDIR/rbh-backup-undo-rm
	CMD=rbh-backup
	ARCH_STR="Starting backup"
	ARCH_STR2="Archived"
	REL_STR="Purged"
	mkdir -p $BKROOT
elif [[ $PURPOSE = "SHOOK" ]]; then
	is_lhsm=0
	is_hsmlite=1
	shook=1

	RH="$RBH_BINDIR/rbh-shook $RBH_OPT"
	REPORT="$RBH_BINDIR/rbh-shook-report $RBH_OPT"
	RECOV="$RBH_BINDIR/rbh-shook-recov $RBH_OPT"
	IMPORT="$RBH_BINDIR/rbh-shook-import $RBH_OPT"
	FIND=$RBH_BINDIR/rbh-shook-find
	DU=$RBH_BINDIR/rbh-shook-du
    DIFF=$RBH_BINDIR/rbh-shook-diff
    UNDELETE=$RBH_BINDIR/rbh-shook-undo-rm
	CMD=rbh-shook
	ARCH_STR="Starting backup"
	ARCH_STR2="Archived"
	REL_STR="Purged"
	mkdir -p $BKROOT
fi

LVERSION=`rpm -qa "lustre[-_]*modules*" --qf "%{Version}"`

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
	  # if the agent is on the same node as the writter, we are not sure
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

    $LFS df $ROOT > /tmp/lfsdf.1
    while (( 1 )); do
        sleep 5
        $LFS df $ROOT > /tmp/lfsdf.2
        diff /tmp/lfsdf.1 /tmp/lfsdf.2 > /dev/null && break
        echo "waiting for df update..."
        mv -f /tmp/lfsdf.2 /tmp/lfsdf.1
    done
}


lustre_major=$(cat /proc/fs/lustre/version | grep "lustre:" | awk '{print $2}' | cut -d '.' -f 1)

if [[ -z "$NOLOG" || $NOLOG = "0" ]]; then
	no_log=0
else
	no_log=1
fi

PROC=$CMD
CFG_SCRIPT="../../scripts/rbh-config"

CLEAN="rh_chglogs.log rh_migr.log rh_rm.log rh.pid rh_purge.log rh_report.log rh_syntax.log recov.log rh_scan.log /tmp/rh_alert.log rh_rmdir.log"

SUMMARY="/tmp/test_${PROC}_summary.$$"

NB_ERROR=0
RC=0
SKIP=0
SUCCES=0
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
	for f in $CLEAN; do
		if [ -s $f ]; then
			cp /dev/null $f
		fi
	done
}


function wait_done
{
	max_sec=$1
	sec=0
	if [[ -n "$MDS" ]]; then
		cmd="ssh $MDS egrep -v SUCCEED|CANCELED /proc/fs/lustre/mdt/lustre-MDT0000/hsm/actions"
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

	echo "Cleaning filesystem..."
	if [[ -n "$ROOT" ]]; then
		 find "$ROOT" -mindepth 1 -delete 2>/dev/null
	fi

	if (( $is_hsmlite + $is_lhsm != 0 )); then
		if [[ -n "$BKROOT" ]]; then
			echo "Cleaning backend content..."
			find "$BKROOT" -mindepth 1 -delete 2>/dev/null
		fi
	fi

	echo "Destroying any running instance of robinhood..."
	pkill robinhood
	pkill rbh-lhsm

	if [ -f rh.pid ]; then
		echo "killing remaining robinhood process..."
		kill `cat rh.pid`
		rm -f rh.pid
	fi

	sleep 1
#	echo "Impacting rm in HSM..."
#	$RH -f ./cfg/immediate_rm.conf --readlog --hsm-remove -l DEBUG -L rh_rm.log --once || error ""
	echo "Cleaning robinhood's DB..."
	$CFG_SCRIPT empty_db robinhood_lustre > /dev/null

	echo "Cleaning changelogs..."
	if (( $no_log==0 )); then
	   $LFS changelog_clear lustre-MDT0000 cl1 0
	fi

}

function ensure_init_backend()
{
	mnted=`mount | grep $BKROOT | grep loop | wc -l`
    if (( $mnted == 0 )); then
        LOOP_FILE=/tmp/rbh.loop.cont
        if [[ ! -s $LOOP_FILE ]]; then
            echo "creating file container $LOOP_FILE..."
            dd if=/dev/zero of=$LOOP_FILE bs=1M count=400 || return 1
            echo "formatting as ext3..."
            mkfs.ext3 -q -F $LOOP_FILE || return 1
        fi

        echo "Mounting $LOOP_FILE as $BKROOT"
        mount -o loop -t ext4 $LOOP_FILE $BKROOT || return 1
    	echo "Cleaning backend content..."
		find "$BKROOT" -mindepth 1 -delete 2>/dev/null
    fi
    return 0
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
  POOL_CREATED=1
}

function check_db_error
{
        grep DB_REQUEST_FAILED $1 && error "DB request error"
}

function get_id
{
    local p=$1
    if (( $lustre_major >= 2 )); then
        lfs path2fid $p | tr -d '[]'
    else
         stat -c "/%i" $p
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

	# create and fill 10 files

	echo "1-Modifing files..."
	for i in a `seq 1 10`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"
	done

	echo "2-Reading changelogs..."
	# read changelogs
	if (( $no_log )); then
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
    	check_db_error rh_chglogs.log

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error ""

	nb_migr=`grep "$ARCH_STR" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != 0)); then
		error "********** TEST FAILED: No migration expected, $nb_migr started"
	else
		echo "OK: no files migrated"
	fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "3-Applying migration policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once

	nb_migr=`grep "$ARCH_STR" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != $expected_migr)); then
		error "********** TEST FAILED: $expected_migr migrations expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"
	fi
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
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"
	done

	count=0
	echo "2-Trying to migrate files before we know them..."
	for i in a `seq 1 10`; do
		$RH -f ./cfg/$config_file --migrate-file $ROOT/file.$i -L rh_migr.log -l EVENT 2>/dev/null
		grep "$ROOT/file.$i" rh_migr.log | grep "not known in database" && count=$(($count+1))
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
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error ""
	else
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error ""
	fi
    	check_db_error rh_chglogs.log

	count=0
	cp /dev/null rh_migr.log
	echo "4-Applying migration policy ($policy_str)..."
	# files should not be migrated this time: do not match policy
	for i in a `seq 1 10`; do
		$RH -f ./cfg/$config_file --migrate-file $ROOT/file.$i -l EVENT -L rh_migr.log 2>/dev/null
		grep "$ROOT/file.$i" rh_migr.log | grep "whitelisted" && count=$(($count+1))
	done

	if (( $count == $expected_migr )); then
		echo "OK: all $expected_migr files are not eligible for migration"
	else
		error "$count files are not eligible, $expected_migr expected"
	fi

	nb_migr=`grep "$ARCH_STR" rh_migr.log | grep hints | wc -l`
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
		$RH -f ./cfg/$config_file --migrate-file $ROOT/file.$i -l EVENT -L rh_migr.log 2>/dev/null
		grep "$ROOT/file.$i" rh_migr.log | grep "successful" && count=$(($count+1))
	done

	if (( $count == $expected_migr )); then
		echo "OK: all $expected_migr files have been migrated successfully"
	else
		error "$count files migrated, $expected_migr expected"
	fi

	nb_migr=`grep "$ARCH_STR" rh_migr.log | grep hints | wc -l`
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
	ln -s "this is a symlink" "$ROOT/link.1" || error "creating symlink"

	echo "2-Reading changelogs..."
	# read changelogs
	if (( $no_log )); then
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
	else
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
	fi
    	check_db_error rh_chglogs.log

	count=0
	echo "3-Applying migration policy ($policy_str)..."
	# files should not be migrated this time: do not match policy
	$RH -f ./cfg/$config_file --migrate-file $ROOT/link.1 -l EVENT -L rh_migr.log 2>/dev/null
	grep "$ROOT/link.1" rh_migr.log | grep "whitelisted" && count=$(($count+1))

	if (( $count == 1 )); then
		echo "OK: symlink not eligible for migration"
	else
		error "$count entries are not eligible, 1 expected"
	fi

	nb_migr=`grep "$ARCH_STR" rh_migr.log | grep hints | wc -l`
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
	$RH -f ./cfg/$config_file --migrate-file $ROOT/link.1 -l EVENT -L rh_migr.log 2>/dev/null
	grep "$ROOT/link.1" rh_migr.log | grep "successful" && count=$(($count+1))

	if (( $count == 1 )); then
		echo "OK: symlink has been migrated successfully"
	else
		error "$count symlink migrated, 1 expected"
	fi

	nb_migr=`grep "$ARCH_STR" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != 1)); then
		error "********** TEST FAILED: 1 migration expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"
	fi

	echo "6-Scanning..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
    	check_db_error rh_chglogs.log

	count=`$REPORT -f ./cfg/$config_file --fs-info --csv -q 2>/dev/null | grep synchro | wc -l`
	if  (($count == 1)); then
		echo "OK: 1 synchro symlink"
	else
		error "1 symlink is expected to be synchro (found $count)"
	fi

	cp /dev/null rh_migr.log
	echo "7-Applying migration policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --migrate-file $ROOT/link.1 -l EVENT -L rh_migr.log 2>/dev/null

	count=`grep "$ROOT/link.1" rh_migr.log | grep "skipping entry" | wc -l`

	if (( $count == 1 )); then
		echo "OK: symlink already migrated"
	else
		error "$count symlink skipped, 1 expected"
	fi
}

# test rmdir policies
function test_rmdir
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	if (( $is_lhsm + $is_hsmlite != 0 )); then
		echo "No rmdir policy for hsm flavors: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# initial scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log
    	check_db_error rh_chglogs.log

	EMPTY=empty
	NONEMPTY=smthg
	RECURSE=remove_me

	echo "1-Create test directories"

	# create 3 empty directories
	mkdir "$ROOT/$EMPTY.1" "$ROOT/$EMPTY.2" "$ROOT/$EMPTY.3" || error "creating empty directories"
	# create non-empty directories
	mkdir "$ROOT/$NONEMPTY.1" "$ROOT/$NONEMPTY.2" "$ROOT/$NONEMPTY.3" || error "creating directories"
	touch "$ROOT/$NONEMPTY.1/f" "$ROOT/$NONEMPTY.2/f" "$ROOT/$NONEMPTY.3/f" || error "populating directories"
	# create "deep" directories for testing recurse rmdir
	mkdir "$ROOT/$RECURSE.1"  "$ROOT/$RECURSE.2" || error "creating directories"
	mkdir "$ROOT/$RECURSE.1/subdir.1" "$ROOT/$RECURSE.1/subdir.2" || error "creating directories"
	touch "$ROOT/$RECURSE.1/subdir.1/file.1" "$ROOT/$RECURSE.1/subdir.1/file.2" "$ROOT/$RECURSE.1/subdir.2/file" || error "populating directories"

	echo "2-Reading changelogs..."
	# read changelogs
	if (( $no_log )); then
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
	else
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
	fi
    	check_db_error rh_chglogs.log

	echo "3-Applying rmdir policy ($policy_str)..."
	$RH -f ./cfg/$config_file --rmdir --once -l FULL -L rh_purge.log 2>/dev/null

	grep "Empty dir removal summary" rh_purge.log || error "did not find summary line in log"
	grep "Recursive dir removal summary" rh_purge.log || error "did not find summary line in log"

	cnt_empty=`grep "Empty dir removal summary" rh_purge.log | cut -d '|' -f 2 | awk '{print $5}'`
	cnt_recurs=`grep "Recursive dir removal summary" rh_purge.log | cut -d '|' -f 2 | awk '{print $5}'`

	if (( $cnt_empty == 0 )); then
		echo "OK: no empty directory removed for now"
	else
		error "$cnt_empty directories removed (too young)"
	fi
	if (( $cnt_recurs == 2 )); then
		echo "OK: 2 top-level directories removed"
	else
		error "$cnt_recurs directories removed (2 expected)"
	fi

	cp /dev/null rh_purge.log
	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "5-Applying rmdir policy again ($policy_str)..."
	# files should not be migrated this time: do not match policy
	$RH -f ./cfg/$config_file --rmdir --once -l EVENT -L rh_purge.log 2>/dev/null

	grep "Empty dir removal summary" rh_purge.log || error "did not file summary line in log"
	grep "Recursive dir removal summary" rh_purge.log || error "did not file summary line in log"

	cnt_empty=`grep "Empty dir removal summary" rh_purge.log | cut -d '|' -f 2 | awk '{print $5}'`
	cnt_recurs=`grep "Recursive dir removal summary" rh_purge.log | cut -d '|' -f 2 | awk '{print $5}'`

	if (( $cnt_empty == 3 )); then
		echo "OK: 3 empty directories removed"
	else
		error "$cnt_empty directories removed (3 expected)"
	fi
	if (( $cnt_recurs == 0 )); then
		echo "OK: no top-level directories removed"
	else
		error "$cnt_resurs directories removed (none expected)"
	fi
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
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i"
	done
	echo "sleeping $cr_sleep seconds..."
    sleep $cr_sleep
	echo -n "  Creating files 4 5 6 7 8 9, "
	for i in {4..9}; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i"
	done
	echo "sleeping $cr_sleep seconds..."
    sleep $cr_sleep
    # modification times
	echo -n "  Modifying files 2 3 4 6 7, "
	for i in 2 3 4 6 7; do
	    echo "data" > $ROOT/file.$i || error "modifying file.$i"
	done
	echo "sleeping $cr_sleep seconds..."
    sleep $cr_sleep
    # update last access times
	echo -n "  Reading files 1 4 5 7, "
    for i in 1 4 5 7; do
 	    cat $ROOT/file.$i >/dev/null || error "reading file.$i"
    done
	echo "sleeping $cr_sleep seconds..."
    sleep $cr_sleep

	echo "2-Reading changelogs..."
	# read changelogs
    # TODO: creation time is different when scanning (ctime at discovery time) and when reading
    # changelogs (changelog timestamp)
	if (( $no_log )); then
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
    check_db_error rh_chglogs.log
    # md_update of entries must be > 0 for policy application
    sleep 1

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time

	$RH -f ./cfg/$config_file --migrate -l FULL -L rh_migr.log  --once || error ""

    # if robinhood logs fids, convert them to files
    if (( $is_lhsm > 0 )); then
        migr=`grep "$ARCH_STR" rh_migr.log | grep hints | sed 's/^.*(\(.*\),.*).*$/\1/' |\
              awk -F, '{print $1}' | xargs -n 1 -r lfs fid2path $ROOT |\
              awk -F. '{print $NF}' | sort | tr '\n' ' ' | xargs` # xargs does the trimming
    else
        migr=`grep "$ARCH_STR" rh_migr.log | grep hints | sed 's/^.*(\(.*\),.*).*$/\1/' |\
              awk -F. '{print $NF}' | sort | tr '\n' ' ' | xargs` # xargs does the trimming
    fi

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
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once

    if (( $is_lhsm > 0 )); then
        migr=`grep "$ARCH_STR" rh_migr.log | grep hints | sed 's/^.*(\(.*\),.*).*$/\1/' |\
              awk -F, '{print $1}' | xargs -n 1 -r lfs fid2path $ROOT |\
              awk -F. '{print $NF}' | sort | tr '\n' ' ' | xargs` # xargs does the trimming
    else
	    migr=`grep "$ARCH_STR" rh_migr.log | grep hints | sed 's/^.*(\(.*\),.*).*$/\1/' |\
              awk -F. '{print $NF}' | sort | tr '\n' ' ' | xargs` # xargs does the trimming
    fi
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
    grep "$REL_STR" $l | grep -n "'$ROOT/file.$i'" | cut -d ':' -f 1
}


function test_purge_lru
{
	config_file=$1
    export SORT_PARAM=$2
	policy_str="$3"

	if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi

    echo "SORT=$SORT_PARAM"

	clean_logs

    # initial scan
    $RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""

    # create 6 files 
  	for i in {1..6}; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i"
        sleep 1
	done

    # access 4 files 
  	for i in {1..4}; do
		dd if=$ROOT/file.$i of=/dev/null bs=1M count=1 >/dev/null 2>/dev/null || error "reading file.$i"
        touch -a $ROOT/file.$i
        sleep 1
	done
    
 	# read changelogs
	if (( $no_log )); then
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi

	# flush data for HSM flavors
    if (( ($is_hsmlite != 0) || ($is_lhsm != 0) )); then
		echo "Archiving files"
		$RH -f ./cfg/$config_file --sync -l FULL  -L rh_migr.log || error "archiving files"

        # all entries must be found
        cnt=$(grep "$ARCH_STR2 '" rh_migr.log | wc -l)
        [[ $cnt == 6 ]] || error "All entries should have been archived (only $cnt/6)"

        # DB request must not have access time criteria
        grep "new request" rh_migr.log | grep last_mod && error "last_mod shouldn't be in request criteria"

		if (($is_lhsm != 0)); then
            wait_done 60
            # update file status
		    $RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
        fi

	fi

    # md_update for purge must be > previous md updates
    sleep 1

    $RH -f ./cfg/$config_file --purge-fs=0 --once -l FULL  -L rh_purge.log || error "purging files"

    # if sorted: order should be 5 6 1 2 3 4
    exp_rank=(3 4 5 6 1 2)
    # if not: can be any order

    if [[ $SORT_PARAM == yes ]]; then
      	for i in {1..6}; do
            idx=$(($i-1))
            rank=$(lru_order_of rh_purge.log $ROOT/file.$i)
            echo "file.$i purge rank $rank/#${exp_rank[$idx]}"
            [[ $rank == ${exp_rank[$idx]} ]] || error "file.$i should have been purge in #${exp_rank[$idx]} (got $rank)"
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
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i"
	done
    sleep 1
	for i in $(seq 1 ${nb_files_error}); do
		dd if=/dev/zero of=$ROOT/file.$i.fail bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.$i.fail"
	done

    # read fs content
	if (( $no_log )); then
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
    check_db_error rh_chglogs.log

	echo "2-Sleeping $sleep_time sec..."
    sleep $sleep_time

	echo "3-Applying migration policy ($policy_str)..."

    # make the archive fail
	if (( $is_lhsm != 0 )); then
        for i in $(seq 1 ${nb_files_error}); do
            $LFS hsm_set --noarchive $ROOT/file.$i.fail
        done
    fi

	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error ""

    nb_fail_match=$(grep "hints='fail'" rh_migr.log | wc -l)
    nb_ok_match=$(grep "<no_hints>" rh_migr.log | wc -l)

    echo "$nb_fail_match failed copies, $nb_ok_match successful copies"
    (($nb_ok_match == $nb_files_ok)) || error "expected $nb_files_ok successful copies (got $nb_ok_match)"
    # migration should have been stopped before migrating all
    (($nb_fail_match == $nb_files_error)) && error "migration should have stopped before migrating all"
    grep "suspending migration" rh_migr.log || error "migration should have been suspended"
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

	# create and fill 10 files

	echo "1-Modifing files..."
	for i in `seq 1 3`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"
	done

	echo "2-Setting xattrs..."
	echo "$ROOT/file.1: xattr.user.foo=1"
	setfattr -n user.foo -v 1 $ROOT/file.1
	echo "$ROOT/file.2: xattr.user.bar=1"
	setfattr -n user.bar -v 1 $ROOT/file.2
	echo "$ROOT/file.3: none"

	# read changelogs
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
    	check_db_error rh_chglogs.log

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error ""

	nb_migr=`grep "$ARCH_STR" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != 0)); then
		error "********** TEST FAILED: No migration expected, $nb_migr started"
	else
		echo "OK: no files migrated"
	fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "3-Applying migration policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once

	nb_migr=`grep "$ARCH_STR" rh_migr.log | grep hints |  wc -l`
	if (($nb_migr != 3)); then
		error "********** TEST FAILED: $expected_migr migrations expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"

		if (( $is_hsmlite != 0 )); then
			# checking policy
			nb_migr_arch1=`grep "hints='fileclass=xattr_bar'" rh_migr.log | wc -l`
			nb_migr_arch2=`grep "hints='fileclass=xattr_foo'" rh_migr.log | wc -l`
			nb_migr_arch3=`grep "using policy 'default'" rh_migr.log | wc -l`
			if (( $nb_migr_arch1 != 1 || $nb_migr_arch2 != 1 || $nb_migr_arch3 != 1 )); then
				error "********** wrong policy cases: 1x$nb_migr_arch1/2x$nb_migr_arch2/3x$nb_migr_arch3 (1x1/2x1/3x1 expected)"
				cp rh_migr.log /tmp/xattr_test.$$
				echo "Log saved as /tmp/xattr_test.$$"
			else
				echo "OK: 1 file for each policy case"
			fi
		else
			# checking archive nums
			nb_migr_arch1=`grep "archive_id=1" rh_migr.log | wc -l`
			nb_migr_arch2=`grep "archive_id=2" rh_migr.log | wc -l`
			nb_migr_arch3=`grep "archive_id=3" rh_migr.log | wc -l`
			if (( $nb_migr_arch1 != 1 || $nb_migr_arch2 != 1 || $nb_migr_arch3 != 1 )); then
				error "********** wrong archive_ids: 1x$nb_migr_arch1/2x$nb_migr_arch2/3x$nb_migr_arch3 (1x1/2x1/3x1 expected)"
			else
				echo "OK: 1 file to each archive_id"
			fi
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
	$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --detach --pid-file=rh.pid || error ""

	sleep 1

	# write file.1 and force immediate migration
	echo "2-Writing data to file.1..."
	dd if=/dev/zero of=$ROOT/file.1 bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.1"

	sleep $cl_delay

	if (( $is_lhsm != 0 )); then
		echo "3-Archiving file....1"
		flush_data
		$LFS hsm_archive $ROOT/file.1 || error "executing lfs hsm_archive"

		echo "3bis-Waiting for end of data migration..."
		wait_done 60 || error "Migration timeout"
	elif (( $is_hsmlite != 0 )); then
		$RH -f ./cfg/$config_file --sync -l DEBUG  -L rh_migr.log || error "executing $CMD --sync"
	fi

	# create links on file.1 files
	echo "4-Creating hard links to $ROOT/file.1..."
	ln $ROOT/file.1 $ROOT/link.1 || error "ln"
	ln $ROOT/file.1 $ROOT/link.2 || error "ln"

	sleep 1

	# removing all files
        echo "5-Removing all links to file.1..."
	rm -f $ROOT/link.* $ROOT/file.1

	sleep $cl_delay

	echo "Checking report..."
	$REPORT -f ./cfg/$config_file --deferred-rm --csv -q > rh_report.log
	nb_ent=`wc -l rh_report.log | awk '{print $1}'`
	if (( $nb_ent != $expected_rm )); then
		error "Wrong number of deferred rm reported: $nb_ent"
	fi
	grep "$ROOT/file.1" rh_report.log > /dev/null || error "$ROOT/file.1 not found in deferred rm list"

	# deferred remove delay is not reached: nothing should be removed
	echo "6-Performing HSM remove requests (before delay expiration)..."
	$RH -f ./cfg/$config_file --hsm-remove -l DEBUG -L rh_rm.log --once || error "hsm-remove"

	nb_rm=`grep "Remove request successful" rh_rm.log | wc -l`
	if (($nb_rm != 0)); then
		echo "********** test failed: no removal expected, $nb_rm done"
	else
		echo "OK: no rm done"
	fi

	echo "7-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "8-Performing HSM remove requests (after delay expiration)..."
	$RH -f ./cfg/$config_file --hsm-remove -l DEBUG -L rh_rm.log --once || error ""

	nb_rm=`grep "Remove request successful" rh_rm.log | wc -l`
	if (($nb_rm != $expected_rm)); then
		error "********** TEST FAILED: $expected_rm removals expected, $nb_rm done"
	else
		echo "OK: $nb_rm files removed from archive"
	fi

	# kill event handler
	pkill -9 $PROC

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
	for i in `seq 1 $entries`; do
		((dir_c=$i % 10))
		((subdir_c=$i % 100))
		dir=$ROOT/dir.$dir_c/subdir.$subdir_c
		mkdir -p $dir || error "creating directory $dir"
		echo "file.$i" > $dir/file.$i || error "creating file $dir/file.$i"
	done

	# how many subdirs in dir.1?
	nbsubdirs=$( ls $ROOT/dir.1 | grep subdir | wc -l )

	echo "2-Initial scan..."
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning filesystem"
    	check_db_error rh_scan.log

	grep "Full scan of" rh_scan.log | tail -1

	sleep 1

	# archiving files
	echo "3-Archiving files..."

	if (( $is_lhsm != 0 )); then
		flush_data
		$RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log || error "flushing data to backend"

		echo "3bis-Waiting for end of data migration..."
		wait_done 120 || error "Migration timeout"
		echo "update db content..."
		$RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_chglogs.log || error "reading chglog"

	elif (( $is_hsmlite != 0 )); then
		$RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log || error "flushing data to backend"
	fi
	grep "Migration summary" rh_migr.log

	echo "Checking stats after 1st scan..."
	$REPORT -f ./cfg/$config_file --fs-info --csv -q | grep -v "n/a" > fsinfo.1
	cat fsinfo.1
	$REPORT -f ./cfg/$config_file --deferred-rm --csv -q > deferred.1
	(( `wc -l fsinfo.1 | awk '{print $1}'` == 1 )) || error "a single file status is expected after data migration"
	status=`cat fsinfo.1 | cut -d "," -f 2 | tr -d ' '`
	nb=`cat fsinfo.1 | grep synchro | cut -d "," -f 3 | tr -d ' '`
	[[ -n $nb ]] || nb=0
	[[ "$status"=="synchro" ]] || error "status expected after data migration: synchro, got $status"
	(( $nb == $entries )) || error "$entries entries expected, got $nb"
	(( `wc -l deferred.1 | awk '{print $1}'`==0 )) || error "no deferred rm expected after first scan"
	rm -f fsinfo.1 deferred.1

	# removing some files
        echo "4-Removing files in $ROOT/dir.1..."
	rm -rf "$ROOT/dir.1" || error "removing files in $ROOT/dir.1"

	# at least 1 second must be enlapsed since last entry change (sync)
	sleep 1

	echo "5-Update DB with a new scan..."
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning filesystem"
    	check_db_error rh_scan.log

	grep "Full scan of" rh_scan.log | tail -1

	echo "Checking stats after 2nd scan..."
	$REPORT -f ./cfg/$config_file --fs-info --csv -q | grep -v "n/a" > fsinfo.2
	cat fsinfo.2
	$REPORT -f ./cfg/$config_file --deferred-rm --csv -q > deferred.2
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

	if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# initial scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log
    	check_db_error rh_chglogs.log

	# fill 10 files and archive them

	echo "1-Modifing files..."
	for i in a `seq 1 10`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "$? writing file.$i"

		if (( $is_lhsm != 0 )); then
			flush_data
			$LFS hsm_archive $ROOT/file.$i || error "lfs hsm_archive"
		fi
	done
	if (( $is_lhsm != 0 )); then
		wait_done 60 || error "Copy timeout"
	fi

	sleep 1
	if (( $no_log )); then
		echo "2-Scanning the FS again to update file status (after 1sec)..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs to update file status (after 1sec)..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""

		if (($is_lhsm != 0)); then
			((`grep "archive,rc=0" rh_chglogs.log | wc -l` == 11)) || error "Not enough archive events in changelog!"
		fi
	fi
    	check_db_error rh_chglogs.log

	# use robinhood for flushing
	if (( $is_hsmlite != 0 )); then
		echo "2bis-Archiving files"
		$RH -f ./cfg/$config_file --sync -l DEBUG  -L rh_migr.log || error "executing migrate-file"
		arch_count=`grep "$ARCH_STR" rh_migr.log | grep hints | wc -l`
		(( $arch_count == 11 )) || error "$11 archive commands expected"
	fi

	echo "3-Applying purge policy ($policy_str)..."
	# no purge expected here
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || error ""

        nb_purge=`grep $REL_STR rh_purge.log | wc -l`

        if (($nb_purge != 0)); then
                error "********** TEST FAILED: No release actions expected, $nb_purge done"
        else
                echo "OK: no file released"
        fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "5-Applying purge policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || error ""

    nb_purge=`grep $REL_STR rh_purge.log | wc -l`

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

	if (( $is_hsmlite + $is_lhsm > 0 )); then
		echo "No custom purge for HSM purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# initial scan
	echo "Populating filesystem..."
	for i in `seq 1 10`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"
	done
    # create malicious file names to test vulnerability
    touch "$ROOT/foo1 \`pkill -9 $CMD\`" || error "couldn't create file"
    touch "$ROOT/foo2 ; exit 1" || error "couldn't create file"
    touch "$ROOT/foo3' ';' 'exit' '1'" || error "couldn't create file"

	echo "Inital scan..."
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log
	check_db_error rh_scan.log

	echo "Sleeping $sleep_time seconds..."
	sleep $sleep_time

    fsname=$(df $ROOT/. | xargs | awk '{print $(NF-5)}' | awk -F '/' '{print $(NF)}')
	if (( $no_log == 0 )); then
		# get fids of entries
		fids=()
		for i in `seq 1 10`; do
			fids[$i]=$($LFS path2fid $ROOT/file.$i)
        done
        i=11
        for f in  "$ROOT/foo1 \`pkill -9 $CMD\`" "$ROOT/foo2 ; exit 1" "$ROOT/foo3' ';' 'exit' '1'" ; do
			fids[$i]=$($LFS path2fid "$f")
            ((i=$i+1))
        done
        [ "$DEBUG" = "1" ] && echo "fsname=$fsname, fids=${fids[*]}"
    fi

	echo "Applying purge policy ($policy_str)..."
	$RH -f ./cfg/$config_file --purge-fs=0 -l FULL -L rh_purge.log --once || error "purging files"
	check_db_error rh_purge.log

	nb_purge=`grep "Purged" rh_purge.log | wc -l`
	if (($nb_purge != 13)); then
		error "********** TEST FAILED: 13 purge actions expected, $nb_purge done"
	else
		echo "OK: 13 actions done"
	fi

	# checking that the custom command was called for each file
	for  i in `seq 1 10`; do
		line=$(grep "Executing " rh_purge.log | grep '/bin/rm' | grep $ROOT/file.$i)
        if [ -z "$line" ]; then
            error "No action found on $ROOT/file.$i"
            continue
        fi
        # split args
        args=($(echo "$line" | sed -e "s/.*rm -f//" | tr -d "'"))
        fn=${args[0]}
        id=${args[1]}
        p=${args[2]}
        [ "$DEBUG" = "1" ] && echo "action: fsname=$fn, fid=$id, path=$p"

        [ $fn = $fsname ] || error "invalid fsname $fn != $fsname"
        # only compare fids for lustre 2.x
        if (( $no_log == 0 )); then
            [ $id = ${fids[$i]} ] || error "invalid fid $id != ${fids[$i]}"
        fi
        [ $p = $ROOT/file.$i ] || error "invalid path $p != $ROOT/file.$i"

        [ -f $ROOT/file.$i ] && error "$ROOT/file.$i still exists after purge command"
	done

    # same test for special file names
    i=11
    for f in  "$ROOT/foo1 \`pkill -9 $CMD\`" "$ROOT/foo2 ; exit 1" "$ROOT/foo3' ';' 'exit' '1'" ; do
        f0=$(echo "$f" | awk '{print $1}')
		line=$(grep "Executing " rh_purge.log | grep '/bin/rm' | grep "$f0")
        if [ -z "$line" ]; then
            error "No action found on $f"
            continue
        fi
        # split args
        args=($(echo "$line" | sed -e "s/.*rm -f//" | tr -d "'" | tr -d '\\' | cut -d '>' -f 1))
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
        f2=$(echo $f | tr -d "'")
        [ "$p" = "$f2" ] || error "invalid path $p != $f2"

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
            touch $ROOT/$pre.$suf || error "touch $ROOT/$pre.$suf"
        done
    done

    # wait for entries to be eligible
    sleep 1

	# initial scan
    $RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log || error "Initial scan"
    check_db_error rh_chglogs.log

	# archive the file (if applicable)
	if (( $is_hsmlite + $is_lhsm != 0 )); then
        $RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log || error "Migration"

        # check archived files
        # *.B files must be archived. other files should be.
        nb_b=$(grep "$ARCH_STR2 '" rh_migr.log | grep -E "$ROOT/[XYZ]\.B"| wc -l)
        nb_ac=$(grep "$ARCH_STR2 '" rh_migr.log | grep -E "$ROOT/[XYZ]\.[AC]"| wc -l)

        [ "$DEBUG" = "1" ] && grep "$ARCH_STR2" rh_migr.log

        (( $nb_b != 3 )) && error "unexpected number of migrated *.B files: $nb_b != 3"
        (( $nb_ac != 0 )) && error "unexpected number of migrated *.[AC] files: $nb_ac != 0"
    fi
    
    # purge the files (if applicable)
    if (( ($is_hsmlite == 0) || ($shook != 0) )); then

        if (($is_lhsm != 0)); then
    		wait_done 60 || error "Migration timeout"

            # read changelogs to be aware of migration success
            :> rh_chglogs.log
            $RH -f ./cfg/$config_file --readlog --once  -l DEBUG -L rh_chglogs.log || error "reading changelog"
            check_db_error rh_chglogs.log
        fi

        # wait for entries to be eligible
        sleep 1

        $RH -f ./cfg/$config_file --purge-fs=0 --once -l DEBUG -L rh_purge.log || error "Purge"

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
        
        nbp=$(grep "$REL_STR" rh_purge.log | grep -E "$ROOT/$purge_pat"| wc -l)
        nbnp=$(grep "$REL_STR" rh_purge.log | grep -vE "$ROOT/$purge_pat" | wc -l)

        [ "$DEBUG" = "1" ] && grep "$REL_STR" rh_purge.log

        (( $nbp != $nb_purge )) && error "unexpected number of purged files matching $purge_pat : $nbp != $nb_purge"
        (( $nbnp != 0 )) && error "unexpected number of purged files matching $purge_pat: $nbnp != 0"
    fi


	# stop RH in background
#	kill %1
}

function test_undelete
{
	config_file=$1
	policy_str="$2"

	clean_logs

	if (( $is_hsmlite == 0 )); then
		echo "No undelete for this flavor"
		set_skipped
		return 1
    fi

    FILES="$ROOT/dir1/file1 $ROOT/dir1/file2 $ROOT/dir2/file1 $ROOT/dir2/file2"

    mkdir -p $ROOT/dir1 || error "mkdir"
    mkdir -p $ROOT/dir2 || error "mkdir"
    for f in $FILES ; do echo 123 > $f || error "write"; done
    sz1=$(stat -c '%s' $ROOT/dir2/file1)

    # initial scan + archive all
    $RH -f ./cfg/$config_file --scan --once --sync -l DEBUG -L rh_chglogs.log || error "Initial scan and sync"
    check_db_error rh_chglogs.log

    # remove all and read the changelog
    rm -rf $ROOT/dir1 $ROOT/dir2
    start=$(date +%s)
    $RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_chglogs.log || error "Reading changelog"
    check_db_error rh_chglogs.log

    # list all deleted entries
	$UNDELETE -f ./cfg/$config_file -L $ROOT | grep "Last known path" | awk '{print $(NF)}' > rh_report.log
    (( $(wc -l rh_report.log | awk '{print $1}') == 4 )) || error "invalid file count in undelete list"
    for f in $FILES; do grep $f rh_report.log || error "missing $f in undelete list"; done
    # list all deleted entried from dir1
	$UNDELETE -f ./cfg/$config_file -L $ROOT/dir1 | grep "Last known path" | awk '{print $(NF)}' > rh_report.log
    (( $(wc -l rh_report.log | awk '{print $1}') == 2 )) || error "invalid file count in undelete list"
    for f in dir1/file1 dir1/file2; do grep $f rh_report.log || error "missing $f in undelete list"; done
    # recover all deleted entries from dir2
	$UNDELETE -f ./cfg/$config_file -R $ROOT/dir2 | grep Restoring | cut -d "'" -f 2 > rh_report.log
    (( $(wc -l rh_report.log | awk '{print $1}') == 2 )) || error "invalid undeleted file count"
    for f in dir2/file1 dir2/file2; do grep $f rh_report.log || error "missing $f in undelete list"; done
    [ ! -f $ROOT/dir2/file1 ] && error "Missing $ROOT/dir2/file1 in FS after undelete"
    [ ! -f $ROOT/dir2/file2 ] && error "Missing $ROOT/dir2/file2 in FS after undelete"

    # check final size
    sz2=$(stat -c '%s' $ROOT/dir2/file1)
    (( $sz1 == $sz2 )) || error "final size $sz2 doesn't match $sz1"
}




function purge_size_filesets
{
	config_file=$1
	sleep_time=$2
	count=$3
	policy_str="$4"

	if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# initial scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log
    	check_db_error rh_chglogs.log

	# fill 3 files of different sizes and mark them archived non-dirty

	j=1
	for size in 0 1 10 200; do
		echo "1.$j-Writing files of size " $(( $size*10 )) "kB..."
		((j=$j+1))
		for i in `seq 1 $count`; do
			dd if=/dev/zero of=$ROOT/file.$size.$i bs=10k count=$size >/dev/null 2>/dev/null || error "writing file.$size.$i"

			if (( $is_lhsm != 0 )); then
				flush_data
				$LFS hsm_archive $ROOT/file.$size.$i || error "lfs hsm_archive"
				wait_done 60 || error "Copy timeout"
			fi
		done
	done

	sleep 1
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs to update file status (after 1sec)..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
    	check_db_error rh_chglogs.log

	if (( $is_hsmlite != 0 )); then
		$RH -f ./cfg/$config_file --sync -l DEBUG  -L rh_migr.log || error "executing $CMD --sync"
    fi

	echo "3-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "4-Applying purge policy ($policy_str)..."
	# no purge expected here
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || error ""

	# counting each matching policy $count of each
	for policy in very_small mid_file default; do
	        nb_purge=`grep 'using policy' rh_purge.log | grep $policy | wc -l`
		if (($nb_purge != $count)); then
			error "********** TEST FAILED: $count release actions expected using policy $policy, $nb_purge done"
		else
			echo "OK: $nb_purge files released using policy $policy"
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
	policy_str="$4"
	delay_min=$5  		# in seconds

	if (( $is_lhsm + $is_hsmlite == 0 )); then
		echo "HSM test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# writing data
	echo "1-Writing files..."
	for i in `seq 1 4`; do
		echo "file.$i" > $ROOT/file.$i || error "creating file $ROOT/file.$i"
	done
	t0=`date +%s`

	# read changelogs
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error "scanning filesystem"
	else
		echo "2-Reading changelogs..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error "reading changelogs"
	fi
    	check_db_error rh_chglogs.log

    	# migrate (nothing must be migrated, no maint mode reported)
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error "executing --migrate action"
	grep "Maintenance time" rh_migr.log && error "No maintenance mode expected"
	grep "Currently in maintenance mode" rh_migr.log && error "No maintenance mode expected"

	# set maintenance mode (due is window +10s)
	maint_time=`perl -e "use POSIX; print strftime(\"%Y%m%d%H%M%S\" ,localtime($t0 + $window + 10))"`
	$REPORT -f ./cfg/$config_file --next-maintenance=$maint_time || error "setting maintenance time"

	# right now, migration window is in the future
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error "executing --migrate action"
	grep "maintenance window will start in" rh_migr.log || errot "Future maintenance not report in the log"

	# sleep enough to be in the maintenance window
	sleep 11

	# split maintenance window in 4
	((delta=$window / 4))
	(( $delta == 0 )) && delta=1

	arch_done=0

	# start migrations while we do not reach maintenance time
	while (( `date +%s` < $t0 + $window + 10 )); do
		cp /dev/null rh_migr.log
		$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error "executing --migrate action"
		grep "Currently in maintenance mode" rh_migr.log || error "Should be in maintenance window now"

		# check that files are migrated after min_delay and before the policy delay
		if grep "$ARCH_STR" rh_migr.log ; then
			arch_done=1
			now=`date +%s`
			# delay_min must be enlapsed
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
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error "executing --migrate action"
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
		mkdir $ROOT/dir.$i
		echo "1.$i-Writing files to $ROOT/dir.$i..."
		# write i MB to each directory
		for j in `seq 1 $i`; do
			dd if=/dev/zero of=$ROOT/dir.$i/file.$j bs=1M count=1 >/dev/null 2>/dev/null || error "writing $ROOT/dir.$i/file.$j"
		done
	done

	echo "1bis. Wait for IO completion..."
	sync

	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
    	check_db_error rh_chglogs.log

	echo "3.Checking reports..."
	for i in `seq 1 $dircount`; do
		$REPORT -f ./cfg/$config_file -l MAJOR --csv -U 1 -P "$ROOT/dir.$i/*" > rh_report.log
		used=`tail -n 1 rh_report.log | cut -d "," -f 3`
		if (( $used != $i*1024*1024 )); then
			error ": $used != " $(($i*1024*1024))
		else
			echo "OK: $i MB in $ROOT/dir.$i"
		fi
	done

}

#test report using accounting table
function test_rh_acct_report
{
        config_file=$1
        dircount=$2
        descr_str="$3"

        clean_logs

        for i in `seq 1 $dircount`; do
                mkdir $ROOT/dir.$i
                echo "1.$i-Writing files to $ROOT/dir.$i..."
                # write i MB to each directory
                for j in `seq 1 $i`; do
                        dd if=/dev/zero of=$ROOT/dir.$i/file.$j bs=1M count=1 >/dev/null 2>/dev/null || error "$? writing $ROOT/dir.$i/file.$j"
                done
        done

        echo "1bis. Wait for IO completion..."
        sync

        echo "2-Scanning..."
        $RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

        echo "3.Checking reports..."
        $REPORT -f ./cfg/$config_file -l MAJOR --csv --force-no-acct --top-user > rh_no_acct_report.log
        $REPORT -f ./cfg/$config_file -l MAJOR --csv --top-user > rh_acct_report.log

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
                mkdir $ROOT/dir.$i || error "creating directory $ROOT/dir.$i"
                echo "1.$i-Writing files to $ROOT/dir.$i..."
                # write i MB to each directory
                for j in `seq 1 $i`; do
                        dd if=/dev/zero of=$ROOT/dir.$i/file.$j bs=1M count=1 >/dev/null 2>/dev/null || error "writing $ROOT/dir.$i/file.$j"
                done
        done

        echo "1bis. Wait for IO completion..."
        sync

        echo "2-Scanning..."
        $RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

        echo "3.Checking reports..."
        $REPORT -f ./cfg/$config_file -l MAJOR --csv --user-info $option | head --lines=-2 > rh_report_no_split.log
        $REPORT -f ./cfg/$config_file -l MAJOR --csv --user-info --split-user-groups $option | head --lines=-2 > rh_report_split.log

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
						echo "Splitted report: "
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
						echo "Splitted report: "
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
        descr_str="$3"

        clean_logs

        for i in `seq 1 $dircount`; do
	        mkdir $ROOT/dir.$i
                echo "1.$i-Writing files to $ROOT/dir.$i..."
                # write i MB to each directory
                for j in `seq 1 $i`; do
                        dd if=/dev/zero of=$ROOT/dir.$i/file.$j bs=1M count=1 >/dev/null 2>/dev/null || error "writing $ROOT/dir.$i/file.$j"
                done
        done

        echo "1bis. Wait for IO completion..."
        sync

        echo "2-Scanning..."
        $RH -f ./cfg/$config_file --scan -l VERB -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

        echo "3.Checking acct table and triggers creation"
        grep -q "Table ACCT_STAT created successfully" rh_scan.log && echo "ACCT table creation: OK" || error "creating ACCT table"
        grep -q "Trigger ACCT_ENTRY_INSERT created successfully" rh_scan.log && echo "ACCT_ENTRY_INSERT trigger creation: OK" || error "creating ACCT_ENTRY_INSERT trigger"
        grep -q "Trigger ACCT_ENTRY_UPDATE created successfully" rh_scan.log && echo "ACCT_ENTRY_INSERT trigger creation: OK" || error "creating ACCT_ENTRY_UPDATE trigger"
        grep -q "Trigger ACCT_ENTRY_DELETE created successfully" rh_scan.log && echo "ACCT_ENTRY_INSERT trigger creation: OK" || error "creating ACCT_ENTRY_DELETE trigger"

}

#test dircount reports
function test_dircount_report
{
	config_file=$1
	dircount=$2
	descr_str="$3"
	emptydir=5

	clean_logs

	# inital scan
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
	check_db_error rh_chglogs.log

	# create several dirs with different entry count (+10 for each)

    match_empty1=0
    match_dir1=0
	for i in `seq 1 $dircount`; do
                mkdir $ROOT/dir.$i
                [[ $i == 1* ]] && ((match_dir1++))
                echo "1.$i-Creating files in $ROOT/dir.$i..."
                # write i MB to each directory
                for j in `seq 1 $((10*$i))`; do
                        dd if=/dev/zero of=$ROOT/dir.$i/file.$j bs=1 count=$i 2>/dev/null || error "creating $ROOT/dir.$i/file.$j"
                done
        done
	if [ $PURPOSE = "TMP_FS_MGR" ]; then
                echo "1bis. Creating empty directories..."
		# create 5 empty dirs
		for i in `seq 1 $emptydir`; do
			mkdir $ROOT/empty.$i
            [[ $i == 1* ]] && ((match_empty1++))
		done
	fi


	# read changelogs
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
	else
		echo "2-Reading changelogs..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading chglog"
	fi
	check_db_error rh_chglogs.log

	echo "3.Checking dircount report..."
	# dircount+1 because $ROOT may be returned
	$REPORT -f ./cfg/$config_file --topdirs=$((dircount+1)) --csv > report.out

    [ "$DEBUG" = "1" ] && cat report.out

	# check that dircount is right for each dir

	# check if $ROOT is in topdirs. If so, check its position
	is_root=0
	line=`grep "$ROOT," report.out`
	[[ -n $line ]] && is_root=1
	if (( ! $is_root )); then
		id=`stat -c "%D/%i" $ROOT/. | tr '[:lower:]' '[:upper:]'`
		line=`grep "$id," report.out`
		[[ -n $line ]] && is_root=1
	fi
	if (( $is_root )); then
		root_rank=`echo $line | cut -d ',' -f 1 | tr -d ' '`
		echo "FS root $ROOT was returned in top dircount (rank=$root_rank)"
	fi
	for i in `seq 1 $dircount`; do
		line=`grep "$ROOT/dir.$i," report.out` || error "$ROOT/dir.$i not found in report"
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
	$REPORT -f ./cfg/$config_file --topdirs=$((dircount+1)) --filter-path="$ROOT/dir.1" --csv -q > report.out
    [ "$DEBUG" = "1" ] && echo && cat report.out
    # only one line expected
    lines=$(wc -l report.out | awk '{print $1}')
    (( $lines == 1 )) || error "1 single dir expected in output (found $lines)"
    line=`grep "$ROOT/dir.1," report.out` || error "$ROOT/dir.1 not found in report"

    $REPORT -f ./cfg/$config_file --topdirs=$((dircount+1)) --filter-path="$ROOT/dir.1*" --csv -q > report.out
    [ "$DEBUG" = "1" ] && echo && cat report.out
    lines=$(wc -l report.out | awk '{print $1}')
    (( $lines == $match_dir1 )) || error "$match_dir1 expected in output (found $lines)"


	if [ $PURPOSE = "TMP_FS_MGR" ]; then
		echo "4. Check empty dirs..."
		# check empty dirs
		$REPORT -f ./cfg/$config_file --toprmdir --csv > report.out
		for i in `seq 1 $emptydir`; do
			grep "$ROOT/empty.$i" report.out > /dev/null || error "$ROOT/empty.$i not found in top rmdir"
		done

        # test with filterpath
        $REPORT -f ./cfg/$config_file --toprmdir --csv -q --filter-path="$ROOT/empty.1" > report.out
        [ "$DEBUG" = "1" ] && echo && cat report.out
        # only one line expected
        lines=$(wc -l report.out | awk '{print $1}')
        (( $lines == 1 )) || error "1 single dir expected in output (found $lines)"
        line=`grep "$ROOT/empty.1," report.out` || error "$ROOT/empty.1 not found in report"

        $REPORT -f ./cfg/$config_file --toprmdir --csv -q --filter-path="$ROOT/empty.1*" > report.out
        [ "$DEBUG" = "1" ] && echo && cat report.out
        lines=$(wc -l report.out | awk '{print $1}')
        (( $lines == $match_empty1 )) || error "$match_empty1 expected in output (found $lines)"
	fi

    rm -f report.out
}

# test report options: avg_size, by-count, count-min and reverse
function    test_sort_report
{
    config_file=$1
    dummy=$2
    descr_str="$3"

    clean_logs

    # get 3 different users (from /etc/passwd)
    users=( $(head -n 3 /etc/passwd | cut -d ':' -f 1) )

    echo "1-Populating filesystem with test files..."

    # populate the filesystem with data of these users
    for i in `seq 0 2`; do
        u=${users[$i]}
        mkdir $ROOT/dir.$u || error "creating directory  $ROOT/dir.$u"
        if (( $i == 0 )); then
            # first user:  20 files of size 1k to 20k
            for f in `seq 1 20`; do
                dd if=/dev/zero of=$ROOT/dir.$u/file.$f bs=1k count=$f 2>/dev/null || error "writing $f KB to $ROOT/dir.$u/file.$f"
            done
        elif (( $i == 1 )); then
            # second user: 10 files of size 10k to 100k
            for f in `seq 1 10`; do
                dd if=/dev/zero of=$ROOT/dir.$u/file.$f bs=10k count=$f 2>/dev/null || error "writing $f x10 KB to $ROOT/dir.$u/file.$f"
            done
        else
            # 3rd user:    5 files of size 100k to 500k
            for f in `seq 1 5`; do
                dd if=/dev/zero of=$ROOT/dir.$u/file.$f bs=100k count=$f 2>/dev/null || error "writing $f x100 KB to $ROOT/dir.$u/file.$f"
            done
        fi
        chown -R $u $ROOT/dir.$u || error "changing owner of $ROOT/dir.$u"
    done

    # flush data to OSTs
    sync

    # scan!
    echo "2-Scanning..."
    $RH -f ./cfg/$config_file --scan -l VERB -L rh_scan.log  --once || error "scanning filesystem"
    check_db_error rh_scan.log

    echo "3-checking reports..."

    # sort users by volume
    $REPORT -f ./cfg/$config_file -l MAJOR --csv -q --top-user > report.out || error "generating topuser report by volume"
    first=$(head -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    last=$(tail -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    [ $first = ${users[2]} ] || error "first user expected in top volume: ${users[2]} (got $first)"
    [ $last = ${users[0]} ] || error "last user expected in top volume: ${users[0]} (got $last)"

    # sort users by volume (reverse)
    $REPORT -f ./cfg/$config_file -l MAJOR --csv -q --top-user --reverse > report.out || error "generating topuser report by volume (reverse)"
    first=$(head -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    last=$(tail -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    [ $first = ${users[0]} ] || error "first user expected in top volume: ${users[0]} (got $first)"
    [ $last = ${users[2]} ] || error "last user expected in top volume: ${users[2]} (got $last)"

    # sort users by count
    $REPORT -f ./cfg/$config_file -l MAJOR --csv -q --top-user --by-count > report.out || error "generating topuser report by count"
    first=$(head -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    last=$(tail -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    [ $first = ${users[0]} ] || error "first user expected in top count: ${users[0]} (got $first)"
    [ $last = ${users[2]} ] || error "last user expected in top count: ${users[2]} (got $last)"

    # sort users by count (reverse)
    $REPORT -f ./cfg/$config_file -l MAJOR --csv -q --top-user --by-count --reverse > report.out || error "generating topuser report by count (reverse)"
    first=$(head -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    last=$(tail -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    [ $first = ${users[2]} ] || error "first user expected in top count: ${users[2]} (got $first)"
    [ $last = ${users[0]} ] || error "last user expected in top count: ${users[0]} (got $last)"

    # sort users by avg size
    $REPORT -f ./cfg/$config_file -l MAJOR --csv -q --top-user --by-avgsize > report.out || error "generating topuser report by avg size"
    first=$(head -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    last=$(tail -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    [ $first = ${users[2]} ] || error "first user expected in top avg size: ${users[2]} (got $first)"
    [ $last = ${users[0]} ] || error "last user expected in top avg size: ${users[0]} (got $last)"

    # sort users by avg size (reverse)
    $REPORT -f ./cfg/$config_file -l MAJOR --csv -q --top-user --by-avgsize --reverse > report.out || error "generating topuser report by avg size (reverse)"
    first=$(head -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    last=$(tail -n 1 report.out | cut -d ',' -f 2 | tr -d ' ')
    [ $first = ${users[0]} ] || error "first user expected in top avg size: ${users[0]} (got $first)"
    [ $last = ${users[2]} ] || error "last user expected in top avg size: ${users[2]} (got $last)"

    # filter users by min count
    # only user 0 and 1 have 10 entries or more
    $REPORT -f ./cfg/$config_file -l MAJOR --csv -q --top-user --count-min=10 > report.out || error "generating topuser with at least 10 entries"
    (( $(wc -l report.out | awk '{print$1}') == 2 )) || error "only 2 users expected with more than 10 entries"
    grep ${users[2]} report.out && error "${users[2]} is not expected to have more than 10 entries"

    rm -f report.out
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

	# create test tree

	mkdir -p $ROOT/dir1
	mkdir -p $ROOT/dir1/subdir1
	mkdir -p $ROOT/dir1/subdir2
	mkdir -p $ROOT/dir1/subdir3/subdir4
	# 2 matching files for fileclass absolute_path
	echo "data" > $ROOT/dir1/subdir1/A
	echo "data" > $ROOT/dir1/subdir2/A
	# 2 unmatching
	echo "data" > $ROOT/dir1/A
	echo "data" > $ROOT/dir1/subdir3/subdir4/A

	mkdir -p $ROOT/dir2
	mkdir -p $ROOT/dir2/subdir1
	# 2 matching files for fileclass absolute_tree
	echo "data" > $ROOT/dir2/X
	echo "data" > $ROOT/dir2/subdir1/X

	mkdir -p $ROOT/one_dir/dir3
	mkdir -p $ROOT/other_dir/dir3
	mkdir -p $ROOT/yetanother_dir
	mkdir -p $ROOT/dir3
	mkdir -p $ROOT/one_dir/one_dir/dir3
	# 2 matching files for fileclass path_depth2
	echo "data" > $ROOT/one_dir/dir3/X
	echo "data" > $ROOT/other_dir/dir3/Y
	# 2 unmatching files for fileclass path_depth2
	echo "data" > $ROOT/dir3/X
	echo "data" > $ROOT/one_dir/one_dir/dir3/X

	mkdir -p $ROOT/one_dir/dir4/subdir1
	mkdir -p $ROOT/other_dir/dir4/subdir1
	mkdir -p $ROOT/dir4
	mkdir -p $ROOT/one_dir/one_dir/dir4
	# 3 matching files for fileclass tree_depth2
	echo "data" > $ROOT/one_dir/dir4/subdir1/X
	echo "data" > $ROOT/other_dir/dir4/subdir1/X
    echo "data" > $ROOT/yetanother_dir/dir4 # tree root should match too!
	# unmatching files for fileclass tree_depth2
	echo "data" > $ROOT/dir4/X
	echo "data" > $ROOT/one_dir/one_dir/dir4/X

	mkdir -p $ROOT/dir5
	mkdir -p $ROOT/subdir/dir5
	# 2 matching files for fileclass relative_path
	echo "data" > $ROOT/dir5/A
	echo "data" > $ROOT/dir5/B
	# 2 unmatching files for fileclass relative_path
	echo "data" > $ROOT/subdir/dir5/A
	echo "data" > $ROOT/subdir/dir5/B

	mkdir -p $ROOT/dir6/subdir
	mkdir -p $ROOT/subdir/dir6
	# 3 matching files for fileclass relative_tree
	echo "data" > $ROOT/dir6/A
	echo "data" > $ROOT/dir6/subdir/A
    echo "data" > $ROOT/file.6 # tree root should match too!
	# 2 unmatching files for fileclass relative_tree
	echo "data" > $ROOT/subdir/dir6/A
	echo "data" > $ROOT/subdir/dir6/B


	mkdir -p $ROOT/dir7/subdir
	mkdir -p $ROOT/dir71/subdir
	mkdir -p $ROOT/subdir/subdir/dir7
	mkdir -p $ROOT/subdir/subdir/dir72
	# 3 matching files for fileclass any_root_tree
	echo "data" > $ROOT/dir7/subdir/file
	echo "data" > $ROOT/subdir/subdir/dir7/file
    echo "data" > $ROOT/yetanother_dir/dir7 # tree root should match too!
	# 2 unmatching files for fileclass any_root_tree
	echo "data" > $ROOT/dir71/subdir/file
	echo "data" > $ROOT/subdir/subdir/dir72/file

	mkdir -p $ROOT/dir8
	mkdir -p $ROOT/dir81/subdir
	mkdir -p $ROOT/subdir/subdir/dir8
	# 2 matching files for fileclass any_root_path
	echo "data" > $ROOT/dir8/file.1
	echo "data" > $ROOT/subdir/subdir/dir8/file.1
	# 3 unmatching files for fileclass any_root_path
	echo "data" > $ROOT/dir8/file.2
	echo "data" > $ROOT/dir81/file.1
	echo "data" > $ROOT/subdir/subdir/dir8/file.2

	mkdir -p $ROOT/dir9/subdir/dir10/subdir
	mkdir -p $ROOT/dir9/subdir/dir10x/subdir
	mkdir -p $ROOT/dir91/subdir/dir10
	# 3 matching files for fileclass any_level_tree
	echo "data" > $ROOT/dir9/subdir/dir10/file
	echo "data" > $ROOT/dir9/subdir/dir10/subdir/file
	echo "data" > $ROOT/dir9/subdir/dir10x/dir10  # tree root should match too!
	# 2 unmatching files for fileclass any_level_tree
	echo "data" > $ROOT/dir9/subdir/dir10x/subdir/file
	echo "data" > $ROOT/dir91/subdir/dir10/file

	mkdir -p $ROOT/dir11/subdir/subdir
	mkdir -p $ROOT/dir11x/subdir
	# 2 matching files for fileclass any_level_path
	echo "data" > $ROOT/dir11/subdir/file
	echo "data" > $ROOT/dir11/subdir/subdir/file
	# 2 unmatching files for fileclass any_level_path
	echo "data" > $ROOT/dir11/subdir/file.x
	echo "data" > $ROOT/dir11x/subdir/file


	echo "1bis-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	# read changelogs
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
	check_db_error rh_chglogs.log


	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error ""

	# count the number of file for each policy
	nb_pol1=`grep hints rh_migr.log | grep absolute_path | wc -l`
	nb_pol2=`grep hints rh_migr.log | grep absolute_tree | wc -l`
	nb_pol3=`grep hints rh_migr.log | grep path_depth2 | wc -l`
	nb_pol4=`grep hints rh_migr.log | grep tree_depth2 | wc -l`
	nb_pol5=`grep hints rh_migr.log | grep relative_path | wc -l`
	nb_pol6=`grep hints rh_migr.log | grep relative_tree | wc -l`

	nb_pol7=`grep hints rh_migr.log | grep any_root_tree | wc -l`
	nb_pol8=`grep hints rh_migr.log | grep any_root_path | wc -l`
	nb_pol9=`grep hints rh_migr.log | grep any_level_tree | wc -l`
	nb_pol10=`grep hints rh_migr.log | grep any_level_path | wc -l`

	nb_unmatch=`grep hints rh_migr.log | grep unmatch | wc -l`

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
		t=$(( `date "+%s"` - $init ))
		echo "loop 1.$i: many 'touch' within $event_updt_min sec (t=$t)"
		clean_logs

		# start log reader (DEBUG level displays needed attrs)
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L $LOG --detach --pid-file=rh.pid || error ""

		start=`date "+%s"`
		# generate a lot of TIME events within 'event_updt_min'
		# => must only update once
		while (( `date "+%s"` - $start < $event_updt_min - 2 )); do
			touch $ROOT/file
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

		(( $nb_getattr == $expect_attr )) || error "********** TEST FAILED: wrong count of getattr: $nb_getattr (t=$t)"
		# the path may be retrieved at the first loop (at creation)
		# but not during the next loop (as long as enlapsed time < update_period)
		if (( $i > 1 )) && (( `date "+%s"` - $init < $update_period )); then
			nb_getpath=`grep getpath=1 $LOG | wc -l`
			grep "getpath=1" $LOG
			echo "nb path update: $nb_getpath"
			(( $nb_getpath == 0 )) || error "********** TEST FAILED: wrong count of getpath: $nb_getpath (t=$t)"
		fi

		# wait for 5s to be fully enlapsed
		while (( `date "+%s"` - $start <= $event_updt_min )); do
			usleep 100000
		done
	done

	init=`date "+%s"`

	for i in `seq 1 3`; do
		echo "loop 2.$i: many 'rename' within $event_updt_min sec"
		clean_logs

		# start log reader (DEBUG level displays needed attrs)
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L $LOG --detach --pid-file=rh.pid || error ""

		start=`date "+%s"`
		# generate a lot of TIME events within 'event_updt_min'
		# => must only update once
		while (( `date "+%s"` - $start < $event_updt_min - 2 )); do
			mv $ROOT/file $ROOT/file.2
			usleep 10000
			mv $ROOT/file.2 $ROOT/file
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
		# but not during the next loop (as long as enlapsed time < update_period)
		if (( $i > 1 )) && (( `date "+%s"` - $init < $update_period )); then
			nb_getattr=`grep getattr=1 $LOG | wc -l`
			echo "nb attr update: $nb_getattr"
			(( $nb_getattr == 0 )) || error "********** TEST FAILED: wrong count of getattr: $nb_getattr"
		fi
	done

	echo "Waiting $update_period seconds..."
	clean_logs

	# check that getattr+getpath are performed after update_period, even if the event is not related:
	$RH -f ./cfg/$config_file --readlog -l DEBUG -L $LOG --detach --pid-file=rh.pid || error ""
	sleep $update_period

	if (( $is_lhsm != 0 )); then
		# chg something different that path or POSIX attributes
		$LFS hsm_set --noarchive $ROOT/file
	else
		touch $ROOT/file
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
		nb_getstatus=`grep getstatus=1 $LOG | wc -l`
		echo "nb status update: $nb_getstatus"
		(( $nb_getstatus == 1 )) || error "********** TEST FAILED: wrong count of getstatus: $nb_getstatus"
	fi

	# kill remaning event handler
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
	touch $ROOT/ignore1
	touch $ROOT/whitelist1
	touch $ROOT/migrate1
	touch $ROOT/default1

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log
	check_db_error rh_chglogs.log

	# now apply policies
	$RH -f ./cfg/$config_file --migrate --dry-run -l FULL -L rh_migr.log --once || error ""

	#we must have 4 lines like this: "Need to update fileclass (not set)"
	nb_updt=`grep "Need to update fileclass (not set)" rh_migr.log | wc -l`
	nb_migr_match=`grep "matches the condition for policy 'migr_match'" rh_migr.log | wc -l`
	nb_default=`grep "matches the condition for policy 'default'" rh_migr.log | wc -l`

	(( $nb_updt == 4 )) || error "********** TEST FAILED: wrong count of fileclass update: $nb_updt"
	(( $nb_migr_match == 1 )) || error "********** TEST FAILED: wrong count of files matching 'migr_match': $nb_migr_match"
	(( $nb_default == 1 )) || error "********** TEST FAILED: wrong count of files matching 'default': $nb_default"

        (( $nb_updt == 4 )) && (( $nb_migr_match == 1 )) && (( $nb_default == 1 )) \
		&& echo "OK: initial fileclass matching successful"

	# rematch entries: should not update fileclasses
	clean_logs
	$RH -f ./cfg/$config_file --migrate --dry-run -l FULL -L rh_migr.log --once || error ""

	nb_default_valid=`grep "fileclass '@default@' is still valid" rh_migr.log | wc -l`
	nb_migr_valid=`grep "fileclass 'to_be_migr' is still valid" rh_migr.log | wc -l`
	nb_updt=`grep "Need to update fileclass" rh_migr.log | wc -l`

	(( $nb_default_valid == 1 )) || error "********** TEST FAILED: wrong count of cached fileclass for default policy: $nb_default_valid"
	(( $nb_migr_valid == 1 )) || error "********** TEST FAILED: wrong count of cached fileclass for 'migr_match' : $nb_migr_valid"
	(( $nb_updt == 0 )) || error "********** TEST FAILED: no expected fileclass update: $nb_updt updated"

        (( $nb_updt == 0 )) && (( $nb_default_valid == 1 )) && (( $nb_migr_valid == 1 )) \
		&& echo "OK: fileclasses do not need update"

	echo "Waiting $update_period sec..."
	sleep $update_period

	# rematch entries: should update all fileclasses
	clean_logs
	$RH -f ./cfg/$config_file --migrate --dry-run -l FULL -L rh_migr.log --once || error ""

	nb_valid=`grep "is still valid" rh_migr.log | wc -l`
	nb_updt=`grep "Need to update fileclass (out-of-date)" rh_migr.log | wc -l`

	(( $nb_valid == 0 )) || error "********** TEST FAILED: fileclass should need update : $nb_valid still valid"
	(( $nb_updt == 4 )) || error "********** TEST FAILED: all fileclasses should be updated : $nb_updt/4"

        (( $nb_valid == 0 )) && (( $nb_updt == 4 )) \
		&& echo "OK: all fileclasses updated"
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
	touch $ROOT/ignore1
	touch $ROOT/whitelist1
	touch $ROOT/migrate1
	touch $ROOT/default1

    echo "1. scan..."
	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log || error "scanning"
	check_db_error rh_chglogs.log
    # check that all files have been properly matched

    $REPORT -f ./cfg/$config_file --dump -q  > report.out
    st1=`grep ignore1 report.out | cut -d ',' -f 6 | tr -d ' '`
    st2=`grep whitelist1 report.out  | cut -d ',' -f 6 | tr -d ' '`
    st3=`grep migrate1 report.out  | cut -d ',' -f 6 | tr -d ' '`
    st4=`grep default1 report.out  | cut -d ',' -f 6 | tr -d ' '`

    [ "$st1" = "to_be_ignored" ] || error "file should be in class 'to_be_ignored'"
    [ "$st2" = "[ignored]" ] || error "file should be in class '[ignored]'"
    [ "$st3" = "to_be_migr" ] || error "file should be in class 'to_be_migr'"
    [ "$st4" = "[default]" ] || error "file should be in class '[default]'"

    echo "2. migrate..."

	# now apply policies
	$RH -f ./cfg/$config_file --migrate --dry-run -l FULL -L rh_migr.log --once || error "running migration"

    $REPORT -f ./cfg/$config_file --dump -q  > report.out
    st1=`grep ignore1 report.out | cut -d ',' -f 6 | tr -d ' '`
    st2=`grep whitelist1 report.out  | cut -d ',' -f 6 | tr -d ' '`
    st3=`grep migrate1 report.out  | cut -d ',' -f 6 | tr -d ' '`
    st4=`grep default1 report.out  | cut -d ',' -f 6 | tr -d ' '`

    [ "$st1" = "to_be_ignored" ] || error "file should be in class 'to_be_ignored'"
    [ "$st2" = "[ignored]" ] || error "file should be in class '[ignored]'"
    [ "$st3" = "to_be_migr" ] || error "file should be in class 'to_be_migr'"
    [ "$st4" = "[default]" ] || error "file should be in class '[default]'"

	#we must have 4 lines like this: "Need to update fileclass (not set)"
	nb_migr_match=`grep "matches the condition for policy 'migr_match'" rh_migr.log | wc -l`
	nb_default=`grep "matches the condition for policy 'default'" rh_migr.log | wc -l`

	(( $nb_migr_match == 1 )) || error "********** TEST FAILED: wrong count of files matching 'migr_match': $nb_migr_match"
	(( $nb_default == 1 )) || error "********** TEST FAILED: wrong count of files matching 'default': $nb_default"

    (( $nb_migr_match == 1 )) && (( $nb_default == 1 )) \
		&& echo "OK: initial fileclass matching successful"

	# rematch entries
	clean_logs
	$RH -f ./cfg/$config_file --migrate --dry-run -l FULL -L rh_migr.log --once || error "running $RH --migrate"

	nb_default_valid=`grep "fileclass '@default@' is still valid" rh_migr.log | wc -l`
	nb_migr_valid=`grep "fileclass 'to_be_migr' is still valid" rh_migr.log | wc -l`
	nb_updt=`grep "Need to update fileclass" rh_migr.log | wc -l`

	(( $nb_default_valid == 1 )) || error "********** TEST FAILED: wrong count of cached fileclass for default policy: $nb_default_valid"
	(( $nb_migr_valid == 1 )) || error "********** TEST FAILED: wrong count of cached fileclass for 'migr_match' : $nb_migr_valid"
	(( $nb_updt == 0 )) || error "********** TEST FAILED: no expected fileclass update: $nb_updt updated"

        (( $nb_updt == 0 )) && (( $nb_default_valid == 1 )) && (( $nb_migr_valid == 1 )) \
		&& echo "OK: fileclasses do not need update"

    # check effectively migrated files
    m1_arch=`grep "$ARCH_STR2" rh_migr.log | grep migrate1 | wc -l`
    d1_arch=`grep "$ARCH_STR2" rh_migr.log | grep default1 | wc -l`
    w1_arch=`grep "$ARCH_STR2" rh_migr.log | grep whitelist1 | wc -l`
    i1_arch=`grep "$ARCH_STR2" rh_migr.log | grep ignore1 | wc -l`

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

	if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi

	if (( $is_lhsm + $is_hsmlite == 0 )); then
        # no status nor migration state: status field=5
        stf=5
    else
        # status + migration state: status field=7
        stf=7
    fi

	clean_logs

	#create test tree
	touch $ROOT/ignore1
	touch $ROOT/whitelist1
	touch $ROOT/purge1
	touch $ROOT/default1

    echo "1. scan..."
	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log || error "scanning"
	check_db_error rh_chglogs.log
    # check that all files have been properly matched

    $REPORT -f ./cfg/$config_file --dump -q  > report.out

    st1=`grep ignore1 report.out | cut -d ',' -f $stf | tr -d ' '`
    st2=`grep whitelist1 report.out  | cut -d ',' -f $stf | tr -d ' '`
    st3=`grep purge1 report.out  | cut -d ',' -f $stf | tr -d ' '`
    st4=`grep default1 report.out  | cut -d ',' -f $stf | tr -d ' '`

    [ "$st1" = "to_be_ignored" ] || error "file should be in class 'to_be_ignored'"
    [ "$st2" = "[ignored]" ] || error "file should be in class '[ignored]'"
    [ "$st3" = "to_be_released" ] || error "file should be in class 'to_be_released'"
    [ "$st4" = "[default]" ] || error "file should be in class '[default]'"

    if (( $is_lhsm + $is_hsmlite > 0 )); then
        echo "1bis. migrate..."

        # now apply policies
        if (( $is_lhsm != 0 )); then
                flush_data
                $RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log || error "flushing data to backend"

                echo "1ter. Waiting for end of data migration..."
                wait_done 120 || error "Migration timeout"
		echo "update db content..."
		$RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_chglogs.log || error "reading chglog"

        elif (( $is_hsmlite != 0 )); then
                $RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log || error "flushing data to backend"
        fi

        # check that release class is still correct
        $REPORT -f ./cfg/$config_file --dump -q  > report.out

        st1=`grep ignore1 report.out | cut -d ',' -f $stf | tr -d ' '`
        st2=`grep whitelist1 report.out  | cut -d ',' -f $stf | tr -d ' '`
        st3=`grep purge1 report.out  | cut -d ',' -f $stf | tr -d ' '`
        st4=`grep default1 report.out  | cut -d ',' -f $stf | tr -d ' '`

        [ "$st1" = "to_be_ignored" ] || error "file should be in class 'to_be_ignored'"
        [ "$st2" = "[ignored]" ] || error "file should be in class '[ignored]'"
        [ "$st3" = "to_be_released" ] || error "file should be in class 'to_be_released'"
        [ "$st4" = "[default]" ] || error "file should be in class '[default]'"
    fi
    sleep 1
    echo "2. purge/release..."

    # now apply policies
    $RH -f ./cfg/$config_file --purge-fs=0 -l FULL -L rh_purge.log --once || error "running purge"

    $REPORT -f ./cfg/$config_file --dump -q  > report.out
    st1=`grep ignore1 report.out | cut -d ',' -f $stf | tr -d ' '`
    st2=`grep whitelist1 report.out  | cut -d ',' -f $stf | tr -d ' '`

    [ "$st1" = "to_be_ignored" ] || error "file should be in class 'to_be_ignored'"
    [ "$st2" = "[ignored]" ] || error "file should be in class '[ignored]'"

	#we must have 2 lines like this: "Need to update fileclass (not set)"
	nb_purge_match=`grep "matches the condition for policy 'purge_match'" rh_purge.log | wc -l`
	nb_default=`grep "matches the condition for policy 'default'" rh_purge.log | wc -l`

	(( $nb_purge_match == 1 )) || error "********** TEST FAILED: wrong count of files matching 'purge_match': $nb_purge_match"
	(( $nb_default == 1 )) || error "********** TEST FAILED: wrong count of files matching 'default': $nb_default"

    (( $nb_purge_match == 1 )) && (( $nb_default == 1 )) \
		&& echo "OK: initial fileclass matching successful"

    # check effectively purged files
    p1_arch=`grep $REL_STR rh_purge.log | grep purge1 | wc -l`
    d1_arch=`grep $REL_STR rh_purge.log | grep default1 | wc -l`
    w1_arch=`grep $REL_STR rh_purge.log | grep whitelist1 | wc -l`
    i1_arch=`grep $REL_STR rh_purge.log | grep ignore1 | wc -l`

    (( $w1_arch == 0 )) || error "whitelist1 should not have been purged"
    (( $i1_arch == 0 )) || error "ignore1 should not have been purged"
    (( $p1_arch == 1 )) || error "purge1 should have been purged"
    (( $d1_arch == 1 )) || error "default1 should have been purged"

    (( $w1_arch == 0 )) && (( $i1_arch == 0 )) && (( $p1_arch == 1 )) \
    && (( $d1_arch == 1 )) && echo "OK: All expected files released"

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

	if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

	echo "Writing and archiving files..."
	#create test tree of archived files
	for file in ignore1 whitelist1 purge1 default1 ; do
		touch $ROOT/$file

		if (( $is_lhsm != 0 )); then
			flush_data
			$LFS hsm_archive $ROOT/$file
		fi
	done
	if (( $is_lhsm != 0 )); then
		wait_done 60 || error "Copy timeout"
	fi

	echo "FS Scan..."
	if (( $is_hsmlite != 0 )); then
		$RH -f ./cfg/$config_file --scan --sync -l DEBUG  -L rh_migr.log || error "executing $CMD --sync"
		check_db_error rh_migr.log
	    else
    		$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log || error "executing $CMD --scan"
		check_db_error rh_chglogs.log
	fi

	# now apply policies
	$RH -f ./cfg/$config_file --purge-fs=0 --dry-run -l FULL -L rh_purge.log --once || error ""

	nb_updt=`grep "Need to update fileclass (not set)" rh_purge.log | wc -l`
	nb_purge_match=`grep "matches the condition for policy 'purge_match'" rh_purge.log | wc -l`
	nb_default=`grep "matches the condition for policy 'default'" rh_purge.log | wc -l`

	# we must have 4 lines like this: "Need to update fileclass (not set)"
	(( $nb_updt == 4 )) || error "********** TEST FAILED: wrong count of fileclass update: $nb_updt"
	(( $nb_purge_match == 1 )) || error "********** TEST FAILED: wrong count of files matching 'purge_match': $nb_purge_match"
	(( $nb_default == 1 )) || error "********** TEST FAILED: wrong count of files matching 'default': $nb_default"

        (( $nb_updt == 4 )) && (( $nb_purge_match == 1 )) && (( $nb_default == 1 )) \
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
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log
	check_db_error rh_chglogs.log

	echo "Waiting $update_period sec..."
	sleep $update_period

	$RH -f ./cfg/$config_file --purge-fs=0 --dry-run -l FULL -L rh_purge.log --once || error ""

	nb_valid=`grep "is still valid" rh_purge.log | wc -l`
	nb_updt=`grep "Need to update fileclass (out-of-date)" rh_purge.log | wc -l`
	nb_not_set=`grep "Need to update fileclass (not set)" rh_purge.log | wc -l`

	(( $nb_valid == $already )) || error "********** TEST FAILED: fileclass should need update : $nb_valid still valid"
	(( $nb_updt == 4 - $already - $new )) || error "********** TEST FAILED: wrong number of fileclasses should be updated : $nb_updt"
	(( $nb_not_set == $new )) || error "********** TEST FAILED:  wrong number of fileclasse fileclasses should be matched : $nb_not_set"

        (( $nb_valid == $already )) && (( $nb_updt == 4 - $already - $new )) \
		&& echo "OK: fileclasses correctly updated"
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
	$RH -f ./cfg/$config_file --readlog -l DEBUG -L $LOG --detach || error "starting chglog reader"
    sleep 1

    # create a small file and write it (20 bytes, incl \n)
    echo "qqslmdkqslmdkqlmsdk" > $ROOT/file
    sleep 1

    [ "$DEBUG" = "1" ] && $FIND $ROOT/file -f ./cfg/$config_file -ls
    size=$($FIND $ROOT/file -f ./cfg/$config_file -ls | awk '{print $(NF-3)}')
    if [ -z "$size" ]; then
       echo "db not yet updated, waiting changelog processing delay ($cl_delay sec)..."
       sleep $cl_delay
       size=$($FIND $ROOT/file -f ./cfg/$config_file -ls | awk '{print $(NF-3)}')
    fi

    if (( $size != 20 )); then
        error "unexpected size value: $size != 20 (is Lustre version < 2.3?)"
    fi

    # now appending the file (+20 bytes, incl \n)
    echo "qqslmdkqslmdkqlmsdk" >> $ROOT/file
    sleep 1

    [ "$DEBUG" = "1" ] && $FIND $ROOT/file -f ./cfg/$config_file -ls
    size=$($FIND $ROOT/file -f ./cfg/$config_file -ls | awk '{print $(NF-3)}')
    if [ -z "$size" ]; then
       echo "db not yet updated, waiting one more second..."
       sleep 1
       size=$($FIND $ROOT/file -f ./cfg/$config_file -ls | awk '{print $(NF-3)}')
    fi

    if (( $size != 40 )); then
        error "unexpected size value: $size != 40"
    fi

    pkill -9 $PROC
}


function test_cnt_trigger
{
	config_file=$1
	file_count=$2
	exp_purge_count=$3
	policy_str="$4"

	if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

	if (( $is_hsmlite != 0 )); then
        # this mode may create an extra inode in filesystem: inital scan
        # to take it into account
		$RH -f ./cfg/$config_file --scan --once -l MAJOR -L rh_scan.log || error "executing $CMD --scan"
		check_db_error rh_scan.log
    fi

	# initial inode count
	empty_count=`df -i $ROOT/ | grep "$ROOT" | xargs | awk '{print $(NF-3)}'`
    export high_cnt=$((file_count + $empty_count))
    export low_cnt=$(($high_cnt - $exp_purge_count))

    [ "$DEBUG" = "1" ] && echo "Initial inode count $empty_count, creating additional $file_count files"

	#create test tree of archived files (1M each)
	for i in `seq 1 $file_count`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writing $ROOT/file.$i"

		if (( $is_lhsm != 0 )); then
			$LFS hsm_archive $ROOT/file.$i
		fi
	done

	if (( $is_lhsm != 0 )); then
		wait_done 60 || error "Copy timeout"
	fi

	# wait for df sync
	sync; sleep 1

	if (( $is_hsmlite != 0 )); then
        # scan and sync
		$RH -f ./cfg/$config_file --scan --sync -l DEBUG  -L rh_migr.log || error "executing $CMD --sync"
		check_db_error rh_migr.log
    else
       	# scan
	    	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log || error "executing $CMD --scan"
		check_db_error rh_chglogs.log
    fi

	# apply purge trigger
	$RH -f ./cfg/$config_file --purge --once -l FULL -L rh_purge.log

	if (($is_lhsm != 0 )); then
		nb_release=`grep "Released" rh_purge.log | wc -l`
	else
		nb_release=`grep "Purged" rh_purge.log | wc -l`
	fi

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

	if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

    # reset df values
    wait_stable_df

	empty_vol=`$LFS df $ROOT | grep OST0000 | awk '{print $3}'`
	empty_vol=$(($empty_vol/1024))

    if (($empty_vol >= $mb_h_threshold)); then
        error "OST IS ALREADY OVER HIGH THRESHOLD (cannot run test)"
        return 1
    fi

    [ "$DEBUG" = "1" ] && echo "empty_vol OST0000: $empty_vol MB, HW: $mb_h_threshold MB"

	$LFS setstripe -c 2 -i 0 $LFS_SS_SZ_OPT 1m $ROOT || echo "error setting stripe_count=2"

	#create test tree of archived files (2M each=1MB/ost) until we reach high threshold
	((count=$mb_h_threshold - $empty_vol + 1))
	for i in `seq $empty_vol $mb_h_threshold`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=2  >/dev/null 2>/dev/null || error "writing $ROOT/file.$i"

		if (( $is_lhsm != 0 )); then
			flush_data
			$LFS hsm_archive $ROOT/file.$i
		fi
	done
	if (( $is_lhsm != 0 )); then
		wait_done 60 || error "Copy timeout"
	fi

	if (( $is_hsmlite != 0 )); then
		$RH -f ./cfg/$config_file --scan --sync -l DEBUG  -L rh_migr.log || error "executing $CMD --sync"
    fi

	# wait for df sync
    wait_stable_df

	if (( $is_lhsm != 0 )); then
		arch_count=`$LFS hsm_state $ROOT/file.* | grep "exists archived" | wc -l`
		(( $arch_count == $count )) || error "File count $count != archived count $arch_count"
	fi

	full_vol=`$LFS df  $ROOT | grep OST0000 | awk '{print $3}'`
	full_vol=$(($full_vol/1024))
	delta=$(($full_vol-$empty_vol))
	echo "OST#0 usage increased of $delta MB (total usage = $full_vol MB)"
	((need_purge=$full_vol-$mb_l_threshold))
	echo "Need to purge $need_purge MB on OST#0"

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log
	check_db_error rh_chglogs.log

	$REPORT -f ./cfg/$config_file -i

	# apply purge trigger
	$RH -f ./cfg/$config_file --purge --once -l DEBUG -L rh_purge.log || error "applying purge policy"

	grep summary rh_purge.log || error "No purge was done"
    [ "$DEBUG" = "1" ] && cat rh_purge.log

	stat_purge=`grep summary rh_purge.log | grep "OST #0" | awk '{print $(NF-9)" "$(NF-3)" "$(NF-2)}' | sed -e "s/[^0-9 ]//g"`

	purged_ost=`echo $stat_purge | awk '{print $1}'`
	purged_total=`echo $stat_purge | awk '{print $2}'`
	needed_ost=`echo $stat_purge | awk '{print $3}'`

    [ "$DEBUG" = "1" ] && echo "purged_ost=$purged_ost, total_purged=$purged_total, ost_purge_needed=$needed_ost"

	# change blocks to MB (*512/1024/1024 == /2048)
	((purged_ost=$purged_ost/2048))
	((purged_total=$purged_total/2048))
	((needed_ost=$needed_ost/2048))

	# checks
	# - needed_ost must be equal to the amount we computed (need_purge)
	# - purged_ost must be over the amount we computed and under need_purge+1MB
	# - purged_total must be twice purged_ost
	(( $needed_ost == $need_purge )) || error ": invalid amount of data computed ($needed_ost != $need_purge)"
	(( $purged_ost >= $need_purge )) && (( $purged_ost <= $need_purge + 1 )) || error ": invalid amount of data purged ($purged_ost < $need_purge)"
	(( $purged_total == 2*$purged_ost )) || error ": invalid total volume purged ($purged_total != 2*$purged_ost)"

	(( $needed_ost == $need_purge )) && (( $purged_ost >= $need_purge )) && (( $purged_ost <= $need_purge + 1 )) \
		&& (( $purged_total == 2*$purged_ost )) && echo "OK: purge of OST#0 succeeded"

    # sync df values before checking df return
    wait_stable_df

	full_vol1=`$LFS df $ROOT | grep OST0001 | awk '{print $3}'`
	full_vol1=$(($full_vol1/1024))
	purge_ost1=`grep summary rh_purge.log | grep "OST #1" | wc -l`

	if (($full_vol1 > $mb_h_threshold )); then
		error ": OST#1 is not expected to exceed high threshold!"
	elif (($purge_ost1 != 0)); then
		error ": no purge expected on OST#1"
	else
		echo "OK: no purge on OST#1 (usage=$full_vol1 MB)"
	fi

	# restore default striping
	$LFS setstripe -c 2 -i -1 $ROOT
}

function test_ost_order
{
	config_file=$1
	policy_str="$2"
	clean_logs

    # reset df values
    wait_stable_df

	if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi


    # nb OSTs?
    nbost=`$LFS df $ROOT | grep OST | wc -l`
    maxidx=$((nbost -1))

    # get low watermark = max current OST usage
    local min_kb=0
    for i in $(seq 0 $maxidx); do
    	empty_vol=`$LFS df $ROOT | grep OST000$i | awk '{print $3}'`
        (( $empty_vol > $min_kb )) && min_kb=$empty_vol
    done

    export ost_low_vol="${min_kb}KB"
    local trig_kb=$(($min_kb + 1024 )) # low thresh. +1MB
    export ost_high_vol="${trig_kb}KB"

    [ "$DEBUG" = "1" ] && $LFS df $ROOT
    echo "setting low threshold = $ost_low_vol, high_threshold = $ost_high_vol"

    # create nothing on OST0000 (should not be purged)
    # ensure OST1 usage is trig_kb + 1M
    # ensure OST2 usage is trig_kb + 2M
    # etc...
    for i in $(seq 1 $maxidx); do
        vol=`$LFS df $ROOT | grep OST000$i | awk '{print $3}'`
        nbkb=$(($trig_kb + 1024*$i - $vol))
        nbmb=$(($nbkb/1024+1))
        for f in $(seq 1 $nbmb); do
            $LFS setstripe -c 1 -i $i $ROOT/test_ost_order.ost_$i.$f || error "lfs setstripe"
            dd if=/dev/zero of=$ROOT/test_ost_order.ost_$i.$f bs=1M count=$nbmb || error "dd"
        done
    done

    wait_stable_df

    # check thresholds only, then purge
    for opt in "--check-thresholds" "--purge"; do
        :> rh_purge.log
        $RH -f ./cfg/$config_file $opt --once -l DEBUG -L rh_purge.log || error "command $opt error"
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

	if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

    wait_stable_df

	if (( $is_hsmlite != 0 )); then
        # this mode may create an extra inode in filesystem: inital scan
        # to take it into account
		$RH -f ./cfg/$config_file --scan --once -l MAJOR -L rh_scan.log || error "executing $CMD --scan"
		check_db_error rh_scan.log
    fi

	# triggers to be checked
	# - inode count > max_count
	# - fs volume	> max_vol
	# - root quota  > user_quota

	# initial inode count
	empty_count=`df -i $ROOT/ | xargs | awk '{print $(NF-3)}'`
    empty_count_user=0

#	((file_count=$max_count-$empty_count))
	file_count=$max_count

	# compute file size to exceed max vol and user quota
	empty_vol=`df -k $ROOT  | xargs | awk '{print $(NF-3)}'`
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
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=$file_size  >/dev/null 2>/dev/null || error "writing $ROOT/file.$i"

		if (( $is_lhsm != 0 )); then
			flush_data
			$LFS hsm_archive $ROOT/file.$i
		fi
	done

	if (( $is_lhsm != 0 )); then
		wait_done 60 || error "Copy timeout"
	fi

	# wait for df sync
    wait_stable_df

	if (( $is_hsmlite != 0 )); then
        # scan and sync
		$RH -f ./cfg/$config_file --scan --sync -l DEBUG  -L rh_migr.log || error "executing $CMD --sync"
		check_db_error rh_migr.log
    else
	  # scan
  	  $RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log
  		check_db_error rh_chglogs.log
    fi

	# check purge triggers
	$RH -f ./cfg/$config_file --check-thresholds --once -l FULL -L rh_purge.log

	((expect_count=$empty_count+$file_count-$target_count))
	((expect_vol_fs=$empty_vol+$file_count*$file_size-$target_fs_vol))
	((expect_vol_user=$file_count*$file_size-$target_user_vol))
	((expect_count_user=$file_count+$empty_count_user-$target_user_count))

	echo "over trigger limits: $expect_count entries, $expect_vol_fs MB, $expect_vol_user MB for user root, $expect_count_user entries for user root"

	if (($is_lhsm != 0 )); then
		nb_release=`grep "Released" rh_purge.log | wc -l`
	else
		nb_release=`grep "Purged" rh_purge.log | wc -l`
	fi

	count_trig=`grep " entries must be purged in Filesystem" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`
	[ -n "$count_trig" ] || count_trig=0

	vol_fs_trig=`grep " blocks (x512) must be purged on Filesystem" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`
	((vol_fs_trig_mb=$vol_fs_trig/2048)) # /2048 == *512/1024/1024

	vol_user_trig=`grep " blocks (x512) must be purged for user" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`
	((vol_user_trig_mb=$vol_user_trig/2048)) # /2048 == *512/1024/1024

	cnt_user_trig=`grep " files must be purged for user" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`
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

function test_periodic_trigger
{
	config_file=$1
	sleep_time=$2
	policy_str=$3

	if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

	t0=`date +%s`
	echo "1-Populating filesystem..."
	# create 3 files of each type
	# (*.1, *.2, *.3, *.4)
	for i in `seq 1 4`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "$? writing $ROOT/file.$i"
		dd if=/dev/zero of=$ROOT/foo.$i bs=1M count=1 >/dev/null 2>/dev/null || error "$? writing $ROOT/foo.$i"
		dd if=/dev/zero of=$ROOT/bar.$i bs=1M count=1 >/dev/null 2>/dev/null || error "$? writing $ROOT/bar.$i"

    	flush_data
		if (( $is_lhsm != 0 )); then
			$LFS hsm_archive $ROOT/file.$i $ROOT/foo.$i $ROOT/bar.$i
		fi
	done

	if (( $is_lhsm != 0 )); then
		wait_done 60 || error "Copy timeout"
	fi


	# scan
	echo "2-Populating robinhood database (scan)..."
	if (( $is_hsmlite != 0 )); then
        # scan and sync
		$RH -f ./cfg/$config_file --scan --sync -l DEBUG  -L rh_migr.log || error "executing $CMD --sync"
  		check_db_error rh_migr.log
    else
	    $RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log || error "executing $CMD --scan"
  		check_db_error rh_scan.log
    fi

	# make sure files are old enough
	sleep 2

	# start periodic trigger in background
	echo "3.1-checking trigger for first policy..."
	$RH -f ./cfg/$config_file --purge -l DEBUG -L rh_purge.log &
	sleep 2

	t1=`date +%s`
	((delta=$t1 - $t0))

    clean_caches # blocks is cached
	# it first must have purged *.1 files (not others)
	check_released "$ROOT/file.1" || error "$ROOT/file.1 should have been released after $delta s"
	check_released "$ROOT/foo.1"  || error "$ROOT/foo.1 should have been released after $delta s"
	check_released "$ROOT/bar.1"  || error "$ROOT/bar.1 should have been released after $delta s"
	check_released "$ROOT/file.2" && error "$ROOT/file.2 shouldn't have been released after $delta s"
	check_released "$ROOT/foo.2"  && error "$ROOT/foo.2 shouldn't have been released after $delta s"
	check_released "$ROOT/bar.2"  && error "$ROOT/bar.2 shouldn't have been released after $delta s"

	((sleep_time=$sleep_time-$delta))
	sleep $(( $sleep_time + 2 ))
	# now, *.2 must have been purged
	echo "3.2-checking trigger for second policy..."

	t2=`date +%s`
	((delta=$t2 - $t0))

    clean_caches # blocks is cached
	check_released "$ROOT/file.2" || error "$ROOT/file.2 should have been released after $delta s"
	check_released "$ROOT/foo.2" || error "$ROOT/foo.2 should have been released after $delta s"
	check_released "$ROOT/bar.2" || error "$ROOT/bar.2 should have been released after $delta s"
	check_released "$ROOT/file.3" && error "$ROOT/file.3 shouldn't have been released after $delta s"
	check_released "$ROOT/foo.3"  && error "$ROOT/foo.3 shouldn't have been released after $delta s"
	check_released "$ROOT/bar.3" && error "$ROOT/bar.3 shouldn't have been released after $delta s"

	# wait 20 more secs (so another purge policy is applied)
	sleep 20
	# now, it's *.3
	# *.4 must be preserved
	echo "3.3-checking trigger for third policy..."

	t3=`date +%s`
	((delta=$t3 - $t0))

    clean_caches # blocks is cached
	check_released "$ROOT/file.3" || error "$ROOT/file.3 should have been released after $delta s"
	check_released "$ROOT/foo.3"  || error "$ROOT/foo.3 should have been released after $delta s"
	check_released "$ROOT/bar.3"  || error "$ROOT/bar.3 should have been released after $delta s"
	check_released "$ROOT/file.4" && error "$ROOT/file.4 shouldn't have been released after $delta s"
	check_released "$ROOT/foo.4"  && error "$ROOT/foo.4 shouldn't have been released after $delta s"
	check_released "$ROOT/bar.4"  && error "$ROOT/bar.4 shouldn't have been released after $delta s"

	# final check: 3x "Purge summary: 3 entries"
	nb_pass=`grep "Purge summary: 3 entries" rh_purge.log | wc -l`
	if (( $nb_pass == 3 )); then
		echo "OK: triggered 3 times"
	else
		error "unexpected trigger count $nb_pass (in $delta sec)"
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

	# create test tree

	mkdir -p $ROOT/dir_A
	mkdir -p $ROOT/dir_B
	mkdir -p $ROOT/dir_C

	# classes are:
	# 1) even_and_B
	# 2) even_and_not_B
	# 3) odd_or_A
	# 4) other

	echo "data" > $ROOT/dir_A/file.0 #2
	echo "data" > $ROOT/dir_A/file.1 #3
	echo "data" > $ROOT/dir_A/file.2 #2
	echo "data" > $ROOT/dir_A/file.3 #3
	echo "data" > $ROOT/dir_A/file.x #3
	echo "data" > $ROOT/dir_A/file.y #3

	echo "data" > $ROOT/dir_B/file.0 #1
	echo "data" > $ROOT/dir_B/file.1 #3
	echo "data" > $ROOT/dir_B/file.2 #1
	echo "data" > $ROOT/dir_B/file.3 #3

	echo "data" > $ROOT/dir_C/file.0 #2
	echo "data" > $ROOT/dir_C/file.1 #3
	echo "data" > $ROOT/dir_C/file.2 #2
	echo "data" > $ROOT/dir_C/file.3 #3
	echo "data" > $ROOT/dir_C/file.x #4
	echo "data" > $ROOT/dir_C/file.y #4

	# => 2x 1), 4x 2), 8x 3), 2x 4)

	echo "1bis-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	# read changelogs
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi
	check_db_error rh_chglogs.log

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error ""

	# count the number of file for each policy
	nb_pol1=`grep hints rh_migr.log | grep even_and_B | wc -l`
	nb_pol2=`grep hints rh_migr.log | grep even_and_not_B | wc -l`
	nb_pol3=`grep hints rh_migr.log | grep odd_or_A | wc -l`
	nb_pol4=`grep hints rh_migr.log | grep unmatched | wc -l`

	#nb_pol1=`grep "matches the condition for policy 'inter_migr'" rh_migr.log | wc -l`
	#nb_pol2=`grep "matches the condition for policy 'union_migr'" rh_migr.log | wc -l`
	#nb_pol3=`grep "matches the condition for policy 'not_migr'" rh_migr.log | wc -l`
	#nb_pol4=`grep "matches the condition for policy 'default'" rh_migr.log | wc -l`

	(( $nb_pol1 == 2 )) || error "********** TEST FAILED: wrong count of matching files for fileclass 'even_and_B': $nb_pol1"
	(( $nb_pol2 == 4 )) || error "********** TEST FAILED: wrong count of matching files for fileclass 'even_and_not_B': $nb_pol2"
	(( $nb_pol3 == 8 )) || error "********** TEST FAILED: wrong count of matching files for fileclass 'odd_or_A': $nb_pol3"
	(( $nb_pol4 == 2 )) || error "********** TEST FAILED: wrong count of matching files for fileclass 'unmatched': $nb_pol4"

	(( $nb_pol1 == 2 )) && (( $nb_pol2 == 4 )) && (( $nb_pol3 == 8 )) \
		&& (( $nb_pol4 == 2 )) && echo "OK: test successful"
}

function test_info_collect
{
	config_file=$1
	sleep_time1=$2
	sleep_time2=$3
	policy_str="$4"

	clean_logs

	# test reading changelogs or scanning with strange names, etc...
	mkdir $ROOT'/dir with blanks'
	mkdir $ROOT'/dir with "quotes"'
	mkdir "$ROOT/dir with 'quotes'"

	touch $ROOT'/dir with blanks/file 1'
	touch $ROOT'/dir with blanks/file with "double" quotes'
	touch $ROOT'/dir with "quotes"/file with blanks'
	touch "$ROOT/dir with 'quotes'/file with 1 quote: '"

	sleep $sleep_time1

	# read changelogs
	if (( $no_log )); then
		echo "1-Scanning..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
		nb_cr=0
	else
        [ "$DEBUG" = "1" ] && $LFS changelog lustre
		echo "1-Reading changelogs..."
		#$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
		$RH -f ./cfg/$config_file --readlog -l FULL -L rh_chglogs.log  --once || error ""
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

	if (( $no_log == 0 )); then
        if (( $nb_close != 4 )); then
            if [[ $LVERSION = 2.[01]* ]] ; then
                # CLOSE record is only expected since Lustre 2.2
                # for previous versions, just display a warning
                echo "warning: no close record (lustre version $LVERSION)"
            elif [[ $LVERSION = 2.[234]* ]] ; then
                # CLOSE is expected from 2.2 to 2.4
                error ": unexpected number of close: $nb_close / 4"
            else
                echo "warning: no close record (lustre version $LVERSION)"
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
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
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
	$RH -f ./cfg/$config_file --readlog -l FULL -L rh_chglogs.log  --once || error "reading logs"
	grep "DB query failed" rh_chglogs.log && error ": a DB query failed: `grep 'DB query failed' rh_chglogs.log | tail -1`"
	clean_logs
}

function scan_chk
{
	local config_file=$1

	echo "Scanning..."
        $RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error "scanning filesystem"
	grep "DB query failed" rh_chglogs.log && error ": a DB query failed: `grep 'DB query failed' rh_chglogs.log | tail -1`"
	clean_logs
}

function diff_chk
{
    local config_file=$1

    echo "Scanning with rbh-diff..."
    $DIFF -f ./cfg/$config_file --apply=db -l DEBUG > rh_chglogs.log  2>&1 || error "scanning filesystem"
    grep "DB query failed" rh_chglogs.log && error ": a DB query failed: `grep 'DB query failed' rh_chglogs.log | tail -1`"
    clean_logs
}


function test_info_collect2
{
	config_file=$1
	flavor=$2
	policy_str="$3"

	clean_logs

	if (($no_log != 0 && $flavor != 1 )); then
		echo "Changelogs not supported on this config: skipped"
		set_skipped
		return 1
	fi

	# create 10k entries
	../fill_fs.sh $ROOT 10000 >/dev/null

	# flavor 1: scan only x3
	# flavor 2: mixed (readlog/scan/readlog/scan)
	# flavor 3: mixed (readlog/readlog/scan/scan)
	# flavor 4: mixed (scan/scan/readlog/readlog)
	# flavor 5: diff --apply=db x2

	if (( $flavor == 1 )); then
		scan_chk $config_file
		scan_chk $config_file
		scan_chk $config_file
	elif (( $flavor == 2 )); then
		readlog_chk $config_file
		scan_chk    $config_file
		# touch entries before reading log
		../fill_fs.sh $ROOT 10000 >/dev/null
		readlog_chk $config_file
		scan_chk    $config_file
	elif (( $flavor == 3 )); then
		readlog_chk $config_file
		# touch entries before reading log again
		../fill_fs.sh $ROOT 10000 >/dev/null
		readlog_chk $config_file
		scan_chk    $config_file
		scan_chk    $config_file
	elif (( $flavor == 4 )); then
		scan_chk    $config_file
		scan_chk    $config_file
		readlog_chk $config_file
		# touch entries before reading log again
		../fill_fs.sh $ROOT 10000 >/dev/null
		readlog_chk $config_file
	elif (( $flavor == 5 )); then
        diff_chk $config_file
        diff_chk $config_file
	else
		error "Unexpexted test flavor '$flavor'"
	fi
}

function get_db_info
{
    local config_file=$1
    local field=$2
    local entry=$3
    
    $REPORT -f ./cfg/$config_file -e $entry -c | egrep -E "^$field," | cut -d ',' -f 2 | sed -e 's/^ //g'
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
    local d=$ROOT/subdir
    local f=$ROOT/subdir/file
    mkdir $d || error "creating directory $d"
    id1=$(get_id $d)
    touch $f || error "creating file $f"
    id2=$(get_id $f)
    idr=$(get_id $ROOT/.)

    [ "$DEBUG" = "1" ] && echo -e "$ROOT: $idr\n$d: $id1\n$f: $id2"

    # read the changelog
    readlog_chk $config_file

    # check the id, path and parent for $ROOT, $d and $f
    idrb=$(get_db_info $config_file id $idr | tr -d '[]')
    [ "$idr" = "$idrb" ] || error "id doesn't match: $idr != $idrb"
    pathr=$(get_db_info $config_file path $idr)
    # path must be empty or match $ROOT
    [ "$pathr" = "" ] || [ "$pathr" = "$ROOT" ] || error "path doesn't match: $ROOT != $pathr"

    # name and parent are supposed to be empty for ROOT
    nr=$(get_db_info $config_file name $idr)
    [ "$nr" = "" ] || error "name for $ROOT is not empty: '$nr'"
    pr=$(get_db_info $config_file parent_id $idr)
    [ "$pr" = "" ] || error "parent_id for $ROOT is not empty: '$pr'"

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

    # generate an event on $ROOT and do the checks again
    touch $ROOT/.
    sleep 1
    # read the changelog
    readlog_chk $config_file
    
    # check the id, path and parent for $ROOT, $d and $f
    idrb=$(get_db_info $config_file id $idr | tr -d '[]')
    [ "$idr" = "$idrb" ] || error "id doesn't match: $idr != $idrb"
    pathr=$(get_db_info $config_file path $idr)
    # path must be empty or match $ROOT
    [ "$pathr" = "" ] || [ "$pathr" = "$ROOT" ] || error "path doesn't match: $ROOT != $pathr"

    # name and parent are supposed to be empty for ROOT
    nr=$(get_db_info $config_file name $idr)
    [ "$nr" = "" ] || error "name for $ROOT is not empty: '$nr'"
    pr=$(get_db_info $config_file parent_id $idr)
    [ "$pr" = "" ] || error "parent_id for $ROOT is not empty: '$pr'"

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
    mkdir -p $ROOT/dir1/dir2
    mkdir -p $ROOT/dir3
    touch $ROOT/file1
    touch $ROOT/dir1/file2
    touch $ROOT/dir1/dir2/file3
    touch $ROOT/dir3/file4

    # initial scan
    $RH -f ./cfg/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing inital scan"
    check_db_error rh_scan.log

    # remove a path component from the DB
    id=$(get_id $ROOT/dir1/dir2)
    [ -z $id ] && error "could not get id"
    # FIXEME only for Lustre 2.x
    mysql robinhood_lustre -e "DELETE FROM NAMES WHERE id='$id'" || error "DELETE request"

	if (( $is_hsmlite + $is_lhsm > 0 )); then
        # check how a child entry is archived
        $RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log
        check_db_error rh_migr.log
        if (( $is_hsmlite > 0 )); then
            name=$(find $BKROOT -type f -name "file3__*")
            cnt=$(echo $name | wc -w)
            (( $cnt == 1 )) || error "1 file expected to match file 3 in backend, $cnt found"
            echo "file3 archived as $name"
        fi
    fi

    # check what --dump reports
    f3=$($REPORT -f ./cfg/$config_file --dump --csv -q | grep "file3" | awk '{print $(NF)}')
    echo "file3 reported with path $f3"
    [[ $f3 = /* ]] && [[ $f3 != $ROOT/dir1/dir2/file3 ]] && error "$f3 : invalid fullpath"
    
    # check filter path behavior
    # should report at least file2 (and optionnally file3 : must check its path is valid)
    f2=$($REPORT -f ./cfg/$config_file --dump --csv -q -P "$ROOT/dir1" | grep file2 | awk '{print $(NF)}')
    [[ -n $f2 ]] && echo "file2 reported with path $f2"
    [[ $f2 != $ROOT/dir1/file2 ]] && error "wrong path reported for file2: $f2"

    f3=$($REPORT -f ./cfg/$config_file --dump --csv -q -P "$ROOT/dir1" | grep file3 | awk '{print $(NF)}')
    [[ -n $f3 ]] && echo "file3 reported with path $f3"
    [[ $f3 = /* ]] && [[ $f3 != $ROOT/dir1/dir2/file3 ]] && error "$f3 : invalid fullpath"

    f3=$($REPORT -f ./cfg/$config_file --dump --csv -q -P "$ROOT/dir1/dir2" | grep file)
    [[ -n $f3 ]] && echo "file3 reported with path $f3"
    [[ $f3 = /* ]] && [[ $f3 != $ROOT/dir1/dir2/file3 ]] && error "$f3 : invalid fullpath"

    # check find behavior
    # find cannot go into dir2
    $FIND -f ./cfg/$config_file $ROOT/dir1 | grep dir2 && echo "$ROOT/dir1/dir2 reported?!"
    # starting from dir2 fid, it can list file3 in it
    f3=$($FIND -f ./cfg/$config_file $ROOT/dir1/dir2 | grep file3)
    echo "find: $f3"
    [[ $f3 = $ROOT/dir1/dir2/file3 ]] || error "$f3 : invalid fullpath"

    # like find, should count file3
    fc=$($DU -d -f ./cfg/$config_file $ROOT/dir1/dir2 | grep "file count" | cut -d ':' -f 2 | cut -d ',' -f 1)
    [[ $fc = 1 ]] || error "expected filecount in $ROOT/dir1/dir2: 1 (got $fc)"

    # check -e report
    # dir2 should be in DB, even with no path
    $REPORT -f ./cfg/$config_file --csv -e "$ROOT/dir1/dir2" | grep "md updt" || error "$ROOT/dir1/dir2 should have a DB entry"

    $REPORT -f ./cfg/$config_file --csv -e "$ROOT/dir1/dir2/file3"  > report.log || error "report error for $ROOT/dir1/dir2/file3"
    grep "md updt" report.log || error "$ROOT/dir1/dir2/file3 should have a DB entry"
    f3=$(egrep "^path," report.log)
    [[ $f3 = /* ]] && [[ $f3 != $ROOT/dir1/dir2/file3 ]] && error "$f3 : invalid fullpath"
	if (( $is_hsmlite > 0 )); then
        b3=$(grep "backend_path," report.log | cut -d ',' -f 2)
        # b3 should be in 'dir2' or in '__unknown_path'
        echo $b3 | egrep "dir1/dir2|unknown_path" || error "unexpected backend path $b3"
    fi

    # check what rm does (+undelete)
	if (( $no_log==0 )); then
	   $LFS changelog_clear lustre-MDT0000 cl1 0

        rm -f $ROOT/dir1/dir2/file3
        readlog_chk $config_file 

	    if (( $is_lhsm + $is_hsmlite > 0 )); then
            $REPORT -f ./cfg/$config_file -Rcq > report.log
            nb=$(cat report.log | grep file3 | wc -l)
            (($nb == 1)) || error "file3 not reported in remove-pending list"
            f3=$(cut -d "," -f 3 report.log)
            [[ $f3 = /* ]] && [[ $f3 != $ROOT/dir1/dir2/file3 ]] && error "$f3 : invalid fullpath"
        fi

        if (( $is_hsmlite > 0 )); then
            b3=$(cut -d "," -f 6 report.log)
            echo $b3 | egrep "dir1/dir2|unknown_path" || error "unexpected backend path $b3"

            b3=$($UNDELETE -f ./cfg/$config_file -L '*/file3' | grep "Backend path" | cut -d ':' -f 2- | tr -d ' ')
            echo $b3 | egrep "dir1/dir2|unknown_path" || error "unexpected backend path $b3"

            $UNDELETE -f ./cfg/$config_file -R '*/file3' -l DEBUG || error "undeleting file3"
            find $ROOT -name "file3" -ls | tee report.log
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

    export fs_path=$ROOT/subdir # retrieved from env when parsing config file

    local dir_rel="dir1 dir2"
    local file_rel="dir1/file.1 dir1/file.2 dir2/file.3 file.4"

    for d in $dir_rel; do
        mkdir -p $fs_path/$d || error mkdir
    done
    for f in $file_rel; do
        touch $fs_path/$f || error touch
    done

    # scan the filesystem
    $RH -f ./cfg/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing inital scan"
    check_db_error rh_scan.log

    # check that rbh-find output is correct (2 methods)
    for opt in "-nobulk $fs_path" "$fs_path" "-nobulk" ""; do
        echo "checking output for rbh-find $opt..."
        $FIND -f ./cfg/$config_file $opt > rh_report.log
        for e in $dir_rel $file_rel; do
            egrep -E "^$fs_path/$e$" rh_report.log || error "$e not found in rbh-find output"
        done
    done

    # check that rbh-report output is correct
    $REPORT -f ./cfg/$config_file -q --dump | awk '{print $(NF)}'> rh_report.log
    [ "$DEBUG" = "1" ] && cat rh_report.log
    for e in $dir_rel $file_rel; do
        egrep -E "^$fs_path/$e$" rh_report.log || error "$e not found in report output"
    done

    # backup: check that backend path is correct
    if (( $is_hsmlite > 0 )); then
        # wait atime > 1s
        sleep 1
        $RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log
        check_db_error rh_migr.log

        for e in $file_rel; do
            ls -d $BKROOT/${e}__* || error "$BKROOT/$e* not found in backend"
        done
    fi
}

function check_status_count
{
    report=$1
    status=$2
    count=$3

    nst=$(grep -E "^$status" $report | cut -d ',' -f 3 | tr -d ' ')
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
        mkdir -p $ROOT/$d || error mkdir
    done
    for f in $file_rel; do
        /bin/cp $src_file $ROOT/$f || error cp
    done

    # scan the filesystem (compress=no)
    export compress=no
    $RH -f ./cfg/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing inital scan"
    check_db_error rh_scan.log

    # check file status
    $REPORT -f ./cfg/$config_file -i -q | grep 'file,' > report.out
    check_status_count report.out new 4

    # check how a child entries is archived
    $RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log
    check_db_error rh_migr.log

    # check file status
    $REPORT -f ./cfg/$config_file -i -q | grep 'file,' > report.out
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
        cat $src_file >> $ROOT/$f || error "appending $f"
    done
    for f in $file_rel_new; do
        /bin/cp $src_file $ROOT/$f || error "creating $f"
    done

    # scan the file system and check file status
    $RH -f ./cfg/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing 2nd scan"
    check_db_error rh_scan.log

    # check file status
    $REPORT -f ./cfg/$config_file -i -q | grep 'file,' > report.out
    check_status_count report.out synchro 2
    check_status_count report.out modified 2
    check_status_count report.out new 3

    # archive all dirty data and check status
    $RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log
    check_db_error rh_migr.log

    # check file status
    $REPORT -f ./cfg/$config_file -i -q | grep 'file,' > report.out
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
        cat $src_file >> $ROOT/$f || error "appending $f"
    done

    export compress=no
    $RH -f ./cfg/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing inital scan"
    check_db_error rh_scan.log

    # check file status
    $REPORT -f ./cfg/$config_file -i -q | grep 'file,' > report.out
    check_status_count report.out synchro 5
    check_status_count report.out modified 2

    $RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log
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
    $REPORT -f ./cfg/$config_file -i -q | grep 'file,' > report.out
    check_status_count report.out synchro 7

    # test disaster recovery with compressed files
    local before=/tmp/before.$$
    local after=/tmp/after.$$
    local diff=/tmp/diff.$$
    # shots before disaster (time is only significant for files)
    find $ROOT -type f -printf "%n %m %T@ %g %u %s %p %l\n" > $before
    find $ROOT -type d -printf "%n %m %g %u %s %p %l\n" >> $before
    find $ROOT -type l -printf "%n %m %g %u %s %p %l\n" >> $before

    # perform 2 disaster recovery with compress=yes and compress=no
    for c in yes no; do
        export compress=$c
        # FS disaster
        if [[ -n "$ROOT" ]]; then
            echo "Disaster: all FS content is lost"
            rm  -rf $ROOT/*
        fi

        # perform the recovery
        echo "Performing recovery (compress=$c)..."
        cp /dev/null recov.log
        $RECOV -f ./cfg/$config_file --start -l DEBUG >> recov.log 2>&1 || error "Error starting recovery"
        $RECOV -f ./cfg/$config_file --resume -l DEBUG >> recov.log 2>&1 || error "Error performing recovery"
        $RECOV -f ./cfg/$config_file --complete -l DEBUG >> recov.log 2>&1 || error "Error completing recovery"

        find $ROOT -type f -printf "%n %m %T@ %g %u %s %p %l\n" > $after
        find $ROOT -type d -printf "%n %m %g %u %s %p %l\n" >> $after
        find $ROOT -type l -printf "%n %m %g %u %s %p %l\n" >> $after

        diff  $before $after > /tmp/diff.$$ || error "unexpected differences between initial and final state"
        [ "$DEBUG" = "1" ] && cat /tmp/diff.$$

        # check that no file in Lustre is restored as compressed file
        lucomp=$(find $ROOT -type f -exec file {} \; | grep "gzip compressed data" | wc -l)
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

	clean_logs

	if (($no_log != 0)); then
		echo "Changelogs not supported on this config: skipped"
		set_skipped
		return 1
	fi

	echo "1-Start reading changelogs in background..."
	# read changelogs
	$RH -f ./cfg/$config_file --readlog -l FULL -L rh_chglogs.log  --detach --pid-file=rh.pid || error "could not start cl reader"

	echo "2-create/unlink sequence"
    for i in $(seq 1 1000); do
        touch $ROOT/file.$i
        rm -f $ROOT/file.$i
        touch $ROOT/file.$i
        rm -f $ROOT/file.$i
    done

    # wait for consumer to read all records
    sleep 2
	check_db_error rh_chglogs.log

    # TODO add addl checks here

	$REPORT -f ./cfg/$config_file --dump-all -cq > report.out || error "report cmd failed"
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
    mkdir $ROOT/dir.1 || error "mkdir"
    chmod 0750 $ROOT/dir.1 || error "chmod"
    mkdir $ROOT/dir.2 || error "mkdir"
    mkdir $ROOT/dir.3 || error "mkdir"
    touch $ROOT/dir.1/a $ROOT/dir.1/b $ROOT/dir.1/c || error "touch"
    touch $ROOT/dir.2/d $ROOT/dir.2/e $ROOT/dir.2/f || error "touch"
    touch $ROOT/file || error "touch"

    # initial scan
    echo "1-Initial scan..."
    $RH -f ./cfg/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing inital scan"

    # new entry (file & dir)
    touch $ROOT/dir.1/file.new || error "touch"
    mkdir $ROOT/dir.new	       || error "mkdir"

    # rm'd entry (file & dir)
    rm -f $ROOT/dir.1/b	|| error "rm"
    rmdir $ROOT/dir.3	|| error "rmdir"

    # apply various changes
    chmod 0700 $ROOT/dir.1 		|| error "chmod"
    chown testuser $ROOT/dir.2		|| error "chown"
    chgrp testgroup $ROOT/dir.1/a	|| error "chgrp"
    echo "zqhjkqshdjkqshdjh" >>  $ROOT/dir.1/c || error "append"
    mv $ROOT/dir.2/d  $ROOT/dir.1/d     || error "mv"
    mv $ROOT/file $ROOT/fname           || error "rename"

    # is swap layout feature available?
    has_swap=0
    lfs help | grep swap_layout > /dev/null && has_swap=1
    # if so invert stripe for e and f
    if [ $has_swap -eq 1 ]; then
	lfs swap_layouts $ROOT/dir.2/e  $ROOT/dir.2/f || error "lfs swap_layouts"
    fi

    # need 1s difference for md and name GC
    sleep 1

    echo "2-diff ($policy_str)..."
    if [ "$flavor" = "diff" ]; then
        $DIFF -f ./cfg/$config_file -l FULL > report.out 2> rh_report.log || error "performing diff"
    elif [ "$flavor" = "diffapply" ]; then
        $DIFF --apply=db -f ./cfg/$config_file -l FULL > report.out 2> rh_report.log || error "performing diff"
    elif [ "$flavor" = "scan" ]; then
        $RH -f ./cfg/$config_file -l FULL --scan --once --diff=all -L rh_report.log > report.out || error "performing scan+diff"
    fi

    [ "$DEBUG" = "1" ] && cat report.out

    # must get:
    # new entries dir.1/file.new and dir.new
    egrep '^++' report.out | grep -v '+++' | grep -E "name='file.new'|path='$ROOT/dir.1/file.new'" | grep type=file || error "missing create dir.1/file.new"
    egrep '^++' report.out | grep -v '+++' | grep -E "name='dir.new'|path='$ROOT/dir.new'" | grep type=dir || error "missing create dir.new"
    # rmd entries dir.1/b and dir.3
    nbrm=$(egrep -e '^--' report.out | grep -v -- '---' | wc -l)
    [ $nbrm  -eq 2 ] || error "$nbrm/2 removal"
    # changes
    grep "^+[^ ]*"$(get_id "$ROOT/dir.1") report.out  | grep mode= || error "missing chmod $ROOT/dir.1"
    grep "^+[^ ]*"$(get_id "$ROOT/dir.2") report.out | grep owner=testuser || error "missing chown $ROOT/dir.2"
    grep "^+[^ ]*"$(get_id "$ROOT/dir.1/a") report.out  | grep group=testgroup || error "missing chgrp $ROOT/dir.1/a"
    grep "^+[^ ]*"$(get_id "$ROOT/dir.1/c") report.out | grep size= || error "missing size change $ROOT/dir.1/c"

    # dir2/d -> dir1/d
    old_parent=$(grep "^-[^ ]*"$(get_id "$ROOT/dir.1/d") report.out | sed -e "s/.*parent=\[\([^]]*\).*/\1/" )
    new_parent=$(grep "^+[^ ]*"$(get_id "$ROOT/dir.1/d") report.out | sed -e "s/.*parent=\[\([^]]*\).*/\1/" )
    [ -z $old_parent ] && error "cannot get old parent of $ROOT/dir.1/d"
    [ -z $new_parent ] && error "cannot get new parent of $ROOT/dir.1/d"
    [ $old_parent = $new_parent ] && error "$ROOT/dir.1/d still has the same parent"

    # file -> fname
    file_fid=$(get_id "$ROOT/fname")
    old_file=$(grep "^-[^ ]*${file_fid}.*name='file'" report.out)
    new_file=$(grep "^+[^ ]*${file_fid}.*name='fname'" report.out)
    [ -z old_file ] && error "missing path change $ROOT/fname"
    [ -z new_file ] && error "missing path change $ROOT/fname"

    if [ $has_swap -eq 1 ]; then
        grep "^+[^ ]*"$(get_id "$ROOT/dir.2/e") report.out | grep stripe || error "missing stripe change $ROOT/dir.2/e"
        grep "^+[^ ]*"$(get_id "$ROOT/dir.2/f") report.out | grep stripe || error "missing stripe change $ROOT/dir.2/f"
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
    $LFS setstripe -c 2 $ROOT/.
    cp -ar /bin $ROOT/bin.1 || error "copy failed"
    cp -ar /bin $ROOT/bin.2 || error "copy failed"

    # run initial scan
    echo "Initial scan..."
    $RH -f ./cfg/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing inital scan"

    # save contents of bin.1
    find $ROOT/bin.1 -printf "%n %y %m %T@ %g %u %p %l\n" > find.out || error "find error"

    # remove it
    echo "removing objects"
    rm -rf "$ROOT/bin.1"

    # cause 1 sec bw initial creation and recovery
    # to check robinhood restore the original date
    sleep 1

    echo "running recovery..."
    strace -f $DIFF -f ./cfg/$config_file --apply=fs > diff.out 2> diff.log || error "rbh-diff error"

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

    find $ROOT/bin.1 -printf "%n %y %m %T@ %g %u %p %l\n" > find2.out || error "find error"

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

    # flavors: OK or KO

    # populate filesystem
    for i in `seq 1 10`; do
        touch $ROOT/file.$i || error "creating entry"
    done

    # do the scan
    echo "scan..."
    $RH -f ./cfg/$config_file --scan --once -l EVENT -L rh_scan.log  || error "performing inital scan"

    # if flavor is OK: completion command must have been called
    if [ "$flavor" = "OK" ]; then
        [ -f out.1 ] || error "file out.1 not found"
        [ -f out.2 ] || error "file out.2 not found"
        # out.1 contains cfg
        grep $config_file out.1 || error "out.1 has unexpected content: $(cat out.1)"
        # out.2 contains fspath
        grep $ROOT out.2 || error "out.2 has unexpected content: $(cat out.2)"
    else
        # matching: CmdParams | ERROR: unmatched '{' in command parameters '../completion.sh {cfg'
        grep "ERROR: unmatched '{' in command parameters" rh_scan.log || error "unreported cmd error"
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

    dirs="$ROOT/dir.1 $ROOT/dir.2 $ROOT/dir.3 $ROOT/dir.3/subdir"
    files="$ROOT/dir.1/file.1  $ROOT/dir.1/file.2  $ROOT/dir.2/file.1 $ROOT/dir.2/file.2 $ROOT/dir.2/file.4 $ROOT/dir.3/subdir/file.1"
    hlink_ref="$ROOT/dir.2/file.3"
    hlink="$ROOT/dir.2/link_file" # initially points to file.3, then file.4

    dirs_tgt="$ROOT/dir.1 $ROOT/dir.2 $ROOT/dir.3 $ROOT/dir.3/subdir.rnm"
    files_tgt="$ROOT/dir.1/file.1.rnm  $ROOT/dir.2/file.2.rnm  $ROOT/dir.2/file.2  $ROOT/dir.2/file.3  $ROOT/dir.2/link_file $ROOT/dir.3/subdir.rnm/file.1"
    deleted="$ROOT/dir.2/file.2"

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
    	$RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
    elif [ "$flavor" = "diff" ]; then
        echo "2. Diff..."
    	$DIFF -f ./cfg/$config_file --apply=db -l DEBUG > rh_scan.log 2>&1 || error "scanning"
    else
        echo "2. Scanning initial state..."
    	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning"
    fi

	if (( $is_lhsm != 0 )); then
		echo "  -archiving all data"
		flush_data
		lfs hsm_archive $files || error "executing lfs hsm_archive"
		echo "  -Waiting for end of data migration..."
		wait_done 60 || error "Migration timeout"
	elif (( $is_hsmlite != 0 )); then
		echo "  -archiving all data"
		$RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log || error "executing $CMD --sync"
        [ "$DEBUG" = "1" ] && find $BKROOT -type f -ls
	fi


    $REPORT -f ./cfg/$config_file --dump-all -q > report.out || error "$REPORT"
    [ "$DEBUG" = "1" ] && cat report.out

    $FIND -f ./cfg/$config_file $ROOT -ls -nobulk > find.out || error "$FIND"
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
        id_from+=( "$(get_id $ROOT/$f)" )
    done

    name_unlnk=(dir.2/file.2 dir.2/link_file)
    id_unlnk=()
    for f in ${name_unlnk[*]}; do
        id_unlnk+=( "$(get_id $ROOT/$f)" )
    done

    # rename entries
    echo "3. Renaming objects..."
    # 1) simple file rename
    mv $ROOT/dir.1/file.1 $ROOT/dir.1/file.1.rnm
    # 2) cross directory file rename
    mv $ROOT/dir.1/file.2 $ROOT/dir.2/file.2.rnm
    # 3) rename that deletes the target
    mv -f $ROOT/dir.2/file.1 $ROOT/dir.2/file.2
    # 4) upper level directory rename
    mv $ROOT/dir.3/subdir $ROOT/dir.3/subdir.rnm
    # 5) overwritting a hardlink
    mv -f $ROOT/dir.2/file.4 $hlink

    # get target fids
    name_to=(dir.1/file.1.rnm dir.2/file.2.rnm dir.2/file.2 dir.3/subdir.rnm dir.3/subdir.rnm dir.2/link_file)
    id_to=()
    for f in ${name_to[*]}; do
        id_to+=( "$(get_id $ROOT/$f)" )
    done

    # namespace GC needs 1s difference
    sleep 1

    # readlog or re-scan
    if [ "$flavor" = "readlog" ]; then
        echo "4. Reading changelogs..."
    	$RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"

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
    	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning"
    elif [ "$flavor" = "diff" ]; then
        echo "4. Diffing again..."
    	$DIFF -f ./cfg/$config_file --apply=db -l DEBUG > rh_scan.log 2>&1 || error "scanning"
    elif [ "$flavor" = "partial" ]; then
        i=0
        for d in $dirs_tgt; do
            # namespace GC needs 1s difference
            sleep 1
            ((i++))
            echo "4.$i Partial scan ($d)..."
        	$RH -f ./cfg/$config_file --scan=$d --once -l DEBUG -L rh_scan.log || error "scanning $d"
        done
    elif [ "$flavor" = "partdiff" ]; then
        i=0
        for d in $dirs_tgt; do
            # namespace GC needs 1s difference
            sleep 1
            ((i++))
            echo "4.$i Partial diff+apply ($d)..."
        	$DIFF -f ./cfg/$config_file --scan=$d --apply=db -l DEBUG  > rh_scan.log 2>&1 || error "scanning $d"
        done
    fi

    $REPORT -f ./cfg/$config_file --dump-all -q > report.out || error "$REPORT"
    [ "$DEBUG" = "1" ] && cat report.out

    $FIND -f ./cfg/$config_file $ROOT -nobulk -ls > find.out || error "$FIND"
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
	    $REPORT -f ./cfg/$config_file --deferred-rm --csv -q > rh_report.log

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
    touch "$ROOT/foo1"
	ln "$ROOT/foo1" "$ROOT/foo2"

	# Check nlink == 2
    $RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
	$FIND -f ./cfg/$config_file $ROOT/foo1 -nobulk -ls > report.out || error "$REPORT"
	nlink=$( cat report.out | awk '{ print $4; }' )
	(( $nlink == 2 )) || error "nlink should be 2 instead of $nlink"

	# Remove one file and check nlink == 1
	rm "$ROOT/foo2"
    $RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
	$FIND -f ./cfg/$config_file $ROOT/foo1 -nobulk -ls > report.out || error "$REPORT"
	nlink=$( cat report.out | awk '{ print $4; }' )
	(( $nlink == 1 )) || error "nlink should be 1 instead of $nlink"

	# Add a new hard link and check nlink == 2
	ln "$ROOT/foo1" "$ROOT/foo3"
    $RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
	$FIND -f ./cfg/$config_file $ROOT/foo1 -nobulk -ls > report.out || error "$REPORT"
	nlink=$( cat report.out | awk '{ print $4; }' )
	(( $nlink == 2 )) || error "nlink should be 1 instead of $nlink"

	# Remove one file and check nlink == 1
	rm "$ROOT/foo3"
    $RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
	$FIND -f ./cfg/$config_file $ROOT/foo1 -nobulk -ls > report.out || error "$REPORT"
	nlink=$( cat report.out | awk '{ print $4; }' )
	(( $nlink == 1 )) || error "nlink should be 1 instead of $nlink"

    # Now create one hardlink, then remove it, but do not run RH in between.
	ln "$ROOT/foo1" "$ROOT/foo2"
	rm "$ROOT/foo2"
	# check nlink == 1
	$RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
	$FIND -f ./cfg/$config_file $ROOT/foo1 -nobulk -ls > report.out || error "$REPORT"
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
    DSTFILE="$ROOT/foo1"
    $LFS setstripe -c 1 $DSTFILE
    dd if=/dev/zero of=$DSTFILE bs=1M count=10
    $LFS migrate -c 2 $DSTFILE

	# Check if a CL_LAYOUT record was emitted and triggered a getstripe().
    $RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
    ngetstripe_zero=$(grep LYOUT rh_scan.log | grep -c "getstripe=0")
    ngetstripe=$(grep LYOUT rh_scan.log | grep -c "getstripe=1")
    (( $ngetstripe_zero == 0 && $ngetstripe > 0 )) || error "CL_LAYOUT should trigger a getstripe() operation."

    $LFS migrate -c 1 $DSTFILE
    fsdiff=$($RH -f ./cfg/$config_file --readlog --diff=stripe --once -l DEBUG -L rh_scan.log)
    (( $? == 0 )) || error "reading changelog (diff)"

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
    $cmd -f ./cfg/$cfg -l FULL > rh.out 2>> rh.log || error "running $cmd"
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
    $REPORT -f ./cfg/$cfg -c -e $f > rh.out 2>> rh.log || error "$f not in RBH DB"
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
    rm -f $ROOT/file.*

    echo "test setup: checking diff=$diff, getstripe allowed=$getstripe, has_swap=$has_swap"

    echo "- non-striped file"
    # case 1 (all Lustre versions): create an unstriped file, then stripe it
    create_nostripe $ROOT/file.1 || error "creating unstriped file"
    run_scan_cmd $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "" "stripe_count=0" 1
    check_stripe $config_file $ROOT/file.1 "none"

    # no update expected for second run
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe" "stripe" 0 # no stripe change expected
    check_stripe $config_file $ROOT/file.1 "none"

    # check if "getstripe -g" exists
    has_gen=0
    $LFS getstripe -g $ROOT/ 2>/dev/null && has_gen=1

    # stripe it
    echo "- stripe file"
    lfs setstripe -c 1 $ROOT/file.1 || error "setting file stripe"
    idx=$(lfs getstripe -i $ROOT/file.1)
    [ "$DEBUG" = "1" ] && echo "$ROOT/file.1: ost$idx"
    [ "$DEBUG" = "1" ] && [ "$has_gen" = "1" ] && echo "$ROOT/file.1: gen $(lfs getstripe -g $ROOT/file.1)"
    run_scan_cmd $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe_count=0" "stripe_count=1" 1
    check_stripe $config_file $ROOT/file.1 "ost#$idx"

    # no update expected for second run
    [ "$DEBUG" = "1" ] &&  [ "$has_gen" = "1" ] && echo "$ROOT/file.1: gen $(lfs getstripe -g $ROOT/file.1)"
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe" "stripe" 0 # no stripe change expected
    check_stripe $config_file $ROOT/file.1 "ost#$idx"

    # other cases: play with layout_swap (skip for Lustre < 2.4)
    if (( $has_swap == 0 )); then
        echo "No layout swap: skipping the end of the test"
        return 0
    fi

    # swap with another striped file
    lfs setstripe -c 1 $ROOT/file.2 || error "creating striped file"
    idx2=$(lfs getstripe -i $ROOT/file.2)
    [ "$DEBUG" = "1" ] && echo "$ROOT/file.2: ost$idx2"
    echo "- swap it with striped file"
    lfs swap_layouts $ROOT/file.1 $ROOT/file.2 || error "swapping file layouts"
    [ "$DEBUG" = "1" ] &&  [ "$has_gen" = "1" ] && echo "$ROOT/file.1: gen $(lfs getstripe -g $ROOT/file.1)"
    run_scan_cmd $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripes={ost#$idx" "stripes={ost#$idx2" 1
    check_stripe $config_file $ROOT/file.1 "ost#$idx2"

    # no update expected for second run
    [ "$DEBUG" = "1" ] &&  [ "$has_gen" = "1" ] && echo "$ROOT/file.1: gen $(lfs getstripe -g $ROOT/file.1)"
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe" "stripe" 0 # no stripe change expected
    check_stripe $config_file $ROOT/file.1 "ost#$idx2"

    # swap with non-striped file
    create_nostripe $ROOT/file.3 || error "creating unstriped file"
    echo "- swap it with non-striped file"
    lfs swap_layouts $ROOT/file.1 $ROOT/file.3 || error "swapping file layouts"
    [ "$DEBUG" = "1" ] &&  [ "$has_gen" = "1" ] && echo "$ROOT/file.1: gen $(lfs getstripe -g $ROOT/file.1)"
    run_scan_cmd $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe_count=1" "stripe_count=0" 1
    check_stripe $config_file $ROOT/file.1 "none"

    [ "$DEBUG" = "1" ] &&  [ "$has_gen" = "1" ] && echo "$ROOT/file.1: gen $(lfs getstripe -g $ROOT/file.1)"
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe" "stripe" 0 # no stripe change expected
    check_stripe $config_file $ROOT/file.1 "none"

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

    rm -f $ROOT/file.*

    echo "test setup: checking diff=$diff, getstripe allowed=$getstripe, has_swap=$has_swap"

    # initial scan
    run_scan_cmd $config_file "scan"

    echo "- non-striped file"
    # case 1 (all Lustre versions): create an unstriped file, then stripe it
    create_nostripe $ROOT/file.1 || error "creating unstriped file"
    # no update expected for the given specified run
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "" "stripe_count=0" 1
    # update db contents
    run_scan_cmd $config_file "scan"
    check_stripe $config_file $ROOT/file.1 "none"

    # check if "getstripe -g" exists
    has_gen=0
    $LFS getstripe -g $ROOT/ 2>/dev/null && has_gen=1

    # stripe it
    echo "- stripe file"
    lfs setstripe -c 1 $ROOT/file.1 || error "setting file stripe"
    idx=$(lfs getstripe -i $ROOT/file.1)
    [ "$DEBUG" = "1" ] && echo "$ROOT/file.1: ost$idx"
    [ "$DEBUG" = "1" ] && [ "$has_gen" = "1" ] && echo "$ROOT/file.1: gen $(lfs getstripe -g $ROOT/file.1)"
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe_count=0" "stripe_count=1" 1
    run_scan_cmd $config_file "scan"
    check_stripe $config_file $ROOT/file.1 "ost#$idx"

    # other cases: play with layout_swap (skip for Lustre < 2.4)

    if (( $has_swap == 0 )); then
        echo "No layout swap: skipping the end of the test"
        return 0
    fi

    # swap with another striped file
    lfs setstripe -c 1 $ROOT/file.2 || error "creating striped file"
    idx2=$(lfs getstripe -i $ROOT/file.2)
    [ "$DEBUG" = "1" ] && echo "$ROOT/file.2: ost$idx2"
    echo "- swap it with striped file"
    lfs swap_layouts $ROOT/file.1 $ROOT/file.2 || error "swapping file layouts"
    [ "$DEBUG" = "1" ] && [ "$has_gen" = "1" ] && echo "$ROOT/file.1: gen $(lfs getstripe -g $ROOT/file.1)"
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripes={ost#$idx" "stripes={ost#$idx2" 1
    run_scan_cmd $config_file "scan"
    check_stripe $config_file $ROOT/file.1 "ost#$idx2"

    # swap with non-striped file
    create_nostripe $ROOT/file.3 || error "creating unstriped file"
    echo "- swap it with non-striped file"
    lfs swap_layouts $ROOT/file.1 $ROOT/file.3 || error "swapping file layouts"
    [ "$DEBUG" = "1" ] && [ "$has_gen" = "1" ] && echo "$ROOT/file.1: gen $(lfs getstripe -g $ROOT/file.1)"
    scan_check_no_update $config_file $flavor
    [ $getstripe = 0 ] && egrep "Getstripe=1" rh.log && error "No getstripe operation expected"
    [ $diff = 1 ] && check_stripe_diff "stripe_count=1" "stripe_count=0" 1
    run_scan_cmd $config_file "scan"
    check_stripe $config_file $ROOT/file.1 "none"

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

    dirs="$ROOT/dir.1 $ROOT/dir.2 $ROOT/dir.3 $ROOT/dir.3/subdir $ROOT/dir.4"
    files="$ROOT/dir.1/file.1  $ROOT/dir.1/file.2  $ROOT/dir.2/file.1 $ROOT/dir.2/file.2 $ROOT/dir.2/file.4 $ROOT/dir.3/subdir/file.1 $ROOT/dir.4/file.3"
    hlink_refs=("$ROOT/dir.2/file.3" "$ROOT/dir.4/file.1" "$ROOT/dir.4/file.2")
    hlinks=("$ROOT/dir.2/link_file" "$ROOT/dir.1/link.1 $ROOT/dir.2/link.1" "$ROOT/dir.2/link.2")
    #[0] file.4 will over write it, [1] one more link will be created, [2]previous path ($ROOT/dir.4/file.2) will be removed

    dirs_tgt="$ROOT/dir.1 $ROOT/dir.2 $ROOT/dir.3 $ROOT/dir.3/subdir.rnm $ROOT/dir.4"
    files_tgt="$ROOT/dir.1/file.1.rnm  $ROOT/dir.2/file.2.rnm  $ROOT/dir.2/file.2  $ROOT/dir.2/file.3  $ROOT/dir.2/link_file $ROOT/dir.3/subdir.rnm/file.1 $ROOT/dir.2/link.2 $ROOT/dir.1/new"
    hlink_refs_tgt=("$ROOT/dir.4/file.1" "$ROOT/dir.2/new")
    hlinks_tgt=("$ROOT/dir.1/link.1 $ROOT/dir.2/link.1 $ROOT/dir.4/link.1" "$ROOT/dir.4/link.new")
        # only previous [1] remaining as [0], [1] is a new link

    deleted="$ROOT/dir.2/file.2 $ROOT/dir.4/file.3"

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
    	$RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
    elif [ "$flavor" = "diff" ]; then
        echo "2. Diff..."
    	$DIFF -f ./cfg/$config_file --apply=db -l DEBUG > rh_scan.log 2>&1 || error "scanning"
    else
        echo "2. Scanning initial state..."
    	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning"
    fi

	if (( $is_lhsm != 0 )); then
		echo "  -archiving all data"
		flush_data
		lfs hsm_archive $files || error "executing lfs hsm_archive"
		echo "  -Waiting for end of data migration..."
		wait_done 60 || error "Migration timeout"
	elif (( $is_hsmlite != 0 )); then
		echo "  -archiving all data"
		$RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log || error "executing $CMD --sync"
        [ "$DEBUG" = "1" ] && find $BKROOT -type f -ls
	fi

    $REPORT -f ./cfg/$config_file --dump-all -q > report.out || error "$REPORT"
    [ "$DEBUG" = "1" ] && cat report.out

    $FIND -f ./cfg/$config_file $ROOT -nobulk -ls > find.out || error "$FIND"
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
    count_path_init=$(grep -v "$ROOT$" find.out | wc -l)
    echo "nbr_inodes=$count_nb_init, nb_paths=$count_path_init, nb_ln=$nb_ln"
    (( $count_path_init == $count_nb_init + $nb_ln )) || error "nb path != nb_inode + nb_ln"

    # rename entries
    echo "3. Linking/unlinking/renaming objects..."
    # 1) simple file rename
    mv $ROOT/dir.1/file.1 $ROOT/dir.1/file.1.rnm
    # 2) cross directory file rename
    mv $ROOT/dir.1/file.2 $ROOT/dir.2/file.2.rnm
    # 3) rename that deletes the target
    mv -f $ROOT/dir.2/file.1 $ROOT/dir.2/file.2
    # 4) upper level directory rename
    mv $ROOT/dir.3/subdir $ROOT/dir.3/subdir.rnm
    # 5) overwritting a hardlink
    mv -f $ROOT/dir.2/file.4 ${hlinks[0]}
    ((nb_ln--))
    # 6) creating new link to "dir.4/file.1"
    ln "$ROOT/dir.4/file.1" "$ROOT/dir.4/link.1"
    ((nb_ln++))
    # 7) removing 1 link (dir.2/link.2 remains)
    rm "$ROOT/dir.4/file.2"
    ((nb_ln--))
    # 8) removing 1 file
    rm "$ROOT/dir.4/file.3"
    # 9) creating 1 file
    touch "$ROOT/dir.1/new"
    # 10) creating 1 file with hardlink
    touch "$ROOT/dir.2/new"
    ln "$ROOT/dir.2/new" "$ROOT/dir.4/link.new"
    ((nb_ln++))

    # namespace GC needs 1s difference
    sleep 1

    # readlog or re-scan
    if [ "$flavor" = "readlog" ]; then
        echo "4. Reading changelogs..."
    	$RH -f ./cfg/$config_file --readlog --once -l DEBUG -L rh_scan.log || error "reading changelog"
    elif [ "$flavor" = "scan" ]; then
        echo "4. Scanning again..."
    	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning"
    elif [ "$flavor" = "diff" ]; then
        echo "4. Diffing again..."
    	$DIFF -f ./cfg/$config_file --apply=db -l DEBUG > rh_scan.log 2>&1 || error "scanning"
    elif [ "$flavor" = "partial" ]; then
        i=0
        for d in $dirs_tgt; do
            # namespace GC needs 1s difference
            sleep 1
            ((i++))
            echo "4.$i Partial scan ($d)..."
        	$RH -f ./cfg/$config_file --scan=$d --once -l DEBUG -L rh_scan.log || error "scanning $d"
        done
    elif [ "$flavor" = "partdiff" ]; then
        i=0
        for d in $dirs_tgt; do
            # namespace GC needs 1s difference
            sleep 1
            ((i++))
            echo "4.$i Partial diff+apply ($d)..."
        	$DIFF -f ./cfg/$config_file --scan=$d --apply=db -l DEBUG  > rh_scan.log 2>&1 || error "scanning $d"
        done
    fi

    $REPORT -f ./cfg/$config_file --dump-all -q > report.out || error "$REPORT"
    [ "$DEBUG" = "1" ] && cat report.out

    $FIND -f ./cfg/$config_file $ROOT -nobulk -ls > find.out || error "$FIND"
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
        $REPORT -f ./cfg/$config_file --deferred-rm --csv -q > rh_report.log
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
    count_path_final=$(grep -v "$ROOT$" find.out | wc -l)

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
        mkdir $ROOT/dir.$d || error "cannot create $ROOT/dir.$d"
    for f in $(seq 1 $fcount); do
        touch $ROOT/dir.$d/file.$f || error "cannot create $ROOT/dir.$d/file.$f"
    done
    done

    # scan
   	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning $ROOT"

    ino=$(( $dcount * $fcount + $dcount ))
    ino_subdir=$(($fcount + 1))

    # reports to be checked:
    #   dump report (9 entries, no root)
    (($($REPORT -f ./cfg/$config_file -D -q | wc -l) == $ino )) || error "wrong count in 'rbh-report -D' output"
    #   dump report with path filter (3 entries)
    (($($REPORT -f ./cfg/$config_file -D -q -P $ROOT/dir.1 | wc -l) == $ino_subdir )) || error "wrong count in 'rbh-report -D -P <path>' output"
    #   dump find output (whole FS) (10 entries, incl. root)
    (($($FIND -f ./cfg/$config_file -nobulk | wc -l) == $ino + 1))  || error "wrong count in 'rbh-find' output"
    #   dump find output (subdir: 3 entries)
    (($($FIND -f ./cfg/$config_file $ROOT/dir.1 -nobulk | wc -l) == $ino_subdir )) || error "wrong count in 'rbh-find <path>' output"

    #   dump summary (9 entries)
    $REPORT -f ./cfg/$config_file -icq > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    typeValues="dir;file"
  	countValues="$dcount;$(($dcount * $fcount))"
    if (( $is_hsmlite + $is_lhsm != 0 )); then
        # type counts are in 3rd column (because of the status column)
   	    colSearch=3
    else
        # type counts are in 2nd column
   	    colSearch=2
    fi
	find_allValuesinCSVreport report.out $typeValues $countValues $colSearch || error "wrong count in 'rbh-report -i' output"

    #   dump summary with path filter (3 entries)
    $REPORT -f ./cfg/$config_file -iq -P $ROOT/dir.1 > report.out
    [ "$DEBUG" = "1" ] && cat report.out
  	countValues="1;$fcount"
	find_allValuesinCSVreport report.out $typeValues $countValues $colSearch || error "wrong count in 'rbh-report -i -P <path>' output"

    # create 1 hardlink per file and recheck
    for d in $(seq 1 $dcount); do
    for f in $(seq 1 $fcount); do
        ln $ROOT/dir.$d/file.$f $ROOT/dir.$d/link.$f || error "cannot create hardlink $ROOT/dir.$d/link.$f -> $ROOT/dir.$d/file.$f"
    done
    done

    # rescan
   	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning $ROOT"

    paths=$(( $dcount * $fcount * 2 + $dcount ))
    paths_subdir=$(($fcount * 2 + 1))

    #   dump report (still 9 entries, no root)
    (($($REPORT -f ./cfg/$config_file -D -q | wc -l) == $ino )) || error "wrong count in 'rbh-report -D' output"
    #   dump report with path filter (still 3 entries)
    (($($REPORT -f ./cfg/$config_file -D -q -P $ROOT/dir.1 | wc -l) == $ino_subdir )) || error "wrong count in 'rbh-report -D -P <path>' output"
    #   dump find output (whole FS) (
    (($($FIND -f ./cfg/$config_file -nobulk | wc -l) == $paths + 1 ))  || error "wrong count in 'rbh-find' output"
    #   dump find output (subdir: 3 entries)
    (($($FIND -f ./cfg/$config_file $ROOT/dir.1 -nobulk | wc -l) == $paths_subdir )) || error "wrong count in 'rbh-find <path>' output"

    #   dump summary (9 entries)
    $REPORT -f ./cfg/$config_file -icq > report.out
    [ "$DEBUG" = "1" ] && cat report.out
  	countValues="$dcount;$(($dcount * $fcount))"
	find_allValuesinCSVreport report.out $typeValues $countValues $colSearch || error "wrong count in 'rbh-report -i' output"

    #   dump summary with path filter (3 entries)
    $REPORT -f ./cfg/$config_file -iq -P $ROOT/dir.1 > report.out
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

	create_pools

	clean_logs

	# create files in different pools (or not)
	touch $ROOT/no_pool.1 || error "creating file"
	touch $ROOT/no_pool.2 || error "creating file"
	$LFS setstripe -p lustre.$POOL1 $ROOT/in_pool_1.a || error "creating file in $POOL1"
	$LFS setstripe -p lustre.$POOL1 $ROOT/in_pool_1.b || error "creating file in $POOL1"
	$LFS setstripe -p lustre.$POOL2 $ROOT/in_pool_2.a || error "creating file in $POOL2"
	$LFS setstripe -p lustre.$POOL2 $ROOT/in_pool_2.b || error "creating file in $POOL2"

	sleep $sleep_time

	# read changelogs
	if (( $no_log )); then
		echo "1.1-scan and match..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "1.1-read changelog and match..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi


	echo "1.2-checking report output..."
	# check classes in report output
	$REPORT -f ./cfg/$config_file --dump-all -c > report.out || error ""
	cat report.out

	echo "1.3-checking robinhood log..."
	grep "Missing attribute" rh_chglogs.log && error "missing attribute when matching classes"

	# purge field index
	if (( $is_lhsm != 0 )); then
		pf=7
	else
		pf=5
	fi

	# no_pool files must match default
	for i in 1 2; do
		(( $is_lhsm + $is_hsmlite != 0 )) &&  \
			( [ `grep "$ROOT/no_pool.$i" report.out | cut -d ',' -f 6 | tr -d ' '` = "[default]" ] || error "bad migr class for no_pool.$i" )
		 (( $is_hsmlite == 0 )) && \
			([ `grep "$ROOT/no_pool.$i" report.out | cut -d ',' -f $pf | tr -d ' '` = "[default]" ] || error "bad purg class for no_pool.$i")
	done

	for i in a b; do
		# in_pool_1 files must match pool_1
		(( $is_lhsm  + $is_hsmlite != 0 )) && \
			 ( [ `grep "$ROOT/in_pool_1.$i" report.out | cut -d ',' -f 6  | tr -d ' '` = "pool_1" ] || error "bad migr class for in_pool_1.$i" )
		(( $is_hsmlite == 0 )) && \
			([ `grep "$ROOT/in_pool_1.$i" report.out | cut -d ',' -f $pf | tr -d ' '` = "pool_1" ] || error "bad purg class for in_pool_1.$i")

		# in_pool_2 files must match pool_2
		(( $is_lhsm + $is_hsmlite != 0 )) && ( [ `grep "$ROOT/in_pool_2.$i" report.out  | cut -d ',' -f 6 | tr -d ' '` = "pool_2" ] || error "bad migr class for in_pool_2.$i" )
		(( $is_hsmlite == 0 )) && \
			([ `grep "$ROOT/in_pool_2.$i" report.out  | cut -d ',' -f $pf | tr -d ' '` = "pool_2" ] || error "bad purg class for in_pool_2.$i")
	done

	# rematch and recheck
	echo "2.1-scan and match..."
	# read changelogs
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""

	echo "2.2-checking report output..."
	# check classes in report output
	$REPORT -f ./cfg/$config_file --dump-all -c  > report.out || error ""
	cat report.out

	# no_pool files must match default
	for i in 1 2; do
		(( $is_lhsm + $is_hsmlite != 0 )) && ( [ `grep "$ROOT/no_pool.$i" report.out | cut -d ',' -f 6 | tr -d ' '` = "[default]" ] || error "bad migr class for no_pool.$i" )
		(( $is_hsmlite == 0 )) && \
			([ `grep "$ROOT/no_pool.$i" report.out | cut -d ',' -f $pf | tr -d ' '` = "[default]" ] || error "bad purg class for no_pool.$i")
	done

	for i in a b; do
		# in_pool_1 files must match pool_1
		(( $is_lhsm + $is_hsmlite != 0 )) &&  ( [ `grep "$ROOT/in_pool_1.$i" report.out | cut -d ',' -f 6  | tr -d ' '` = "pool_1" ] || error "bad migr class for in_pool_1.$i" )
		(( $is_hsmlite == 0 )) && \
			([ `grep "$ROOT/in_pool_1.$i" report.out | cut -d ',' -f $pf | tr -d ' '` = "pool_1" ] || error "bad purg class for in_pool_1.$i")

		# in_pool_2 files must match pool_2
		(( $is_lhsm + $is_hsmlite != 0 )) && ( [ `grep "$ROOT/in_pool_2.$i" report.out  | cut -d ',' -f 6 | tr -d ' '` = "pool_2" ] || error "bad migr class for in_pool_2.$i" )
		(( $is_hsmlite == 0 )) && \
			([ `grep "$ROOT/in_pool_2.$i" report.out  | cut -d ',' -f $pf | tr -d ' '` = "pool_2" ] || error "bad purg class for in_pool_2.$i")
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
	touch $ROOT/file.1 || error "creating file"
	touch $ROOT/file.2 || error "creating file"
	touch $ROOT/file.3 || error "creating file"
	touch $ROOT/file.4 || error "creating file"

	if (( $is_lhsm != 0 )); then
		flush_data
		$LFS hsm_archive $ROOT/file.*
		wait_done 60 || error "Copy timeout"
	fi

	if (( $syslog )); then
		init_msg_idx=`wc -l /var/log/messages | awk '{print $1}'`
	fi

    if (( $is_hsmlite != 0 )); then
        extra_action="--sync"
    else
        extra_action=""
    fi

	# run a scan
	if (( $stdio )); then
		$RH -f ./cfg/$config_file --scan $extra_action -l DEBUG --once >/tmp/rbh.stdout 2>/tmp/rbh.stderr || error "scan error"
	else
		$RH -f ./cfg/$config_file --scan -l DEBUG --once || error "scan error"
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
	if (( `wc -l $log | awk '{print $1}'` > 0 )); then
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
		a1=`egrep -e "[0-9]* alert_file1" $alert | sed -e 's/.* \([0-9]*\) alert_file1/\1/' | xargs`
		a2=`egrep -e "[0-9]* alert_file2" $alert | sed -e 's/.* \([0-9]*\) alert_file2/\1/' | xargs`
		e1=`grep ${ROOT}'/file\.1' $alert | wc -l`
		e2=`grep ${ROOT}'/file\.2' $alert | wc -l`
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
		a1=`grep alert_file1 $alert | wc -l`
		a2=`grep alert_file2 $alert | wc -l`
		e1=`grep 'Entry: '${ROOT}'/file\.1' $alert | wc -l`
		e2=`grep 'Entry: '${ROOT}'/file\.2' $alert | wc -l`
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
			$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG --dry-run >/tmp/rbh.stdout 2>/tmp/rbh.stderr || error ""
		else
			$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG --dry-run || error ""
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
		if (( `wc -l $log | awk '{print $1}'` > 0 )); then
			echo "OK: log file is not empty"
		else
			error ": empty log file"
		fi

		# check alerts (should be impossible to purge at 0%)
		grep "Could not purge" $alert > /dev/null
		if (($?)); then
			error ": alert should have been raised for impossible purge"
		else
			echo "OK: alert raised"
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
	$RH -f ./cfg/$config_file --scan -l DEBUG &

	# rotate the logs
	for l in /tmp/test_log.1 /tmp/test_report.1 /tmp/test_alert.1; do
		mv $l $l.old
	done

	sleep $sleep_time

	# check that there is something written in the log
	if (( `wc -l /tmp/test_log.1 | awk '{print $1}'` > 0 )); then
		echo "OK: log file is not empty"
	else
		error ": empty log file"
	fi

	# check alerts about file.1 and file.2
	a1=`grep alert_file1 /tmp/test_alert.1 | wc -l`
	a2=`grep alert_file2 /tmp/test_alert.1 | wc -l`
	e1=`grep 'Entry: '${ROOT}'/file\.1' /tmp/test_alert.1 | wc -l`
	e2=`grep 'Entry: '${ROOT}'/file\.2' /tmp/test_alert.1 | wc -l`
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

	pkill -9 $PROC
	rm -f /tmp/test_log.1 /tmp/test_report.1 /tmp/test_alert.1
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

	if [[ $flavor == "basic" ]]; then

		if (($is_hsmlite)) ; then
			TEMPLATE=$TEMPLATE_DIR"/hsmlite_basic.conf"
		elif (($is_lhsm)); then
			TEMPLATE=$TEMPLATE_DIR"/hsm_policy_basic.conf"
		else
			TEMPLATE=$TEMPLATE_DIR"/tmp_fs_mgr_basic.conf"
		fi

	elif [[ $flavor == "detailed" ]]; then

		if (($is_hsmlite)) ; then
			TEMPLATE=$TEMPLATE_DIR"/hsmlite_detailed.conf"
		elif (($is_lhsm)); then
			TEMPLATE=$TEMPLATE_DIR"/hsm_policy_detailed.conf"
		else
			TEMPLATE=$TEMPLATE_DIR"/tmp_fs_mgr_detailed.conf"
		fi

	elif [[ $flavor == "generated" ]]; then

		GEN_TEMPLATE="/tmp/template.$CMD"
		TEMPLATE=$GEN_TEMPLATE
		$RH --template=$TEMPLATE || error "generating config template"
	else
		error "invalid test flavor"
		return 1
	fi

	# test parsing
	$RH --test-syntax -f "$TEMPLATE" 2>rh_syntax.log >rh_syntax.log || error " reading config file \"$TEMPLATE\""

	cat rh_syntax.log
	grep "unknown parameter" rh_syntax.log > /dev/null && error "unexpected parameter"
	grep "read successfully" rh_syntax.log > /dev/null && echo "OK: parsing succeeded"

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
        mkdir "$ROOT/dir.$i" || error "$? creating directory $ROOT/dir.$i"
        if (( $i % 3 == 0 )); then
            chmod 755 "$ROOT/dir.$i" || error "$? setting mode of $ROOT/dir.$i"
        elif (( $i % 3 == 1 )); then
            chmod 750 "$ROOT/dir.$i" || error "$? setting mode of $ROOT/dir.$i"
        elif (( $i % 3 == 2 )); then
            chmod 700 "$ROOT/dir.$i" || error "$? setting mode of $ROOT/dir.$i"
        fi

        if (($i > $total - $total_empty)); then
            # last total_empty are empty...
            touch $ROOT/dir.$i/file.$i || error "$? creating $ROOT/file.$i"
        else
            dd if=/dev/zero of=$ROOT/dir.$i/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "$? writing $ROOT/file.$i"
        fi
    done

    echo "1.2-creating symlinks..."
    for i in `seq 1 $(( $total - $total_empty))`; do
        ln -s "symlink_$i" $ROOT/dir.$i/link.$i  >/dev/null 2>/dev/null || error "$? creating symlink $ROOT/dir.$i/link.$"
    done

    # read changelogs
    if (( $no_log )); then
        echo "1.3-scan..."
        $RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "scanning"
    else
        echo "1.3-read changelog..."
        $RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading log"
    fi

    sleep 2

    # all files are new
    new_cnt=`$REPORT -f ./cfg/$config_file -l MAJOR --csv -i | grep file | grep new | cut -d ',' -f 3`
    echo "$new_cnt files are new"
    (( $new_cnt == $total )) || error "20 new files expected"

    na_link=`$REPORT -f ./cfg/$config_file -l MAJOR --csv -i | grep symlink | grep "n/a" | cut -d ',' -f 3`
    new_link=`$REPORT -f ./cfg/$config_file -l MAJOR --csv -i | grep symlink | grep new | cut -d ',' -f 3`
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
            $RH -f ./cfg/$config_file --migrate-file "$ROOT/dir.$i/file.$i" --ignore-policies -l DEBUG -L rh_migr.log 2>/dev/null \
                || error "archiving $ROOT/dir.$i/file.$i"
            $RH -f ./cfg/$config_file --migrate-file "$ROOT/dir.$i/link.$i" --ignore-policies -l DEBUG -L rh_migr.log 2>/dev/null \
                || error "archiving $ROOT/dir.$i/link.$i"
            if (( $arch_slink == 0 )); then
                grep "$ROOT/dir.$i/link.$i" rh_migr.log | grep "bad type for migration" > /dev/null 2> /dev/null \
                    || error "$ROOT/dir.$i/link.$i should not have been migrated"
            fi
        elif (( $i <= $(($nb_full+$nb_rename)) )); then
            $RH -f ./cfg/$config_file --migrate-file "$ROOT/dir.$i/file.$i" --ignore-policies -l DEBUG -L rh_migr.log 2>/dev/null \
                || error "archiving $ROOT/dir.$i/file.$i"
            $RH -f ./cfg/$config_file --migrate-file "$ROOT/dir.$i/link.$i" --ignore-policies -l DEBUG -L rh_migr.log 2>/dev/null \
                || error "archiving $ROOT/dir.$i/link.$i"
            if (( $arch_slink == 0 )); then
                grep "$ROOT/dir.$i/link.$i" rh_migr.log | grep "bad type for migration" > /dev/null 2> /dev/null \
                    || error "$ROOT/dir.$i/link.$i should not have been migrated"
            fi
            mv "$ROOT/dir.$i/file.$i" "$ROOT/dir.$i/file_new.$i" || error "renaming file"
            mv "$ROOT/dir.$i/link.$i" "$ROOT/dir.$i/link_new.$i" || error "renaming link"
            mv "$ROOT/dir.$i" "$ROOT/dir.new_$i" || error "renaming dir"
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta)) )); then
            $RH -f ./cfg/$config_file --migrate-file "$ROOT/dir.$i/file.$i" --ignore-policies -l DEBUG -L rh_migr.log 2>/dev/null \
                || error "archiving $ROOT/dir.$i/file.$i"
            touch "$ROOT/dir.$i/file.$i"
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp)) )); then
            # no backup
            :
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp+$nb_empty)) )); then
            # no backup
            :
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp+$nb_empty+$nb_empty_rename)) )); then
            # no backup, just rename
            mv "$ROOT/dir.$i/file.$i" "$ROOT/dir.$i/file_new.$i" || error "renaming file"
            mv "$ROOT/dir.$i" "$ROOT/dir.new_$i" || error "renaming dir"
        fi
    done

    if (( $no_log )); then
        echo "2.2-scan..."
        $RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "scanning"
    else
        echo "2.2-read changelog..."
        $RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading log"
    fi

    $REPORT -f ./cfg/$config_file -l MAJOR --csv -i > /tmp/report.$$
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
    find $ROOT -type f -printf "%n %m %T@ %g %u %s %p %l\n" > /tmp/before.$$
    find $ROOT -type d -printf "%n %m %g %u %s %p %l\n" >> /tmp/before.$$
    find $ROOT -type l -printf "%n %m %g %u %s %p %l\n" >> /tmp/before.$$

    # FS disaster
    if [[ -n "$ROOT" ]]; then
        echo "3-Disaster: all FS content is lost"
        rm  -rf $ROOT/*
    fi

    # perform the recovery
    echo "4-Performing recovery..."
    cp /dev/null recov.log
    $RECOV -f ./cfg/$config_file --start -l DEBUG >> recov.log 2>&1 || error "Error starting recovery"

    $RECOV -f ./cfg/$config_file --resume -l DEBUG >> recov.log 2>&1 || error "Error performing recovery"

    $RECOV -f ./cfg/$config_file --complete -l DEBUG >> recov.log 2>&1 || error "Error completing recovery"

    find $ROOT -type f -printf "%n %m %T@ %g %u %s %p %l\n" > /tmp/after.$$
    find $ROOT -type d -printf "%n %m %g %u %s %p %l\n" >> /tmp/after.$$
    find $ROOT -type l -printf "%n %m %g %u %s %p %l\n" >> /tmp/after.$$

    diff  /tmp/before.$$ /tmp/after.$$ > /tmp/diff.$$
    [ "$DEBUG" = "1" ] && cat  /tmp/diff.$$

    # checking status and diff result
    for i in `seq 1 $total`; do
        if (( $i <= $nb_full )); then
            check_recov_status recov.log "$ROOT/dir.$i/file.$i" "OK\$"
            grep "$ROOT/dir.$i/file.$i" /tmp/diff.$$ && error "$ROOT/dir.$i/file.$i NOT expected to differ"
            check_recov_status recov.log "$ROOT/dir.$i/link.$i" "OK \(non-file\)"
        elif (( $i <= $(($nb_full+$nb_rename)) )); then
            check_recov_status recov.log "$ROOT/dir.new_$i/file_new.$i" "OK\$"
            grep "$ROOT/dir.new$i/link_new.$i" /tmp/diff.$$ && error "$ROOT/dir_new.$i/link_new.$i NOT expected to differ"
            check_recov_status recov.log "$ROOT/dir.new_$i/link_new.$i" "OK \(non-file\)"
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta)) )); then
            check_recov_status recov.log "$ROOT/dir.$i/file.$i" "OK \(old version\)"
            grep "$ROOT/dir.$i/file.$i" /tmp/diff.$$ >/dev/null || error "$ROOT/dir.$i/file.$i is expected to differ"
            # links are never expected to differ as they are stored in the database
            grep "$ROOT/dir.$i/link.$i" /tmp/diff.$$ >/dev/null && error "$ROOT/dir.$i/link.$i NOT expected to differ"
            check_recov_status recov.log "$ROOT/dir.$i/link.$i" "OK \(non-file\)"
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp)) )); then
            check_recov_status recov.log "$ROOT/dir.$i/file.$i" "No backup"
            grep "$ROOT/dir.$i/file.$i" /tmp/diff.$$ >/dev/null || error "$ROOT/dir.$i/file.$i is expected to differ"
            # links are never expected to differ as they are stored in the database
            grep "$ROOT/dir.$i/link.$i" /tmp/diff.$$ >/dev/null && error "$ROOT/dir.$i/link.$i NOT expected to differ"
            check_recov_status recov.log "$ROOT/dir.$i/link.$i" "OK \(non-file\)"
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp+$nb_empty)) )); then
            check_recov_status recov.log "$ROOT/dir.$i/file.$i" "OK \(empty file\)"
            grep "$ROOT/dir.$i/file.$i" /tmp/diff.$$ >/dev/null && error "$ROOT/dir.$i/file.$i is NOT expected to differ"
        elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp+$nb_empty+$nb_empty_rename)) )); then
            check_recov_status recov.log "$ROOT/dir.new_$i/file_new.$i" "OK \(empty file\)"
            grep "$ROOT/dir.new_$i/file_new.$i" /tmp/diff.$$ >/dev/null && error "$ROOT/dir.$i/file.$i is NOT expected to differ"
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
    mkdir $ROOT/dir.match $ROOT/dir.nomatch || error "mkdir failed"

    for f in full delta rename empty empty_rnm; do
        if [[ $flavor != since ]]; then
            $LFS setstripe -c 1 -i 0 $ROOT/dir.match/$f || error "setstripe failed"
        fi
        $LFS setstripe -c 1 -i 1 $ROOT/dir.nomatch/$f || error "setstripe failed"
    done
    # write data to full and delta
    for f in full delta rename; do
        if [[ $flavor != since ]]; then
            dd if=/dev/zero of=$ROOT/dir.match/$f bs=1M count=5  || error "writing data to $f"
        fi
        dd if=/dev/zero of=$ROOT/dir.nomatch/$f bs=1M count=5  || error "writing data to $f"
    done
    ln -s "this is an initial symlink" $ROOT/dir.nomatch/slink || error "creating symlink"
    if [[ $flavor != ost ]] && [[ $flavor != since ]]; then
        ln -s "this is an initial symlink" $ROOT/dir.match/slink || error "creating symlink slink_new"
    fi

    echo "scan and archive"
    # scan and archive
    $RH -f ./cfg/$config_file --scan --sync -l DEBUG -L rh_scan.log  --once 2>/dev/null || error "scanning or migrating"

    if [[ $flavor == since ]]; then
	    $LFS changelog_clear lustre-MDT0000 cl1 0
        sleep 1
        # only consider entries modifed from now
        since=$(date +'%Y%m%d%H%M%S')

        for f in full delta rename empty empty_rnm; do
            $LFS setstripe -c 1 -i 0 $ROOT/dir.match/$f || error "setstripe failed"
        done
        # write data to full and delta
        for f in full delta rename; do
            dd if=/dev/zero of=$ROOT/dir.match/$f bs=1M count=5  || error "writing data to $f"
        done
        ln -s "this is an initial symlink" $ROOT/dir.match/slink || error "creating symlink slink_new"

        # don't update non-modified objects, migrate other candidates
        $RH -f ./cfg/$config_file --readlog --sync -l DEBUG -L rh_scan.log  --once 2>/dev/null || error "reading changelogs"
    fi

    echo "making deltas"
    for f in empty_new nobkp; do
        $LFS setstripe -c 1 -i 0 $ROOT/dir.match/$f
        [[ $flavor != since ]] && $LFS setstripe -c 1 -i 1 $ROOT/dir.nomatch/$f
    done
    for d in match nomatch ; do
        # skip no match if flavor is 'since'
        [[ $flavor == since ]] && [[ $d == nomatch ]] && continue
        echo "sqdlqsldsqmdl" >> $ROOT/dir.$d/delta || error "appending dir.$d/delta"
        # force modification (in case Lustre don't report small data changes)
        touch $ROOT/dir.$d/delta || error "touching dir.$d/delta"
        echo "qsldjkqlsdkqs" >> $ROOT/dir.$d/nobkp || error "writting to dir.$d/nobkp"
        mv $ROOT/dir.$d/rename $ROOT/dir.$d/rename.mv || error "renaming 'rename'"
        mv $ROOT/dir.$d/empty_rnm $ROOT/dir.$d/empty_rnm.mv || error "renaming 'empty_rnm'"
    done
    if [[ $flavor != since ]]; then
        ln -s "this is a new symlink" $ROOT/dir.nomatch/slink_new || error "creating symlink"
    fi
    if [[ $flavor != ost ]]; then
        ln -s "this is a new symlink" $ROOT/dir.match/slink_new || error "creating symlink"
    fi

    if [[ $flavor == since ]]; then
        [ "$DEBUG" = "1" ] && $LFS changelog lustre
        # don't update non-modified objects
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_scan.log  --once 2>/dev/null || error "reading changelogs"
    else
        echo "rescan (no archive)"
        $RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once 2>/dev/null || error "scanning"
    fi

    $REPORT -f ./cfg/$config_file -l MAJOR --csv -i > report.out
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
    if [[ -n "$ROOT" ]]; then
        echo "3-Disaster: all FS content is lost"
        rm  -rf $ROOT/*
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
            resume_option="--dir=$ROOT/dir.match"
            matching=(full delta empty empty_rnm.mv empty_new rename.mv nobkp slink slink_new)
            status=("OK" "OK \(old version\)" "OK" "OK" "OK \(empty file\)" "OK" "No backup" "OK \(non-file \)" "OK \(non-file \)")
            ;;
    esac

    $RECOV -f ./cfg/$config_file --start $start_option -l FULL >> recov.log 2>&1 || error "Error starting recovery"
    $RECOV -f ./cfg/$config_file --resume $resume_option -l DEBUG >> recov.log 2>&1 || error "Error performing recovery"
    if [[ $flavor != dir ]]; then # for dirs, cannot complete as long as it is only for parallelizing the recovery
        $RECOV -f ./cfg/$config_file --complete -l DEBUG >> recov.log 2>&1 || error "Error completing recovery"
    fi

    # check that all matching entries are recovered with the appropriate status
    [ "$DEBUG" = "1" ] && grep Restoring recov.log
    for i in $(seq 1 ${#matching[@]}); do
        f=${matching[$i]}
        s=${status[$i]}
        check_recov_status recov.log $ROOT/dir.match/$f $s
    done
    (( $(grep Restoring recov.log | wc -l) == ${#matching[@]} )) || error "Too many files restored"

    (( $NB_ERROR == 0 )) && echo OK
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
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log 2>/dev/null || error "scanning"


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
    echo "2- import to $ROOT..."
    $IMPORT -l DEBUG -f ./cfg/$config_file $BKROOT/import  $ROOT/dest > recov.log 2>&1 || error "importing data from backend"

    [ "$DEBUG" = "1" ] && cat recov.log

    # "Import summary: 9 entries imported, 0 errors"
    info=$(grep "Import summary: " recov.log | awk '{print $3"-"$6}')
    [ "$info" = "$expect_cnt-0" ] || error "unexpected count of imported entries or errors: expected $expect_cnt-0, got $info"

    rm -f recov.log

    # check that every dir has been imported to Lustre
    echo "3.1-checking dirs..."
    while read i m u g s t p; do
        newp=$(echo $p | sed -e "s#$BKROOT/import#$ROOT/dest#")
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
        newp=$(echo $p | sed -e "s#$BKROOT/import#$ROOT/dest#")
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
        newp=$(echo $p | sed -e "s#$BKROOT/import#$ROOT/dest#")
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
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "scanning"
	else
		echo "1.2-read changelog..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading log"
	fi

	sleep 2

	# all files are new
	new_cnt=`$REPORT -f ./cfg/$config_file -l MAJOR --csv -i | grep new | cut -d ',' -f 3`
	echo "$new_cnt files are new"
	(( $new_cnt == $total )) || error "20 new files expected"

	echo "2.1-archiving files..."
	# archive and modify files
	for i in `seq 1 $total`; do
		if (( $i <= $nb_full )); then
			$RH -f ./cfg/$config_file --migrate-file "$ROOT/dir.$i/file.$i" --ignore-policies -l DEBUG -L rh_migr.log 2>/dev/null \
				|| error "archiving $ROOT/dir.$i/file.$i"
		elif (( $i <= $(($nb_full+$nb_rename)) )); then
			$RH -f ./cfg/$config_file --migrate-file "$ROOT/dir.$i/file.$i" --ignore-policies -l DEBUG -L rh_migr.log 2>/dev/null \
				|| error "archiving $ROOT/dir.$i/file.$i"
			mv "$ROOT/dir.$i/file.$i" "$ROOT/dir.$i/file_new.$i" || error "renaming file"
			mv "$ROOT/dir.$i" "$ROOT/dir.new_$i" || error "renaming dir"
		elif (( $i <= $(($nb_full+$nb_rename+$nb_delta)) )); then
			$RH -f ./cfg/$config_file --migrate-file "$ROOT/dir.$i/file.$i" --ignore-policies -l DEBUG -L rh_migr.log 2>/dev/null \
				|| error "archiving $ROOT/dir.$i/file.$i"
			touch "$ROOT/dir.$i/file.$i"
		elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp)) )); then
			# no backup
			:
		fi
	done

	if (( $no_log )); then
		echo "2.2-scan..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "scanning"
	else
		echo "2.2-read changelog..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once 2>/dev/null || error "reading log"
	fi

	$REPORT -f ./cfg/$config_file -l MAJOR --csv -i > /tmp/report.$$
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
	find $ROOT -type f -printf "%n %m %T@ %g %u %s %p %l\n" > /tmp/before.$$
	find $ROOT -type d -printf "%n %m %g %u %s %p %l\n" >> /tmp/before.$$
	find $ROOT -type l -printf "%n %m %g %u %s %p %l\n" >> /tmp/before.$$

	# FS disaster
	if [[ -n "$ROOT" ]]; then
		echo "3-Disaster: all FS content is lost"
		rm  -rf $ROOT/*
	fi

	# perform the recovery
	echo "4-Performing recovery..."
	cp /dev/null recov.log
	$RECOV -f ./cfg/$config_file --start -l DEBUG >> recov.log 2>&1 || error "Error starting recovery"

	$RECOV -f ./cfg/$config_file --resume -l DEBUG >> recov.log 2>&1 || error "Error performing recovery"

	$RECOV -f ./cfg/$config_file --complete -l DEBUG >> recov.log 2>&1 || error "Error completing recovery"

	find $ROOT -type f -printf "%n %m %T@ %g %u %s %p %l\n" > /tmp/after.$$
	find $ROOT -type d -printf "%n %m %g %u %s %p %l\n" >> /tmp/after.$$
	find $ROOT -type l -printf "%n %m %g %u %s %p %l\n" >> /tmp/after.$$

	diff  /tmp/before.$$ /tmp/after.$$ > /tmp/diff.$$

	# checking status and diff result
	for i in `seq 1 $total`; do
		if (( $i <= $nb_full )); then
			grep "Restoring $ROOT/dir.$i/file.$i" recov.log | egrep -e "OK\$" >/dev/null || error "Bad status (OK expected)"
			grep "$ROOT/dir.$i/file.$i" /tmp/diff.$$ && error "$ROOT/dir.$i/file.$i NOT expected to differ"
		elif (( $i <= $(($nb_full+$nb_rename)) )); then
			grep "Restoring $ROOT/dir.new_$i/file_new.$i" recov.log	| egrep -e "OK\$" >/dev/null || error "Bad status (OK expected)"
			grep "$ROOT/dir.new_$i/file_new.$i" /tmp/diff.$$ && error "$ROOT/dir.new_$i/file_new.$i NOT expected to differ"
		elif (( $i <= $(($nb_full+$nb_rename+$nb_delta)) )); then
			grep "Restoring $ROOT/dir.$i/file.$i" recov.log	| grep "OK (old version)" >/dev/null || error "Bad status (old version expected)"
			grep "$ROOT/dir.$i/file.$i" /tmp/diff.$$ >/dev/null || error "$ROOT/dir.$i/file.$i is expected to differ"
		elif (( $i <= $(($nb_full+$nb_rename+$nb_delta+$nb_nobkp)) )); then
			grep -A 1 "Restoring $ROOT/dir.$i/file.$i" recov.log | grep "No backup" >/dev/null || error "Bad status (no backup expected)"
			grep "$ROOT/dir.$i/file.$i" /tmp/diff.$$ >/dev/null || error "$ROOT/dir.$i/file.$i is expected to differ"
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

    # same test with '-l option'
    c=`$FIND $args $dir -ls | wc -l`
    (( $c == $count )) || error "find -ls: $count entries expected in $dir, got: $c"
}

function test_find
{
	cfg=./cfg/$1
	opt=$2
	policy_str="$3"

	clean_logs

    # by default stripe all files on 0 and 1
	$LFS setstripe -c 2 -i 0 $ROOT || echo "error setting stripe on root"
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
    touch $ROOT/file.1 || error "creating file"
    touch $ROOT/file.2 || error "creating file"
    mkdir $ROOT/dir.1 || error "creating dir"
    mkdir $ROOT/dir.2 || error "creating dir"
    dd if=/dev/zero of=$ROOT/dir.2/file.1 bs=1k count=10 2>/dev/null || error "creating file"
	$LFS setstripe -c 1 -i 1 $ROOT/dir.2/file.2 || error "creating file with stripe"
    mkdir $ROOT/dir.2/dir.1 || error "creating dir"
    mkdir $ROOT/dir.2/dir.2 || error "creating dir"
    dd if=/dev/zero of=$ROOT/dir.2/dir.2/file.1 bs=1M count=1 2>/dev/null || error "creating file"
	$LFS setstripe -c 1 -i 0 $ROOT/dir.2/dir.2/file.2 || error "creating file with stripe"
    mkdir $ROOT/dir.2/dir.2/dir.1 || error "creating dir"

    # scan FS content
    $RH -f $cfg --scan -l DEBUG -L rh_scan.log --once 2>/dev/null || error "scanning"

    # 2) test find at several levels
    echo "checking find list at all levels..."
    check_find "" "-f $cfg" 12 # should return all (including root)
    check_find "" "-f $cfg -b" 12 # should return all (including root)
    check_find $ROOT "-f $cfg" 12 # should return all (including root)
    check_find $ROOT/file.1 "-f $cfg" 1 # should return only the file
    check_find $ROOT/dir.1 "-f $cfg" 1  # should return dir.1
    check_find $ROOT/dir.2 "-f $cfg" 8  # should return dir.2 + its content
    check_find $ROOT/dir.2/file.2 "-f $cfg" 1  # should return dir.2/file.2
    check_find $ROOT/dir.2/dir.1 "-f $cfg" 1  # should return dir2/dir.1
    check_find $ROOT/dir.2/dir.2 "-f $cfg" 4  # should return dir.2/dir.2 + its content
    check_find $ROOT/dir.2/dir.2/file.1 "-f $cfg" 1  # should return dir.2/dir.2/file.1
    check_find $ROOT/dir.2/dir.2/dir.1 "-f $cfg" 1 # should return dir.2/dir.2/dir.1

    # 3) test -td / -tf
    echo "testing type filter (-type d)..."
    check_find "" "-f $cfg -type d" 6 # should return all (including root)
    check_find "" "-f $cfg -type d -b" 6 # should return all (including root)
    check_find $ROOT "-f $cfg -type d" 6 # 6 including root
    check_find $ROOT/dir.2 "-f $cfg -type d" 4 # 4 including dir.2
    check_find $ROOT/dir.2/dir.2 "-f $cfg -type d" 2 # 2 including dir.2/dir.2
    check_find $ROOT/dir.1 "-f $cfg -type d" 1
    check_find $ROOT/dir.2/dir.1 "-f $cfg -type d" 1
    check_find $ROOT/dir.2/dir.2/dir.1 "-f $cfg -type d" 1

    echo "testing type filter (-type f)..."
    check_find "" "-f $cfg -type f" 6
    check_find "" "-f $cfg -type f -b" 6
    check_find $ROOT "-f $cfg -type f" 6
    check_find $ROOT/dir.2 "-f $cfg -type f" 4
    check_find $ROOT/dir.2/dir.2 "-f $cfg -type f" 2
    check_find $ROOT/dir.1 "-f $cfg -type f" 0
    check_find $ROOT/dir.2/dir.1 "-f $cfg -type f" 0
    check_find $ROOT/dir.2/dir.2/dir.1 "-f $cfg -type f" 0
    check_find $ROOT/file.1 "-f $cfg -type f" 1
    check_find $ROOT/dir.2/file.1 "-f $cfg -type f" 1

    echo "testing name filter..."
    check_find "" "-f $cfg -name dir.*" 5 # 5
    check_find "" "-f $cfg -name dir.* -b" 5 # 5
    check_find $ROOT "-f $cfg -name dir.*" 5 # 5
    check_find $ROOT/dir.2 "-f $cfg -name dir.*" 4 # 4 including dir.2
    check_find $ROOT/dir.2/dir.2 "-f $cfg -name dir.*" 2 # 2 including dir.2/dir.2
    check_find $ROOT/dir.1 "-f $cfg -name dir.*" 1
    check_find $ROOT/dir.2/dir.1 "-f $cfg -name dir.*" 1
    check_find $ROOT/dir.2/dir.2/dir.1 "-f $cfg -name dir.*" 1

    echo "testing size filter..."
    check_find "" "-f $cfg -type f -size +2k" 2
    check_find "" "-f $cfg -type f -size +2k -b" 2
    check_find $ROOT "-f $cfg -type f -size +2k" 2
    check_find $ROOT "-f $cfg -type f -size +11k" 1
    check_find $ROOT "-f $cfg -type f -size +1M" 0
    check_find $ROOT "-f $cfg -type f -size 1M" 1
    check_find $ROOT "-f $cfg -type f -size 10k" 1
    check_find $ROOT "-f $cfg -type f -size -1M" 5
    check_find $ROOT "-f $cfg -type f -size -10k" 4

    echo "testing ost filter..."
    check_find "" "-f $cfg -ost 0" 5 # all files but 1
    check_find "" "-f $cfg -ost 0 -b" 5 # all files but 1
    check_find $ROOT "-f $cfg -ost 0" 5 # all files but 1
    check_find $ROOT "-f $cfg -ost 1" 5 # all files but 1
    check_find $ROOT/dir.2/dir.2 "-f $cfg -ost 1" 1  # all files in dir.2 but 1

    echo "testing mtime filter..."
    check_find "" "-f $cfg -mtime +1d" 0  #none
    check_find "" "-f $cfg -mtime -1d" 12 #all
    check_find "" "-f $cfg -mtime +1d -b" 0  #none
    check_find "" "-f $cfg -mtime -1d -b" 12 #all
    # change last day
    check_find $ROOT "-f $cfg -mtime +1d" 0  #none
    check_find $ROOT "-f $cfg -mtime -1d" 12 #all
    # the same with another syntax
    check_find $ROOT "-f $cfg -mtime +1" 0  #none
    check_find $ROOT "-f $cfg -mtime -1" 12 #all
    # without 2 hour
    check_find $ROOT "-f $cfg -mtime +2h" 0  #none
    check_find $ROOT "-f $cfg -mtime -2h" 12 #all
    # the same with another syntax
    check_find $ROOT "-f $cfg -mtime +120m" 0  #none
    check_find $ROOT "-f $cfg -mtime -120m" 12 #all
    # the same with another syntax
    check_find $ROOT "-f $cfg -mmin +120" 0  #none
    check_find $ROOT "-f $cfg -mmin -120" 12 #all

    # restore default striping
    $LFS setstripe -c 2 -i -1 $ROOT
}

function test_du
{
	cfg=./cfg/$1
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
    dd if=/dev/zero of=$ROOT/file.1 bs=1M count=1 2>/dev/null || error "creating file"
    dd if=/dev/zero of=$ROOT/file.2 bs=1k count=1 2>/dev/null || error "creating file"

    mkdir $ROOT/dir.1 || error "creating dir"
    dd if=/dev/zero of=$ROOT/dir.1/file.1 bs=1k count=2 2>/dev/null || error "creating file"
    dd if=/dev/zero of=$ROOT/dir.1/file.2 bs=10k count=1 2>/dev/null || error "creating file"
    ln -s "content1" $ROOT/dir.1/link.1 || error "creating symlink"

    mkdir $ROOT/dir.2 || error "creating dir"
    dd if=/dev/zero of=$ROOT/dir.2/file.1 bs=1M count=1 2>/dev/null || error "creating file"
	dd if=/dev/zero of=$ROOT/dir.2/file.2 bs=1 count=1 2>/dev/null || error "creating file"
    ln -s "content2" $ROOT/dir.2/link.1 || error "creating symlink"
    mkdir $ROOT/dir.2/dir.1 || error "creating dir"
    mkdir $ROOT/dir.2/dir.2 || error "creating dir"
    touch $ROOT/dir.2/dir.2/file.1 || error "creating file"
	touch $ROOT/dir.2/dir.2/file.2 || error "creating file"
    mkdir $ROOT/dir.2/dir.2/dir.1 || error "creating dir"

    # write blocks to disk
    sync

    # scan FS content
    $RH -f $cfg --scan -l DEBUG -L rh_scan.log --once 2>/dev/null || error "scanning"

    # test byte display on root
    size=$($DU -f $cfg -t f -b $ROOT | awk '{print $1}')
    [ $size = "2110465" ] || error "bad returned size $size: 2110465 expected"

    # test on subdirs
    size=$($DU -f $cfg -t f -b $ROOT/dir.1 | awk '{print $1}')
    [ $size = "12288" ] || error "bad returned size $size: 12288 expected"

    # block count is hard to predict (due to ext3 prealloc)
    # only test 1st digit
    kb=$($DU -f $cfg -t f -k $ROOT | awk '{print $1}')
    [[ $kb = 2??? ]] || error "nb 1K block should be about 2k+smthg (got $kb)"

    # 2 (for 2MB) + 1 for small files
    mb=$($DU -f $cfg -t f -m $ROOT | awk '{print $1}')
    [[ $mb = 3 ]] || error "nb 1M block should be 3 (got $mb)"

    # count are real
    nb_file=$($DU -f $cfg -t f -c $ROOT | awk '{print $1}')
    nb_link=$($DU -f $cfg -t l -c $ROOT | awk '{print $1}')
    nb_dir=$($DU -f $cfg -t d -c $ROOT | awk '{print $1}')
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
		       if (( ($is_hsmlite != 0) && ($shook == 0) )); then
			       echo "No purge for backup purpose: skipped"
                               set_skipped
                               return 1
                       fi
                       cmd='--purge'
                       match='Resource Monitor is disabled'
                       ;;
               migration)
                       if (( $is_hsmlite + $is_lhsm == 0 )); then
                               echo "hsmlite or HSM test only: skipped"
                               set_skipped
                               return 1
                       fi
                       cmd='--migrate'
                       match='Migration module is disabled'
                       ;;
               hsm_remove)
                       if (( $is_hsmlite + $is_lhsm == 0 )); then
                               echo "hsmlite or HSM test only: skipped"
                               set_skipped
                               return 1
                       fi
                       cmd='--hsm-remove'
                       match='HSM removal successfully initialized' # enabled by default
                       ;;
               rmdir)
                       if (( $is_hsmlite + $is_lhsm != 0 )); then
                               echo "No rmdir policy for hsmlite or HSM purpose: skipped"
                               set_skipped
                               return 1
                       fi
                       cmd='--rmdir'
                       match='Directory removal is disabled'
                       ;;
               class)
                       cmd='--scan'
                       match='disabling file class matching'
                       ;;
               *)
                       error "unexpected flavor $flavor"
                       return 1 ;;
       esac

       echo "1.1. Performing action $cmd (daemon mode)..."
        $RH -f ./cfg/$config_file $cmd -l DEBUG -L rh_scan.log -p rh.pid &

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
        $RH -f ./cfg/$config_file $cmd --once -l DEBUG -L rh_scan.log

       grep "$match" rh_scan.log || error "log should contain \"$match\""

}



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
#	echo "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" > $XML
	echo "<?xml version=\"1.0\" encoding=\"ISO8859-2\" ?>" > $XML
	echo "<testsuite name=\"robinhood.LustreTests\" errors=\"0\" failures=\"$failure\" tests=\"$tests\" time=\"$time\">" >> $XML
	cat $TMPXML_PREFIX.tc 		>> $XML
	echo -n "<system-out><![CDATA[" >> $XML
	cat $TMPXML_PREFIX.stdout 	>> $XML
	echo "]]></system-out>"		>> $XML
	echo -n "<system-err><![CDATA[" >> $XML
	cat $TMPXML_PREFIX.stderr 	>> $XML
	echo "]]></system-err>" 	>> $XML
	echo "</testsuite>"		>> $XML
}


function cleanup
{
	echo "cleanup..."
        if (( $quiet == 1 )); then
                clean_fs | tee "rh_test.log" | egrep -i -e "OK|ERR|Fail|skip|pass"
        else
                clean_fs
        fi
}

function run_test
{
	index=$1
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
			grep "Failed" $CLEAN 2>/dev/null
			echo "TEST #$index : *FAILED*" >> $SUMMARY
			RC=$(($RC+1))
			if (( $junit )); then
				junit_report_failure "robinhood.$PURPOSE.Lustre" "Test #$index: $title" "$dur" "ERROR"
			fi
		else
			grep "Failed" $CLEAN 2>/dev/null
			echo "TEST #$index : OK" >> $SUMMARY
			SUCCES=$(($SUCCES+1))
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
	if [ $testKey == "extAttributes" ]; then
		echo " is for extended attributes"
		echo "data" > $ROOT/file.1
		echo "data" > $ROOT/file.2
		echo "data" > $ROOT/file.3
		echo "data" > $ROOT/file.4
		setfattr -n user.foo -v "abc.1.log" $ROOT/file.1
		setfattr -n user.foo -v "abc.6.log" $ROOT/file.3
		setfattr -n user.bar -v "abc.3.log" $ROOT/file.4
	else
		mkdir -p $ROOT/dir1
		dd if=/dev/zero of=$ROOT/dir1/file.1 bs=1k count=11 >/dev/null 2>/dev/null || error "writing file.1"

		mkdir -p $ROOT/dir2
		dd if=/dev/zero of=$ROOT/dir2/file.2 bs=1k count=10 >/dev/null 2>/dev/null || error "writing file.2"
  		chown testuser $ROOT/dir2/file.2 || error "invalid chown on user 'testuser' for $ROOT/dir2/file.2"
		dd if=/dev/zero of=$ROOT/dir2/file.3 bs=1k count=1 >/dev/null 2>/dev/null || error "writing file.3"
		ln -s $ROOT/dir1/file.1 $ROOT/dir1/link.1 || error "creating hardlink $ROOT/dir1/link.1"

		if  [ $testKey == "dircount" ]; then
			# add a folder with one file
			mkdir -p $ROOT/dir3
		    dd if=/dev/zero of=$ROOT/dir3/file.4 bs=1k count=1 >/dev/null 2>/dev/null || error "writing file.4"
		fi
	fi
	# optional sleep process ......................
	if [ $sleepTime != 0 ]; then
		echo "wait $sleepTime seconds ..."
		sleep $sleepTime
	fi
	# specific optional action after sleep process ..........
	if [ $testKey == "lastAccess" ]; then
		head $ROOT/dir1/file.1 > /dev/null || error "opening $ROOT/dir1/file.1"
	elif [ $testKey == "lastModif" ]; then
		echo "data" > $ROOT/dir1/file.1 || error "writing in $ROOT/dir1/file.1"
	fi

	echo "2-Scanning filesystem..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "performing FS scan"

	echo "3-Checking results..."
	logFile=/tmp/rh_alert.log
	case "$testKey" in
		pathName)
			alertKey=Alert_Name
			expectedEntry="file.1 "
			occur=1
			;;
		type)
			alertKey=Alert_Type
			expectedEntry="file.1;file.2;file.3"
			occur=3
			;;
		owner)
			alertKey=Alert_Owner
			expectedEntry="file.1;file.3"
			occur=2
			;;
		size)
			alertKey=Alert_Size
			expectedEntry="file.1;file.2"
			occur=2
			;;
		lastAccess)
			alertKey=Alert_LastAccess
			expectedEntry="file.1 "
			occur=1
			;;
		lastModif)
			alertKey=Alert_LastModif
			expectedEntry="file.1 "
			occur=1
			;;
		dircount)
			alertKey=Alert_Dircount
			expectedEntry="dir1;dir2"
			occur=2
			;;
		extAttributes)
			alertKey=Alert_ExtendedAttribut
			expectedEntry="file.1"
			occur=1
			;;
		*)
			error "unexpected testKey $testKey"
			return 1 ;;
	esac

	# launch the validation for all alerts
	check_alert $alertKey $expectedEntry $occur $logFile
	res=$?

	if (( $res == 1 )); then
		error "Test for $alertKey failed"
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

	clean_logs

	echo "1-Create Pools ..."
	create_pools

	echo "2-Create Files ..."
    for i in `seq 1 2`; do
		$LFS setstripe  -p lustre.$POOL1 $ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

    for i in `seq 3 5`; do
		$LFS setstripe  -p lustre.$POOL2 $ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

	echo "2-Scanning filesystem..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "performing FS scan"

	echo "3-Checking results..."
	logFile=/tmp/rh_alert.log
	alertKey=Alert_OST
	expectedEntry="file.3;file.4;file.5"
	occur=3

	# launch the validation for all alerts
	check_alert $alertKey $expectedEntry $occur $logFile
	res=$?

	if (( $res == 1 )); then
		error "Test for $alertKey failed"
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
#	occur = expected nb of occurences for alertKey
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
		echo "ERROR in check_alert: Bad number of occurences for $alertKey: expected=$occur & found=$nbOccur"
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

	mkdir $ROOT/dir1
	mkdir $ROOT/dir2

	for i in `seq 1 5` ; do
		dd if=/dev/zero of=$ROOT/dir1/file.$i bs=1K count=1 >/dev/null 2>/dev/null || error "writing dir1/file.$i"
	done

	ln -s $ROOT/dir1/file.1 $ROOT/dir1/link.1
	ln -s $ROOT/dir1/file.1 $ROOT/dir1/link.2

	chown root:testgroup $ROOT/dir1/file.2
	chown testuser:testgroup $ROOT/dir1/file.3

	setfattr -n user.foo -v 1 $ROOT/dir1/file.4
	setfattr -n user.bar -v 1 $ROOT/dir1/file.5

	dd if=/dev/zero of=$ROOT/dir2/file.6 bs=1K count=10 >/dev/null 2>/dev/null || error "writing dir2/file.6"
	dd if=/dev/zero of=$ROOT/dir2/file.7 bs=1K count=11 >/dev/null 2>/dev/null || error "writing dir2/file.7"
	dd if=/dev/zero of=$ROOT/dir2/file.8 bs=1K count=1 >/dev/null 2>/dev/null || error "writing dir2/file.8"
}

function update_files_migration
{
	# Update several files for migration tests
	# 	update_files_migration

    for i in `seq 1 500`; do
		echo "aaaaaaaaaaaaaaaaaaaa" >> $ROOT/dir2/file.8
	done
    dd if=/dev/zero of=$ROOT/dir2/file.9 bs=1K count=1 >/dev/null 2>/dev/null || error "writing dir2/file.9"
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
	$RH -f ./cfg/$config_file --scan $migrOpt -l DEBUG -L rh_migr.log --once
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

    nbError=0
    for x in $migrate_arr
    do
        (( $is_lhsm > 0 )) && [[ $x = *"link"* ]] && continue
        # lustre/HSM: search by fid
        (( $is_lhsm > 0 )) && x=$(find $ROOT -name $x | xargs -n 1 -r $LFS path2fid | tr -d '[]')
        countMigrFile=`ls -R $BKROOT | grep $x | wc -l`
        if (($countMigrFile == 0)); then
            error "********** TEST FAILED (File System): $x is not archived"
            ((nbError++))
	    fi
        countMigrLog=`grep "$ARCH_STR" rh_migr.log | grep hints | grep $x | wc -l`
        if (($countMigrLog == 0)); then
            error "********** TEST FAILED (Log): $x is not archived"
            ((nbError++))
	    fi
    done

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
	$RH -f ./cfg/$config_file --scan --migrate-file=$ROOT/dir1/link.1 -l DEBUG -L rh_migr.log --once

    nbError=0
    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
            ((nbError++))
    fi

    for x in $migrate_arr
    do
        (( $is_lhsm > 0 )) && x=$(find $ROOT -name $x | xargs -n 1 -r $LFS path2fid | tr -d '[]')
        countMigrFile=`ls -R $BKROOT | grep $x | wc -l`
        if (($countMigrFile == 0)); then
            error "********** TEST FAILED (File System): $x is not archived"
            ((nbError++))
	    fi
        countMigrLog=`grep "$ARCH_STR" rh_migr.log | grep hints | grep $x | wc -l`
        if (($countMigrLog == 0)); then
            error "********** TEST FAILED (Log): $x is not archived"
            ((nbError++))
	    fi
    done

	echo "Applying migration policy..."
	$RH -f ./cfg/$config_file --migrate-file=$ROOT/dir1/file.1 -l DEBUG -L rh_migr.log --once
    (( $is_lhsm > 0 )) && wait_done 10

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
            ((nbError++))
    fi
    for x in $migrate_arr
    do
        (( $is_lhsm > 0 )) && x=$(find $ROOT -name $x | xargs -n 1 -r $LFS path2fid | tr -d '[]')
        countMigrFile=`ls -R $BKROOT | grep $x | wc -l`
        if (($countMigrFile == 0)); then
            error "********** TEST FAILED (File System): $x is not archived"
            ((nbError++))
	    fi
        countMigrLog=`grep "$ARCH_STR" rh_migr.log | grep hints | grep $x | wc -l`
        if (($countMigrLog == 0)); then
            error "********** TEST FAILED (Log): $x is not archived"
            ((nbError++))
	    fi
    done

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
	$RH -f ./cfg/$config_file --scan --migrate-file=$ROOT/dir1/file.1 -l DEBUG -L rh_migr.log --once
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
	$RH -f ./cfg/$config_file --migrate-file=$ROOT/dir1/file.3 -l DEBUG -L rh_migr.log --once
    (( $is_lhsm > 0 )) && wait_done 10

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
            ((nbError++))
    fi
    for x in $migrate_arr
    do
        (( $is_lhsm > 0 )) && x=$(find $ROOT -name $x | xargs -n 1 -r $LFS path2fid | tr -d '[]')
        countMigrFile=`ls -R $BKROOT | grep $x | wc -l`
        if (($countMigrFile == 0)); then
            error "********** TEST FAILED (File System): $x is not archived"
            ((nbError++))
	    fi
        countMigrLog=`grep "$ARCH_STR" rh_migr.log | grep hints | grep $x | wc -l`
        if (($countMigrLog == 0)); then
            error "********** TEST FAILED (Log): $x is not archived"
            ((nbError++))
	    fi
    done

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
	$RH -f ./cfg/$config_file --scan --migrate-file=$ROOT/dir1/file.1 -l DEBUG -L rh_migr.log --once
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
	$RH -f ./cfg/$config_file --migrate-file=$ROOT/dir1/file.1 -l DEBUG -L rh_migr.log --once
    (( $is_lhsm > 0 )) && wait_done 10

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
            ((nbError++))
    fi
    for x in $migrate_arr
    do
        (( $is_lhsm > 0 )) && x=$(find $ROOT -name $x | xargs -n 1 -r $LFS path2fid | tr -d '[]')
        countMigrFile=`ls -R $BKROOT | grep $x | wc -l`
        if (($countMigrFile == 0)); then
            error "********** TEST FAILED (File System): $x is not archived"
            ((nbError++))
	    fi
        countMigrLog=`grep "$ARCH_STR" rh_migr.log | grep hints | grep $x | wc -l`
        if (($countMigrLog == 0)); then
            error "********** TEST FAILED (Log): $x is not archived"
            ((nbError++))
	    fi
    done

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
	$RH -f ./cfg/$config_file --scan --migrate-file=$ROOT/dir1/file.4 -l DEBUG -L rh_migr.log --once
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
	$RH -f ./cfg/$config_file --scan --migrate-file=$ROOT/dir1/file.5 -l DEBUG -L rh_migr.log --once
    (( $is_lhsm > 0 )) && wait_done 10

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
       ((nbError++))
    fi

	echo "Applying migration policy..."
	$RH -f ./cfg/$config_file --scan --migrate-file=$ROOT/dir1/file.1 -l DEBUG -L rh_migr.log --once
    (( $is_lhsm > 0 )) && wait_done 10

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
       ((nbError++))
    fi

    for x in $migrate_arr
    do
        (( $is_lhsm > 0 )) && x=$(find $ROOT -name $x | xargs -n 1 -r $LFS path2fid | tr -d '[]')
        countMigrFile=`ls -R $BKROOT | grep $x | wc -l`
        if (($countMigrFile == 0)); then
            error "********** TEST FAILED (File System): $x is not archived"
            ((nbError++))
	    fi
        countMigrLog=`grep "$ARCH_STR" rh_migr.log | grep hints | grep $x | wc -l`
        if (($countMigrLog == 0)); then
            error "********** TEST FAILED (Log): $x is not archived"
            ((nbError++))
	    fi
    done

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
		$LFS setstripe  -p lustre.$POOL1 $ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

    for i in `seq 3 4`; do
		$LFS setstripe  -p lustre.$POOL2 $ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

	echo "Applying migration policy..."
	$RH -f ./cfg/$config_file --scan $migrOpt -l DEBUG -L rh_migr.log --once
    (( $is_lhsm > 0 )) && wait_done 60
    nbError=0
    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
        ((nbError++))
    fi

    for x in $migrate_arr
    do
        (( $is_lhsm > 0 )) && x=$(find $ROOT -name $x | xargs -n 1 -r $LFS path2fid | tr -d '[]')
        countMigrFile=`ls -R $BKROOT | grep $x | wc -l`
        if (($countMigrFile == 0)); then
            error "********** TEST FAILED (File System): $x is not archived"
            ((nbError++))
	    fi
        countMigrLog=`grep "$ARCH_STR" rh_migr.log | grep hints | grep $x | wc -l`
        if (($countMigrLog == 0)); then
            error "********** TEST FAILED (Log): $x is not archived"
            ((nbError++))
	    fi
    done

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
		$LFS setstripe  -p lustre.$POOL1 $ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

    for i in `seq 3 4`; do
		$LFS setstripe  -p lustre.$POOL2 $ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

	echo "3-Reading changelogs and Applying migration policy..."
	$RH -f ./cfg/$config_file --scan --migrate-file=$ROOT/file.2 -l DEBUG -L rh_migr.log --once
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
	$RH -f ./cfg/$config_file --scan --migrate-file=$ROOT/file.3 -l DEBUG -L rh_migr.log --once
    (( $is_lhsm > 0 )) && wait_done 10

    countFile=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    countLink=`find $BKROOT -type l -not -name "*.lov" | wc -l`
    count=$(($countFile+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files migrated, but $countFinal expected"
        ((nbError++))
    fi

    for x in $migrate_arr
    do
        (( $is_lhsm > 0 )) && x=$(find $ROOT -name $x | xargs -n 1 -r $LFS path2fid | tr -d '[]')
        countMigrFile=`ls -R $BKROOT | grep $x | wc -l`
        if (($countMigrFile == 0)); then
            error "********** TEST FAILED (File System): $x is not archived"
            ((nbError++))
	    fi
        countMigrLog=`grep "$ARCH_STR" rh_migr.log | grep hints | grep $x | wc -l`
        if (($countMigrLog == 0)); then
            error "********** TEST FAILED (Log): $x is not archived"
            ((nbError++))
	    fi
    done

    if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}

###################################################
############# End Migration Functions #############
###################################################

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

    if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No Purge trigger for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

    $LFS setstripe -c 2 $ROOT || echo "error setting stripe count=2"

	echo "1-Create Files ..."
	elem=`$LFS df $ROOT | grep "filesystem summary" | awk '{ print $6 }' | sed 's/%//'`
	limit=80
    limit_init=$limit
	indice=1
    while [ $elem -lt $limit ]
    do
        # write 2M to fullfill 2 stripes
        dd if=/dev/zero of=$ROOT/file.$indice bs=2M count=1 conv=sync >/dev/null 2>/dev/null
        if (( $? != 0 )); then
            echo "WARNING: failed to write $ROOT/file.$indice"
            # give it a chance to end the loop
            ((limit=$limit-1))
        else
            # reinitialize the limit on success
            limit=$limit_init
        fi

        unset elem
	    elem=`$LFS df $ROOT | grep "filesystem summary" | awk '{ print $6 }' | sed 's/%//'`
        ((indice++))
    done

    echo "2-Reading changelogs and Applying purge trigger policy..."
	$RH -f ./cfg/$config_file --scan --check-thresholds -l DEBUG -L rh_purge.log --once

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

    if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No Purge trigger for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "1-Create Pools ..."
	create_pools

	echo "Calculate big string"
	aaa="azertyuiopqsdfghjklmwxcvbn"
	for i in `seq 0 2000`; do
        aaa="$aaa azertyuiop"
    done

	echo "2-Create Files ..."
	elem=`$LFS df $ROOT | grep "OST:0" | awk '{ print $5 }' | sed 's/%//'`
	limit=80
	indice=1
    while [ $elem -lt $limit ]
    do
        $LFS setstripe -p lustre.$POOL1 $ROOT/file.$indice -c 1 >/dev/null 2>/dev/null
        for i in `seq 0 200`; do
		    echo "$aaa$aaa$aaa" >> $ROOT/file.$indice
            sync
	    done
        unset elem
	    elem=`$LFS df $ROOT | grep "OST:0" | awk '{ print $5 }' | sed 's/%//'`
        ((indice++))
    done

    echo "2-Reading changelogs and Applying purge trigger policy..."
	$RH -f ./cfg/$config_file --scan --check-thresholds -l DEBUG -L rh_purge.log --once

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

    if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No Purge trigger for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

        # force df update
        while (( 1 )); do
                elem=`$LFS df $ROOT | grep "filesystem summary" | awk '{ print $6 }' | sed 's/%//'`
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
    while [ $elem -lt $limit ]
    do
        # write 2M to fullfill 2 stripes
        dd if=/dev/zero of=$ROOT/file.$indice bs=2M count=1 conv=sync >/dev/null 2>$dd_out
        if (( $? != 0 )); then
            [[ -z "$one_error" ]] && one_error="failed to write $ROOT/file.$indice: $(cat $dd_out)"
            ((dd_err_count++))
            ((limit=$limit-1))
        else
            # on success, reinitialize limit
            limit=$limit_init
        fi

        if [[ -s $ROOT/file.$indice ]]; then
            ((last++))
        fi

	# force df update
	clean_caches
	unset elem
	elem=`$LFS df $ROOT | grep "filesystem summary" | awk '{ print $6 }' | sed 's/%//'`
        ((indice++))
    done
    (($dd_err_count > 0)) && echo "WARNING: $dd_err_count errors writing $ROOT/file.*: first error: $one_error"

    rm -f $dd_out

    # limit is 25% => leave half of files with owner root
    ((limit=$last/2))
    ((limit=$limit-1))
    echo "$last files created, changing $limit files to testuser:testgroup"
    df -h $ROOT
    ((indice=1))
    while [ $indice -lt $limit ]
    do
        chown testuser:testgroup $ROOT/file.$indice
        ((indice++))
    done


    echo "2-Reading changelogs and Applying purge trigger policy..."
	$RH -f ./cfg/$config_file --scan --check-thresholds -l DEBUG -L rh_purge.log --once

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

    mkdir $ROOT/dir1
    mkdir $ROOT/dir2

    for i in `seq 1 5` ; do
    	dd if=/dev/zero of=$ROOT/dir1/file.$i bs=1K count=1 >/dev/null 2>/dev/null || error "writing dir1/file.$i"
	done

	ln -s $ROOT/dir1/file.1 $ROOT/dir1/link.1
	ln -s $ROOT/dir1/file.1 $ROOT/dir1/link.2

	chown root:testgroup $ROOT/dir1/file.2
    chown testuser:testgroup $ROOT/dir1/file.3

	setfattr -n user.foo -v 1 $ROOT/dir1/file.4
	setfattr -n user.bar -v 1 $ROOT/dir1/file.5

    dd if=/dev/zero of=$ROOT/dir2/file.6 bs=1K count=10 >/dev/null 2>/dev/null || error "writing dir2/file.6"
    dd if=/dev/zero of=$ROOT/dir2/file.7 bs=1K count=11 >/dev/null 2>/dev/null || error "writing dir2/file.7"
    dd if=/dev/zero of=$ROOT/dir2/file.8 bs=1K count=1 >/dev/null 2>/dev/null || error "writing dir2/file.8"
}

function update_files_Purge
{
	# update files for Purge tests
	#  update_files_migration

    for i in `seq 1 500`; do
		echo "aaaaaaaaaaaaaaaaaaaa" >> $ROOT/dir2/file.8
	done
	more $ROOT/dir2/file.8 >/dev/null 2>/dev/null
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

    if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No Purge for this purpose: skipped"
		set_skipped
		return 1
	fi

	needPurge=0
	((needPurge=10-countFinal))

	clean_logs

	echo "Create Files ..."
	create_files_Purge

	sleep 1
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once

	# use robinhood for flushing
    if (( ($is_hsmlite != 0) || ($is_lhsm != 0) )); then
		echo "Archiving files"
		$RH -f ./cfg/$config_file --sync -l DEBUG  -L rh_migr.log || error "executing Archiving files"
	fi

	if(($sleep_time != 0)); then
	    echo "Sleep $sleep_time"
        sleep $sleep_time

	    echo "update Files"
        update_files_Purge

        if (( ($is_hsmlite != 0) || ($is_lhsm != 0) )); then
	        echo "Update Archiving files"
	        $RH -f ./cfg/$config_file --scan --migrate -l DEBUG  -L rh_migr.log --once
            (( $is_lhsm > 0 )) && wait_done 60
	    fi
    fi

	echo "Scan and apply purge policy..."
	$RH -f ./cfg/$config_file --scan  $purgeOpt --once -l DEBUG -L rh_purge.log

	nbError=0
	nb_purge=`grep $REL_STR rh_purge.log | wc -l`
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

    if (( ($is_hsmlite != 0) || ($is_lhsm != 0) )); then
		echo "No Purge for this purpose: skipped"
		set_skipped
		return 1
	fi

	needPurge=0
	((needPurge=10-countFinal))

	clean_logs

	echo "Create Files ..."
	create_files_Purge

	sleep 1
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once

	if(($sleep_time != 0)); then
	    echo "Sleep $sleep_time"
        sleep $sleep_time

	    echo "update Files"
        update_files_Purge
    fi

	echo "Reading changelogs and Applying purge policy..."
	$RH -f ./cfg/$config_file --scan  $purgeOpt --once -l DEBUG -L rh_purge.log

	nbError=0
	nb_purge=`grep $REL_STR rh_purge.log | wc -l`
	if (( $nb_purge != $needPurge )); then
	    error "********** TEST FAILED (Log): $nb_purge files purged, but $needPurge expected"
        ((nbError++))
	fi

    countFileDir1=`find $ROOT/dir1 -type f | wc -l`
    countFileDir2=`find $ROOT/dir2 -type f | wc -l`
    countLink=`find $ROOT/dir1 -type l | wc -l`
    count=$(($countFileDir1+$countFileDir2+$countLink))
    if (($count != $countFinal)); then
        error "********** TEST FAILED (File System): $count files stayed in filesystem, but $countFinal expected"
        ((nbError++))
    fi

    for x in $purge_arr
    do
        if [ -e "$ROOT/dir1/$x" -o -e "$ROOT/dir2/$x" ]; then
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

    if (( ($is_hsmlite != 0) && ($shook == 0) )); then
		echo "No Purge for this purpose: skipped"
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
		$LFS setstripe  -p lustre.$POOL1 $ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

    for i in `seq 3 4`; do
		$LFS setstripe  -p lustre.$POOL2 $ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

	sleep 1
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once

	# use robinhood for flushing
	if (( $is_hsmlite + $is_lhsm > 0 )); then
		echo "2bis-Archiving files"
		$RH -f ./cfg/$config_file --sync -l DEBUG  -L rh_migr.log || error "executing Archiving files"
        (( $is_lhsm > 0 )) && wait_done 60
	fi

	echo "Reading changelogs and Applying purge policy..."
	$RH -f ./cfg/$config_file --scan $purgeOpt -l DEBUG -L rh_purge.log --once

	nbError=0
	nb_purge=`grep $REL_STR rh_purge.log | wc -l`
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

    if (( ($is_hsmlite != 0) || ($is_lhsm != 0) )); then
		echo "No removing dir for this purpose: skipped"
		set_skipped
		return 1
	fi

	#  clean logs ..............................
	clean_logs

	# prepare data..............................
	echo "1-Preparing Filesystem..."
	mkdir -p $ROOT/dir1
	mkdir -p $ROOT/dir5
	echo "data" > $ROOT/dir5/file.5

	if [ $testKey == "emptyDir" ]; then
		# wait and write more data
		if [ $sleepTime != 0 ]; then
			echo "Please wait $sleepTime seconds ..."
			sleep $sleepTime || error "sleep time"
		fi
		sleepTime=0
		mkdir -p $ROOT/dir6
		mkdir -p $ROOT/dir7
		echo "data" > $ROOT/dir7/file.7

	else
		# in dir1: manage folder owner and attributes
		chown testuser $ROOT/dir1 || error "invalid chown on user 'testuser' for $ROOT/dir1 "  #change owner
		setfattr -n user.foo -v "abc.1.test" $ROOT/dir1
		echo "data" > $ROOT/dir1/file.1
		mkdir -p $ROOT/dir1/dir2
		echo "data" > $ROOT/dir1/dir2/file.2
		mkdir -p $ROOT/dir1/dir3
		echo "data" > $ROOT/dir1/dir3/file.3
	 	mkdir -p $ROOT/dir1/dir4
		chown testuser $ROOT/dir1/dir4 || error "invalid chown on user 'testuser' for $ROOT/dir4" #change owner
		echo "data" > $ROOT/dir1/dir4/file.41
		echo "data" > $ROOT/dir1/dir4/file.42

		# in dir5:
		setfattr -n user.bar -v "abc.1.test" $ROOT/dir5
		echo "data" > $ROOT/dir5/file.5

		# in dir6:
		mkdir -p $ROOT/dir6
		chown testuser $ROOT/dir6 || error "invalid chown on user 'testuser' for $ROOT/dir6" #change owner
	fi

	# launch the scan ..........................
	echo "2-Scanning directories in filesystem ..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once || error "scanning filesystem"

	# optional sleep process ......................
	if [ $sleepTime != 0 ]; then
		echo "Please wait $sleepTime seconds ..."
		sleep $sleepTime
	fi
	# specific optional action after sleep process ..........
	if [ $testKey == "lastAccess" ]; then
	#	ls -R $ROOT/dir1 || error "scaning $ROOT/dir1"
		touch $ROOT/dir1/file.touched || error "touching file in $ROOT/dir1"
	elif [ $testKey == "lastModif" ]; then
		echo "data" > $ROOT/dir1/file.12 || error "writing in $ROOT/dir1/file.12"
	fi

	# launch the rmdir ..........................
	echo "3-Removing directories in filesystem ..."
	if [ $testKey == "lastAccess" ]; then
	$RH -f ./cfg/$config_file --rmdir -l DEBUG -L rh_rmdir.log --once || error "performing FS removing"
	else
	$RH -f ./cfg/$config_file --scan --rmdir -l DEBUG -L rh_rmdir.log --once || error "performing FS removing"
	fi

	# launch the validation ..........................
	echo "4-Checking results ..."
	logFile=/tmp/rh_alert.log
	case "$testKey" in
		pathName)
			existedDirs="$ROOT/dir5;$ROOT/dir6"
			notExistedDirs="$ROOT/dir1"
			;;
		emptyDir)
			existedDirs="$ROOT/dir6;$ROOT/dir5;$ROOT/dir7"
			notExistedDirs="$ROOT/dir1"
			;;
		owner)
			existedDirs="$ROOT/dir5"
			notExistedDirs="$ROOT/dir1;$ROOT/dir6"
			;;
		lastAccess)
			existedDirs="$ROOT/dir1"
			notExistedDirs="$ROOT/dir5;$ROOT/dir6"
			;;
		lastModif)
			existedDirs="$ROOT/dir1"
			notExistedDirs="$ROOT/dir5;$ROOT/dir6"
			;;
		dircount)
			existedDirs="$ROOT/dir5;$ROOT/dir6"
			notExistedDirs="$ROOT/dir1"
			;;
		extAttributes)
			existedDirs="$ROOT/dir5;$ROOT/dir6"
			notExistedDirs="$ROOT/dir1"
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
        echo "OK: Test successfull"
	fi
}

function test_rmdir_mix
{
	config_file=$1
	sleepTime=$2 # for age_rm_empty_dirs

    if (( ($is_hsmlite != 0) || ($is_lhsm != 0) )); then
		echo "No removing dir for this purpose: skipped"
		set_skipped
		return 1
	fi

	#  clean logs
	clean_logs

	# prepare data
	echo "1-Preparing Filesystem..."
    # old dirempty
	mkdir -p $ROOT/no_rm/dirempty
	mkdir -p $ROOT/dirempty
    sleep $sleepTime

    # new dirs
	mkdir -p $ROOT/no_rm/dir1
	mkdir -p $ROOT/no_rm/dir2
	mkdir -p $ROOT/no_rm/dirempty_new
	mkdir -p $ROOT/dir1
	mkdir -p $ROOT/dir2
	mkdir -p $ROOT/dirempty_new
	echo "data" >  $ROOT/no_rm/dir1/file
	echo "data" >  $ROOT/no_rm/dir2/file
	echo "data" >  $ROOT/dir1/file
	echo "data" >  $ROOT/dir2/file

	# launch the scan ..........................
	echo "2-Scanning directories in filesystem ..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once || error "scanning filesystem"

    echo "3-Checking rmdir report"
	$REPORT -f ./cfg/$config_file -l MAJOR -cq --top-rmdir > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    # must report empty dirs (non ignored)
    grep "no_rm/dirempty," report.out && error "top-rmdir report whitelisted dir"
    grep "no_rm/dirempty_new," report.out && error "top-rmdir report whitelisted dir"
    grep "$ROOT/dirempty," report.out | grep expired || error "top-rmdir did not report expired eligible dir"
    grep "$ROOT/dirempty_new," report.out | grep -v expired || error "top-rmdir did not report non-expired eligible dir"

	# launch the rmdir ..........................
	echo "4-Removing directories in filesystem ..."
	$RH -f ./cfg/$config_file --rmdir -l DEBUG -L rh_rmdir.log --once || error "performing rmdir"

	echo "5-Checking results ..."
    exist="$ROOT/no_rm/dirempty;$ROOT/no_rm/dir1;$ROOT/no_rm/dir2;$ROOT/no_rm/dirempty_new;$ROOT/dir2;$ROOT/dirempty_new"
    noexist="$ROOT/dir1;$ROOT/dirempty"

	# launch the validation for all remove process
	exist_dirs_or_not $exist $noexist
	res=$?

	if (( $res == 1 )); then
		error "Test for RemovingDir_mixed failed"
    else
        echo "OK: Test successfull"
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

    if (( ($is_hsmlite != 0) || ($is_lhsm != 0) )); then
		echo "No removing dir for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "Create Pools ..."
	create_pools

	echo "Create Files ..."
	mkdir $ROOT/dir1

	$LFS setstripe  -p lustre.$POOL1 $ROOT/dir1 >/dev/null 2>/dev/null

	$LFS setstripe  -p lustre.$POOL1 $ROOT/dir1/file.1 -c 1 >/dev/null 2>/dev/null
	$LFS setstripe  -p lustre.$POOL1 $ROOT/dir1/file.2 -c 1 >/dev/null 2>/dev/null
	$LFS setstripe  -p lustre.$POOL1 $ROOT/dir1/file.3 -c 1 >/dev/null 2>/dev/null

	mkdir $ROOT/dir2
	$LFS setstripe  -p lustre.$POOL2 $ROOT/dir2 >/dev/null 2>/dev/null

    $LFS setstripe  -p lustre.$POOL2 $ROOT/file.1 -c 1 >/dev/null 2>/dev/null
	$LFS setstripe  -p lustre.$POOL2 $ROOT/dir2/file.2 -c 1 >/dev/null 2>/dev/null
	$LFS setstripe  -p lustre.$POOL2 $ROOT/dir2/file.3 -c 1 >/dev/null 2>/dev/null

	echo "Removing directories in filesystem ..."
	$RH -f ./cfg/$config_file --scan --rmdir -l DEBUG -L rh_rmdir.log --once || error "performing FS removing"

	# launch the validation ..........................
	echo "Checking results ..."
	logFile=/tmp/rh_alert.log
	existedDirs="$ROOT/dir1"
	notExistedDirs="$ROOT/dir2"
	# launch the validation for all remove process
	exist_dirs_or_not $existedDirs $notExistedDirs
	res=$?

	if (( $res == 1 )); then
		error "Test for RemovingDir_ost failed"
	fi

	test -f $ROOT/file.1
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
    # ex: "$ROOT/dir1;$ROOT/dir5"
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
    #	ex: "$ROOT/dir1;$ROOT/dir5"  or "/" to no check command
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

	#  clean logs ..............................
	clean_logs

	# prepare data..............................
	echo -e "\n 1-Preparing Filesystem..."
	# dir1:
	mkdir -p $ROOT/dir1/dir2
	printf "." ; sleep 1
	dd if=/dev/zero of=$ROOT/dir1/file.1 bs=1k count=5 >/dev/null 2>/dev/null || error "writing file.1"
	printf "." ; sleep 1
	dd if=/dev/zero of=$ROOT/dir1/file.2 bs=1M count=1 >/dev/null 2>/dev/null || error "writing file.2"
	printf "." ; sleep 1
	dd if=/dev/zero of=$ROOT/dir1/file.3 bs=1k count=15 >/dev/null 2>/dev/null || error "writing file.3"
	printf "." ; sleep 1
	# link from dir1:
	ln -s $ROOT/dir1/file.1 $ROOT/link.1 || error "creating symbolic link $ROOT/link.1"
	printf "." ; sleep 1
	# dir2 inside dir1:
	ln -s $ROOT/dir1/file.3 $ROOT/dir1/dir2/link.2 || error "creating symbolic link $ROOT/dir1/dir2/link.2"
	printf "." ; sleep 1
	# dir3 inside dir1:
	mkdir -p $ROOT/dir1/dir3
	printf "." ; sleep 1
	#dir4:
	mkdir -p $ROOT/dir4
	printf "." ; sleep 1
	#dir5:
	mkdir -p $ROOT/dir5
	printf "." ; sleep 1
	dd if=/dev/zero of=$ROOT/dir5/file.4 bs=1k count=10 >/dev/null 2>/dev/null || error "writing file.4"
	printf "." ; sleep 1
	dd if=/dev/zero of=$ROOT/dir5/file.5 bs=1k count=20 >/dev/null 2>/dev/null || error "writing file.5"
	printf "." ; sleep 1
	dd if=/dev/zero of=$ROOT/dir5/file.6 bs=1k count=21 >/dev/null 2>/dev/null || error "writing file.6"
	printf "." ; sleep 1
	ln -s $ROOT/dir1/file.2 $ROOT/dir5/link.3 || error "creating symbolic link $ROOT/dir5/link.3"
	printf "." ; sleep 1
	#dir6 and dir8 inside dir5:
	mkdir -p $ROOT/dir5/dir6
	printf "." ; sleep 1
	mkdir -p $ROOT/dir5/dir8
	printf "." ; sleep 1
	# dir7:
	mkdir -p $ROOT/dir7
	printf "." ; sleep 1
    #2links in dir.1
    ln -s $ROOT/dir1 $ROOT/dir1/link.0 || error "creating symbolic link $ROOT/dir1/link.0"
    printf "." ; sleep 1
    ln -s $ROOT/dir1 $ROOT/dir1/link.1 || error "creating symbolic link $ROOT/dir1/link.1"
    printf "." ; sleep 1

    # make sure all data is on disk
    sync

	# manage owner and group
	filesList="$ROOT/link.1 $ROOT/dir1/dir2/link.2"
	chgrp -h testgroup $filesList || error "invalid chgrp on group 'testgroup' for $filesList "
	chown -h testuser $filesList || error "invalid chown on user 'testuser' for $filesList "
	filesList="$ROOT/dir1/file.2 $ROOT/dir1/dir2 $ROOT/dir1/dir3 $ROOT/dir5 $ROOT/dir7 $ROOT/dir5/dir6 $ROOT/dir5/dir8"
	chown testuser:testgroup $filesList || error "invalid chown on user 'testuser' for $filesList "
	filesList="$ROOT/dir1/file.1 $ROOT/dir5/file.6"
	chgrp testgroup $filesList || error "invalid chgrp on group 'testgroup' for $filesList "

	# launch the scan ..........................
	echo -e "\n 2-Scanning Filesystem..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "performing FS scan"

	# launch another scan ..........................
	echo -e "\n 3-Filesystem content statistics..."
	#$REPORT -f ./cfg/$config_file --fs-info -c || error "performing FS statistics (--fs-info)"
	$REPORT -f ./cfg/$config_file --fs-info --csv > report.out || error "performing FS statistics (--fs-info)"
	logFile=report.out

    typeValues="dir;file;symlink"
    countValues="8;6;5"
    if (( $is_hsmlite + $is_lhsm != 0 )); then
        # type counts are in 3rd column (beacause of status column)
        colSearch=3
    else
        # type counts are in 2nd column
        colSearch=2
    fi
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating FS statistics (--fs-info)"


	# launch another scan ..........................
	echo -e "\n 4-FileClasses summary..."
	$REPORT -f ./cfg/$config_file --class-info --csv > report.out || error "performing FileClasses summary (--class)"
    if (( $is_lhsm == 0 )); then
        typeValues="test_file_type;test_link_type"
        countValues="6;5"
    else
        # Lustre/HSM: no fileclass for symlinks
        typeValues="test_file_type"
        countValues="6"
    fi

    if (( $is_hsmlite + $is_lhsm != 0 )); then
    	colSearch=3
    else
    	colSearch=2
    fi

	#echo "arguments= $logFile $typeValues $countValues $colSearch**"
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating FileClasses summary (--class)"
	# launch another scan ..........................
	echo -e "\n 5-User statistics of root..."
	$REPORT -f ./cfg/$config_file --user-info -u root --csv > report.out || error "performing User statistics (--user)"
    typeValues="root.*dir;root.*file;root.*symlink"
    countValues="2;5;3"
	colSearch=3
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating FS User statistics (--user)"

	# launch another scan ..........................
	echo -e "\n 6-Group statistics of testgroup..."
	$REPORT -f ./cfg/$config_file --group-info -g testgroup --csv > report.out || error "performing Group statistics (--group)"
	typeValues="testgroup.*dir;testgroup.*file;testgroup.*symlink"
	countValues="6;3;2"
	colSearch=3
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Group statistics (--group)"

	# launch another scan ..........................
	echo -e "\n 7-Four largest files of Filesystem..."
	$REPORT -f ./cfg/$config_file --top-size=4 --csv > report.out || error "performing Largest files list (--top-size)"
	typeValues="file\.2;file\.6;file\.5;file\.3"
	countValues="1;2;3;4"
	colSearch=1
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Largest files list (--top-size)"

	echo -e "\n 8-Largest directories of Filesystem..."
	$REPORT -f ./cfg/$config_file --top-dirs=3 --csv > report.out || error "performing Largest folders list (--top-dirs)"
	# 2 possible orders
	typeValues="$ROOT/dir1;$ROOT/dir5;$ROOT,"
	typeValuesAlt="$ROOT/dir1;$ROOT,;$ROOT/dir5"
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
        $RH -f ./cfg/$config_file --sync -l DEBUG -L rh_migr.log  --once || error "performing migration"
        $REPORT -f ./cfg/$config_file --top-purge=4 --csv > report.out || error "performing Oldest entries list (--top-purge)"
        typeValues="link\.3;link\.1;link\.2;file\.1"
        countValues="1;2;3;4"
        else
        $REPORT -f ./cfg/$config_file --top-purge=4 --csv > report.out || error "performing Oldest entries list (--top-purge)"
        typeValues="file\.3;file\.4;file\.5;link\.3"
        countValues="1;2;3;4"
        fi
        colSearch=1
        find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Oldest entries list (--top-purge)"
    fi

   echo -e "\n 10-Oldest and empty directories of Filesystem..."
   if (( $is_hsmlite + $is_lhsm != 0 )); then
       echo "No rmdir policy for hsmlite or HSM purpose: skipped"
   else
        $REPORT -f ./cfg/$config_file --top-rmdir --csv > report.out || error "performing Oldest and empty folders list (--top-rmdir)"
        nb_dir3=`grep "dir3" $logFile | wc -l`
        if (( nb_dir3==0 )); then
            error "validating Oldest and empty folders list (--top-rmdir) : dir3 not found"
        fi
        nb_dir4=`grep "dir4" $logFile | wc -l`
        if (( nb_dir4==0 )); then
            error "validating Oldest and empty folders list (--top-rmdir) : dir4 not found"
        fi
        nb_dir6=`grep "dir6" $logFile | wc -l`
        if (( nb_dir6==0 )); then
            error "validating Oldest and empty folders list (--top-rmdir) : dir6 not found"
        fi
        nb_dir7=`grep "dir7" $logFile | wc -l`
        if (( nb_dir7==0 )); then
            error "validating Oldest and empty folders list (--top-rmdir) : dir7 not found"
        fi
    fi

	# launch another scan ..........................
	echo -e "\n 11-Top disk space consumers of Filesystem..."
	$REPORT -f ./cfg/$config_file --top-users --csv > report.out || error "performing disk space consumers (--top-users)"
	typeValues="testuser;root"
	countValues="1;2"
	colSearch=1
    [ "$DEBUG" = "1" ] && cat report.out
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating disk space consumers (--top-users)"

	# launch another scan ..........................
	echo -e "\n 12-Dump entries for one user of Filesystem..."
	$REPORT -f ./cfg/$config_file --dump-user root --csv > report.out || error "dumping entries for one user 'root'(--dump-user)"
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
	$REPORT -f ./cfg/$config_file --dump-group testgroup --csv > report.out || error "dumping entries for one group 'testgroup'(--dump-group)"
	#$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "performing FS scan"
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

	    find_valueInCSVreport $logFile $typeValue $countValue $colSearch
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
    # one line per information; informations separeted by ','
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
    line=$(grep $typeValue $logFile)
    #echo $line
    if (( ${#line} == 0 )); then
	    #echo "=====> NON trouve pour $typeValue"
	    return 1
    fi

    # get found value count for this value type
    foundCount=$(grep $typeValue $logFile | cut -d ',' -f $colSearch | tr -d ' ')
    #echo "foundCount=$foundCount**"
    if (( $foundCount != $countValue )); then
	    #echo "=====> NON trouve pour $typeValue : $countValue =/$foundCount"
	    return 1
    else
	    #echo "=====> trouve pour $typeValue : $countValue "
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
		$LFS setstripe  -p lustre.$POOL1 $ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

    for i in `seq 3 4`; do
		$LFS setstripe  -p lustre.$POOL2 $ROOT/file.$i -c 1 >/dev/null 2>/dev/null
	done

	sleep 1
	$RH -f ./cfg/common.conf --scan -l DEBUG -L rh_scan.log --once


	echo "Generate report..."
	$REPORT -f ./cfg/common.conf --dump-ost 1 >> report.out

	nbError=0
	nb_report=`grep "$ROOT/file." report.out | wc -l`
	if (( $nb_report != 2 )); then
	    error "********** TEST FAILED (Log): $nb_report files purged, but 2 expected"
        ((nbError++))
	fi

	nb_report=`grep "$ROOT/file.3" report.out | wc -l`
	if (( $nb_report != 1 )); then
	    error "********** TEST FAILED (Log): No report for file.3"
        ((nbError++))
	fi

	nb_report=`grep "$ROOT/file.4" report.out | wc -l`
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
    touch $ROOT/file.1 || error "touch file.1"
	touch $ROOT/file.1 || error "touch file.1"
	chmod +x $ROOT/file.1 || error "chmod file.1"

    # Reading changelogs
    echo "2. Scanning ..."
   	$RH -f ./cfg/$config_file --readlog --once -l FULL -L rh_scan.log || error "reading changelog"
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
    	dd if=/dev/zero of=$ROOT/file.$i bs=1K count=1 >/dev/null 2>/dev/null || error "writing file.$i"
	    setfattr -n user.foo -v $i $ROOT/file.$i
	done

	echo "Scan Filesystem"
	sleep 1
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once

	# use robinhood for flushing (
	if (( ($is_hsmlite == 0 && $is_lhsm == 1 && shook == 0) || ($is_hsmlite == 1 && $is_lhsm == 0 && shook == 1) )); then
		echo "Archiving files"
		$RH -f ./cfg/$config_file --sync -l DEBUG  -L rh_migr.log || error "executing Archiving files"
	fi

	echo "Report : --dump --filter-class test_purge"
	$REPORT -f ./cfg/$config_file --dump --filter-class test_purge > report.out

	nbError=0
	nb_entries=`grep "0 entries" report.out | wc -l`
	if (( $nb_entries != 1 )); then
	    error "********** TEST FAILED (Log): not found line \" $nb_entries \" "
        ((nbError++))
	fi

    echo "Create /var/lock/rbh.lock"
	touch "/var/lock/rbh.lock"

	if (( $is_hsmlite == 0 || $shook != 0 || $is_lhsm != 0 )); then
	    echo "Reading changelogs and Applying purge policy..."
	    $RH -f ./cfg/$config_file --scan --purge -l DEBUG -L rh_purge.log --once &

	    sleep 5
	    nbError=0
	    nb_purge=`grep $REL_STR rh_purge.log | wc -l`
	    if (( $nb_purge != 0 )); then
	        error "********** TEST FAILED (Log): $nb_purge files purged, but 0 expected"
            ((nbError++))
	    fi

	    echo "Remove /var/lock/rbh.lock"
	    rm "/var/lock/rbh.lock"

	    echo "wait robinhood"
	    wait

	    nb_purge=`grep $REL_STR rh_purge.log | wc -l`
	    if (( $nb_purge != 10 )); then
	        error "********** TEST FAILED (Log): $nb_purge files purged, but 10 expected"
            ((nbError++))
	    fi
    else #backup mod
	    echo "Launch Migration in background"
	    $RH -f ./cfg/$config_file --scan --migrate -l DEBUG -L rh_migr.log --once &

	    sleep 5
        wait_done 60

        count=`find $BKROOT -type f -not -name "*.lov" | wc -l`
        if (($count != 0)); then
            error "********** TEST FAILED (File System): $count files migrated, but 0 expected"
            ((nbError++))
        fi

        echo "Remove /var/lock/rbh.lock"
	    rm "/var/lock/rbh.lock"

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
    	dd if=/dev/zero of=$ROOT/file.$i bs=10M count=1 >/dev/null 2>/dev/null || error "writing file.$i"
	done
    for i in `seq 6 10` ; do
    	touch $ROOT/file.$i
	done

	sleep 1
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once

	echo "Migrate files"
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log &
	pid=$!

	sleep 5
    (( $is_lhsm > 0 )) && wait_done 60

    nbError=0
	count=`find $BKROOT -type f -not -name "*.lov" | wc -l`
    if (( $count != 10 )); then
        error "********** TEST FAILED (File System): $count files migrated, but 10 expected"
        ((nbError++))
    fi

    # Migration dans fs
    countMigrLog=`grep "$ARCH_STR" rh_migr.log | grep hints | wc -l`
    if (( $countMigrLog != 10 )); then
        error "********** TEST FAILED (Log): $countMigrLog files migrated, but 10 expected"
        ((nbError++))
    fi

    #comptage du nombre de "STATS"
    nb_Stats=`grep "STATS" rh_migr.log | wc -l`

	echo "Sleep 30 seconds"
	sleep 30

    #comptage du nombre de "STATS"
    nb_Stats2=`grep "STATS" rh_migr.log | wc -l`
	if (( $nb_Stats2 <= $nb_Stats )); then
        error "********** TEST FAILED (Stats): $nb_Stats2 \"STATS\" detected, but more than $nb_Stats \"STATS\" expected"
        ((nbError++))
    fi

	echo "Sleep 30 seconds"
	sleep 30


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
    	dd if=/dev/zero of=$ROOT/file.$i bs=1K count=1 >/dev/null 2>/dev/null || error "writing file.$i"
	done

	echo "Archives files"
	$RH -f ./cfg/$config_file --scan --migrate -l DEBUG -L rh_migr.log --once
    (( $is_lhsm > 0 )) && wait_done 60

	nbError=0
	count=`find $BKROOT -type f  -not -name "*.lov" | wc -l`
    if (( $count != 5 )); then
        error "********** TEST FAILED (File System): $count files migrated, but 5 expected"
        ((nbError++))
    fi

    local rmd=()
    for i in `seq 1 5` ; do
        local f=$ROOT/file.$i
        (( $is_lhsm > 0 )) && f=$($LFS path2fid $f | tr -d '[]')
    	rm -f $ROOT/file.$i && rmd+=($f)
	done

	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once

	echo "sleep 30 seconds"
	sleep 30

	echo "HSM Remove"
	$RH -f ./cfg/$config_file --hsm-remove -l DEBUG -L rh_purge.log &
	pid=$!

	echo "sleep 5 seconds"
	sleep 5

	nb_Remove=`grep "Remove request successful for entry" rh_purge.log | wc -l`
	if (( $nb_Remove != 4 )); then
        error "********** TEST FAILED (LOG): $nb_Remove remove detected, but 4 expected"
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
        error "********** TEST FAILED (File System): Wrong count of remaining files: $countRemainFile (1 expected)"
        ((nbError++))
    fi

	echo "sleep 60 seconds"
	sleep 60

	nb_Remove=`grep "Remove request successful for entry" rh_purge.log | wc -l`
	if (( $nb_Remove != 5 )); then
        error "********** TEST FAILED (LOG): $nb_Remove remove detected, but 5 expected"
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
        error "********** TEST FAILED (File System): Wrong count of remaining files: $countRemainFile (0 expected)"
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
	# 	TEST_OTHER_PARAMETERS_4 config_file
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
    	dd if=/dev/zero of=$ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"
	done

	echo "Migrate files (must fail)"
	$RH -f ./cfg/$config_file --scan --migrate -l DEBUG -L rh_migr.log --once
    (( $is_lhsm > 0 )) && wait_done 60

	nbError=0
	count=`find $BKROOT -type f  -not -name "*.lov" | wc -l`
    if (( $count != 0 )); then
        error "********** TEST FAILED (File System): $count files migrated, but 0 expected"
        ((nbError++))
    elif grep "Error initializing backend" rh_migr.log > /dev/null; then
        echo "OK: backend not initialized"
    else
        error "Backend initialization SHOULD have FAILED"
    fi
    :> rh_migr.log

    ensure_init_backend || error "Error initializing backend $BKROOT"

    echo "Migrate files"
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log --once &
	pid=$!
    kill -9 $pid

	nbError=0
	count=`find $BKROOT -type f  -not -name "*.lov" | wc -l`
    if (( $count != 0 )); then
        error "********** TEST FAILED (File System): $count files migrated, but 0 expected"
        ((nbError++))
    fi

    echo "Migrate files"
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log &
	pid=$!

	echo "sleep 30 seconds"
	sleep 30
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

function TEST_OTHER_PARAMETERS_5
{
	# Test for many parameters
	# 	TEST_OTHER_PARAMETERS_5 config_file
	#=>
	# config_file == config file name

	config_file=$1

    if (( ($shook + $is_lhsm) == 0 )); then
		echo "No TEST_OTHER_PARAMETERS_5 for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

    echo "Launch scan in background..."
	$RH -f ./cfg/$config_file --scan --check-thresholds -l DEBUG -L rh_scan.log &
	pid=$!

	sleep 2

	nbError=0
	nb_scan=`grep "Starting scan of" rh_scan.log | wc -l`
	if (( $nb_scan != 1 )); then
        error "********** TEST FAILED (LOG): $nb_scan scan detected, but 1 expected"
        ((nbError++))
    fi

	echo "sleep 60 seconds"
	sleep 60

    echo "Create files"
	elem=`$LFS df $ROOT | grep "filesystem summary" | awk '{ print $6 }' | sed 's/%//'`
	limit=60
	indice=1
    while (( $elem < $limit ))
    do
        dd if=/dev/zero of=$ROOT/file.$indice bs=10M count=1 >/dev/null 2>/dev/null
        if (( $? != 0 )); then
            echo "WARNING: fail writing file.$indice (usage $elem/$limit)"
            # give it a chance to end the loop
            ((limit=$limit-1))
        fi
        unset elem
        elem=`$LFS df $ROOT | grep "filesystem summary" | awk '{ print $6 }' | sed 's/%//'`
        ((indice++))
    done

	echo "sleep 60 seconds"
	sleep 60

	nbError=0
	nb_scan=`grep "Starting scan of" rh_scan.log | wc -l`
	if (( $nb_scan != 3 )); then
        error "********** TEST FAILED (LOG): $nb_scan scan detected, but 3 expected"
        ((nbError++))
    fi

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
run_test 102	update_test test_updt.conf 5 30 "db update policy"
run_test 103a    test_acct_table common.conf 5 "Acct table and triggers creation"
run_test 103b    test_acct_table acct_group.conf 5 "Acct table and triggers creation"
run_test 103c    test_acct_table acct_user.conf 5 "Acct table and triggers creation"
run_test 103d    test_acct_table acct_user_group.conf 5 "Acct table and triggers creation"
run_test 104     test_size_updt test_updt.conf 1 "test size update"
run_test 105     test_enoent test_pipeline.conf "readlog with continuous create/unlink"
run_test 106a    test_diff info_collect2.conf "diff" "rbh-diff"
run_test 106b    test_diff info_collect2.conf "diffapply" "rbh-diff --apply"
run_test 106c    test_diff info_collect2.conf "scan" "robinhood --scan --diff"
run_test 107a    test_completion test_completion.conf OK "scan completion command"
run_test 107b    test_completion test_completion_KO.conf KO "bad scan completion command"
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


#### policy matching tests  ####

run_test 200	path_test test_path.conf 2 "path matching policies"
run_test 201	migration_test test1.conf 11 31 "last_mod>30s"
run_test 202	migration_test test2.conf 5  31 "last_mod>30s and name == \"*[0-5]\""
run_test 203	migration_test test3.conf 5  16 "complex policy with filesets"
run_test 204	migration_test test3.conf 10 31 "complex policy with filesets"
run_test 205	xattr_test test_xattr.conf 5 "xattr-based fileclass definition"
run_test 206	purge_test test_purge.conf 11 41 "last_access > 40s"
run_test 207	purge_size_filesets test_purge2.conf 2 3 "purge policies using size-based filesets"
run_test 208a	periodic_class_match_migr test_updt.conf 10 "periodic fileclass matching (migration)"
run_test 208b	policy_check_migr test_check_migr.conf 10 "test fileclass matching (migration)"
run_test 209a	periodic_class_match_purge test_updt.conf 10 "periodic fileclass matching (purge)"
run_test 209b	policy_check_purge test_check_purge.conf 10 "test fileclass matching (purge)"
run_test 210	fileclass_test test_fileclass.conf 2 "complex policies with unions and intersections of filesets"
run_test 211	test_pools test_pools.conf 1 "class matching with condition on pools"
run_test 212	link_unlink_remove_test test_rm1.conf 1 31 "deferred hsm_remove (30s)"
run_test 213	migration_test_single test1.conf 11 31 "last_mod>30s"
run_test 214a  check_disabled  common.conf  purge      "no purge if not defined in config"
run_test 214b  check_disabled  common.conf  migration  "no migration if not defined in config"
run_test 214c  check_disabled  common.conf  rmdir      "no rmdir if not defined in config"
run_test 214d  check_disabled  common.conf  hsm_remove "hsm_rm is enabled by default"
run_test 214e  check_disabled  common.conf  class      "no class matching if none defined in config"
run_test 215	mass_softrm    test_rm1.conf 31 1000    "rm are detected between 2 scans"
run_test 216   test_maint_mode test_maintenance.conf 30 45 "pre-maintenance mode" 5
run_test 217	migrate_symlink test1.conf 31 		"symlink migration"
run_test 218	test_rmdir 	rmdir.conf 16 		"rmdir policies"
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
run_test 226a  test_purge_lru lru_purge.conf yes "test purge order (sort=yes)"
run_test 226b  test_purge_lru lru_purge.conf no "test purge order (sort=no)"

#### triggers ####

run_test 300	test_cnt_trigger test_trig.conf 151 21 "trigger on file count"
run_test 301    test_ost_trigger test_trig2.conf 150 110 "trigger on OST usage"
run_test 302	test_trigger_check test_trig3.conf 60 110 "triggers check only" 40 80 5 10 40
run_test 303    test_periodic_trigger test_trig4.conf 35 "periodic trigger"
run_test 304    test_ost_order test_trig2.conf "OST purge order"


#### reporting ####
run_test 400	test_rh_report common.conf 3 1 "reporting tool"
run_test 401a   test_rh_acct_report common.conf 5 "reporting tool: config file without acct param"
run_test 401b   test_rh_acct_report acct_user.conf 5 "reporting tool: config file with acct_user=true and acct_group=false"
run_test 401c   test_rh_acct_report acct_group.conf 5 "reporting tool: config file with acct_user=false and acct_group=true"
run_test 401d   test_rh_acct_report no_acct.conf 5 "reporting tool: config file with acct_user=false and acct_group=false"
run_test 401e   test_rh_acct_report acct_user_group.conf 5 "reporting tool: config file with acct_user=true and acct_group=true"
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
run_test 501a 	test_cfg_parsing basic none		"parsing of basic template"
run_test 501b 	test_cfg_parsing detailed none		"parsing of detailed template"
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


#### Tests by Sogeti ####
run_test 601 test_alerts Alert_Path_Name.conf "pathName" 0 "TEST_ALERT_PATH_NAME"
run_test 602 test_alerts Alert_Type.conf "type" 0 "TEST_ALERT_TYPE"
run_test 603 test_alerts Alert_Owner.conf "owner" 0 "TEST_ALERT_OWNER"
run_test 604 test_alerts Alert_Size.conf "size" 0 "TEST_ALERT_SIZE"
run_test 605 test_alerts Alert_LastAccess.conf "lastAccess" 60 "TEST_ALERT_LAST_ACCESS"
run_test 606 test_alerts Alert_LastModification.conf "lastModif" 60 "TEST_ALERT_LAST_MODIFICATION"
run_test 607 test_alerts_OST Alert_OST.conf "TEST_ALERT_OST"
run_test 608 test_alerts Alert_ExtendedAttribute.conf "extAttributes" 0 "TEST_ALERT_EXTENDED_ATTRIBUT"
run_test 609 test_alerts Alert_Dircount.conf "dircount" 0 "TEST_ALERT_DIRCOUNT"

run_test 610 test_migration MigrationStd_Path_Name.conf 0 3 "file.6;file.7;file.8" "--migrate" "TEST_test_migration_PATH_NAME"
run_test 611 test_migration MigrationStd_Type.conf 0 8 "file.1;file.2;file.3;file.4;file.5;file.6;file.7;file.8" "--migrate" "TEST_MIGRATION_STD_TYPE"
run_test 612 test_migration MigrationStd_Owner.conf 0 1 "file.3" "--migrate" "TEST_MIGRATION_STD_OWNER"
run_test 613 test_migration MigrationStd_Size.conf 0 2 "file.6;file.7" "--migrate" "TEST_MIGRATION_STD_SIZE"
run_test 614 test_migration MigrationStd_LastAccess.conf 12 9  "file.1;file.2;file.3;file.4;file.5;file.6;file.7;link.1;link.2" "--migrate" "TEST_MIGRATION_STD_LAST_ACCESS"
run_test 615 test_migration MigrationStd_LastModification.conf 31 2 "file.8;file.9" "--migrate" "TEST_MIGRATION_STD_LAST_MODIFICATION"
run_test 616 migration_OST MigrationStd_OST.conf 2 "file.3;file.4" "--migrate" "TEST_MIGRATION_STD_OST"
run_test 617 test_migration MigrationStd_ExtendedAttribut.conf 0 1 "file.4" "--migrate" "TEST_MIGRATION_STD_EXTENDED_ATTRIBUT"
run_test 618 migration_OST MigrationOST.conf 2 "file.3;file.4" "--migrate-ost=1" "TEST_MIGRATION_OST"
run_test 619 test_migration MigrationClass_Path_Name.conf 0 3 "file.6;file.7;file.8" "--migrate" "TEST_MIGRATION_CLASS_PATH_NAME"
run_test 620 test_migration MigrationClass_Type.conf 0 2 "link.1;link.2" "--migrate" "TEST_MIGRATION_CLASS_TYPE"
run_test 621 test_migration MigrationClass_Owner.conf 0 1 "file.3" "--migrate" "TEST_MIGRATION_CLASS_OWNER"
run_test 622 test_migration MigrationClass_Size.conf 0 2 "file.6;file.7" "--migrate" "TEST_MIGRATION_CLASS_SIZE"
run_test 623 test_migration MigrationClass_LastAccess.conf 31 8 "file.1;file.2;file.4;file.5;file.6;file.7;link.1;link.2" "--migrate" "TEST_MIGRATION_CLASS_LAST_ACCESS"
run_test 624 test_migration MigrationClass_LastModification.conf 31 2 "file.8;file.9" "--migrate" "TEST_MIGRATION_CLASS_LAST_MODIFICATION"
run_test 625 migration_OST MigrationClass_OST.conf 2 "file.3;file.4" "--migrate" "TEST_MIGRATION_CLASS_OST"
run_test 626 test_migration MigrationClass_ExtendedAttribut.conf 0 1 "file.4" "--migrate" "TEST_MIGRATION_CLASS_EXTENDED_ATTRIBUT"
run_test 627 test_migration MigrationUser.conf 0 1 "file.3" "--migrate-user=testuser" "TEST_MIGRATION_USER"
run_test 628 test_migration MigrationGroup.conf 0 2 "file.2;file.3" "--migrate-group=testgroup" "TEST_MIGRATION_GROUP"
run_test 629 test_migration MigrationFile_Path_Name.conf 0 1 "file.1" "--migrate-file=$ROOT/dir1/file.1" "TEST_MIGRATION_FILE_PATH_NAME"
run_test 630 migration_file_type MigrationFile_Type.conf 0 1 "link.1" "TEST_MIGRATION_FILE_TYPE"
run_test 631 migration_file_owner MigrationFile_Owner.conf 0 1 "file.3" "--migrate-file=$ROOT/dir1/file.3" "TEST_MIGRATION_FILE_OWNER"
run_test 632 test_migration MigrationFile_Size.conf 1 1 "file.8" "--migrate-file=$ROOT/dir2/file.8" "TEST_MIGRATION_FILE_SIZE"
run_test 633 migration_file_Last MigrationFile_LastAccess.conf 12 1 "file.1" "TEST_MIGRATION_FILE_LAST_ACCESS"
run_test 634 migration_file_Last MigrationFile_LastModification.conf 12 1 "file.1" "TEST_MIGRATION_FILE_LAST_MODIFICATION"
run_test 635 migration_file_OST MigrationFile_OST.conf 1 "file.3" "TEST_MIGRATION_FILE_OST"
run_test 636 migration_file_ExtendedAttribut MigrationFile_ExtendedAttribut.conf 0 1 "file.4"  "TEST_MIGRATION_FILE_EXTENDED_ATTRIBUT"

run_test 637 trigger_purge_QUOTA_EXCEEDED TriggerPurge_QuotaExceeded.conf "TEST_TRIGGER_PURGE_QUOTA_EXCEEDED"
run_test 638 trigger_purge_OST_QUOTA_EXCEEDED TriggerPurge_OstQuotaExceeded.conf "TEST_TRIGGER_PURGE_OST_QUOTA_EXCEEDED"
run_test 639 trigger_purge_USER_GROUP_QUOTA_EXCEEDED TriggerPurge_UserQuotaExceeded.conf "User 'root'" "TEST_TRIGGER_PURGE_USER_QUOTA_EXCEEDED"
run_test 640 trigger_purge_USER_GROUP_QUOTA_EXCEEDED TriggerPurge_GroupQuotaExceeded.conf "Group 'root'" "TEST_TRIGGER_PURGE_GROUP_QUOTA_EXCEEDED"

run_test 641 test_purge PurgeStd_Path_Name.conf 0 7 "file.6;file.7;file.8" "--purge" "TEST_PURGE_STD_PATH_NAME"
run_test 642 test_purge_tmp_fs_mgr PurgeStd_Type.conf 0 8 "link.1;link.2" "--purge" "TEST_PURGE_STD_TYPE"
run_test 643 test_purge PurgeStd_Owner.conf 0 9 "file.3" "--purge" "TEST_PURGE_STD_OWNER"
run_test 644 test_purge PurgeStd_Size.conf 0 8 "file.6;file.7" "--purge" "TEST_PURGE_STD_SIZE"
run_test 645 test_purge PurgeStd_LastAccess.conf 10 9 "file.8" "--purge" "TEST_PURGE_STD_LAST_ACCESS"
run_test 646 test_purge PurgeStd_LastModification.conf 30 9 "file.8" "--purge" "TEST_PURGE_STD_LAST_MODIFICATION"
run_test 647 purge_OST PurgeStd_OST.conf 2 "file.3;file.4" "--purge" "TEST_PURGE_STD_OST"
run_test 648 test_purge PurgeStd_ExtendedAttribut.conf 0 9 "file.4" "--purge" "TEST_PURGE_STD_EXTENDED_ATTRIBUT"
run_test 649 purge_OST PurgeOST.conf 2 "file.3;file.4" "--purge-ost=1,0" "TEST_PURGE_OST"
run_test 650 test_purge PurgeClass_Path_Name.conf 0 9 "file.1" "--purge" "TEST_PURGE_CLASS_PATH_NAME"
run_test 651 test_purge PurgeClass_Type.conf 0 2 "file.1;file.2;file.3;file.4;file.5;file.6;file.7;file.8" "--purge" "TEST_PURGE_CLASS_TYPE"
run_test 652 test_purge PurgeClass_Owner.conf 0 3 "file.1;file.2;file.4;file.5;file.6;file.7;file.8" "--purge" "TEST_PURGE_CLASS_OWNER"
run_test 653 test_purge PurgeClass_Size.conf 0 8 "file.6;file.7" "--purge" "TEST_PURGE_CLASS_SIZE"
run_test 654 test_purge PurgeClass_LastAccess.conf 60 9 "file.8" "--purge" "TEST_PURGE_CLASS_LAST_ACCESS"
run_test 655 test_purge PurgeClass_LastModification.conf 60 9 "file.8" "--purge" "TEST_PURGE_CLASS_LAST_MODIFICATION"
run_test 656 purge_OST PurgeClass_OST.conf 2 "file.3;file.4" "--purge" "TEST_PURGE_CLASS_OST"
run_test 657 test_purge PurgeClass_ExtendedAttribut.conf 0 9 "file.4" "--purge" "TEST_PURGE_CLASS_EXTENDED_ATTRIBUT"

run_test 658 test_removing RemovingEmptyDir.conf "emptyDir" 31 "TEST_REMOVING_EMPTY_DIR"
run_test 659 test_removing RemovingDir_Path_Name.conf "pathName" 0 "TEST_REMOVING_DIR_PATH_NAME"
run_test 660 test_removing RemovingDir_Owner.conf "owner" 0 "TEST_REMOVING_DIR_OWNER"
run_test 661 test_removing RemovingDir_LastAccess.conf "lastAccess" 31 "TEST_REMOVING_DIR_LAST_ACCESS"
run_test 662 test_removing RemovingDir_LastModification.conf "lastModif" 31 "TEST_REMOVING_DIR_LAST_MODIFICATION"
run_test 663 test_removing_ost RemovingDir_OST.conf "TEST_REMOVING_DIR_OST"
run_test 664 test_removing RemovingDir_ExtendedAttribute.conf "extAttributes" 0 "TEST_REMOVING_DIR_EXTENDED_ATTRIBUT"
run_test 665 test_removing RemovingDir_Dircount.conf "dircount" 0 "TEST_REMOVING_DIR_DIRCOUNT"

run_test 666 test_report_generation_1 Generation_Report_1.conf "TEST_REPORT_GENERATION_1"
run_test 667 report_generation2 "TEST_REPORT_GENERATION_2"

run_test 668 TEST_OTHER_PARAMETERS_1 OtherParameters_1.conf "TEST_OTHER_PARAMETERS_1"
run_test 669 TEST_OTHER_PARAMETERS_2 OtherParameters_2.conf "TEST_OTHER_PARAMETERS_2"
run_test 670 TEST_OTHER_PARAMETERS_3 OtherParameters_3.conf "TEST_OTHER_PARAMETERS_3"
run_test 671 TEST_OTHER_PARAMETERS_4 OtherParameters_4.conf "TEST_OTHER_PARAMETERS_4"
run_test 672 TEST_OTHER_PARAMETERS_5 OtherParameters_5.conf "TEST_OTHER_PARAMETERS_5"

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
	junit_write_xml "$dur" $RC $(( $RC + $SUCCES ))
	rm -f $TMPXML_PREFIX.stderr $TMPXML_PREFIX.stdout $TMPXML_PREFIX.tc
fi

rm -f $SUMMARY
if (( $RC > 0 )); then
	echo "$RC tests FAILED, $SUCCES successful, $SKIP skipped"
else
	echo "All tests passed ($SUCCES successful, $SKIP skipped)"
fi
rm -f $TMPERR_FILE
exit $RC
