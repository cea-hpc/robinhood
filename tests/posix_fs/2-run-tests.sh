#!/bin/bash

ROOT="/tmp/mnt.rbh"
BKROOT="/tmp/backend"
RBH_OPT=""
DB=robinhood_test

RBH_BINDIR="../../src/robinhood"
#RBH_BINDIR="/usr/sbin"

XML="test_report.xml"
TMPXML_PREFIX="/tmp/report.xml.$$"
TMPERR_FILE="/tmp/err_str.$$"

TEMPLATE_DIR='../../doc/templates'

if [[ -z "$PURPOSE" || $PURPOSE = "TMP_FS_MGR" ]]; then
	is_hsmlite=0
	RH="$RBH_BINDIR/robinhood $RBH_OPT"
	REPORT="$RBH_BINDIR/rbh-report $RBH_OPT"
	FIND=$RBH_BINDIR/rbh-find
	DU=$RBH_BINDIR/rbh-du
    DIFF=$RBH_BINDIR/rbh-diff
	CMD=robinhood
	PURPOSE="TMP_FS_MGR"
	REL_STR="Purged"

elif [[ $PURPOSE = "HSM_LITE" ]]; then
	is_hsmlite=1
	RH="$RBH_BINDIR/rbh-hsmlite $RBH_OPT"
	REPORT="$RBH_BINDIR/rbh-hsmlite-report $RBH_OPT"
	FIND=$RBH_BINDIR/rbh-hsmlite-find
	DU=$RBH_BINDIR/rbh-hsmlite-du
    DIFF=$RBH_BINDIR/rbh-hsmlite-diff
	CMD=rbh-hsmlite
fi

PROC=$CMD
CFG_SCRIPT="../../scripts/rbh-config"
CLEAN="rh_scan.log rh_migr.log rh_rm.log rh.pid rh_purge.log rh_report.log rh_syntax.log /tmp/rh_alert.log rh_rmdir.log rh_chglogs.log"

SUMMARY="/tmp/test_${PROC}_summary.$$"

ERROR=0
RC=0
SKIP=0
SUCCES=0
DO_SKIP=0

#notes: root belongs to the group 'testgroup' and a new user 'testuser' has been already added, he belongs to the
#same group
# groupadd testgroup
# useradd testuser -b $ROOT -G testgroup
# sassurer que root  aie bien testgroup
function error_reset
{
	ERROR=0
	DO_SKIP=0
	cp /dev/null $TMPERR_FILE
}

function error
{
	echo "ERROR $@"
	((ERROR=$ERROR+1))

	if (($junit)); then
	 	grep -i "error" *.log | grep -v "(0 errors)" | grep -v "LastScanErrors" >> $TMPERR_FILE
		echo "ERROR $@" >> $TMPERR_FILE
    fi
    # avoid displaying the same log many times
    # but preserve log files for DEBUG mode
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


function clean_fs
{
	echo "Cleaning filesystem..."
	if [[ -n "$ROOT" ]]; then
		rm  -rf $ROOT/*
	fi

#	if (( $is_hsmlite != 0 )); then
#		if [[ -n "$BKROOT" ]]; then
#			rm -rf $BKROOT/*
#		fi
#	fi

	echo "Destroying any running instance of robinhood..."
	pkill robinhood
	pkill rbh-backup

	if [ -f rh.pid ]; then
		echo "killing remaining robinhood process..."
		kill `cat rh.pid`
		rm -f rh.pid
	fi

	sleep 1
#	echo "Impacting rm in HSM..."
#	$RH -f ./cfg/immediate_rm.conf --scan --hsm-remove -l DEBUG -L rh_rm.log --once || error "deferred rm"
	echo "Cleaning robinhood's DB..."
	$CFG_SCRIPT empty_db $DB > /dev/null
}

function get_id
{
    stat -c "/%i" $1
}

function check_db_error
{
	grep DB_REQUEST_FAILED $1 && error "DB request error"
}

function migration_test
{
	config_file=$1
	expected_migr=$2
	sleep_time=$3
	policy_str="$4"

	if (( $is_hsmlite == 0 )); then
		echo "hsmlite test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# create and fill 10 files

	echo "1-Writing files..."
	for i in a `seq 1 10`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"
	done

	echo "2-Scanning filesystem..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "performing FS scan"
	check_db_error rh_scan.log

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error "performing migration"

	nb_migr=`grep "Start archiving" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != 0)); then
		error "********** TEST FAILED: No migration expected, $nb_migr started"
	else
		echo "OK: no files migrated"
	fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "5-Applying migration policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once

	nb_migr=`grep "Start archiving" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != $expected_migr)); then
		error "********** TEST FAILED: $expected_migr migrations expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"
	fi
}

# test rmdir policies
function test_rmdir
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	if [[ -n $is_backup ]] && (( $is_backup != 0 )); then
		echo "No rmdir policy for hsm flavors: skipped"
		set_skipped
		return 1
	fi

	clean_logs

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

	echo "2-Scanning..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once 2>/dev/null || error "scanning filesystem"
	check_db_error rh_scan.log

	echo "3-Applying rmdir policy ($policy_str)..."
	# files should not be migrated this time: do not match policy
	$RH -f ./cfg/$config_file --rmdir --once -l EVENT -L rh_purge.log 2>/dev/null

	grep "Empty dir removal summary" rh_purge.log || error "did not file summary line in log"
	grep "Recursive dir removal summary" rh_purge.log || error "did not file summary line in log"

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
		error "$cnt_resurs directories removed (2 expected)"
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


function xattr_test
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	if (( $is_hsmlite == 0 )); then
		echo "hsmlite test only: skipped"
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

	# scanning filesystem
	echo "3-Scanning..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

	echo "4-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error "performing migration"

	nb_migr=`grep "Start archiving" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != 0)); then
		error "********** TEST FAILED: No migration expected, $nb_migr started"
	else
		echo "OK: no files migrated"
	fi

	echo "5-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "6-Applying migration policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once

	nb_migr=`grep "Start archiving" rh_migr.log | grep hints |  wc -l`
	if (($nb_migr != 3)); then
		error "********** TEST FAILED: $expected_migr migrations expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"

		if (( $is_hsmlite != 0 )); then
			# checking policy
			nb_migr_arch1=`grep "fileclass=xattr_bar" rh_migr.log | wc -l`
			nb_migr_arch2=`grep "fileclass=xattr_foo" rh_migr.log | wc -l`
			nb_migr_arch3=`grep "using policy 'default'" rh_migr.log | wc -l`
			if (( $nb_migr_arch1 != 1 || $nb_migr_arch2 != 1 || $nb_migr_arch3 != 1 )); then
				error "********** wrong policy cases: 1x$nb_migr_arch1/2x$nb_migr_arch2/3x$nb_migr_arch3 (1x1/2x1/3x1 expected)"
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

	if (( $is_hsmlite == 0 )); then
		echo "hsmlite test only: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	$RH -f ./cfg/$config_file --scan --once -l DEBUG  -L rh_migr.log || error "scanning filesystem"
	check_db_error rh_migr.log

	# write file.1 and force immediate migration
	echo "1-Writing data to file.1..."
	dd if=/dev/zero of=$ROOT/file.1 bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.1"

	$RH -f ./cfg/$config_file --sync -l DEBUG  -L rh_migr.log || error "sync'ing data"

	# create links on file.1 files
	echo "2-Creating hard links to $ROOT/file.1..."
	ln $ROOT/file.1 $ROOT/link.1 || error "creating hardlink"
	ln $ROOT/file.1 $ROOT/link.2 || error "creating hardlink"

	# removing all files
    echo "3-Removing all links to file.1..."
	rm -f $ROOT/link.* $ROOT/file.1

	# deferred remove delay is not reached: nothing should be removed
	echo "4-Performing HSM remove requests (before delay expiration)..."
	$RH -f ./cfg/$config_file --hsm-remove -l DEBUG -L rh_rm.log --once || error "performing deferred removal"

	nb_rm=`grep "Remove request successful" rh_rm.log | wc -l`
	if (($nb_rm != 0)); then
		echo "********** test failed: no removal expected, $nb_rm done"
	else
		echo "OK: no rm done"
	fi

	echo "5-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "6-Performing HSM remove requests (after delay expiration)..."
	$RH -f ./cfg/$config_file --hsm-remove -l DEBUG -L rh_rm.log --once || error "erforming deferred removal"

	nb_rm=`grep "Remove request successful" rh_rm.log | wc -l`
	if (($nb_rm != $expected_rm)); then
		error "********** TEST FAILED: $expected_rm removals expected, $nb_rm done"
	else
		echo "OK: $nb_rm files removed from archive"
	fi

	# kill event handler
	pkill -9 $PROC

}

function purge_test
{
	config_file=$1
	expected_purge=$2
	sleep_time=$3
	policy_str="$4"

	if (( $is_hsmlite != 0 )); then
		echo "No purge for hsmlite purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# initial scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log
	check_db_error rh_scan.log

	# fill 10 files and mark them archived+non dirty

	echo "1-Modifing files..."
	for i in a `seq 1 10`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"
	done

	sleep 1
	echo "2-Scanning the FS again to update file status (after 1sec)..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

	echo "3-Applying purge policy ($policy_str)..."
	# no purge expected here
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || error "purging files"

    nb_purge=`grep "Purged" rh_purge.log | wc -l`

    if (($nb_purge != 0)); then
            error "********** TEST FAILED: No release actions expected, $nb_purge done"
    else
            echo "OK: no file released"
    fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "5-Applying purge policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || error "purging files"

    nb_purge=`grep "Purged" rh_purge.log | wc -l`

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

	if (( $is_hsmlite != 0 )); then
		echo "No purge for hsmlite purpose: skipped"
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

	echo "Applying purge policy ($policy_str)..."
	# no purge expected here
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || error "purging files"
	check_db_error rh_purge.log

	nb_purge=`grep "Purged" rh_purge.log | wc -l`
	if (($nb_purge != 13)); then
		error "********** TEST FAILED: 13 purge actions expected, $nb_purge done"
	else
		echo "OK: 13 actions done"
	fi

	# checking that the custom command was called for each
	for  i in `seq 1 10`; do
		grep "Executing " rh_purge.log | grep '/bin/rm' | grep $ROOT/file.$i || error "No action found on $ROOT/file.$i"
        # check the entry has been deleted
        [ -f $ROOT/file.$i ] && error "$ROOT/file.$i still exists after purge command"
	done

    for f in  "$ROOT/foo1 \`pkill -9 $CMD\`" "$ROOT/foo2 ; exit 1" "$ROOT/foo3' ';' 'exit' '1'" ; do
        f0=$(echo "$f" | awk '{print $1}')
		line=$(grep "Executing " rh_purge.log | grep '/bin/rm' | grep "$f0")
        if [ -z "$line" ]; then
            error "No action found on $f"
            continue
        fi
        [ -f "$f" ] && error "$f still exists after purge command"
    done

	return 0
}


function purge_size_filesets
{
	config_file=$1
	sleep_time=$2
	count=$3
	policy_str="$4"

	if (( $is_hsmlite != 0 )); then
		echo "No purge for hsmlite purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# initial scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log
	check_db_error rh_scan.log

	# fill 3 files of different sizes and mark them archived non-dirty

	j=1
	for size in 0 1 10 200; do
		echo "1.$j-Writing files of size " $(( $size*10 )) "kB..."
		((j=$j+1))
		for i in `seq 1 $count`; do
			dd if=/dev/zero of=$ROOT/file.$size.$i bs=10k count=$size >/dev/null 2>/dev/null || error "writing file.$size.$i"
		done
	done

	sleep 1
	echo "2-Scanning..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

	echo "3-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "4-Applying purge policy ($policy_str)..."
	# no purge expected here
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || error "purging files"

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

	echo "2.Scanning..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

	echo "3.Checking reports..."
	# posix FS do some block preallocation, so we don't know the exact space used:
	# compare with 'du' return instead.
	for i in `seq 1 $dircount`; do
		real=`du -B 512 -c $ROOT/dir.$i/* | grep total | awk '{print $1}'`
		real=`echo "$real*512" | bc -l`
		$REPORT -f ./cfg/$config_file -l MAJOR --csv  -U 1 -P "$ROOT/dir.$i/*" > rh_report.log
		used=`tail -n 1 rh_report.log | cut -d "," -f 3`
		if (( $used != $real )); then
			error ": $used != $real"
		else
			echo "OK: space used by files in $ROOT/dir.$i is $real bytes"
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
                        dd if=/dev/zero of=$ROOT/dir.$i/file.$j bs=1M count=1 >/dev/null 2>/dev/null || error "writing $ROOT/dir.$i/file.$j"
                done
        done

        echo "1bis. Wait for IO completion..."
        sync

	echo "2-Scanning..."
        $RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

	echo "2bis.Checking reports..."
	compare_reports_with_F $config_file

	echo "3-Remove a file and scan again..."
	# remove one file and scan again
	rm -f $ROOT/dir.1/file.1 || error "couldn't remove file $ROOT/dir.1/file.1"
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

	echo "3bis.Checking reports..."
	compare_reports_with_F $config_file

	echo "4-Truncate a file and scan again..."
	# truncate one file and scan again
	cp /dev/null $ROOT/dir.2/file.2 || error "couldn't truncate file $ROOT/dir.2/file.2"
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

	echo "4bis.Checking reports..."
	compare_reports_with_F $config_file
}

function compare_reports_with_F
{
	config_file=$1

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
	config_file_scan=$1
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
        $RH -f ./cfg/$config_file_scan --scan -l VERB -L rh_scan.log  --once || error "scanning filesystem"
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
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once 2>/dev/null || error "reading chglog"
	check_db_error rh_scan.log

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

	echo "2-Scanning..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once 2>/dev/null || error "reading chglog"
	check_db_error rh_scan.log

	echo "3.Checking dircount report..."
	# dircount+1 because $ROOT may be returned
	$REPORT -f ./cfg/$config_file --topdirs=$((dircount+1)) --csv > report.out

    [ "$DEBUG" = "1" ] && cat report.out
	# check that dircount is right for each dir

	# check if $ROOT is in topdirs. If so, check its position
	# else, check with its path and its dev/ino
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
		line=`grep "$ROOT/dir.$i," report.out`
		rank=`echo $line | cut -d ',' -f 1 | tr -d ' '`
		count=`echo $line | cut -d ',' -f 3 | tr -d ' '`
        avg=`echo $line | cut -d ',' -f 4 | tr -d ' '`
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
}


function path_test
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	if (( $is_hsmlite == 0 )); then
		echo "hsmlite test only: skipped"
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
	# 2 matching files for fileclass tree_depth2
	echo "data" > $ROOT/one_dir/dir4/subdir1/X
	echo "data" > $ROOT/other_dir/dir4/subdir1/X
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
	# 2 matching files for fileclass relative_tree
	echo "data" > $ROOT/dir6/A
	echo "data" > $ROOT/dir6/subdir/A
	# 2 unmatching files for fileclass relative_tree
	echo "data" > $ROOT/subdir/dir6/A
	echo "data" > $ROOT/subdir/dir6/B


	mkdir -p $ROOT/dir7/subdir
	mkdir -p $ROOT/dir71/subdir
	mkdir -p $ROOT/subdir/subdir/dir7
	mkdir -p $ROOT/subdir/subdir/dir72
	# 2 matching files for fileclass any_root_tree
	echo "data" > $ROOT/dir7/subdir/file
	echo "data" > $ROOT/subdir/subdir/dir7/file
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
	# 2 matching files for fileclass any_level_tree
	echo "data" > $ROOT/dir9/subdir/dir10/file
	echo "data" > $ROOT/dir9/subdir/dir10/subdir/file
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

	echo "2-Scanning..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
    	check_db_error rh_scan.log

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error "migrating data"

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
	(( $nb_pol4 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'tree_depth2': $nb_pol4"
	(( $nb_pol5 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'relative_path': $nb_pol5"
	(( $nb_pol6 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'relative_tree': $nb_pol6"

	(( $nb_pol7 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'any_root_tree': $nb_pol7"
	(( $nb_pol8 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'any_root_path': $nb_pol8"
	(( $nb_pol9 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'any_level_tree': $nb_pol9"
	(( $nb_pol10 == 2 )) || error "********** TEST FAILED: wrong count of matching files for policy 'any_level_tree': $nb_pol10"
	(( $nb_unmatch == 19 )) || error "********** TEST FAILED: wrong count of unmatching files: $nb_unmatch"

	(( $nb_pol1 == 2 )) && (( $nb_pol2 == 2 )) && (( $nb_pol3 == 2 )) && (( $nb_pol4 == 2 )) \
        	&& (( $nb_pol5 == 2 )) && (( $nb_pol6 == 2 )) && (( $nb_pol7 == 2 )) \
		&& (( $nb_pol8 == 2 )) && (( $nb_pol9 == 2 )) && (( $nb_pol10 == 2 )) \
		&& (( $nb_unmatch == 19 )) \
		&& echo "OK: test successful"
}


function periodic_class_match_migr
{
	config_file=$1
	update_period=$2
	policy_str="$3"

	if (( $is_hsmlite == 0 )); then
		echo "hsmlite test only: skipped"
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
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning filesystem"
    	check_db_error rh_scan.log

	# now apply policies
	$RH -f ./cfg/$config_file --migrate --dry-run -l FULL -L rh_migr.log --once || error "migrating data"

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
	$RH -f ./cfg/$config_file --migrate --dry-run -l FULL -L rh_migr.log --once || error "migrating data"

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
	$RH -f ./cfg/$config_file --migrate --dry-run -l FULL -L rh_migr.log --once || error "migrating data"

	nb_valid=`grep "is still valid" rh_migr.log | wc -l`
	nb_updt=`grep "Need to update fileclass (out-of-date)" rh_migr.log | wc -l`

	(( $nb_valid == 0 )) || error "********** TEST FAILED: fileclass should need update : $nb_valid still valid"
	(( $nb_updt == 4 )) || error "********** TEST FAILED: all fileclasses should be updated : $nb_updt/4"

        (( $nb_valid == 0 )) && (( $nb_updt == 4 )) \
		&& echo "OK: all fileclasses updated"
}

function periodic_class_match_purge
{
	config_file=$1
	update_period=$2
	policy_str="$3"

	if (( $is_hsmlite != 0 )); then
		echo "No purge for hsmlite purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

	#create test tree of archived files
	for file in ignore1 whitelist1 purge1 default1 ; do
		touch $ROOT/$file
	done

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log
    	check_db_error rh_scan.log

	# now apply policies
	$RH -f ./cfg/$config_file --purge-fs=0 --dry-run -l FULL -L rh_purge.log --once || error "purging files"

	already=0

	nb_updt=`grep "Need to update fileclass (not set)" rh_purge.log | wc -l`
	nb_purge_match=`grep "matches the condition for policy 'purge_match'" rh_purge.log | wc -l`
	nb_default=`grep "matches the condition for policy 'default'" rh_purge.log | wc -l`

	(( $nb_updt == 4 - $already )) || error "********** TEST FAILED: wrong count of fileclass update: $nb_updt"
	(( $nb_purge_match == 1 )) || error "********** TEST FAILED: wrong count of files matching 'purge_match': $nb_purge_match"
	(( $nb_default == 1 )) || error "********** TEST FAILED: wrong count of files matching 'default': $nb_default"

        (( $nb_updt == 4 - $already )) && (( $nb_purge_match == 1 )) && (( $nb_default == 1 )) \
		&& echo "OK: initial fileclass matching successful"

	# update db content and rematch entries: should update all fileclasses
	clean_logs
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log
    	check_db_error rh_scan.log

	echo "Waiting $update_period sec..."
	sleep $update_period

	$RH -f ./cfg/$config_file --purge-fs=0 --dry-run -l FULL -L rh_purge.log --once || error "purging files"

	# TMP_FS_MGR:  whitelisted status is always checked at scan time
	# 	2 entries are new (default and to_be_released)
	already=0
	new=2

	nb_valid=`grep "is still valid" rh_purge.log | wc -l`
	nb_updt=`grep "Need to update fileclass (out-of-date)" rh_purge.log | wc -l`
	nb_not_set=`grep "Need to update fileclass (not set)" rh_purge.log | wc -l`

	(( $nb_valid == $already )) || error "********** TEST FAILED: fileclass should need update : $nb_valid still valid"
	(( $nb_updt == 4 - $already - $new )) || error "********** TEST FAILED: wrong number of fileclasses should be updated : $nb_updt"
	(( $nb_not_set == $new )) || error "********** TEST FAILED:  wrong number of fileclasse fileclasses should be matched : $nb_not_set"

        (( $nb_valid == $already )) && (( $nb_updt == 4 - $already - $new )) \
		&& echo "OK: fileclasses correctly updated"
}

function test_cnt_trigger
{
	config_file=$1
	file_count=$2
	exp_purge_count=$3
	policy_str="$4"

	if (( $is_hsmlite != 0 )); then
		echo "No purge for hsmlite purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

	# initial inode count
	empty_count=`df -i $ROOT/ | grep "$ROOT" | xargs | awk '{print $(NF-3)}'`
	(( file_count=$file_count - $empty_count ))

	#create test tree of archived files (1M each)
	for i in `seq 1 $file_count`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "writting $ROOT/file.$i"
	done

	# wait for df sync
	sync; sleep 1

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log
    	check_db_error rh_scan.log

	# apply purge trigger
	$RH -f ./cfg/$config_file --purge --once -l FULL -L rh_purge.log

	nb_release=`grep "Purged" rh_purge.log | wc -l`

	if (($nb_release == $exp_purge_count)); then
		echo "OK: $nb_release files released"
	else
		error ": $nb_release files released, $exp_purge_count expected"
	fi
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
	target_user_count=$8

	if (( $is_hsmlite != 0 )); then
		echo "No purge for hsmlite purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

	# triggers to be checked
	# - inode count > max_count
	# - fs volume	> max_vol
	# - root quota  > user_quota

	# initial inode count
	empty_count=`df -i $ROOT/ | xargs | awk '{print $(NF-3)}'`
	empty_count_user=0
	#((file_count=$max_count-$empty_count))
	file_count=$max_count

	# compute file size to exceed max vol and user quota
	empty_vol=`df -k $ROOT  | xargs | awk '{print $(NF-3)}'`
	((empty_vol=$empty_vol/1024))

	if (( $empty_vol < $max_vol_mb )); then
		((missing_mb=$max_vol_mb-$empty_vol))
	else
		missing_mb=0
	fi

	# file_size = missing_mb/file_count + 1
	((file_size=$missing_mb/$file_count + 1 ))

	echo "$file_count files missing, $file_size MB each"

	#create test tree of archived files (file_size MB each)
	for i in `seq 1 $file_count`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=$file_size >/dev/null 2>/dev/null || error "writting $ROOT/file.$i"

	done

	# wait for df sync
	sync; sleep 1

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log
    	check_db_error rh_scan.log

	# check purge triggers
	$RH -f ./cfg/$config_file --check-thresholds --once -l FULL -L rh_purge.log

	((expect_count=$empty_count+$file_count-$target_count))
	((expect_vol_fs=$empty_vol+$file_count*$file_size-$target_fs_vol))
	((expect_vol_user=$file_count*$file_size-$target_user_vol))
	((expect_count_user=$empty_count_user+$file_count-$target_user_count))

	echo "over trigger limits: $expect_count entries, $expect_vol_fs MB, $expect_vol_user MB for user root, $expect_count_user entries for user root"

	nb_release=`grep "Purged" rh_purge.log | wc -l`

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

function test_periodic_trigger
{
	config_file=$1
	sleep_time=$2
	policy_str=$3

	if (( $is_hsmlite != 0 )); then
		echo "No purge for hsmlite purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

	# create 3 files of each type
	# (*.1, *.2, *.3, *.4)
	for i in `seq 1 4`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=1 >/dev/null 2>/dev/null || error "$? writting $ROOT/file.$i"
		dd if=/dev/zero of=$ROOT/foo.$i bs=1M count=1 >/dev/null 2>/dev/null || error "$? writting $ROOT/foo.$i"
		dd if=/dev/zero of=$ROOT/bar.$i bs=1M count=1 >/dev/null 2>/dev/null || error "$? writting $ROOT/bar.$i"
	done

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log
    	check_db_error rh_scan.log

	# make sure files are old enough
	sleep 2

	# start periodic trigger in background
	$RH -f ./cfg/$config_file --purge -l DEBUG -L rh_purge.log &
	sleep 2

	# it first must have purged *.1 files (not others)
	[ -f $ROOT/file.1 ] && error "$ROOT/file.1 should have been removed"
	[ -f $ROOT/foo.1 ] && error "$ROOT/foo.1 should have been removed"
	[ -f $ROOT/bar.1 ] && error "$ROOT/bar.1 should have been removed"
	[ -f $ROOT/file.2 ] || error "$ROOT/file.2 shouldn't have been removed"
	[ -f $ROOT/foo.2 ] || error "$ROOT/foo.2 shouldn't have been removed"
	[ -f $ROOT/bar.2 ] || error "$ROOT/bar.2 shouldn't have been removed"

	sleep $(( $sleep_time + 2 ))
	# now, *.2 must have been purged

	[ -f $ROOT/file.2 ] && error "$ROOT/file.2 should have been removed"
	[ -f $ROOT/foo.2 ] && error "$ROOT/foo.2 should have been removed"
	[ -f $ROOT/bar.2 ] && error "$ROOT/bar.2 should have been removed"
	[ -f $ROOT/file.3 ] || error "$ROOT/file.3 shouldn't have been removed"
	[ -f $ROOT/foo.3 ] || error "$ROOT/foo.3 shouldn't have been removed"
	[ -f $ROOT/bar.3 ] || error "$ROOT/bar.3 shouldn't have been removed"

	sleep $(( $sleep_time + 2 ))
	# now, it's *.3
	# *.4 must be preserved

	[ -f $ROOT/file.3 ] && error "$ROOT/file.3 should have been removed"
	[ -f $ROOT/foo.3 ] && error "$ROOT/foo.3 should have been removed"
	[ -f $ROOT/bar.3 ] && error "$ROOT/bar.3 should have been removed"
	[ -f $ROOT/file.4 ] || error "$ROOT/file.4 shouldn't have been removed"
	[ -f $ROOT/foo.4 ] || error "$ROOT/foo.4 shouldn't have been removed"
	[ -f $ROOT/bar.4 ] || error "$ROOT/bar.4 shouldn't have been removed"

	# final check: 3x "Purge summary: 3 entries"
	nb_pass=`grep "Purge summary: 3 entries" rh_purge.log | wc -l`
	if (( $nb_pass == 3 )); then
		echo "OK: triggered 3 times"
	else
        grep "Purge summary" rh_purge.log >&2
		error "unexpected trigger count $nb_pass"
	fi

	# terminate
	pkill -9 $PROC
}


function fileclass_test
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	if (( $is_hsmlite == 0 )); then
		echo "hsmlite test only: skipped"
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

	echo "2-Scanning..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
    	check_db_error rh_scan.log

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error "migrating data"

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

	# test scanning with strange names, etc...
	mkdir $ROOT'/dir with blanks'
	mkdir $ROOT'/dir with "quotes"'
	mkdir "$ROOT/dir with 'quotes'"

	touch $ROOT'/dir with blanks/file 1'
	touch $ROOT'/dir with blanks/file with "double" quotes'
	touch $ROOT'/dir with "quotes"/file with blanks'
	touch "$ROOT/dir with 'quotes'/file with 1 quote: '"

	sleep $sleep_time1

	echo "1-Scanning..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log
	nb_cr=0

	sleep $sleep_time2

	grep "DB query failed" rh_scan.log && error ": a DB query failed when scanning"

	nb_create=`grep ChangeLog rh_scan.log | grep 01CREAT | wc -l`
	nb_db_apply=`grep ': DB_APPLY' rh_scan.log | tail -1 | cut -d '|' -f 6 | cut -d ':' -f 2 |
		     cut -d ',' -f 1 | tr -d ' '`

	if (( $is_hsmlite != 0 )); then
		db_expect=4
	else
		db_expect=7
	fi
	# 4 files have been created, 4 db operations expected (files)
	# tmp_fs_mgr purpose: +3 for mkdir operations
	if (( $nb_create == $nb_cr && $nb_db_apply == $db_expect )); then
		echo "OK: $nb_cr files created, $db_expect database operations"
	else
		error ": unexpected number of operations: $nb_create files created, $nb_db_apply database operations"
		return 1
	fi

	clean_logs

	echo "2-Scanning..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	check_db_error rh_scan.log

	grep "DB query failed" rh_scan.log && error ": a DB query failed when scanning"
	nb_db_apply=`grep ': DB_APPLY' rh_scan.log | tail -1 | cut -d '|' -f 6 | cut -d ':' -f 2 |
		     cut -d ',' -f 1 |  tr -d ' '`

	# 4 db operations expected (1 for each file)
	if (( $nb_db_apply == $db_expect )); then
		echo "OK: $db_expect database operations"
	else
		error ": unexpected number of operations: $nb_db_apply database operations"
	fi
}

function scan_chk
{
       config_file=$1

       echo "Scanning..."
        $RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
       grep "DB query failed" rh_scan.log && error ": a DB query failed: `grep 'DB query failed' rh_scan.log | tail -1`"
       clean_logs
}

function test_info_collect2
{
       config_file=$1
       dummy=$2
       policy_str="$3"

       clean_logs

       # create 10k entries
       ../fill_fs.sh $ROOT 10000 >/dev/null

       scan_chk $config_file
       scan_chk $config_file
       scan_chk $config_file
}

function test_diff
{
	config_file=$1
	flavor=$2
	policy_str="$3"

	clean_logs

    # flavors:
    # scan + diff option (various)
    # diff (various), no apply
    # diff (various) + apply to DB

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
    nbrm=$(egrep '^--' report.out | grep -v -- '---' | wc -l)
    [ $nbrm  -eq 2 ] || error "$nbrm/2 removal"
    # changes
    grep "^+[^ ]*"$(get_id "$ROOT/dir.1") report.out  | grep mode= || error "missing chmod $ROOT/dir.1"
    grep "^+[^ ]*"$(get_id "$ROOT/dir.2") report.out | grep owner=testuser || error "missing chown $ROOT/dir.2"
    grep "^+[^ ]*"$(get_id "$ROOT/dir.1/a") report.out  | grep group=testgroup || error "missing chgrp $ROOT/dir.1/a"
    grep "^+[^ ]*"$(get_id "$ROOT/dir.1/c") report.out | grep size= || error "missing size change $ROOT/dir.1/c"
    grep "^+[^ ]*"$(get_id "$ROOT/dir.1/d") report.out | grep path= || error "missing path change $ROOT/dir.1/d"
    grep "^+[^ ]*"$(get_id "$ROOT/fname") report.out | grep path= || error "missing path change $ROOT/fname"

    # TODO check the content of the DB for scan and diff --apply
}

function test_diff_apply_fs # test diff --apply=fs in particular for entry recovery
{
    config_file=$1
    flavor=$2
    policy_str="$3"

    clean_logs
    # clean any previous files used for this test
    rm -f diff.out diff.log find.out find2.out
    
    # copy 2 instances /bin in the filesystem
    echo "Populating filesystem..."
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

    rm -f  diff.out diff.log find.out find2.out
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

    if [ "$flavor" = "diff" ]; then
        echo "2. Diff..."
    	$DIFF -f ./cfg/$config_file --apply=db -l DEBUG > rh_scan.log 2>&1 || error "scanning"
    else
        echo "2. Scanning initial state..."
    	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning"
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

    # hlink_ref hlink must be in report.out
    grep -E " $hlink$" report.out > /dev/null ||  grep -E " $hlink_ref$" report.out > /dev/null || error "$hlink or $hlink_ref must be in report output"
    # both hlink_ref hlink must be in find.out
    grep -E " $hlink$" find.out > /dev/null || error "$hlink must be in rbh-find output"
    grep -E " $hlink_ref$" find.out > /dev/null || error "$hlink must be in rbh-find output"

    count_nb_init=$(wc -l report.out | awk '{print $1}')
    count_path_init=$(wc -l find.out | awk '{print $1}')

    # rename entries
    echo "3. Renaming objects..."
    # simple file rename
    mv $ROOT/dir.1/file.1 $ROOT/dir.1/file.1.rnm
    # cross directory file rename
    mv $ROOT/dir.1/file.2 $ROOT/dir.2/file.2.rnm
    # upper level directory rename
    mv $ROOT/dir.3/subdir $ROOT/dir.3/subdir.rnm
    # overwritting a hardlink
    mv -f $ROOT/dir.2/file.4 $hlink
    # rename that deletes the target (do it at last
    # to avoid reusing the deleted inoded, which
    # would fail the test)
    mv -f $ROOT/dir.2/file.1 $ROOT/dir.2/file.2

    # namespace GC needs 1s difference
    sleep 1

    # readlog or re-scan
    if [ "$flavor" = "scan" ]; then
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

    grep "$rmid" find.out && error "id of deleted file ($rmid) found in rbh-find output"

    count_nb_final=$(wc -l report.out | awk '{print $1}')
    count_path_final=$(wc -l find.out | awk '{print $1}')

    (($count_nb_final == $count_nb_init - 1)) || error "1 entry should have been removed (rename target)"
    (($count_path_final == $count_path_init - 2)) || error "2 paths should have been removed (rename target)"

    rm -f report.out find.out
}


# test link/unlink/rename
# flavors=readlog, scan, partial scan
function test_hardlinks
{
    config_file=$1
    flavor=$2

    clean_logs

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
    if [ "$flavor" = "diff" ]; then
        echo "2. Diff..."
    	$DIFF -f ./cfg/$config_file --apply=db -l DEBUG > rh_scan.log 2>&1 || error "scanning"
    else
        echo "2. Scanning initial state..."
    	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_scan.log || error "scanning"
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
    (($count_path_init == $count_nb_init + $nb_ln)) || error "nb path != nb_inode + nb_ln"

    # rename entries
    echo "3. Linking/unlinking/renaming objects..."
    # simple file rename
    mv $ROOT/dir.1/file.1 $ROOT/dir.1/file.1.rnm
    # cross directory file rename
    mv $ROOT/dir.1/file.2 $ROOT/dir.2/file.2.rnm
    # upper level directory rename
    mv $ROOT/dir.3/subdir $ROOT/dir.3/subdir.rnm
    # overwritting a hardlink
    mv -f $ROOT/dir.2/file.4 ${hlinks[0]}
    ((nb_ln--))
    # creating new link to "dir.4/file.1"
    ln "$ROOT/dir.4/file.1" "$ROOT/dir.4/link.1"
    ((nb_ln++))
    # removing 1 link (dir.2/link.2 remains)
    rm "$ROOT/dir.4/file.2"
    ((nb_ln--))
    # creating 1 file
    touch "$ROOT/dir.1/new"
    # creating 1 file with hardlink
    touch "$ROOT/dir.2/new"
    ln "$ROOT/dir.2/new" "$ROOT/dir.4/link.new"
    ((nb_ln++))

    # delete at the end to avoid reusing deleted inum
    # which could fails the test
    # rename that deletes the target
    mv -f $ROOT/dir.2/file.1 $ROOT/dir.2/file.2
    # removing 1 file
    rm "$ROOT/dir.4/file.3"

    # namespace GC needs 1s difference
    sleep 1

    # readlog or re-scan
    if [ "$flavor" = "scan" ]; then
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
        egrep -E "^[0-9A-F]*$f" find.out && error "deleted id ($f) found in find output"
    done

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
    count_nb_final=$(wc -l report.out | awk '{print $1}')
    count_path_final=$(grep -v "$ROOT$" find.out | wc -l)

    echo "nbr_inodes=$count_nb_final, nb_paths=$count_path_final, nb_ln=$nb_ln"
    (($count_nb_final == $count_nb_init)) || error "same entry count ($count_nb_init) expected (2 deleted, 2 created)"
    (($count_path_final == $count_nb_final + $nb_ln)) || error "nb path != nb_inode + nb_ln"

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
    (($($FIND -f ./cfg/$config_file -nobulk $ROOT/dir.1 | wc -l) == $ino_subdir )) || error "wrong count in 'rbh-find <path>' output"

    #   dump summary (9 entries)
    $REPORT -f ./cfg/$config_file -icq > report.out
    [ "$DEBUG" = "1" ] && cat report.out
    typeValues="dir;file"
  	countValues="$dcount;$(($dcount * $fcount))"
    if (( $is_hsmlite != 0 )); then
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
    (($($FIND -f ./cfg/$config_file -nobulk $ROOT/dir.1 | wc -l) == $paths_subdir )) || error "wrong count in 'rbh-find <path>' output"

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


function test_logs
{
	config_file=$1
	flavor=$2
	policy_str="$3"

	sleep_time=430 # log rotation time (300) + scan interval (100) + scan duration (30)

	clean_logs
	rm -f /tmp/test_log.1 /tmp/test_report.1 /tmp/test_alert.1

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

	if (( $syslog )); then
		init_msg_idx=`wc -l /var/log/messages | awk '{print $1}'`
	fi

	# run a scan
	if (( $stdio )); then
		$RH -f ./cfg/$config_file --scan -l DEBUG --once >/tmp/rbh.stdout 2>/tmp/rbh.stderr || error "scanning filesystem"
	else
		$RH -f ./cfg/$config_file --scan -l DEBUG --once || error "scanning filesystem"
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
		grep -v ALERT /tmp/rbh.stdout | grep "$CMD[^ ]*\[" > /tmp/extract_report
		alert="/tmp/extract_alert"
		report="/tmp/extract_report"
	elif (( $syslog )); then
        # wait for syslog to flush logs to disk
        sync; sleep 2
		tail -n +"$init_msg_idx" /var/log/messages | grep $CMD > /tmp/extract_all
		egrep -v 'ALERT' /tmp/extract_all | grep  ': [A-Za-z_ ]* \|' > /tmp/extract_log
		egrep -v 'ALERT|: [A-Za-z_ ]* \|' /tmp/extract_all > /tmp/extract_report
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
			$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG --dry-run >/tmp/rbh.stdout 2>/tmp/rbh.stderr || error "purging files"
		else
			$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG --dry-run || error "purging files"
		fi

		# extract new syslog messages
		if (( $syslog )); then
            # wait for syslog to flush logs to disk
            sync; sleep 2
			tail -n +"$init_msg_idx" /var/log/messages | grep $CMD > /tmp/extract_all
			egrep -v 'ALERT' /tmp/extract_all | grep  ': [A-Za-Z ]* \|' > /tmp/extract_log
			egrep -v 'ALERT|: [A-Za-Z ]* \|' /tmp/extract_all > /tmp/extract_report
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
		else
			TEMPLATE=$TEMPLATE_DIR"/tmp_fs_mgr_basic.conf"
		fi

	elif [[ $flavor == "detailed" ]]; then

		if (($is_hsmlite)) ; then
			TEMPLATE=$TEMPLATE_DIR"/hsmlite_detailed.conf"
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
	touch $ROOT/dir.2/file.2 || error "creating file"
    mkdir $ROOT/dir.2/dir.1 || error "creating dir"
    mkdir $ROOT/dir.2/dir.2 || error "creating dir"
    dd if=/dev/zero of=$ROOT/dir.2/dir.2/file.1 bs=1M count=1 2>/dev/null || error "creating file"
	touch $ROOT/dir.2/dir.2/file.2 || error "creating file"
    mkdir $ROOT/dir.2/dir.2/dir.1 || error "creating dir"

    # scan FS content
    $RH -f $cfg --scan -l DEBUG -L rh_scan.log --once 2>/dev/null || error "scanning"

    # 2) test find at several levels
    echo "checking find list at all levels..."
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
    check_find $ROOT "-f $cfg -type d" 6 # 6 including root
    check_find $ROOT/dir.2 "-f $cfg -type d" 4 # 4 including dir.2
    check_find $ROOT/dir.2/dir.2 "-f $cfg -type d" 2 # 2 including dir.2/dir.2
    check_find $ROOT/dir.1 "-f $cfg -type d" 1
    check_find $ROOT/dir.2/dir.1 "-f $cfg -type d" 1
    check_find $ROOT/dir.2/dir.2/dir.1 "-f $cfg -type d" 1

    echo "testing type filter (-type f)..."
    check_find $ROOT "-f $cfg -type f" 6
    check_find $ROOT/dir.2 "-f $cfg -type f" 4
    check_find $ROOT/dir.2/dir.2 "-f $cfg -type f" 2
    check_find $ROOT/dir.1 "-f $cfg -type f" 0
    check_find $ROOT/dir.2/dir.1 "-f $cfg -type f" 0
    check_find $ROOT/dir.2/dir.2/dir.1 "-f $cfg -type f" 0
    check_find $ROOT/file.1 "-f $cfg -type f" 1
    check_find $ROOT/dir.2/file.1 "-f $cfg -type f" 1

    echo "testing name filter..."
    check_find $ROOT "-f $cfg -name dir.*" 5 # 5
    check_find $ROOT/dir.2 "-f $cfg -name dir.*" 4 # 4 including dir.2
    check_find $ROOT/dir.2/dir.2 "-f $cfg -name dir.*" 2 # 2 including dir.2/dir.2
    check_find $ROOT/dir.1 "-f $cfg -name dir.*" 1
    check_find $ROOT/dir.2/dir.1 "-f $cfg -name dir.*" 1
    check_find $ROOT/dir.2/dir.2/dir.1 "-f $cfg -name dir.*" 1

    echo "testing size filter..."
    check_find $ROOT "-f $cfg -type f -size +2k" 2
    check_find $ROOT "-f $cfg -type f -size +11k" 1
    check_find $ROOT "-f $cfg -type f -size +1M" 0
    check_find $ROOT "-f $cfg -type f -size 1M" 1
    check_find $ROOT "-f $cfg -type f -size 10k" 1
    check_find $ROOT "-f $cfg -type f -size -1M" 5
    check_find $ROOT "-f $cfg -type f -size -10k" 4

    echo "testing mtime filter..."
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
			if (( $is_hsmlite != 0 )); then
				echo "No purge for hsmlite purpose: skipped"
				set_skipped
				return 1
			fi
			cmd='--purge'
			match='Resource Monitor is disabled'
			;;
		migration)
			if (( $is_hsmlite == 0 )); then
				echo "hsmlite test only: skipped"
				set_skipped
				return 1
			fi
			cmd='--migrate'
			match='Migration module is disabled'
			;;
		hsm_remove)
			if (( $is_hsmlite == 0 )); then
				echo "hsmlite test only: skipped"
				set_skipped
				return 1
			fi
			cmd='--hsm-remove'
                        match='HSM removal successfully initialized' # enabled by default
			;;
		rmdir)
			if (( $is_hsmlite != 0 )); then
				echo "No rmdir policy for hsmlite purpose: skipped"
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

	sleep 2
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
	echo "<testsuite name=\"robinhood.PosixTests\" errors=\"0\" failures=\"$failure\" tests=\"$tests\" time=\"$time\">" >> $XML
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
		elif (( $ERROR > 0 )); then
			grep "Failed" $CLEAN 2>/dev/null
			echo "TEST #$index : *FAILED*" >> $SUMMARY
			RC=$(($RC+1))
			if (( $junit )); then
				junit_report_failure "robinhood.$PURPOSE.Posix" "Test #$index: $title" "$dur" "ERROR"
			fi
		else
			grep "Failed" $CLEAN 2>/dev/null
			echo "TEST #$index : OK" >> $SUMMARY
			SUCCES=$(($SUCCES+1))
			if (( $junit )); then
				junit_report_success "robinhood.$PURPOSE.Posix" "Test #$index: $title" "$dur"
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

	# check available modes ..............
	if (( $is_hsmlite != 0 )); then
		echo "No Alert for HSM_LITE purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# create specific file if it does not exist
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
			mkdir -p $ROOT/dir3
		    dd if=/dev/zero of=$ROOT/dir3/file.4 bs=1k count=1 >/dev/null 2>/dev/null || error "writing file.4"
		fi
	fi
	# optional sleep process ......................
	if [ $sleepTime != 0 ]; then
		echo "Please wait $sleepTime seconds ..."
		sleep $sleepTime || error "sleep time"
	fi
	# specific optional action after sleep process ..........
	if [ $testKey == "lastAccess" ]; then
		head $ROOT/dir1/file.1 > /dev/null || error "opening $ROOT/dir1/file.1"
	elif [ $testKey == "lastModif" ]; then
		echo "data" > $ROOT/dir1/file.1 || error "writing in $ROOT/dir1/file.1"
	fi

	# launch the scan ..........................
	echo -e "\n 2-Scanning filesystem ..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "performing FS scan"

	# check the scan
	echo -e "\n 3-Checking results ..."
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
	check_alert $alertKey $expectedEntry $occur $logFile || error "Test for $alertKey failed"
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

   	if (( $is_hsmlite != 0 )); then
		echo "No Purge trigger for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "1-Create Files ..."
	elem=`df $ROOT | grep "/" | awk '{ print $5 }' | sed 's/%//'`
	limit=80
	indice=1
    while [ $elem -lt $limit ]
    do
        dd if=/dev/zero of=$ROOT/file.$indice bs=1M count=1 >/dev/null 2>/dev/null
        if (( $? != 0 )); then
            echo "WARNING: fail writting $ROOT/file.$indice (usage: $elem/$limit)"
            # give it a chance to end the loop
            ((limit=$limit-1))
        fi
        unset elem
	    elem=`df $ROOT | grep "/" | awk '{ print $5 }' | sed 's/%//'`
        ((indice++))
    done
    echo "2-Scanning and Applying purge trigger policy..."
	$RH -f ./cfg/$config_file --scan --check-thresholds -l DEBUG -L rh_purge.log --once

    countMigrLog=`grep "High threshold reached on Filesystem" rh_purge.log | wc -l`
    if (($countMigrLog == 0)); then
        error "********** TEST FAILED **********"
    else
        echo "OK: test successful"
    fi
}


function trigger_purge_USER_GROUP_QUOTA_EXCEEDED
{
	# Function to test the trigger system when a quota is exceeded in OST filesytem (Lustre)
	# 	trigger_purge_OST_QUOTA_EXCEEDED config_file
	#=>
	# config_file == config file name

	config_file=$1
	usage=$2

   	if (( $is_hsmlite != 0 )); then
		echo "No Purge trigger for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "1-Create Files ..."

	elem=` df $ROOT |  grep "/"  | awk '{ print $5 }' | sed 's/%//'`
	limit=80
	indice=1
    while [ $elem -lt $limit ]
    do
        dd if=/dev/zero of=$ROOT/file.$indice bs=1M count=1 >/dev/null 2>/dev/null
        if (( $? != 0 )); then
            echo "WARNING: fail writting $ROOT/file.$indice (usage: $elem/$limit)"
            # give it a change to end the loop
            ((limit=$limit-1))
        fi
        unset elem
	    elem=`df $ROOT | grep "/" | awk '{ print $5 }' | sed 's/%//'`
        ((indice++))
    done

    ((limit=$indice/2))
    ((limit=$limit-1))
    echo "$indice files created, changing $limit files to testuser:testgroup"
    df -h $ROOT
    ((indice=1))
    while [ $indice -lt $limit ]
    do
        chown testuser:testgroup $ROOT/file.$indice
        ((indice++))
    done


    echo "2-Scanning and Applying purge trigger policy..."
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
	cat $ROOT/dir2/file.8 >/dev/null 2>/dev/null
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

   	if (( $is_hsmlite != 0 )); then
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
	echo "Scanning..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once

	if (($sleep_time != 0)); then
	    echo "Sleep $sleep_time"
        sleep $sleep_time

	    echo "update Files"
        update_files_Purge
	fi

	echo "Scanning and Applying purge policy..."
	$RH -f ./cfg/$config_file --scan  $purgeOpt --once -l DEBUG -L rh_purge.log

	nbError=0
	nb_purge=`grep $REL_STR rh_purge.log | wc -l`
	if (( $nb_purge != $needPurge )); then
	    error "********** TEST FAILED (Log): $nb_purge files purged, but $needPurge expected"
        ((nbError++))
	fi

	# If we are in tmp_fs_mgr mod, we can test if files are removed in file system
    if (( ($is_hsmlite == 0) )); then
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

    if (( $is_hsmlite != 0 )); then
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

	echo "Scanning and Applying purge policy..."
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


###################################################
############# End Purge Functions #################
###################################################

##################################################
############# Removing Functions #################
##################################################

function test_removing
{
	# remove directory/ies in accordance to the input file and configuration
	# 	test_removing config_file forExtAttributes sleepTime
	#=>
	# config_file == config file name
	# testKey == 'emptyDir' for testing extended attributes
	# 	     'lastAction' for testing last access or modification
	# sleepTime == expected time in second to sleep for the test, if=0 no sleep

	# get input parameters ....................
	config_file=$1
	testKey=$2  #== key word for specific tests
	sleepTime=$3

	# check available modes ...................
	if (( $is_hsmlite != 0 )); then
		echo "No Removing for HSM_LITE purpose: skipped"
		set_skipped
		return 1
	fi

	#  clean logs ..............................
	clean_logs

	# prepare data..............................
	echo -e "\n 1-Preparing Filesystem..."
	mkdir -p $ROOT/dir1
	mkdir -p $ROOT/dir5
	echo "data" > $ROOT/dir5/file.5

	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_rmdir.log --once || error "performing FS removing"

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

	# launch the rmdir ..........................
	echo -e "\n 2-Scanning directories in filesystem ..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once || error "performing FS removing"

	# optional sleep process ......................
	if [ $sleepTime != 0 ]; then
		echo "Please wait $sleepTime seconds ..."
		sleep $sleepTime
	fi
	# specific optional action after sleep process ..........
	if [ $testKey == "lastAccess" ]; then
		touch $ROOT/dir1/file.touched || error "touching file in $ROOT/dir1"
	elif [ $testKey == "lastModif" ]; then
		echo "data" > $ROOT/dir1/file.12 || error "writing in $ROOT/dir1/file.12"
	fi

	# launch the rmdir ..........................
	echo -e "\n 3-Removing directories in filesystem ..."
	$RH -f ./cfg/$config_file --rmdir -l DEBUG -L rh_rmdir.log --once || error "performing FS removing"

	# launch the validation ..........................
	echo -e "\n 3-Checking results ..."
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
	exist_dirs_or_not $existedDirs $notExistedDirs || error "Test for RemovingDir_$testKey failed"
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

	# check available modes ..............
	if (( $is_hsmlite != 0 )); then
		echo "No Report Generation for HSM_LITE purpose: skipped"
		set_skipped
		return 1
	fi

	#  clean logs ..............................
	clean_logs

	# prepare data..............................
	echo -e "\n 1-Preparing Filesystem..."
	# dir1:
	mkdir -p $ROOT/dir1/dir2
	sleep 1
	dd if=/dev/zero of=$ROOT/dir1/file.1 bs=1k count=5 >/dev/null 2>/dev/null || error "writing file.1"
	sleep 1
	dd if=/dev/zero of=$ROOT/dir1/file.2 bs=1k count=10 >/dev/null 2>/dev/null || error "writing file.2"
	sleep 1
	dd if=/dev/zero of=$ROOT/dir1/file.3 bs=1k count=15 >/dev/null 2>/dev/null || error "writing file.3"
	sleep 1
	# link from dir1:
	ln -s $ROOT/dir1/file.1 $ROOT/link.1 || error "creating symbolic link $ROOT/link.1"
	sleep 1
	# dir2 inside dir1:
	ln -s $ROOT/dir1/file.3 $ROOT/dir1/dir2/link.2 || error "creating symbolic link $ROOT/dir1/dir2/link.2"
	sleep 1
	# dir3 inside dir1:
	mkdir -p $ROOT/dir1/dir3
	sleep 1
	#dir4:
	mkdir -p $ROOT/dir4
	sleep 1
	#dir5:
	mkdir -p $ROOT/dir5
	sleep 1
	dd if=/dev/zero of=$ROOT/dir5/file.4 bs=1k count=10 >/dev/null 2>/dev/null || error "writing file.4"
	sleep 1
	dd if=/dev/zero of=$ROOT/dir5/file.5 bs=1k count=20 >/dev/null 2>/dev/null || error "writing file.5"
	sleep 1
	dd if=/dev/zero of=$ROOT/dir5/file.6 bs=1k count=21 >/dev/null 2>/dev/null || error "writing file.6"
	sleep 1
	ln -s $ROOT/dir1/file.2 $ROOT/dir5/link.3 || error "creating symbolic link $ROOT/dir5/link.3"
	sleep 1
	#dir6 inside dir5:
	mkdir -p $ROOT/dir5/dir6
	sleep 1
	# dir7:
	mkdir -p $ROOT/dir7
	sleep 1
	#link in dir.1
	ln -s $ROOT/dir1 $ROOT/dir1/link.0 || error "creating symbolic link $ROOT/dir1/link.0"
	sleep 1

	# manage owner and group
	filesList="$ROOT/link.1 $ROOT/dir1/dir2/link.2"
	chgrp -h testgroup $filesList || error "invalid chgrp on group 'testgroup' for $filesList "
	chown -h testuser $filesList || error "invalid chown on user 'testuser' for $filesList "
	filesList="$ROOT/dir1/file.2 $ROOT/dir1/dir2 $ROOT/dir1/dir3 $ROOT/dir5 $ROOT/dir7 $ROOT/dir5/dir6"
	chown testuser:testgroup $filesList || error "invalid chown on user 'testuser' for $filesList "
	filesList="$ROOT/dir1/file.1 $ROOT/dir5/file.6"
	chgrp testgroup $filesList || error "invalid chgrp on group 'testgroup' for $filesList "

	# launch the scan ..........................
	echo -e "\n 2-Scanning Filesystem..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "performing FS scan"

	# launch another scan ..........................
	echo -e "\n 3-Filesystem content statistics..."
	#$REPORT -f ./cfg/$config_file --fs-info -c || error "performing FS statistics (--fs)"
	$REPORT -f ./cfg/$config_file --fs-info --csv > report.out || error "performing FS statistics (--fs)"
	logFile=report.out
	typeValues="dir;file;symlink"
	countValues="7;6;4"
	colSearch=2
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating FS statistics (--fs)"


	# launch another scan ..........................
	echo -e "\n 4-FileClasses summary..."
	$REPORT -f ./cfg/$config_file --class-info --csv > report.out || error "performing FileClasses summary (--class)"
	typeValues="test_file_type;test_link_type"
	#typeValues="test_file_type"
	countValues="6;4"
	#countValues="6"
	colSearch=2
	#echo "arguments= $logFile $typeValues $countValues $colSearch**"
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating FileClasses summary (--class)"
	# launch another scan ..........................
	echo -e "\n 5-User statistics of root..."
	$REPORT -f ./cfg/$config_file --user-info -u root --csv > report.out || error "performing User statistics (--user)"
	typeValues="root.*dir;root.*file;root.*symlink"
	countValues="2;5;2"
	colSearch=3
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating FS User statistics (--user)"

	# launch another scan ..........................
	echo -e "\n 6-Group statistics of testgroup..."
	$REPORT -f ./cfg/$config_file --group-info -g testgroup --csv > report.out || error "performing Group statistics (--group)"
	typeValues="testgroup.*dir;testgroup.*file;testgroup.*symlink"
	countValues="5;3;2"
	colSearch=3
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Group statistics (--group)"

	# launch another scan ..........................
	echo -e "\n 7-Largest files of Filesystem..."
	$REPORT -f ./cfg/$config_file --top-size=3 --csv > report.out || error "performing Largest files list (--top-size)"
	typeValues="file\.6;file\.5;file\.3"
	countValues="1;2;3"
	colSearch=1
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Largest files list (--top-size)"

	# launch another scan ..........................
	echo -e "\n 8-Largest directories of Filesystem..."
	$REPORT -f ./cfg/$config_file --top-dirs=3 --csv > report.out || error "performing Largest folders list (--top-dirs)"
	# 2 possible orders
	typeValues="$ROOT/dir1;$ROOT/dir5;$ROOT,"
	typeValuesAlt="$ROOT/dir1;$ROOT,;$ROOT/dir5"
	countValues="1;2;3"
	colSearch=1
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || \
	find_allValuesinCSVreport $logFile $typeValuesAlt $countValues $colSearch || \
	error "validating Largest folders list (--top-dirs)"

	# launch another scan ..........................
	echo -e "\n 9-Four oldest entries of Filesystem..."
	$REPORT -f ./cfg/$config_file --top-purge=4 --csv > report.out || error "performing Oldest entries list (--top-purge)"
	typeValues="file\.3;file\.4;file\.5;link\.3"
	countValues="1;2;3;4"
	colSearch=1
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Oldest entries list (--top-purge)"

	# launch another scan ..........................
	echo -e "\n 10-Oldest and empty directories of Filesystem..."
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

	# launch another scan ..........................
	echo -e "\n 11-Top disk space consumers of Filesystem..."
	$REPORT -f ./cfg/$config_file --top-users --csv > report.out || error "performing disk space consumers (--top-users)"
	typeValues="root;testuser"
	countValues="1;2"
	colSearch=1
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating disk space consumers (--top-users)"

	# launch another scan ..........................
	echo -e "\n 12-Dump entries for one user of Filesystem..."
	$REPORT -f ./cfg/$config_file --dump-user root --csv > report.out || error "dumping entries for one user 'root'(--dump-user)"
	typeValues="root.*[root|testgroup].*dir1$;root.*[root|testgroup].*file\.1;root.*[root|testgroup].*file\.3;root.*[root|testgroup].*dir4$;"
	countValues="dir;file;file;dir"
	colSearch=1
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating entries for one user 'root'(--dump-user)"
	typeValues="root.*[root|testgroup].*file\.4;root.*[root|testgroup].*file\.5;root.*[root|testgroup].*file\.6;root.*[root|testgroup].*link\.3;"
	countValues="file;file;file;symlink"
	colSearch=1
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating entries for one user 'root'(--dump-user)"
	typeValue="root.*[root|testgroup]"
	if (( $(grep $typeValue $logFile | wc -l) != 9 )) ; then
		 error "validating entries for one user 'root'(--dump-user)"
	fi

	# launch another scan ..........................
	echo -e "\n 13-Dump entries for one group of Filesystem..."
	$REPORT -f ./cfg/$config_file --dump-group testgroup --csv > report.out || error "dumping entries for one group 'testgroup'(--dump-group)"
	#$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "performing FS scan"
	typeValues="testgroup.*link\.1;testgroup.*file\.1;testgroup.*file\.2;testgroup.*link\.2;testgroup.*file\.6"
	countValues="symlink;file;file;symlink;file"
	colSearch=1
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Group entries for one group 'testgroup'(--dump-group)"
	typeValues="testgroup.*dir2$;testgroup.*dir3$;testgroup.*dir5$;testgroup.*dir6$;testgroup.*dir7$"
	countValues="dir;dir;dir;dir;dir"
	colSearch=1
	find_allValuesinCSVreport $logFile $typeValues $countValues $colSearch || error "validating Group entries for one group 'testgroup'(--dump-group)"
	typeValue="testgroup"
	if (( $(grep $typeValue $logFile | wc -l) != 10 )) ; then
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

    [ "$DEBUG" = "1" ] && echo "find_allValuesinCSVreport: log=$logFile, type=$typeValues, count=$countValues, col=$colSearch" >&2

    # get typeValue and associated countvalue
    splitTypes=$(echo $typeValues | tr ";" "\n")
    tabTypes=""
    unset tabTypes
    j=1
    for entry in $splitTypes
       do
       	tabTypes[$j]=$entry
	    j=$(($j+1))
    done
    iDataMax=$j

    splitValues=$(echo $countValues | tr ";" "\n")
    tabValues=""
    unset tabValues
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
    while (( $iData < $iDataMax ))
    do
	    # get current typeValue & countvalue
	    typeValue=${tabTypes[$iData]}
	    countValue=${tabValues[$iData]}

	    find_valueInCSVreport $logFile $typeValue $countValue $colSearch
	    res=$?
	    if (( $res == 1 )); then
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
    # find line contains expected value type
    line=$(grep $typeValue $logFile)
    if (( ${#line} == 0 )); then
	    return 1
    fi


    # get found value count for this value type
    foundCount=$(grep $typeValue $logFile | cut -d ',' -f $colSearch | tr -d ' ')
    [[ -z $foundCount ]] && foundCount=0
    if (( $foundCount != $countValue )); then
	    return 1
    else
	    return 0
    fi
}

###################################################################
############### End report generation Functions ###################
###################################################################

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

	if (( $is_hsmlite != 0 )); then
		echo "No TEST_OTHER_PARAMETERS_1 for this purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	echo "Create Files ..."
    for i in `seq 1 10` ; do
    	dd if=/dev/zero of=$ROOT/file.$i bs=1K count=1 >/dev/null 2>/dev/null || error "writing file.$i"
	    setfattr -n user.foo -v $i $ROOT/file.$i
	done

	echo "Scan Filesystem"
	sleep 1
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log --once

	# use robinhood for flushing
	if (( $is_hsmlite != 0 )); then
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

	# Launch in background
	echo "Launch Purge in background"
	$RH -f ./cfg/$config_file --scan --purge -l DEBUG -L rh_purge.log --once &

	sleep 5

	echo "Count purged files number"
	nb_purge=`grep $REL_STR rh_purge.log | wc -l`
	if (( $nb_purge != 0 )); then
	    error "********** TEST FAILED (Log): $nb_purge files purged, but 0 expected"
        ((nbError++))
	fi

    echo "Remove /var/lock/rbh.lock"
	rm "/var/lock/rbh.lock"

	echo "wait robinhood"
	wait

	echo "Count purged files number"
	nb_purge2=`grep $REL_STR rh_purge.log | wc -l`
	((nb_purge2=nb_purge2-nb_purge))

	if (( $nb_purge2 != 10 )); then
	    error "********** TEST FAILED (Log): $nb_purge2 files purged, but 10 expected"
        ((nbError++))
	fi

	if (($nbError == 0 )); then
        echo "OK: test successful"
    else
        error "********** TEST FAILED **********"
    fi
}



function TEST_OTHER_PARAMETERS_5
{
	# Test for many parameters
	# 	TEST_OTHER_PARAMETERS_5 config_file
	#=>
	# config_file == config file name

	config_file=$1

    if (( $is_hsmlite != 0 )); then
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
	elem=`df $ROOT | grep "/" | awk '{ print $5 }' | sed 's/%//'`
	limit=95
	indice=1
    while [ $elem -lt $limit ]
    do
        dd if=/dev/zero of=$ROOT/file.$indice bs=10M count=1 >/dev/null 2>/dev/null
        if (( $? != 0 )); then
            echo "WARNING: fail writting $ROOT/file.$indice (usage: $elem/$limit)"
            # give it a change to end the loop
            ((limit=$limit-1))
        fi
        unset elem
	    elem=`df $ROOT | grep "/" | awk '{ print $5 }' | sed 's/%//'`
        ((indice++))
    done

       #echo "Launch scan in background..."
#$RH -f ./cfg/$config_file --scan --check-thresholds -l DEBUG -L rh_scan.log &
#pid=$!

	echo "sleep 60 seconds"
	sleep 60

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
# 3xx - alerts and triggers
# 4xx - reporting
# 5xx - internals, misc.
# 6xx - Tests by Sogeti
################################

##### info collect. + DB tests #####

run_test 100	test_info_collect info_collect.conf 1 1 "escape string in SQL requests"
run_test 101a    test_info_collect2  info_collect2.conf  1 "scan x3"
#TODO run_test 102	update_test test_updt.conf 5 30 "db update policy"
run_test 103a    test_acct_table common.conf 5 "Acct table and triggers creation"
run_test 103b    test_acct_table acct_group.conf 5 "Acct table and triggers creation"
run_test 103c    test_acct_table acct_user.conf 5 "Acct table and triggers creation"
run_test 103d    test_acct_table acct_user_group.conf 5 "Acct table and triggers creation"

run_test 106a    test_diff info_collect2.conf "diff" "rbh-diff"
run_test 106b    test_diff info_collect2.conf "diffapply" "rbh-diff --apply"
run_test 106c    test_diff info_collect2.conf "scan" "robinhood --scan --diff"
run_test 107a    test_completion test_completion.conf OK "scan completion command"
run_test 107b    test_completion test_completion_KO.conf KO "bad scan completion command"
run_test 108a    test_rename info_collect.conf scan "rename cases (scan)"
run_test 108c    test_rename info_collect.conf partial "rename cases (partial scans)"
run_test 108d    test_rename info_collect.conf diff "rename cases (diff+apply)"
run_test 108e    test_rename info_collect.conf partdiff "rename cases (partial diffs+apply)"
run_test 109a    test_hardlinks info_collect.conf scan "hardlinks management (scan)"
run_test 109c    test_hardlinks info_collect.conf partial "hardlinks management (partial scans)"
run_test 109d    test_hardlinks info_collect.conf diff "hardlinks management (diff+apply)"
run_test 109e    test_hardlinks info_collect.conf partdiff "hardlinks management (partial diffs+apply)"
run_test 112     test_hl_count info_collect.conf "reports with hardlinks"
run_test 113     test_diff_apply_fs info_collect2.conf  "diff"  "rbh-diff --apply=fs"
run_test 116     test_mnt_point  test_mnt_point.conf "test with mount point != fs_path"

#### policy matching tests  ####

run_test 200	path_test test_path.conf 2 "path matching policies"
run_test 201	migration_test test1.conf 11 31 "last_mod>30s"
run_test 202	migration_test test2.conf 5  31 "last_mod>30s and name == \"*[0-5]\""
run_test 203	migration_test test3.conf 5  16 "complex policy with filesets"
run_test 204	migration_test test3.conf 10 31 "complex policy with filesets"
run_test 205	xattr_test test_xattr.conf 5 "xattr-based fileclass definition"
run_test 206	purge_test test_purge.conf 11 41 "last_access > 40s"
run_test 207	purge_size_filesets test_purge2.conf 2 3 "purge policies using size-based filesets"
run_test 208	periodic_class_match_migr test_updt.conf 10 "periodic fileclass matching (migration)"
run_test 209	periodic_class_match_purge test_updt.conf 10 "periodic fileclass matching (purge)"
run_test 210	fileclass_test test_fileclass.conf 2 "complex policies with unions and intersections of filesets"
# test 211 is on Lustre pools (not for POSIX FS)
run_test 212	link_unlink_remove_test test_rm1.conf 1 31 "deferred hsm_remove (30s)"
# test 213 is about migration
run_test 214a 	check_disabled	common.conf  purge	"no purge if not defined in config"
run_test 214b 	check_disabled	common.conf  migration	"no migration if not defined in config"
run_test 214c 	check_disabled	common.conf  rmdir	"no rmdir if not defined in config"
run_test 214d 	check_disabled	common.conf  hsm_remove	"hsm_rm is enabled by default"
run_test 214e 	check_disabled	common.conf  class	"no class matching if none defined in config"
#test 215-217 are HSM specific
run_test 218	test_rmdir 	rmdir.conf 16 		"rmdir policies"

run_test 221	test_custom_purge test_custom_purge.conf 2	"custom purge command"

#### triggers ####

run_test 300	test_cnt_trigger test_trig.conf 101 21 "trigger on file count"
# test 301 is about OST: not for POSIX FS
run_test 302	test_trigger_check test_trig3.conf 60 110 "triggers check only" 40 80 5 40
run_test 303    test_periodic_trigger test_trig4.conf 10 "periodic trigger"

#### reporting ####
run_test 400	test_rh_report common.conf 3 1 "reporting tool"

run_test 401a	test_rh_acct_report common.conf 5 "reporting tool: config file without acct param"
run_test 401b   test_rh_acct_report acct_user.conf 5 "reporting tool: config file with acct_user=true and acct_group=false"
run_test 401c   test_rh_acct_report acct_group.conf 5 "reporting tool: config file with acct_user=false and acct_group=true"
run_test 401d   test_rh_acct_report no_acct.conf 5 "reporting tool: config file with acct_user=false and acct_group=false"
run_test 401e   test_rh_acct_report acct_user_group.conf 5 "reporting tool: config file with acct_user=true and acct_group=true"

run_test 402a   test_rh_report_split_user_group common.conf 5 "" "report with split-user-groups option"
run_test 402b   test_rh_report_split_user_group common.conf 5 "--force-no-acct" "report with split-user-groups and force-no-acct option"

run_test 403    test_sort_report common.conf 0 "Sort options of reporting command"
run_test 404   test_dircount_report common.conf 20  "dircount reports"

run_test 405    test_find common.conf ""  "rbh-find command"
run_test 406    test_du   common.conf ""  "rbh-du command"

#### misc, internals #####
run_test 500a	test_logs log1.conf file_nobatch 	"file logging without alert batching"
run_test 500b	test_logs log2.conf syslog_nobatch 	"syslog without alert batching"
run_test 500c	test_logs log3.conf stdio_nobatch 	"stdout and stderr without alert batching"
run_test 500d	test_logs log1b.conf file_batch 	"file logging with alert batching"
run_test 500e	test_logs log2b.conf syslog_batch 	"syslog with alert batching"
run_test 500f	test_logs log3b.conf stdio_batch 	"stdout and stderr with alert batching"

run_test 501a 	test_cfg_parsing basic none		"parsing of basic template"
run_test 501b 	test_cfg_parsing detailed none	"parsing of detailed template"
run_test 501c 	test_cfg_parsing generated none	"parsing of generated template"


#### Tests by Sogeti ####
run_test 601 test_alerts Alert_Path_Name.conf "pathName" 0 "TEST_ALERT_PATH_NAME"
run_test 602 test_alerts Alert_Type.conf "type" 0 "TEST_ALERT_TYPE"
run_test 603 test_alerts Alert_Owner.conf "owner" 0 "TEST_ALERT_OWNER"
run_test 604 test_alerts Alert_Size.conf "size" 0 "TEST_ALERT_SIZE"
run_test 605 test_alerts Alert_LastAccess.conf "lastAccess" 60 "TEST_ALERT_LAST_ACCESS"
run_test 606 test_alerts Alert_LastModification.conf "lastModif" 60 "TEST_ALERT_LAST_MODIFICATION"
run_test 608 test_alerts Alert_ExtendedAttribute.conf "extAttributes" 0 "TEST_ALERT_EXTENDED_ATTRIBUT"
run_test 609 test_alerts Alert_Dircount.conf "dircount" 0 "TEST_ALERT_DIRCOUNT"

run_test 637 trigger_purge_QUOTA_EXCEEDED TriggerPurge_QuotaExceeded.conf "TEST_TRIGGER_PURGE_QUOTA_EXCEEDED"
run_test 639 trigger_purge_USER_GROUP_QUOTA_EXCEEDED TriggerPurge_UserQuotaExceeded.conf "User 'root'" "TEST_TRIGGER_PURGE_USER_GROUP_QUOTA_EXCEEDED"
run_test 640 trigger_purge_USER_GROUP_QUOTA_EXCEEDED TriggerPurge_GroupQuotaExceeded.conf "Group 'root'" "TEST_TRIGGER_PURGE_USER_GROUP_QUOTA_EXCEEDED"

run_test 641 test_purge PurgeStd_Path_Name.conf 0 7 "file.6;file.7;file.8" "--purge" "TEST_PURGE_STD_PATH_NAME"
run_test 642 test_purge_tmp_fs_mgr PurgeStd_Type.conf 0 8 "link.1;link.2" "--purge" "TEST_PURGE_STD_TYPE"
run_test 643 test_purge PurgeStd_Owner.conf 0 9 "file.3" "--purge" "TEST_PURGE_STD_OWNER"
run_test 644 test_purge PurgeStd_Size.conf 0 8 "file.6;file.7" "--purge" "TEST_PURGE_STD_SIZE"
run_test 645 test_purge PurgeStd_LastAccess.conf 10 9 "file.8" "--purge" "TEST_PURGE_STD_LAST_ACCESS"
run_test 646 test_purge PurgeStd_LastModification.conf 30 9 "file.8" "--purge" "TEST_PURGE_STD_LAST_MODIFICATION"
run_test 648 test_purge PurgeStd_ExtendedAttribut.conf 0 9 "file.4" "--purge" "TEST_PURGE_STD_EXTENDED_ATTRIBUT"
run_test 650 test_purge PurgeClass_Path_Name.conf 0 9 "file.1" "--purge" "TEST_PURGE_CLASS_PATH_NAME"
run_test 651 test_purge PurgeClass_Type.conf 0 2 "file.1;file.2;file.3;file.4;file.5;file.6;file.7;file.8" "--purge"
run_test 652 test_purge PurgeClass_Owner.conf 0 3 "file.1;file.2;file.4;file.5;file.6;file.7;file.8" "--purge"
run_test 653 test_purge PurgeClass_Size.conf 0 8 "file.6;file.7" "--purge" "TEST_PURGE_CLASS_SIZE"
run_test 654 test_purge PurgeClass_LastAccess.conf 60 9 "file.8" "--purge" "TEST_PURGE_CLASS_LAST_ACCESS"
run_test 655 test_purge PurgeClass_LastModification.conf 60 9 "file.8" "--purge" "TEST_PURGE_CLASS_LAST_MODIFICATION"
run_test 656 test_purge PurgeClass_ExtendedAttribut.conf 0 9 "file.4" "--purge" "TEST_PURGE_CLASS_EXTENDED_ATTRIBUT"

run_test 658 test_removing RemovingEmptyDir.conf "emptyDir" 31 "TEST_REMOVING_EMPTY_DIR"
run_test 659 test_removing RemovingDir_Path_Name.conf "pathName" 0 "TEST_REMOVING_DIR_PATH_NAME"
run_test 660 test_removing RemovingDir_Owner.conf "owner" 0 "TEST_REMOVING_DIR_OWNER"
run_test 661 test_removing RemovingDir_LastAccess.conf "lastAccess" 31  "TEST_REMOVING_DIR_LAST_ACCESS"
run_test 662 test_removing RemovingDir_LastModification.conf "lastModif" 31 "TEST_REMOVING_DIR_LAST_MODIFICATION"
run_test 664 test_removing RemovingDir_ExtendedAttribute.conf "extAttributes" 0 "TEST_REMOVING_DIR_EXTENDED_ATTRIBUT"
run_test 665 test_removing RemovingDir_Dircount.conf "dircount" 0 "TEST_REMOVING_DIR_DIRCOUNT"

run_test 666  test_report_generation_1 Generation_Report_1.conf "TEST_REPORT_GENERATION_1"

run_test 668 TEST_OTHER_PARAMETERS_1 OtherParameters_1.conf "TEST_OTHER_PARAMETERS_1"
run_test 672 TEST_OTHER_PARAMETERS_5 OtherParameters_5.conf "TEST_OTHER_PARAMETERS_5"

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
