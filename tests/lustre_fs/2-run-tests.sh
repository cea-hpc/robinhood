#/bin/sh

ROOT="/mnt/lustre"
BKROOT="/tmp/backend"
RBH_OPT=""

TEMPLATE_DIR='../../doc/templates'

if [[ ! -d $ROOT ]]; then
	echo "Creating directory $ROOT"
	mkdir -p "$ROOT"
else
	echo "Creating directory $ROOT"
fi

if [[ -z "$PURPOSE" || $PURPOSE = "LUSTRE_HSM" ]]; then
	is_hsm=1
	is_backup=0
	RH="../../src/robinhood/rbh-hsm $RBH_OPT"
	REPORT=../../src/robinhood/rbh-hsm-report
	CMD=rbh-hsm
elif [[ $PURPOSE = "TMP_FS_MGR" ]]; then
	is_hsm=0
	is_backup=0
	RH="../../src/robinhood/robinhood $RBH_OPT"
	REPORT="../../src/robinhood/rbh-report $RBH_OPT"
	CMD=robinhood
elif [[ $PURPOSE = "BACKUP" ]]; then
	is_hsm=0
	is_backup=1
	RH="../../src/robinhood/rbh-backup $RBH_OPT"
	REPORT="../../src/robinhood/rbh-backup-report $RBH_OPT"
	CMD=rbh-backup
fi

if [[ -z "$NOLOG" || $NOLOG = "0" ]]; then
	no_log=0
else
	no_log=1
fi

PROC=$CMD
CFG_SCRIPT="../../scripts/rbh-config"
CLEAN="rh_chglogs.log rh_migr.log rh_rm.log rh.pid rh_purge.log rh_report.log report.out"

SUMMARY="/tmp/test_$PROC_summary.$$"

ERROR=0
RC=0

function error_reset
{
	ERROR=0
}

function error
{
	echo "ERROR $@"
	((ERROR=$ERROR+1))
	((RC=$RC+1))
}

function set_skipped
{
	ERROR="skip"
}

function clean_logs
{
	for f in $CLEAN; do
		if [ -s $f ]; then
			cp /dev/null $f
		fi
	done
}


function clean_fs
{
	if (( $is_hsm != 0 )); then
		echo "Cancelling agent actions..."
		echo "purge" > /proc/fs/lustre/mdt/*/hsm_control

		echo "Waiting for end of data migration..."
		while egrep "WAITING|RUNNING|STARTED" /proc/fs/lustre/mdt/lustre-MDT0000/hsm/agent_actions > /dev/null ; do sleep 1; done
	fi

	echo "Cleaning filesystem..."
	if [[ -n "$ROOT" ]]; then
		rm  -rf $ROOT/*
	fi

#	if (( $is_backup != 0 )); then
#		if [[ -n "$BKROOT" ]]; then
#			rm -rf $BKROOT/*
#		fi
#	fi

	echo "Destroying any running instance of robinhood..."
	pkill -f robinhood
	pkill -f rbh-hsm

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
	lfs changelog_clear lustre-MDT0000 cl1 0

}

POOL1=ost0
POOL2=ost1
POOL_CREATED=0

function create_pools
{
  (($POOL_CREATED != 0 )) && return
  lfs pool_list lustre | grep lustre.$POOL1 && POOL_CREATED=1
  lfs pool_list lustre | grep lustre.$POOL2 && ((POOL_CREATED=$POOL_CREATED+1))
  (($POOL_CREATED == 2 )) && return

  lctl pool_new lustre.$POOL1 || error "creating pool $POOL1"
  lctl pool_add lustre.$POOL1 lustre-OST0000 || error "adding OST0000 to pool $POOL1"
  lctl pool_new lustre.$POOL2 || error "creating pool $POOL2"
  lctl pool_add lustre.$POOL2 lustre-OST0001 || error "adding OST0001 to pool $POOL2"
  POOL_CREATED=1
}

function migration_test
{
	config_file=$1
	expected_migr=$2
	sleep_time=$3
	policy_str="$4"

	if (( $is_hsm + $is_backup == 0 )); then
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

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error ""

	nb_migr=`grep "Start archiving" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != 0)); then
		error "********** TEST FAILED: No migration expected, $nb_migr started"
	else
		echo "OK: no files migrated"
	fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "3-Applying migration policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once

	nb_migr=`grep "Start archiving" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != $expected_migr)); then
		error "********** TEST FAILED: $expected_migr migrations expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"
	fi
}

function xattr_test
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	if (( $is_hsm + $is_backup == 0 )); then
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

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || error ""

	nb_migr=`grep "Start archiving" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != 0)); then
		error "********** TEST FAILED: No migration expected, $nb_migr started"
	else
		echo "OK: no files migrated"
	fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "3-Applying migration policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once

	nb_migr=`grep "Start archiving" rh_migr.log | grep hints |  wc -l`
	if (($nb_migr != 3)); then
		error "********** TEST FAILED: $expected_migr migrations expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"

		if (( $is_backup != 0 )); then
			# checking policy
			nb_migr_arch1=`grep "fileclass=xattr_bar" rh_migr.log | wc -l`
			nb_migr_arch2=`grep "fileclass=xattr_foo" rh_migr.log | wc -l`
			nb_migr_arch3=`grep "using policy 'default'" rh_migr.log | wc -l`
			if (( $nb_migr_arch1 != 1 || $nb_migr_arch2 != 1 || $nb_migr_arch3 != 1 )); then
				echo "********** ERROR: wrong policy cases: 1x$nb_migr_arch1/2x$nb_migr_arch2/3x$nb_migr_arch3 (1x1/2x1/3x1 expected)"
			else
				echo "OK: 1 file for each policy case"
			fi
		else
			# checking archive nums
			nb_migr_arch1=`grep "archive_num=1" rh_migr.log | wc -l`
			nb_migr_arch2=`grep "archive_num=2" rh_migr.log | wc -l`
			nb_migr_arch3=`grep "archive_num=3" rh_migr.log | wc -l`
			if (( $nb_migr_arch1 != 1 || $nb_migr_arch2 != 1 || $nb_migr_arch3 != 1 )); then
				echo "********** ERROR: wrong archive_nums: 1x$nb_migr_arch1/2x$nb_migr_arch2/3x$nb_migr_arch3 (1x1/2x1/3x1 expected)"
			else
				echo "OK: 1 file to each archive_num"
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

	if (( $is_hsm + $is_backup == 0 )); then
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

	# write file.1 and force immediate migration
	echo "2-Writing data to file.1..."
	dd if=/dev/zero of=$ROOT/file.1 bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.1"

	if (( $is_hsm != 0 )); then
		echo "3-Archiving file....1"
		lfs hsm_archive $ROOT/file.1 || error ""

		echo "3bis-Waiting for end of data migration..."
		while egrep "WAITING|RUNNING|STARTED" /proc/fs/lustre/mdt/lustre-MDT0000/hsm/agent_actions ; do sleep 1; done
	elif (( $is_backup != 0 )); then
		$RH -f ./cfg/$config_file --sync -l DEBUG  -L rh_migr.log || error ""
	fi

	# create links on file.1 files
	echo "4-Creating hard links to $ROOT/file.1..."
	ln $ROOT/file.1 $ROOT/link.1 || error ""
	ln $ROOT/file.1 $ROOT/link.2 || error ""

	# removing all files
        echo "5-Removing all links to file.1..."
	rm -f $ROOT/link.* $ROOT/file.1 

	# deferred remove delay is not reached: nothing should be removed
	echo "6-Performing HSM remove requests (before delay expiration)..."
	$RH -f ./cfg/$config_file --hsm-remove -l DEBUG -L rh_rm.log --once || error ""

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
	pkill -9 -f $PROC

}

function purge_test
{
	config_file=$1
	expected_purge=$2
	sleep_time=$3
	policy_str="$4"

	if (( $is_backup != 0 )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# initial scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log 

	# fill 10 files and mark them archived+non dirty

	echo "1-Modifing files..."
	for i in a `seq 1 10`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=10 >/dev/null 2>/dev/null || error "writing file.$i"

		if (( $is_hsm != 0 )); then
			lfs hsm_set --exists --archived $ROOT/file.$i
			lfs hsm_clear --dirty $ROOT/file.$i
		fi
	done
	
	echo "2-Reading changelogs to update file status (after 1sec)..."
	sleep 1
	if (( $no_log )); then
		echo "2-Scanning the FS again to update file status (after 1sec)..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs to update file status (after 1sec)..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi

	echo "3-Applying purge policy ($policy_str)..."
	# no purge expected here
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || error ""

	if (( $is_hsm != 0 )); then
	        nb_purge=`grep "Releasing" rh_purge.log | wc -l`
	else
	        nb_purge=`grep "Purged" rh_purge.log | wc -l`
	fi

        if (($nb_purge != 0)); then
                error "********** TEST FAILED: No release actions expected, $nb_purge done"
        else
                echo "OK: no file released"
        fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "5-Applying purge policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || error ""

	if (( $is_hsm != 0 )); then
	        nb_purge=`grep "Releasing" rh_purge.log | wc -l`
	else
	        nb_purge=`grep "Purged" rh_purge.log | wc -l`
	fi

        if (($nb_purge != $expected_purge)); then
                error "********** TEST FAILED: $expected_purge release actions expected, $nb_purge done"
        else
                echo "OK: $nb_purge files released"
        fi

	# stop RH in background
#	kill %1
}

function purge_size_filesets
{
	config_file=$1
	sleep_time=$2
	count=$3
	policy_str="$4"

	if (( $is_backup != 0 )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi

	clean_logs

	# initial scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log 

	# fill 3 files of different sizes and mark them archived non-dirty

	j=1
	for size in 0 1 10 200; do
		echo "1.$j-Writing files of size " $(( $size*10 )) "kB..."
		((j=$j+1))
		for i in `seq 1 $count`; do
			dd if=/dev/zero of=$ROOT/file.$size.$i bs=10k count=$size >/dev/null 2>/dev/null || error "writing file.$size.$i"

			if (( $is_hsm != 0 )); then
				lfs hsm_set --exists --archived $ROOT/file.$size.$i
				lfs hsm_clear --dirty $ROOT/file.$size.$i
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

	echo "3.Checking reports..."
	for i in `seq 1 $dircount`; do
		$REPORT -f ./cfg/$config_file -l MAJOR --csv -U 1 -P $ROOT/dir.$i > rh_report.log
		used=`tail -n 1 rh_report.log | cut -d "," -f 3`
		if (( $used != $i*1024*1024 )); then
			error ": $used != " $(($i*1024*1024))
		else
			echo "OK: $i MB in $ROOT/dir.$i"
		fi
	done
	
}

function path_test
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	if (( $is_hsm + $is_backup == 0 )); then
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

	# read changelogs
	if (( $no_log )); then
		echo "2-Scanning..."
		$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
	else
		echo "2-Reading changelogs..."
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
	fi


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
		echo "loop 1.$i: many 'touch' within $event_updt_min sec"
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
		pkill -f $PROC
		sleep 1

		nb_getattr=`grep getattr=1 $LOG | wc -l`
		echo "nb attr update: $nb_getattr"
		(( $nb_getattr == 1 )) || error "********** TEST FAILED: wrong count of getattr: $nb_getattr"

		# the path may be retrieved at the first loop (at creation)
		# but not during the next loop (as long as enlapsed time < update_period)
		if (( $i > 1 )) && (( `date "+%s"` - $init < $update_period )); then
			nb_getpath=`grep getpath=1 $LOG | wc -l`
			echo "nb path update: $nb_getpath"
			(( $nb_getpath == 0 )) || error "********** TEST FAILED: wrong count of getpath: $nb_getpath"
		fi
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
		pkill -f $PROC
		sleep 1

		nb_getpath=`grep getpath=1 $LOG | wc -l`
		echo "nb path update: $nb_getpath"
		(( $nb_getpath == 1 )) || error "********** TEST FAILED: wrong count of getpath: $nb_getpath"

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

	if (( $is_hsm != 0 )); then
		# chg something different that path or POSIX attributes
		lfs hsm_set --exists $ROOT/file
	else
		touch $ROOT/file
	fi

	# force flushing log
	sleep 1
	pkill -f $PROC
	sleep 1

	nb_getattr=`grep getattr=1 $LOG | wc -l`
	echo "nb attr update: $nb_getattr"
	(( $nb_getattr == 1 )) || error "********** TEST FAILED: wrong count of getattr: $nb_getattr"
	nb_getpath=`grep getpath=1 $LOG | wc -l`
	echo "nb path update: $nb_getpath"
	(( $nb_getpath == 1 )) || error "********** TEST FAILED: wrong count of getpath: $nb_getpath"

	if (( $is_hsm != 0 )); then
		# also check that the status is to be retrieved
		nb_getstatus=`grep getstatus=1 $LOG | wc -l`
		echo "nb status update: $nb_getstatus"
		(( $nb_getstatus == 1 )) || error "********** TEST FAILED: wrong count of getstatus: $nb_getstatus"
	fi

	# kill remaning event handler
	sleep 1
	pkill -9 -f $PROC
}

function periodic_class_match_migr
{
	config_file=$1
	update_period=$2
	policy_str="$3"

	if (( $is_hsm + $is_backup == 0 )); then
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

function periodic_class_match_purge
{
	config_file=$1
	update_period=$2
	policy_str="$3"

	if (( $is_backup != 0 )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

	#create test tree of archived files
	for file in ignore1 whitelist1 purge1 default1 ; do
		touch $ROOT/$file

		if (( $is_hsm != 0 )); then
			lfs hsm_set --exists --archived $ROOT/$file
		fi
	done

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log

	# now apply policies
	$RH -f ./cfg/$config_file --purge-fs=0 --dry-run -l FULL -L rh_purge.log --once || error ""

	# HSM: we must have 4 lines like this: "Need to update fileclass (not set)"
	# TMP_FS_MGR:  whitelisted status is always checked at scan time
	# 	so 2 entries have already been matched (ignore1 and whitelist1)
	if (( $is_hsm == 0 )); then
		already=2
	else
		already=0
	fi

	nb_updt=`grep "Need to update fileclass (not set)" rh_purge.log | wc -l`
	nb_purge_match=`grep "matches the condition for policy 'purge_match'" rh_purge.log | wc -l`
	nb_default=`grep "matches the condition for policy 'default'" rh_purge.log | wc -l`

	(( $nb_updt == 4 - $already )) || error "********** TEST FAILED: wrong count of fileclass update: $nb_updt"
	(( $nb_purge_match == 1 )) || error "********** TEST FAILED: wrong count of files matching 'purge_match': $nb_purge_match"
	(( $nb_default == 1 )) || error "********** TEST FAILED: wrong count of files matching 'default': $nb_default"

        (( $nb_updt == 4 - $already )) && (( $nb_purge_match == 1 )) && (( $nb_default == 1 )) \
		&& echo "OK: initial fileclass matching successful"

	# update db content and rematch entries: should not update fileclasses
	clean_logs
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log
	$RH -f ./cfg/$config_file --purge-fs=0 --dry-run -l FULL -L rh_purge.log --once || error ""

	nb_default_valid=`grep "fileclass '@default@' is still valid" rh_purge.log | wc -l` # impossible: it should have been removed from DB during the last purge!
	nb_purge_valid=`grep "fileclass 'to_be_released' is still valid" rh_purge.log | wc -l` # impossible: it should have been removed from DB during the last purge!
	nb_updt=`grep "Need to update fileclass" rh_purge.log | wc -l`

#	(( $nb_default_valid == 1 )) || error "********** TEST FAILED: wrong count of cached fileclass for default policy: $nb_default_valid"
#	(( $nb_purge_valid == 1 )) || error "********** TEST FAILED: wrong count of cached fileclass for 'purge_match' : $nb_purge_valid"
#	(( $nb_updt == 0 )) || error "********** TEST FAILED: no expected fileclass update: $nb_updt updated"

	(( $nb_default_valid == 0 )) || error "********** TEST FAILED: wrong count of cached fileclass for default policy: $nb_default_valid"
	(( $nb_purge_valid == 0 )) || error "********** TEST FAILED: wrong count of cached fileclass for 'purge_match' : $nb_purge_valid"
	(( $nb_updt == 2 )) || error "********** TEST FAILED: no expected fileclass update: $nb_updt updated"

        #(( $nb_updt == 0 )) && (( $nb_default_valid == 1 )) && (( $nb_purge_valid == 1 )) \
        (( $nb_updt == 2 )) && (( $nb_default_valid == 0 )) && (( $nb_purge_valid == 0 )) \
		&& echo "OK: fileclasses do not need update"
	
	# update db content and rematch entries: should update all fileclasses
	clean_logs
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log

	echo "Waiting $update_period sec..."
	sleep $update_period

	$RH -f ./cfg/$config_file --purge-fs=0 --dry-run -l FULL -L rh_purge.log --once || error ""

	# TMP_FS_MGR:  whitelisted status is always checked at scan time
	# 	2 entries are new (default and to_be_released)
	if (( $is_hsm == 0 )); then
		already=0
		new=2
	else
		already=0
		new=0
	fi

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

	if (( $is_backup != 0 )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

	# initial inode count
	empty_count=`df -i $ROOT/ | grep "$ROOT" | awk '{print $(NF-3)}'`
	(( file_count=$file_count - $empty_count ))

	#create test tree of archived files (1M each)
	for i in `seq 1 $file_count`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=1

		if (( $is_hsm != 0 )); then
			lfs hsm_set --exists --archived $ROOT/file.$i
		fi
	done

	# wait for df sync
	sync; sleep 1

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log

	# apply purge trigger
	$RH -f ./cfg/$config_file --purge --once -l FULL -L rh_purge.log

	if (($is_hsm != 0 )); then
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
	mb_h_watermark=$2
	mb_l_watermark=$3
	policy_str="$4"

	if (( $is_backup != 0 )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

	empty_vol=`lfs df  | grep OST0000 | awk '{print $3}'`
	empty_vol=$(($empty_vol/1024))

	lfs setstripe --count 2 --offset 0 $ROOT || error "setting stripe_count=2"

	#create test tree of archived files (2M each=1MB/ost) until we reach high watermark
	for i in `seq $empty_vol $mb_h_watermark`; do
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=2

		if (( $is_hsm != 0 )); then
			lfs hsm_set --exists --archived $ROOT/file.$i
		fi
	done

	# wait for df sync
	sync; sleep 1

	full_vol=`lfs df  | grep OST0000 | awk '{print $3}'`
	full_vol=$(($full_vol/1024))
	delta=$(($full_vol-$empty_vol))
	echo "OST#0 usage increased of $delta MB (total usage = $full_vol MB)"
	((need_purge=$full_vol-$mb_l_watermark))
	echo "Need to purge $need_purge MB on OST#0"

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log

	# apply purge trigger
	$RH -f ./cfg/$config_file --purge --once -l DEBUG -L rh_purge.log

	grep summary rh_purge.log
	stat_purge=`grep summary rh_purge.log | grep "OST #0" | awk '{print $(NF-9)" "$(NF-3)" "$(NF-2)}' | sed -e "s/[^0-9 ]//g"`

	purged_ost=`echo $stat_purge | awk '{print $1}'`
	purged_total=`echo $stat_purge | awk '{print $2}'`
	needed_ost=`echo $stat_purge | awk '{print $3}'`

	# change blocks to MB (*512/1024/1024 == /2048)
	((purged_ost=$purged_ost/2048))
	((purged_total=$purged_total/2048))
	((needed_ost=$needed_ost/2048))

	# checks
	# - needed_ost must be equal to the amount we computed (need_purge)
	# - purged_ost must be over the amount we computed and under need_purge+1MB
	# - purged_total must be twice purged_ost
	(( $needed_ost == $need_purge )) || error ": invalid amount of data computed"
	(( $purged_ost >= $need_purge )) && (( $purged_ost <= $need_purge + 1 )) || error ": invalid amount of data purged"
	(( $purged_total == 2*$purged_ost )) || error ": invalid total volume purged"

	(( $needed_ost == $need_purge )) && (( $purged_ost >= $need_purge )) && (( $purged_ost <= $need_purge + 1 )) \
		&& (( $purged_total == 2*$purged_ost )) && echo "OK: purge of OST#0 succeeded"

	full_vol1=`lfs df  | grep OST0001 | awk '{print $3}'`
	full_vol1=$(($full_vol1/1024))
	purge_ost1=`grep summary rh_purge.log | grep "OST #1" | wc -l`

	if (($full_vol1 > $mb_h_watermark )); then
		error ": OST#1 is not expected to exceed high watermark!"
	elif (($purge_ost1 != 0)); then
		error ": no purge expected on OST#1"
	else
		echo "OK: no purge on OST#1 (usage=$full_vol1 MB)"
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

	if (( $is_backup != 0 )); then
		echo "No purge for backup purpose: skipped"
		set_skipped
		return 1
	fi
	clean_logs

	# triggers to be checked
	# - inode count > max_count
	# - fs volume	> max_vol
	# - root quota  > user_quota

	# initial inode count
	empty_count=`df -i $ROOT/ | grep "$ROOT" | awk '{print $(NF-3)}'`
	((file_count=$max_count-$empty_count))

	# compute file size to exceed max vol and user quota
	empty_vol=`df -k $ROOT  | grep "$ROOT" | awk '{print $(NF-3)}'`
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
		dd if=/dev/zero of=$ROOT/file.$i bs=1M count=$file_size

		if (( $is_hsm != 0 )); then
			lfs hsm_set --exists --archived $ROOT/file.$i
		fi
	done

	# wait for df sync
	sync; sleep 1

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log

	# check purge triggers
	$RH -f ./cfg/$config_file --check-watermarks --once -l FULL -L rh_purge.log

	((expect_count=$empty_count+$file_count-$target_count))
	((expect_vol_fs=$empty_vol+$file_count*$file_size-$target_fs_vol))
	((expect_vol_user=$file_count*$file_size-$target_user_vol))
	echo "over trigger limits: $expect_count entries, $expect_vol_fs MB, $expect_vol_user MB for user root"

	if (($is_hsm != 0 )); then
		nb_release=`grep "Released" rh_purge.log | wc -l`
	else
		nb_release=`grep "Purged" rh_purge.log | wc -l`
	fi

	count_trig=`grep " entries must be purged in Filesystem" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`

	vol_fs_trig=`grep " blocks (x512) must be purged on Filesystem" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`
	((vol_fs_trig_mb=$vol_fs_trig/2048)) # /2048 == *512/1024/1024

	vol_user_trig=`grep " blocks (x512) must be purged for user" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`
	((vol_user_trig_mb=$vol_user_trig/2048)) # /2048 == *512/1024/1024
	
	echo "triggers reported: $count_trig entries, $vol_fs_trig_mb MB, $vol_user_trig_mb MB"

	# check then was no actual purge
	if (($nb_release > 0)); then
		error ": $nb_release files released, no purge expected"
	elif (( $count_trig != $expect_count )); then
		error ": trigger reported $count_trig files over watermark, $expect_count expected"
	elif (( $vol_fs_trig_mb != $expect_vol_fs )); then
		error ": trigger reported $vol_fs_trig_mb MB over watermark, $expect_vol_fs expected"
	elif (( $vol_user_trig_mb != $expect_vol_user )); then
		error ": trigger reported $vol_user_trig_mb MB over watermark, $expect_vol_user expected"
	else
		echo "OK: all checks successful"
	fi
}


function fileclass_test
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	if (( $is_hsm + $is_backup == 0 )); then
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
		echo "1-Reading changelogs..."
		#$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || error ""
		$RH -f ./cfg/$config_file --readlog -l FULL -L rh_chglogs.log  --once || error ""
		nb_cr=4
	fi

	sleep $sleep_time2

	grep "DB query failed" rh_chglogs.log && error ": a DB query failed when reading changelogs"

	nb_create=`grep ChangeLog rh_chglogs.log | grep 01CREAT | wc -l`
	nb_db_apply=`grep STAGE_DB_APPLY rh_chglogs.log | tail -1 | cut -d '|' -f 6 | cut -d ':' -f 2 | tr -d ' '`

	if (( $is_hsm + $is_backup != 0 )); then
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
		exit 1
	fi

	clean_logs

	echo "2-Scanning..."
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_chglogs.log  --once || error ""
 
	grep "DB query failed" rh_chglogs.log && error ": a DB query failed when scanning"
	nb_db_apply=`grep STAGE_DB_APPLY rh_chglogs.log | tail -1 | cut -d '|' -f 6 | cut -d ':' -f 2 | tr -d ' '`

	# 4 db operations expected (1 for each file)
	if (( $nb_db_apply == $db_expect )); then
		echo "OK: $db_expect database operations"
	else
		error ": unexpected number of operations: $nb_db_apply database operations"
	fi
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
	lfs setstripe -p lustre.$POOL1 $ROOT/in_pool_1.a || error "creating file in $POOL1"
	lfs setstripe -p lustre.$POOL1 $ROOT/in_pool_1.b || error "creating file in $POOL1"
	lfs setstripe -p lustre.$POOL2 $ROOT/in_pool_2.a || error "creating file in $POOL2"
	lfs setstripe -p lustre.$POOL2 $ROOT/in_pool_2.b || error "creating file in $POOL2"

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
	if (( $is_hsm != 0 )); then
		pf=7
	else
		pf=5
	fi	

	# no_pool files must match default
	for i in 1 2; do
		(( $is_hsm + $is_backup != 0 )) &&  \
			( [ `grep "$ROOT/no_pool.$i" report.out | cut -d ',' -f 6 | tr -d ' '` = "[default]" ] || error "bad migr class for no_pool.$i" )
		 (( $is_backup == 0 )) && \
			([ `grep "$ROOT/no_pool.$i" report.out | cut -d ',' -f $pf | tr -d ' '` = "[default]" ] || error "bad purg class for no_pool.$i")
	done

	for i in a b; do
		# in_pool_1 files must match pool_1
		(( $is_hsm  + $is_backup != 0 )) && \
			 ( [ `grep "$ROOT/in_pool_1.$i" report.out | cut -d ',' -f 6  | tr -d ' '` = "pool_1" ] || error "bad migr class for in_pool_1.$i" )
		(( $is_backup == 0 )) && \
			([ `grep "$ROOT/in_pool_1.$i" report.out | cut -d ',' -f $pf | tr -d ' '` = "pool_1" ] || error "bad purg class for in_pool_1.$i")

		# in_pool_2 files must match pool_2
		(( $is_hsm + $is_backup != 0 )) && ( [ `grep "$ROOT/in_pool_2.$i" report.out  | cut -d ',' -f 6 | tr -d ' '` = "pool_2" ] || error "bad migr class for in_pool_2.$i" )
		(( $is_backup == 0 )) && \
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
		(( $is_hsm + $is_backup != 0 )) && ( [ `grep "$ROOT/no_pool.$i" report.out | cut -d ',' -f 6 | tr -d ' '` = "[default]" ] || error "bad migr class for no_pool.$i" )
		(( $is_backup == 0 )) && \
			([ `grep "$ROOT/no_pool.$i" report.out | cut -d ',' -f $pf | tr -d ' '` = "[default]" ] || error "bad purg class for no_pool.$i")
	done

	for i in a b; do
		# in_pool_1 files must match pool_1
		(( $is_hsm + $is_backup != 0 )) &&  ( [ `grep "$ROOT/in_pool_1.$i" report.out | cut -d ',' -f 6  | tr -d ' '` = "pool_1" ] || error "bad migr class for in_pool_1.$i" )
		(( $is_backup == 0 )) && \
			([ `grep "$ROOT/in_pool_1.$i" report.out | cut -d ',' -f $pf | tr -d ' '` = "pool_1" ] || error "bad purg class for in_pool_1.$i")

		# in_pool_2 files must match pool_2
		(( $is_hsm + $is_backup != 0 )) && ( [ `grep "$ROOT/in_pool_2.$i" report.out  | cut -d ',' -f 6 | tr -d ' '` = "pool_2" ] || error "bad migr class for in_pool_2.$i" )
		(( $is_backup == 0 )) && \
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

	if (( $is_hsm != 0 )); then
		lfs hsm_set --exists --archived $ROOT/file.*
		lfs hsm_clear --dirty $ROOT/file.*
	fi

	if (( $syslog )); then
		init_msg_idx=`wc -l /var/log/messages | awk '{print $1}'`
	fi

	# run a scan
	if (( $stdio )); then
		$RH -f ./cfg/$config_file --scan -l DEBUG --once >/tmp/rbh.stdout 2>/tmp/rbh.stderr || error ""
	else
		$RH -f ./cfg/$config_file --scan -l DEBUG --once || error ""
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
			alert_lines=(`grep -n ALERT /var/tmp/rbh.stdout | cut -d ':' -f 1 | xargs`)
			IFS="$old_ifs"
		#	echo ${alert_lines[0]}
		#	echo ${alert_lines[1]}
			((nbl=${alert_lines[1]}-${alert_lines[0]}+1))
			# extract nbl lines stating from line alert_lines[0]:
			tail -n +${alert_lines[0]} /var/tmp/rbh.stdout | head -n $nbl > /tmp/extract_alert
		else
			grep ALERT /tmp/rbh.stdout > /tmp/extract_alert
		fi
		# grep 'robinhood\[' => don't select lines with no headers
		grep -v ALERT /tmp/rbh.stdout | grep "$CMD[^ ]*\[" > /tmp/extract_report
		alert="/tmp/extract_alert"
		report="/tmp/extract_report"
	elif (( $syslog )); then
		tail -n +"$init_msg_idx" /var/log/messages | grep $CMD > /tmp/extract_all
		egrep -v 'ALERT' /tmp/extract_all | grep  ': [A-Za-Z ]* \|' > /tmp/extract_log
		egrep -v 'ALERT|: [A-Za-Z ]* \|' /tmp/extract_all > /tmp/extract_report
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
	
	if (( $is_backup == 0 )); then

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
			tail -n +"$init_msg_idx" /var/log/messages | grep $CMD > /tmp/extract_all
			egrep -v 'ALERT' /tmp/extract_all | grep  ': [A-Za-Z ]* \|' > /tmp/extract_log
			egrep -v 'ALERT|: [A-Za-Z ]* \|' /tmp/extract_all > /tmp/extract_report
			grep 'ALERT' /tmp/extract_all > /tmp/extract_alert
		elif (( $stdio )); then
			grep ALERT /tmp/rbh.stdout > /tmp/extract_alert
			# grep 'robinhood\[' => don't select lines with no headers
			grep -v ALERT /tmp/rbh.stdout | grep "$CMD[^ ]*\[" > /tmp/extract_report
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

	pkill -9 -f $PROC
	rm -f /tmp/test_log.1 /tmp/test_report.1 /tmp/test_alert.1
	rm -f /tmp/test_log.1.old /tmp/test_report.1.old /tmp/test_alert.1.old
}

function test_cfg_parsing
{
	flavor=$1
	dummy=$2
	policy_str="$3"

	# needed for reading password file
	if [[ ! -f /etc/robinhood.d/.dbpassword ]]; then
		if [[ ! -d /etc/robinhood.d ]]; then
			mkdir /etc/robinhood.d
		fi
		echo robinhood > /etc/robinhood.d/.dbpassword
	fi

	if [[ $flavor == "basic" ]]; then

		if (($is_backup)) ; then
			TEMPLATE=$TEMPLATE_DIR"/backup_basic.conf"
		elif (($is_hsm)); then
			TEMPLATE=$TEMPLATE_DIR"/hsm_policy_basic.conf"
		else
			TEMPLATE=$TEMPLATE_DIR"/tmp_fs_mgr_basic.conf"
		fi

	elif [[ $flavor == "detailed" ]]; then

		if (($is_backup)) ; then
			TEMPLATE=$TEMPLATE_DIR"/backup_detailed.conf"
		elif (($is_hsm)); then
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
	$RH --test-syntax -f "$TEMPLATE" || error " reading config file \"$TEMPLATE\""
}


only_test=""
quiet=0

if [[ "$1" == "-q" ]]; then
	quiet=1
	shift
fi
if [[ -n "$1" ]]; then
	only_test=$1
fi

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
	if [[ -n $6 ]]; then args=$6; else args=$5 ; fi

	index=$1
	shift

	index_clean=`echo $index | sed -e 's/[a-z]//'`

	if [[ -z $only_test || "$only_test" = "$index" || "$only_test" = "$index_clean" ]]; then
		cleanup
		echo
		echo "==== TEST #$index $2 ($args) ===="

		error_reset

		if (( $quiet == 1 )); then
			"$@" 2>&1 | tee "rh_test.log" | egrep -i -e "OK|ERR|Fail|skip|pass"
		else
			"$@"
		fi

		if [[ $ERROR = "skip" ]]; then
			echo "(TEST #$index : skipped)" >> $SUMMARY
		elif (( $ERROR > 0 )); then
			echo "TEST #$index : FAILED" >> $SUMMARY
		else
			echo "TEST #$index : OK" >> $SUMMARY
		fi
	fi
}

# clear summary
cp /dev/null $SUMMARY

#1
run_test 1	path_test test_path.conf 2 "path matching policies"
run_test 2	update_test test_updt.conf 5 30 "db update policy"
run_test 3	migration_test test1.conf 11 31 "last_mod>30s"
run_test 4	migration_test test2.conf 5  31 "last_mod>30s and name == \"*[0-5]\""
run_test 5	migration_test test3.conf 5  16 "complex policy with filesets"
run_test 6	migration_test test3.conf 10 31 "complex policy with filesets"
run_test 7	xattr_test test_xattr.conf 5 "xattr-based fileclass definition"
run_test 8	purge_test test_purge.conf 11 21 "last_access > 20s"
run_test 9	purge_size_filesets test_purge2.conf 2 3 "purge policies using size-based filesets"
run_test 10	test_rh_report common.conf 3 1 "reporting tool"
run_test 11	periodic_class_match_migr test_updt.conf 10 "periodic fileclass matching (migration)"
run_test 12	periodic_class_match_purge test_updt.conf 10 "periodic fileclass matching (purge)"
run_test 13	test_cnt_trigger test_trig.conf 101 21 "trigger on file count"
run_test 14	test_ost_trigger test_trig2.conf 100 80 "trigger on OST usage"
run_test 15	fileclass_test test_fileclass.conf 2 "complex policies with unions and intersections of filesets"
run_test 16	test_trigger_check test_trig3.conf 60 110 "triggers check only" 40 80 5
run_test 17	test_info_collect info_collect.conf 1 1 "escape string in SQL requests"
run_test 18	test_pools test_pools.conf 1 "class matching with condition on pools"
run_test 19	link_unlink_remove_test test_rm1.conf 1 31 "deferred hsm_remove (30s)"

run_test 20a	test_logs log1.conf file_nobatch 	"file logging without alert batching"
run_test 20b	test_logs log2.conf syslog_nobatch 	"syslog without alert batching"
run_test 20c	test_logs log3.conf stdio_nobatch 	"stdout and stderr without alert batching"
run_test 20d	test_logs log1b.conf file_batch 	"file logging with alert batching"
run_test 20e	test_logs log2b.conf syslog_batch 	"syslog with alert batching"
run_test 20f	test_logs log3b.conf stdio_batch 	"stdout and stderr with alert batching"

run_test 21a 	test_cfg_parsing basic none		"parsing of basic template"
run_test 21b 	test_cfg_parsing detailed none	"parsing of detailed template"
run_test 21c 	test_cfg_parsing generated none	"parsing of generated template"

echo
echo "========== TEST SUMMARY =========="
cat $SUMMARY
echo "=================================="
rm -f $SUMMARY
if (( $RC > 0 )); then
	echo "$RC tests failed"
else
	echo "All tests passed"
fi
exit $RC
