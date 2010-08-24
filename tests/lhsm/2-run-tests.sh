#/bin/sh

RH=../../src/robinhood/rbh-hsm
CFG_SCRIPT="../../scripts/rbh-config"


function clean_fs
{
	echo "Cancelling agent actions..."
	echo "purge" > /proc/fs/lustre/mdt/*/hsm_control

	echo "Waiting for end of data migration..."
	while egrep "WAITING|RUNNING|STARTED" /proc/fs/lustre/mdt/lustre-MDT0000/hsm/agent_actions > /dev/null ; do sleep 1; done

	echo "Cleaning filesystem..."
	rm  -rf /mnt/lustre/*
	
	sleep 1
	echo "Impacting rm in HSM..."
#	$RH -f ./cfg/immediate_rm.conf --readlog --hsm-remove -l DEBUG -L rh_rm.log --once || echo "ERROR"
	echo "Cleaning robinhood's DB..."
	$CFG_SCRIPT empty_db robinhood_lustre

	echo "Cleaning changelogs..."
	lfs changelog_clear lustre-MDT0000 cl1 0

	if [ -f rh.pid ]; then
		echo "killing remaining robinhood process..."
		kill `cat rh.pid`
	fi
}

function migration_test
{
	config_file=$1
	expected_migr=$2
	sleep_time=$3
	policy_str="$4"

	CLEAN="rh_chglogs.log rh_migr.log"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	# create and fill 10 files

	echo "1-Modifing files..."
	for i in a `seq 1 10`; do
		dd if=/dev/zero of=/mnt/lustre/file.$i bs=1M count=10 >/dev/null 2>/dev/null || echo "ERROR writing file.$i"
	done

	echo "2-Reading changelogs..."
	# read changelogs
	$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || echo "ERROR"

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || echo "ERROR"

	nb_migr=`grep "Start archiving" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != 0)); then
		echo "********** TEST FAILED: No migration expected, $nb_migr started"
	else
		echo "OK: no files migrated"
	fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "3-Applying migration policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once

	nb_migr=`grep "Start archiving" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != $expected_migr)); then
		echo "********** TEST FAILED: $expected_migr migrations expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"
	fi
}

function xattr_test
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	CLEAN="rh_chglogs.log rh_migr.log"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	# create and fill 10 files

	echo "1-Modifing files..."
	for i in `seq 1 3`; do
		dd if=/dev/zero of=/mnt/lustre/file.$i bs=1M count=10 >/dev/null 2>/dev/null || echo "ERROR writing file.$i"
	done

	echo "2-Setting xattrs..."
	echo "/mnt/lustre/file.1: xattr.user.foo=1"
	setfattr -n user.foo -v 1 /mnt/lustre/file.1
	echo "/mnt/lustre/file.2: xattr.user.bar=1"
	setfattr -n user.bar -v 1 /mnt/lustre/file.2
	echo "/mnt/lustre/file.3: none"

	echo "2-Reading changelogs..."
	# read changelogs
	$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || echo "ERROR"

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || echo "ERROR"

	nb_migr=`grep "Start archiving" rh_migr.log | grep hints | wc -l`
	if (($nb_migr != 0)); then
		echo "********** TEST FAILED: No migration expected, $nb_migr started"
	else
		echo "OK: no files migrated"
	fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "3-Applying migration policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once

	nb_migr=`grep "Start archiving" rh_migr.log | grep hints |  wc -l`
	if (($nb_migr != 3)); then
		echo "********** TEST FAILED: $expected_migr migrations expected, $nb_migr started"
	else
		echo "OK: $nb_migr files migrated"

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
	
}

function link_unlink_remove_test
{
	config_file=$1
	expected_rm=$2
	sleep_time=$3
	policy_str="$4"

	# read changelogs
	CLEAN="rh_chglogs.log rh_rm.log rh.pid"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	echo "1-Start reading changelogs in background..."
	# read changelogs
	$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --detach --pid-file=rh.pid || echo "ERROR"

	# write file.1 and force immediate migration
	echo "2-Writing data to file.1..."
	dd if=/dev/zero of=/mnt/lustre/file.1 bs=1M count=10 >/dev/null 2>/dev/null || echo "ERROR writing file.1"

	echo "3-Archiving file....1"
	lfs hsm_archive /mnt/lustre/file.1 || echo "ERROR"

	echo "3bis-Waiting for end of data migration..."
	while egrep "WAITING|RUNNING|STARTED" /proc/fs/lustre/mdt/lustre-MDT0000/hsm/agent_actions ; do sleep 1; done

	# create links on file.1 files
	echo "4-Creating hard links to /mnt/lustre/file.1..."
	ln /mnt/lustre/file.1 /mnt/lustre/link.1 || echo "ERROR"
	ln /mnt/lustre/file.1 /mnt/lustre/link.2 || echo "ERROR"

	# removing all files
        echo "5-Removing all links to file.1..."
	rm -f /mnt/lustre/link.* /mnt/lustre/file.1 

	# deferred remove delay is not reached: nothing should be removed
	echo "6-Performing HSM remove requests (before delay expiration)..."
	$RH -f ./cfg/$config_file --hsm-remove -l DEBUG -L rh_rm.log --once || echo "ERROR"

	nb_rm=`grep "Remove request successful" rh_rm.log | wc -l`
	if (($nb_rm != 0)); then
		echo "********** test failed: no removal expected, $nb_rm done"
	else
		echo "OK: no rm done"
	fi

	echo "7-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "8-Performing HSM remove requests (after delay expiration)..."
	$RH -f ./cfg/$config_file --hsm-remove -l DEBUG -L rh_rm.log --once || echo "ERROR"

	nb_rm=`grep "Remove request successful" rh_rm.log | wc -l`
	if (($nb_rm != $expected_rm)); then
		echo "********** TEST FAILED: $expected_rm removals expected, $nb_rm done"
	else
		echo "OK: $nb_rm files removed from archive"
	fi

	# kill event handler
	pkill -9 -f rbh-hsm

}

function purge_test
{
	config_file=$1
	expected_purge=$2
	sleep_time=$3
	policy_str="$4"

	CLEAN="rh_chglogs.log rh_purge.log"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	# initial scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log 

	# fill 10 files and mark them archived+non dirty

	echo "1-Modifing files..."
	for i in a `seq 1 10`; do
		dd if=/dev/zero of=/mnt/lustre/file.$i bs=1M count=10 >/dev/null 2>/dev/null || echo "ERROR writing file.$i"
		lfs hsm_set --exists --archived /mnt/lustre/file.$i
		lfs hsm_clear --dirty /mnt/lustre/file.$i
	done
	
	echo "2-Reading changelogs to update file status (after 1sec)..."
	sleep 1
	$RH -f ./cfg/$config_file --readlog -l DEBUG --once -L rh_chglogs.log

	echo "3-Applying purge policy ($policy_str)..."
	# no purge expected here
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || echo "ERROR"

        nb_purge=`grep "Releasing" rh_purge.log | wc -l`
        if (($nb_purge != 0)); then
                echo "********** TEST FAILED: No release actions expected, $nb_purge done"
        else
                echo "OK: no file released"
        fi

	echo "4-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "5-Applying purge policy again ($policy_str)..."
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || echo "ERROR"

        nb_purge=`grep "Releasing" rh_purge.log | wc -l`
        if (($nb_purge != $expected_purge)); then
                echo "********** TEST FAILED: $expected_purge release actions expected, $nb_purge done"
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

	CLEAN="rh_chglogs.log rh_purge.log"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	# initial scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log 

	# fill 3 files of different sizes and mark them archived non-dirty

	j=1
	for size in 0 1 10 200; do
		echo "1.$j-Writing files of size " $(( $size*10 )) "kB..."
		((j=$j+1))
		for i in `seq 1 $count`; do
			dd if=/dev/zero of=/mnt/lustre/file.$size.$i bs=10k count=$size >/dev/null 2>/dev/null || echo "ERROR writing file.$size.$i"
			lfs hsm_set --exists --archived /mnt/lustre/file.$size.$i
			lfs hsm_clear --dirty /mnt/lustre/file.$size.$i
		done
	done
	
	echo "2-Reading changelogs to update file status (after 1sec)..."
	sleep 1
	$RH -f ./cfg/$config_file --readlog -l DEBUG --once -L rh_chglogs.log

	echo "3-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "4-Applying purge policy ($policy_str)..."
	# no purge expected here
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || echo "ERROR"

	# counting each matching policy $count of each
	for policy in very_small mid_file default; do
	        nb_purge=`grep 'using policy' rh_purge.log | grep $policy | wc -l`
		if (($nb_purge != $count)); then
			echo "********** TEST FAILED: $count release actions expected using policy $policy, $nb_purge done"
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

	CLEAN="rh_chglogs.log rh_report.log"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	for i in `seq 1 $dircount`; do
		mkdir /mnt/lustre/dir.$i
		echo "1.$i-Writing files to /mnt/lustre/dir.$i..."
		# write i MB to each directory
		for j in `seq 1 $i`; do
			dd if=/dev/zero of=/mnt/lustre/dir.$i/file.$j bs=1M count=1 >/dev/null 2>/dev/null || echo "ERROR writing /mnt/lustre/dir.$i/file.$j"
		done
	done

	echo "1bis. Wait for IO completion..."
	sync

	echo "2.Reading ChangeLogs..."
	$RH -f ./cfg/$config_file --readlog -l DEBUG --once -L rh_chglogs.log

	echo "3.Checking reports..."
	for i in `seq 1 $dircount`; do
		$RH-report -f ./cfg/$config_file -l MAJOR --csv -U 1 -P /mnt/lustre/dir.$i > rh_report.log
		used=`tail -n 1 rh_report.log | cut -d "," -f 3`
		if (( $used != $i*1024*1024 )); then
			echo "ERROR: $used != " $(($i*1024*1024))
		else
			echo "OK: $i MB in /mnt/lustre/dir.$i"
		fi
	done

	exit 1
	
}

function path_test
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	CLEAN="rh_chglogs.log rh_migr.log"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	# create test tree

	mkdir -p /mnt/lustre/dir1
	mkdir -p /mnt/lustre/dir1/subdir1
	mkdir -p /mnt/lustre/dir1/subdir2
	mkdir -p /mnt/lustre/dir1/subdir3/subdir4
	# 2 matching files for fileclass absolute_path
	echo "data" > /mnt/lustre/dir1/subdir1/A
	echo "data" > /mnt/lustre/dir1/subdir2/A
	# 2 unmatching
	echo "data" > /mnt/lustre/dir1/A
	echo "data" > /mnt/lustre/dir1/subdir3/subdir4/A

	mkdir -p /mnt/lustre/dir2
	mkdir -p /mnt/lustre/dir2/subdir1
	# 2 matching files for fileclass absolute_tree
	echo "data" > /mnt/lustre/dir2/X
	echo "data" > /mnt/lustre/dir2/subdir1/X

	mkdir -p /mnt/lustre/one_dir/dir3
	mkdir -p /mnt/lustre/other_dir/dir3
	mkdir -p /mnt/lustre/dir3
	mkdir -p /mnt/lustre/one_dir/one_dir/dir3
	# 2 matching files for fileclass path_depth2
	echo "data" > /mnt/lustre/one_dir/dir3/X
	echo "data" > /mnt/lustre/other_dir/dir3/Y
	# 2 unmatching files for fileclass path_depth2
	echo "data" > /mnt/lustre/dir3/X
	echo "data" > /mnt/lustre/one_dir/one_dir/dir3/X

	mkdir -p /mnt/lustre/one_dir/dir4/subdir1
	mkdir -p /mnt/lustre/other_dir/dir4/subdir1
	mkdir -p /mnt/lustre/dir4
	mkdir -p /mnt/lustre/one_dir/one_dir/dir4
	# 2 matching files for fileclass tree_depth2
	echo "data" > /mnt/lustre/one_dir/dir4/subdir1/X
	echo "data" > /mnt/lustre/other_dir/dir4/subdir1/X
	# unmatching files for fileclass tree_depth2
	echo "data" > /mnt/lustre/dir4/X
	echo "data" > /mnt/lustre/one_dir/one_dir/dir4/X
	
	mkdir -p /mnt/lustre/dir5
	mkdir -p /mnt/lustre/subdir/dir5
	# 2 matching files for fileclass relative_path
	echo "data" > /mnt/lustre/dir5/A
	echo "data" > /mnt/lustre/dir5/B
	# 2 unmatching files for fileclass relative_path
	echo "data" > /mnt/lustre/subdir/dir5/A
	echo "data" > /mnt/lustre/subdir/dir5/B

	mkdir -p /mnt/lustre/dir6/subdir
	mkdir -p /mnt/lustre/subdir/dir6
	# 2 matching files for fileclass relative_tree
	echo "data" > /mnt/lustre/dir6/A
	echo "data" > /mnt/lustre/dir6/subdir/A
	# 2 unmatching files for fileclass relative_tree
	echo "data" > /mnt/lustre/subdir/dir6/A
	echo "data" > /mnt/lustre/subdir/dir6/B


	mkdir -p /mnt/lustre/dir7/subdir
	mkdir -p /mnt/lustre/dir71/subdir
	mkdir -p /mnt/lustre/subdir/subdir/dir7
	mkdir -p /mnt/lustre/subdir/subdir/dir72
	# 2 matching files for fileclass any_root_tree
	echo "data" > /mnt/lustre/dir7/subdir/file
	echo "data" > /mnt/lustre/subdir/subdir/dir7/file
	# 2 unmatching files for fileclass any_root_tree
	echo "data" > /mnt/lustre/dir71/subdir/file
	echo "data" > /mnt/lustre/subdir/subdir/dir72/file

	mkdir -p /mnt/lustre/dir8
	mkdir -p /mnt/lustre/dir81/subdir
	mkdir -p /mnt/lustre/subdir/subdir/dir8
	# 2 matching files for fileclass any_root_path
	echo "data" > /mnt/lustre/dir8/file.1
	echo "data" > /mnt/lustre/subdir/subdir/dir8/file.1
	# 3 unmatching files for fileclass any_root_path
	echo "data" > /mnt/lustre/dir8/file.2
	echo "data" > /mnt/lustre/dir81/file.1
	echo "data" > /mnt/lustre/subdir/subdir/dir8/file.2

	mkdir -p /mnt/lustre/dir9/subdir/dir10/subdir
	mkdir -p /mnt/lustre/dir9/subdir/dir10x/subdir
	mkdir -p /mnt/lustre/dir91/subdir/dir10
	# 2 matching files for fileclass any_level_tree
	echo "data" > /mnt/lustre/dir9/subdir/dir10/file
	echo "data" > /mnt/lustre/dir9/subdir/dir10/subdir/file
	# 2 unmatching files for fileclass any_level_tree
	echo "data" > /mnt/lustre/dir9/subdir/dir10x/subdir/file
	echo "data" > /mnt/lustre/dir91/subdir/dir10/file

	mkdir -p /mnt/lustre/dir11/subdir/subdir
	mkdir -p /mnt/lustre/dir11x/subdir
	# 2 matching files for fileclass any_level_path
	echo "data" > /mnt/lustre/dir11/subdir/file
	echo "data" > /mnt/lustre/dir11/subdir/subdir/file
	# 2 unmatching files for fileclass any_level_path
	echo "data" > /mnt/lustre/dir11/subdir/file.x
	echo "data" > /mnt/lustre/dir11x/subdir/file


	echo "1bis-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "2-Reading changelogs..."
	# read changelogs
	$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || echo "ERROR"

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || echo "ERROR"

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

	(( $nb_pol1 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'absolute_path': $nb_pol1"
	(( $nb_pol2 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'absolute_tree': $nb_pol2"
	(( $nb_pol3 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'path_depth2': $nb_pol3"
	(( $nb_pol4 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'tree_depth2': $nb_pol4"
	(( $nb_pol5 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'relative_path': $nb_pol5"
	(( $nb_pol6 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'relative_tree': $nb_pol6"

	(( $nb_pol7 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'any_root_tree': $nb_pol7"
	(( $nb_pol8 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'any_root_path': $nb_pol8"
	(( $nb_pol9 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'any_level_tree': $nb_pol9"
	(( $nb_pol10 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'any_level_tree': $nb_pol10"
	(( $nb_unmatch == 19 )) || echo "********** TEST FAILED: wrong count of unmatching files: $nb_unmatch"

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

	LOG="rh_chglogs.log"

	init=`date "+%s"`

	for i in `seq 1 3`; do
		echo "loop 1.$i: many 'touch' within $event_updt_min sec"
		cp /dev/null $LOG

		# start log reader (DEBUG level displays needed attrs)
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L $LOG --detach --pid-file=rh.pid || echo "ERROR"

		start=`date "+%s"`
		# generate a lot of TIME events within 'event_updt_min'
		# => must only update once
		while (( `date "+%s"` - $start < $event_updt_min - 2 )); do
			touch /mnt/lustre/file
			usleep 10000
		done

		# force flushing log
		sleep 1
		pkill -f rbh-hsm
		sleep 1

		nb_getattr=`grep getattr=1 $LOG | wc -l`
		echo "nb attr update: $nb_getattr"
		(( $nb_getattr == 1 )) || echo "********** TEST FAILED: wrong count of getattr: $nb_getattr"

		# the path may be retrieved at the first loop (at creation)
		# but not during the next loop (as long as enlapsed time < update_period)
		if (( $i > 1 )) && (( `date "+%s"` - $init < $update_period )); then
			nb_getpath=`grep getpath=1 $LOG | wc -l`
			echo "nb path update: $nb_getpath"
			(( $nb_getpath == 0 )) || echo "********** TEST FAILED: wrong count of getpath: $nb_getpath"
		fi
	done

	init=`date "+%s"`

	for i in `seq 1 3`; do
		echo "loop 2.$i: many 'rename' within $event_updt_min sec"
		cp /dev/null $LOG

		# start log reader (DEBUG level displays needed attrs)
		$RH -f ./cfg/$config_file --readlog -l DEBUG -L $LOG --detach --pid-file=rh.pid || echo "ERROR"

		start=`date "+%s"`
		# generate a lot of TIME events within 'event_updt_min'
		# => must only update once
		while (( `date "+%s"` - $start < $event_updt_min - 2 )); do
			mv /mnt/lustre/file /mnt/lustre/file.2
			usleep 10000
			mv /mnt/lustre/file.2 /mnt/lustre/file
			usleep 10000
		done

		# force flushing log
		sleep 1
		pkill -f rbh-hsm
		sleep 1

		nb_getpath=`grep getpath=1 $LOG | wc -l`
		echo "nb path update: $nb_getpath"
		(( $nb_getpath == 1 )) || echo "********** TEST FAILED: wrong count of getpath: $nb_getpath"

		# attributes may be retrieved at the first loop (at creation)
		# but not during the next loop (as long as enlapsed time < update_period)
		if (( $i > 1 )) && (( `date "+%s"` - $init < $update_period )); then
			nb_getattr=`grep getattr=1 $LOG | wc -l`
			echo "nb attr update: $nb_getattr"
			(( $nb_getattr == 0 )) || echo "********** TEST FAILED: wrong count of getattr: $nb_getattr"
		fi
	done

	echo "Waiting $update_period seconds..."
	cp /dev/null $LOG

	# check that getattr+getpath are performed after update_period, even if the event is not related:
	$RH -f ./cfg/$config_file --readlog -l DEBUG -L $LOG --detach --pid-file=rh.pid || echo "ERROR"
	sleep $update_period
	lfs hsm_set --exists /mnt/lustre/file

	# force flushing log
	sleep 1
	pkill -f rbh-hsm
	sleep 1

	nb_getattr=`grep getattr=1 $LOG | wc -l`
	echo "nb attr update: $nb_getattr"
	(( $nb_getattr == 1 )) || echo "********** TEST FAILED: wrong count of getattr: $nb_getattr"
	nb_getpath=`grep getpath=1 $LOG | wc -l`
	echo "nb path update: $nb_getpath"
	(( $nb_getpath == 1 )) || echo "********** TEST FAILED: wrong count of getpath: $nb_getpath"

	# also check that the status is to be retrieved
	nb_getstatus=`grep getstatus=1 $LOG | wc -l`
	echo "nb status update: $nb_getstatus"
	(( $nb_getstatus == 1 )) || echo "********** TEST FAILED: wrong count of getstatus: $nb_getstatus"

	# kill remaning event handler
	sleep 1
	pkill -9 -f rbh-hsm
}

function periodic_class_match_migr
{
	config_file=$1
	update_period=$2
	policy_str="$3"

	CLEAN="rh_chglogs.log rh_migr.log"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	#create test tree
	touch /mnt/lustre/ignore1
	touch /mnt/lustre/whitelist1
	touch /mnt/lustre/migrate1
	touch /mnt/lustre/default1

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log

	# now apply policies
	$RH -f ./cfg/$config_file --migrate --dry-run -l FULL -L rh_migr.log --once || echo "ERROR"

	#we must have 4 lines like this: "Need to update fileclass (not set)"
	nb_updt=`grep "Need to update fileclass (not set)" rh_migr.log | wc -l`
	nb_migr_match=`grep "matches the condition for policy 'migr_match'" rh_migr.log | wc -l`
	nb_default=`grep "matches the condition for policy 'default'" rh_migr.log | wc -l`

	(( $nb_updt == 4 )) || echo "********** TEST FAILED: wrong count of fileclass update: $nb_updt"
	(( $nb_migr_match == 1 )) || echo "********** TEST FAILED: wrong count of files matching 'migr_match': $nb_migr_match"
	(( $nb_default == 1 )) || echo "********** TEST FAILED: wrong count of files matching 'default': $nb_default"

        (( $nb_updt == 4 )) && (( $nb_migr_match == 1 )) && (( $nb_default == 1 )) \
		&& echo "OK: initial fileclass matching successful"

	# rematch entries: should not update fileclasses
	cp /dev/null rh_migr.log
	$RH -f ./cfg/$config_file --migrate --dry-run -l FULL -L rh_migr.log --once || echo "ERROR"

	nb_default_valid=`grep "fileclass '@default@' is still valid" rh_migr.log | wc -l`
	nb_migr_valid=`grep "fileclass 'to_be_migr' is still valid" rh_migr.log | wc -l`
	nb_updt=`grep "Need to update fileclass" rh_migr.log | wc -l`

	(( $nb_default_valid == 1 )) || echo "********** TEST FAILED: wrong count of cached fileclass for default policy: $nb_default_valid"
	(( $nb_migr_valid == 1 )) || echo "********** TEST FAILED: wrong count of cached fileclass for 'migr_match' : $nb_migr_valid"
	(( $nb_updt == 0 )) || echo "********** TEST FAILED: no expected fileclass update: $nb_updt updated"

        (( $nb_updt == 0 )) && (( $nb_default_valid == 1 )) && (( $nb_migr_valid == 1 )) \
		&& echo "OK: fileclasses do not need update"
	
	echo "Waiting $update_period sec..."
	sleep $update_period

	# rematch entries: should update all fileclasses
	cp /dev/null rh_migr.log
	$RH -f ./cfg/$config_file --migrate --dry-run -l FULL -L rh_migr.log --once || echo "ERROR"

	nb_valid=`grep "is still valid" rh_migr.log | wc -l`
	nb_updt=`grep "Need to update fileclass (out-of-date)" rh_migr.log | wc -l`

	(( $nb_valid == 0 )) || echo "********** TEST FAILED: fileclass should need update : $nb_valid still valid"
	(( $nb_updt == 4 )) || echo "********** TEST FAILED: all fileclasses should be updated : $nb_updt/4"

        (( $nb_valid == 0 )) && (( $nb_updt == 4 )) \
		&& echo "OK: all fileclasses updated"
}

function periodic_class_match_purge
{
	config_file=$1
	update_period=$2
	policy_str="$3"

	CLEAN="rh_chglogs.log rh_purge.log"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	#create test tree of archived files
	for file in ignore1 whitelist1 purge1 default1 ; do
		touch /mnt/lustre/$file
		lfs hsm_set --exists --archived /mnt/lustre/$file
	done

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log

	# now apply policies
	$RH -f ./cfg/$config_file --purge-fs=0 --dry-run -l FULL -L rh_purge.log --once || echo "ERROR"

	#we must have 4 lines like this: "Need to update fileclass (not set)"
	nb_updt=`grep "Need to update fileclass (not set)" rh_purge.log | wc -l`
	nb_purge_match=`grep "matches the condition for policy 'purge_match'" rh_purge.log | wc -l`
	nb_default=`grep "matches the condition for policy 'default'" rh_purge.log | wc -l`

	(( $nb_updt == 4 )) || echo "********** TEST FAILED: wrong count of fileclass update: $nb_updt"
	(( $nb_purge_match == 1 )) || echo "********** TEST FAILED: wrong count of files matching 'purge_match': $nb_purge_match"
	(( $nb_default == 1 )) || echo "********** TEST FAILED: wrong count of files matching 'default': $nb_default"

        (( $nb_updt == 4 )) && (( $nb_purge_match == 1 )) && (( $nb_default == 1 )) \
		&& echo "OK: initial fileclass matching successful"

	# update db content and rematch entries: should not update fileclasses
	cp /dev/null rh_purge.log
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log
	$RH -f ./cfg/$config_file --purge-fs=0 --dry-run -l FULL -L rh_purge.log --once || echo "ERROR"

	nb_default_valid=`grep "fileclass '@default@' is still valid" rh_purge.log | wc -l`
	nb_purge_valid=`grep "fileclass 'to_be_released' is still valid" rh_purge.log | wc -l`
	nb_updt=`grep "Need to update fileclass" rh_purge.log | wc -l`

	(( $nb_default_valid == 1 )) || echo "********** TEST FAILED: wrong count of cached fileclass for default policy: $nb_default_valid"
	(( $nb_purge_valid == 1 )) || echo "********** TEST FAILED: wrong count of cached fileclass for 'purge_match' : $nb_purge_valid"
	(( $nb_updt == 0 )) || echo "********** TEST FAILED: no expected fileclass update: $nb_updt updated"

        (( $nb_updt == 0 )) && (( $nb_default_valid == 1 )) && (( $nb_purge_valid == 1 )) \
		&& echo "OK: fileclasses do not need update"
	
	echo "Waiting $update_period sec..."
	sleep $update_period

	# update db content and rematch entries: should update all fileclasses
	cp /dev/null rh_purge.log
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log
	$RH -f ./cfg/$config_file --purge-fs=0 --dry-run -l FULL -L rh_purge.log --once || echo "ERROR"

	nb_valid=`grep "is still valid" rh_purge.log | wc -l`
	nb_updt=`grep "Need to update fileclass (out-of-date)" rh_purge.log | wc -l`

	(( $nb_valid == 0 )) || echo "********** TEST FAILED: fileclass should need update : $nb_valid still valid"
	(( $nb_updt == 4 )) || echo "********** TEST FAILED: all fileclasses should be updated : $nb_updt/4"

        (( $nb_valid == 0 )) && (( $nb_updt == 4 )) \
		&& echo "OK: all fileclasses updated"
}

function test_cnt_trigger
{
	config_file=$1
	file_count=$2
	exp_purge_count=$3
	policy_str="$4"

	CLEAN="rh_chglogs.log rh_purge.log"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	# initial inode count
	empty_count=`df -i /mnt/lustre/ | grep "/mnt/lustre" | awk '{print $(NF-3)}'`
	(( file_count=$file_count - $empty_count ))

	#create test tree of archived files (1M each)
	for i in `seq 1 $file_count`; do
		dd if=/dev/zero of=/mnt/lustre/file.$i bs=1M count=1
		lfs hsm_set --exists --archived /mnt/lustre/file.$i
	done

	# wait for df sync
	sync; sleep 1

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log

	# apply purge trigger
	$RH -f ./cfg/$config_file --purge --once -l FULL -L rh_purge.log

	nb_release=`grep "Released" rh_purge.log | wc -l`
	if (($nb_release == $exp_purge_count)); then
		echo "OK: $nb_release files released"
	else
		echo "ERROR: $nb_release files released, $exp_purge_count expected"
	fi
}


function test_ost_trigger
{
	config_file=$1
	mb_h_watermark=$2
	mb_l_watermark=$3
	policy_str="$4"

	CLEAN="rh_chglogs.log rh_purge.log"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	empty_vol=`lfs df  | grep OST0000 | awk '{print $3}'`
	empty_vol=$(($empty_vol/1024))

	lfs setstripe --count 2 --offset 0 /mnt/lustre || echo "ERROR setting stripe_count=2"

	#create test tree of archived files (2M each=1MB/ost) until we reach high watermark
	for i in `seq $empty_vol $mb_h_watermark`; do
		dd if=/dev/zero of=/mnt/lustre/file.$i bs=1M count=2
		lfs hsm_set --exists --archived /mnt/lustre/file.$i
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
	(( $needed_ost == $need_purge )) || echo "ERROR: invalid amount of data computed"
	(( $purged_ost >= $need_purge )) && (( $purged_ost <= $need_purge + 1 )) || echo "ERROR: invalid amount of data purged"
	(( $purged_total == 2*$purged_ost )) || echo "ERROR: invalid total volume purged"

	(( $needed_ost == $need_purge )) && (( $purged_ost >= $need_purge )) && (( $purged_ost <= $need_purge + 1 )) \
		&& (( $purged_total == 2*$purged_ost )) && echo "OK: purge of OST#0 succeeded"

	full_vol1=`lfs df  | grep OST0001 | awk '{print $3}'`
	full_vol1=$(($full_vol1/1024))
	purge_ost1=`grep summary rh_purge.log | grep "OST #1" | wc -l`

	if (($full_vol1 > $mb_h_watermark )); then
		echo "ERROR: OST#1 is not expected to exceed high watermark!"
	elif (($purge_ost1 != 0)); then
		echo "ERROR: no purge expected on OST#1"
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

	CLEAN="rh_chglogs.log rh_purge.log"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	# triggers to be checked
	# - inode count > max_count
	# - fs volume	> max_vol
	# - root quota  > user_quota

	# initial inode count
	empty_count=`df -i /mnt/lustre/ | grep "/mnt/lustre" | awk '{print $(NF-3)}'`
	((file_count=$max_count-$empty_count))

	# compute file size to exceed max vol and user quota
	empty_vol=`df -k /mnt/lustre  | grep "/mnt/lustre" | awk '{print $(NF-3)}'`
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
		dd if=/dev/zero of=/mnt/lustre/file.$i bs=1M count=$file_size
		lfs hsm_set --exists --archived /mnt/lustre/file.$i
	done

	# wait for df sync
	sync; sleep 1

	# scan
	$RH -f ./cfg/$config_file --scan --once -l DEBUG -L rh_chglogs.log

	# check purge triggers
	$RH -f ./cfg/$config_file --check-triggers --once -l FULL -L rh_purge.log

	((expect_count=$empty_count+$file_count-$target_count))
	((expect_vol_fs=$empty_vol+$file_count*$file_size-$target_fs_vol))
	((expect_vol_user=$file_count*$file_size-$target_user_vol))
	echo "over trigger limits: $expect_count entries, $expect_vol_fs MB, $expect_vol_user MB for user root"

	nb_release=`grep "Released" rh_purge.log | wc -l`

	count_trig=`grep " entries must be purged in Filesystem" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`

	vol_fs_trig=`grep " blocks (x512) must be purged on Filesystem" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`
	((vol_fs_trig_mb=$vol_fs_trig/2048)) # /2048 == *512/1024/1024

	vol_user_trig=`grep " blocks (x512) must be purged for user" rh_purge.log | cut -d '|' -f 2 | awk '{print $1}'`
	((vol_user_trig_mb=$vol_user_trig/2048)) # /2048 == *512/1024/1024
	
	echo "triggers reported: $count_trig entries, $vol_fs_trig_mb MB, $vol_user_trig_mb MB"

	# check then was no actual purge
	if (($nb_release > 0)); then
		echo "ERROR: $nb_release files released, no purge expected"
	elif (( $count_trig != $expect_count )); then
		echo "ERROR: trigger reported $count_trig files over watermark, $expect_count expected"
	elif (( $vol_fs_trig_mb != $expect_vol_fs )); then
		echo "ERROR: trigger reported $vol_fs_trig_mb MB over watermark, $expect_vol_fs expected"
	elif (( $vol_user_trig_mb != $expect_vol_user )); then
		echo "ERROR: trigger reported $vol_user_trig_mb MB over watermark, $expect_vol_user expected"
	else
		echo "OK: all checks successful"
	fi
}


function fileclass_test
{
	config_file=$1
	sleep_time=$2
	policy_str="$3"

	CLEAN="rh_chglogs.log rh_migr.log"

	for f in $CLEAN; do
		if [ -f $f ]; then
			cp /dev/null $f
		fi
	done

	# create test tree

	mkdir -p /mnt/lustre/dir_A
	mkdir -p /mnt/lustre/dir_B
	mkdir -p /mnt/lustre/dir_C

	# classes are:
	# 1) even_and_B
	# 2) even_and_not_B
	# 3) odd_or_A
	# 4) other

	echo "data" > /mnt/lustre/dir_A/file.0 #2
	echo "data" > /mnt/lustre/dir_A/file.1 #3
	echo "data" > /mnt/lustre/dir_A/file.2 #2
	echo "data" > /mnt/lustre/dir_A/file.3 #3
	echo "data" > /mnt/lustre/dir_A/file.x #3
	echo "data" > /mnt/lustre/dir_A/file.y #3

	echo "data" > /mnt/lustre/dir_B/file.0 #1
	echo "data" > /mnt/lustre/dir_B/file.1 #3
	echo "data" > /mnt/lustre/dir_B/file.2 #1
	echo "data" > /mnt/lustre/dir_B/file.3 #3

	echo "data" > /mnt/lustre/dir_C/file.0 #2
	echo "data" > /mnt/lustre/dir_C/file.1 #3
	echo "data" > /mnt/lustre/dir_C/file.2 #2
	echo "data" > /mnt/lustre/dir_C/file.3 #3
	echo "data" > /mnt/lustre/dir_C/file.x #4
	echo "data" > /mnt/lustre/dir_C/file.y #4

	# => 2x 1), 4x 2), 8x 3), 2x 4)

	echo "1bis-Sleeping $sleep_time seconds..."
	sleep $sleep_time

	echo "2-Reading changelogs..."
	# read changelogs
	$RH -f ./cfg/$config_file --readlog -l DEBUG -L rh_chglogs.log  --once || echo "ERROR"

	echo "3-Applying migration policy ($policy_str)..."
	# start a migration files should notbe migrated this time
	$RH -f ./cfg/$config_file --migrate -l DEBUG -L rh_migr.log  --once || echo "ERROR"

	# count the number of file for each policy
	nb_pol1=`grep hints rh_migr.log | grep even_and_B | wc -l`
	nb_pol2=`grep hints rh_migr.log | grep even_and_not_B | wc -l`
	nb_pol3=`grep hints rh_migr.log | grep odd_or_A | wc -l`
	nb_pol4=`grep hints rh_migr.log | grep unmatched | wc -l`

	#nb_pol1=`grep "matches the condition for policy 'inter_migr'" rh_migr.log | wc -l`
	#nb_pol2=`grep "matches the condition for policy 'union_migr'" rh_migr.log | wc -l`
	#nb_pol3=`grep "matches the condition for policy 'not_migr'" rh_migr.log | wc -l`
	#nb_pol4=`grep "matches the condition for policy 'default'" rh_migr.log | wc -l`

	(( $nb_pol1 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for fileclass 'even_and_B': $nb_pol1"
	(( $nb_pol2 == 4 )) || echo "********** TEST FAILED: wrong count of matching files for fileclass 'even_and_not_B': $nb_pol2"
	(( $nb_pol3 == 8 )) || echo "********** TEST FAILED: wrong count of matching files for fileclass 'odd_or_A': $nb_pol3"
	(( $nb_pol4 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for fileclass 'unmatched': $nb_pol4"

	(( $nb_pol1 == 2 )) && (( $nb_pol2 == 4 )) && (( $nb_pol3 == 8 )) \
		&& (( $nb_pol4 == 2 )) && echo "OK: test successful"
}

only_test=""
index=1
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
                clean_fs | tee "rh_test.log" | egrep -i -e "OK|ERR|Fail"
        else
                clean_fs
        fi
}

function run_test
{
	if [[ -n $5 ]]; then args=$5; else args=$4 ; fi

	if [[ -z $only_test || "$only_test" = "$index" ]]; then
		cleanup
		echo "TEST #$index: $1 ($args)"

		if (( $quiet == 1 )); then
			"$@" 2>&1 | tee "rh_test.log" | egrep -i -e "OK|ERR|Fail"
		else
			"$@"
		fi
	fi
	((index=$index+1))
}

#1
run_test 	path_test test_path.conf 2 "path matching policies"
#2
run_test 	update_test test_updt.conf 5 30 "db update policy"
#3
run_test 	migration_test test1.conf 11 31 "last_mod>30s"
#4
run_test 	migration_test test2.conf 5  31 "last_mod>30s and name == \"*[0-5]\""
#5
run_test 	migration_test test3.conf 5  16 "complex policy with filesets"
#6
run_test 	migration_test test3.conf 10 31 "complex policy with filesets"
#7
run_test 	xattr_test test_xattr.conf 5 "xattr-based fileclass definition"
#8
run_test 	purge_test test_purge.conf 11 21 "last_access > 20s"
#9
run_test	purge_size_filesets test_purge2.conf 2 3 "purge policies using size-based filesets"
#10
run_test 	test_rh_report common.conf 3 1 "reporting tool"
#11
run_test	periodic_class_match_migr test_updt.conf 10 "periodic fileclass matching (migration)"
#12
run_test	periodic_class_match_purge test_updt.conf 10 "periodic fileclass matching (purge)"
#13
run_test	test_cnt_trigger test_trig.conf 101 21 "trigger on file count"
#14
run_test	test_ost_trigger test_trig2.conf 100 80 "trigger on OST usage"
#15
run_test 	fileclass_test test_fileclass.conf 2 "complex policies with unions and intersections of filesets"
#16
run_test 	test_trigger_check test_trig3.conf 60 110 "triggers check only" 40 80 5

#17
#run_test 	link_unlink_remove_test test_rm1.conf 1 31 "deferred hsm_remove (30s)"
