#/bin/sh

RH=../../src/Robinhood/rbh-hsm


function clean_fs
{
	echo "Waiting for end of data migration..."
	while egrep "WAITING|RUNNING" /proc/fs/lustre/mdt/lustre-MDT0000/hsm/agent_actions > /dev/null ; do sleep 1; done

	echo "Cleaning filesystem..."
	rm  -rf /mnt/lustre/*
	
	sleep 1
	echo "Impacting rm in HSM..."
#	$RH -f ./cfg/immediate_rm.conf --readlog --hsm-remove -l DEBUG -L rh_rm.log --once || echo "ERROR"
	echo "Cleaning Robinhood's DB..."
	mysql --user=robinhood --password=robinhood --host=localhost robinhood_lustre < ../dropall.sql

	echo "Cleaning changelogs..."
	lfs changelog_clear lustre-MDT0000 cl1 0
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
	while egrep "WAITING|RUNNING" /proc/fs/lustre/mdt/lustre-MDT0000/hsm/agent_actions > /dev/null ; do sleep 1; done

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

	echo "7-Sleeping $sleep_time second..."
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
	pkill -9 rh-hsm

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

# start RH for reading changelogs + migration
#	$RH -f ./cfg/$config_file --readlog --migrate -l DEBUG -L rh_chglogs.log &

	# fill 10 files and migrate them

	echo "1-Modifing files..."
	for i in a `seq 1 10`; do
		dd if=/dev/zero of=/mnt/lustre/file.$i bs=1M count=10 >/dev/null 2>/dev/null || echo "ERROR writing file.$i"
	done
	
	echo "2-Flushing files (after 2sec)..."
	sleep 2 # sleep 2 to match migration condition
	$RH -f ./cfg/$config_file --readlog --migrate -l FULL --once -L rh_chglogs.log

	echo "3-Reading changelogs to know migrated files (after 10sec)..."
	sleep 10 # wait for file migration to complete
	echo "3bis-reading now"
	$RH -f ./cfg/$config_file --readlog -l FULL --once -L rh_chglogs.log

	echo "4-Applying purge policy ($policy_str)..."
	# no purge expected here
	$RH -f ./cfg/$config_file --purge-fs=0 -l DEBUG -L rh_purge.log --once || echo "ERROR"

        nb_purge=`grep "Releasing" rh_purge.log | wc -l`
        if (($nb_purge != 0)); then
                echo "********** TEST FAILED: No release actions expected, $nb_purge done"
        else
                echo "OK: no file released"
        fi

	echo "3-Sleeping $sleep_time second..."
	sleep $sleep_time

	echo "4-Applying purge policy again ($policy_str)..."
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

# start RH for reading changelogs + migration
#	$RH -f ./cfg/$config_file --readlog --migrate -l DEBUG -L rh_chglogs.log &

	# fill 3 files of different sizes and migrate them

	j=1
	for size in 0 1 10 200; do
		echo "1.$j-Writing files of size " $(( $size*10 )) "kB..."
		((j=$j+1))
		for i in `seq 1 $count`; do
			dd if=/dev/zero of=/mnt/lustre/file.$size.$i bs=10k count=$size >/dev/null 2>/dev/null || echo "ERROR writing file.$size.$i"
		done
	done
	
	echo "2-Flushing files (after 2sec)..."
	sleep 2 # sleep 2 to match migration condition
	$RH -f ./cfg/$config_file --readlog --migrate -l FULL --once -L rh_chglogs.log

	echo "3-Reading changelogs to know migrated files (after 10sec)..."
	sleep 10 # wait for file migration to complete
	echo "3bis-reading now"
	$RH -f ./cfg/$config_file --readlog -l FULL --once -L rh_chglogs.log

	echo "3ter-Sleeping $sleep_time seconds..."
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
	$RH -f ./cfg/$config_file --readlog -l FULL --once -L rh_chglogs.log

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
	# 2 matching files for fileclass path_any_root
	echo "data" > /mnt/lustre/one_dir/dir3/X
	echo "data" > /mnt/lustre/other_dir/dir3/Y
	# 2 unmatching files for fileclass path_any_root
	echo "data" > /mnt/lustre/dir3/X
	echo "data" > /mnt/lustre/one_dir/one_dir/dir3/X

	mkdir -p /mnt/lustre/one_dir/dir4/subdir1
	mkdir -p /mnt/lustre/other_dir/dir4/subdir1
	mkdir -p /mnt/lustre/dir4
	mkdir -p /mnt/lustre/one_dir/one_dir/dir4
	# 2 matching files for fileclass tree_any_root
	echo "data" > /mnt/lustre/one_dir/dir4/subdir1/X
	echo "data" > /mnt/lustre/other_dir/dir4/subdir1/X
	# unmatching files for fileclass tree_any_root
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
	nb_pol3=`grep hints rh_migr.log | grep path_any_root | wc -l`
	nb_pol4=`grep hints rh_migr.log | grep tree_any_root | wc -l`
	nb_pol5=`grep hints rh_migr.log | grep relative_path | wc -l`
	nb_pol6=`grep hints rh_migr.log | grep relative_tree | wc -l`
	nb_unmatch=`grep hints rh_migr.log | grep unmatch | wc -l`

	(( $nb_pol1 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'absolute_path': $nb_pol1"
	(( $nb_pol2 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'absolute_tree': $nb_pol2"
	(( $nb_pol3 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'path_any_root': $nb_pol3"
	(( $nb_pol4 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'tree_any_root': $nb_pol4"
	(( $nb_pol5 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'relative_path': $nb_pol5"
	(( $nb_pol6 == 2 )) || echo "********** TEST FAILED: wrong count of matching files for policy 'relative_tree': $nb_pol6"
	(( $nb_unmatch == 10 )) || echo "********** TEST FAILED: wrong count of unmatching files: $nb_unmatch"

	(( $nb_pol1 == 2 )) && (( $nb_pol2 == 2 )) && (( $nb_pol3 == 2 )) && (( $nb_pol4 == 2 )) \
        	&& (( $nb_pol5 == 2 )) && (( $nb_pol6 == 2 )) && (( $nb_unmatch == 10 )) \
		&& echo "OK: test successful"
}



if [[ "$1" == "-q" ]]; then
	quiet=1
else
	quiet=0
fi

index=1

function run_test
{
	if [[ -n $5 ]]; then args=$5; else args=$4 ; fi
	echo "TEST #$index: $1 ($args)"
	((index=$index+1))

	if (( $quiet == 1 )); then
		$* 2>&1 | tee "rh_test.log" | egrep -i -e "OK|ERR|Fail"
	else
		$*
	fi
}

function cleanup
{
	echo "cleanup..."
        if (( $quiet == 1 )); then
                clean_fs | tee "rh_test.log" | egrep -i -e "OK|ERR|Fail"
        else
                clean_fs
        fi
}

cleanup
run_test 	path_test test_path.conf 2 "path matching policies"
cleanup
run_test 	migration_test test1.conf 11 31 "last_mod>30s"
cleanup
run_test 	migration_test test2.conf 5  31 "last_mod>30s and name == \"*[0-5]\""
cleanup
run_test 	migration_test test3.conf 5  16 "complex policy with filesets"
cleanup
run_test 	migration_test test3.conf 10 31 "complex policy with filesets"
cleanup
run_test 	xattr_test test_xattr.conf 2 "xattr-based fileclass definition"
cleanup
run_test 	purge_test test_purge.conf 11 31 "last_access > 30s"
cleanup
run_test	purge_size_filesets test_purge2.conf 2 3 "purge policies using size-based filesets"
cleanup
run_test 	test_rh_report common.conf 3 1 "reporting tool"
cleanup
run_test 	link_unlink_remove_test test_rm1.conf 1 31 "deferred hsm_remove (30s)"
cleanup
