#!/bin/sh

# This benchmark performs several kind of metadata changes:
# pass 1) rename
# pass 2) link/unlink
# pass 3) time change
# pass 4) other attr change (owner/group)

function stats_init
{
	/usr/sbin/lctl get_param mdc.*.stats > /tmp/rpc.init
}

function stats_end
{
	/usr/sbin/lctl get_param mdc.*.stats > /tmp/rpc.end

	echo "Request summary:"

	for param in `grep -v '=' /tmp/rpc.end | awk '{print $1}'`; do

		v_init=`egrep "$param " /tmp/rpc.init | awk '{print $2}'`
		v_end=`egrep "$param " /tmp/rpc.end | awk '{print $2}'`

		if [[ $v_init = *.* ]]; then
		 	echo "    $param:" `echo $v_end-$v_init | bc -l`
		else
			echo "    $param:" $(($v_end-$v_init))
		fi

	done

}

function err
{
	msg=$*
	echo "ERROR executing $msg: $?"
	exit 1
}

function run
{
	$* || err "$*"
}

# rename operations
function test_1
{
	file=$1
	dir=`dirname $file`
	name1=`basename $file`
	name2=$name1.rnm
	echo "#1: RENAME"
	for i in `seq 1 1000`; do
		run mv $dir/$name1 $dir/$name2
		run mv $dir/$name2 $dir/$name1
	done
}

# link/unlink operations
function test_2
{
	file=$1
	dir=`dirname $file`
	link1=`basename $file`
	link2=$name1.lnk
	echo "#2: LINK/UNLINK"
	for i in `seq 1 1000`; do
		run ln $dir/$link1 $dir/$link2
		run unlink $dir/$link1
		run ln $dir/$link2 $dir/$link1
		run unlink $dir/$link2
	done
}

# read operations (atime change)
function test_3a
{
	file=$1
	echo "#3a: atime change (read)"
	for i in `seq 1 100`; do
		# read operation (can cause atime change)
		run od -x $file > /dev/null
	done
}

# time change operations
function test_3b
{
	file=$1
	echo "#3b.1: mtime change (append)"
	for i in `seq 1 1000`; do
		# append operation (can cause mtime change)
                run od -x -v -N 100 /dev/zero >> $file
	done
	echo "#3b.2: all times change (touch)"
	for i in `seq 1 1000`; do
		# touch opration (change all times)
                run touch $file
	done
}


# attr change operations
function test_4
{
	file=$1
	echo "#4.1: mode change (chmod)"
	for i in `seq 1 1000`; do
		run chmod 600 $file
		run chmod 644 $file
	done
	echo "#4.2: owner change (chown)"
	for i in `seq 1 1000`; do
                run chown bin $file
                run chown root $file
	done
}


arg=$1
if [ -z $arg ]; then
	echo "Usage: $0 <file>"
	exit 1
fi

stats_init
test_1 $arg
test_2 $arg
test_3a $arg
test_3b $arg
#test_4 $arg
stats_end
