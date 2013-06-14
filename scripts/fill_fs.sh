#!/bin/sh

# -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=4:tabstop=4:

# this script fills a filesystem continuously
# while migration and purges are triggered
# by Policy Engine.

ROOT=$1

if [[ -z $ROOT ]]; then
    echo "Usage: $0 <path>";
    exit 1;
fi

MAX_DEPTH=4
SUBDIRS=30 # subdirs at each level
LEAVES=30 # nbr of files at lower level
FILE_KB_MAX=4 # file size max +1

# give a set of users here
USERS="root foo charlie"
NB_USERS=`echo $USERS | wc -w`

TOTAL_FILES=0
TIME_START=`date +%s.%N`

function random256
{
	od -N 1 -i /dev/urandom | head -1 | awk '{print $2}'
}

function check_usage
{
	ifree=`df -i $ROOT/. | xargs | awk '{print $(NF-2)}'`
	kfree=`df -k $ROOT/. | xargs | awk '{print $(NF-2)}'`

	(( $ifree < 10000 )) && echo "Free inodes is low ($ifree): stopping" && return 1
	(( $kfree < 100000 )) && echo "Free space is low ($kfree KB < 100 MB): stopping" && return 1
	return 0
}

last=0

function mksubtree
{
    local DIR=$1
    local LVL=$2
    local d
    local f

    if (( $LVL >= MAX_DEPTH )); then
        for f in `seq 1 $LEAVES`; do
	   # pseuso-random file size
	   sz=$((`random256` % $FILE_KB_MAX))
       if (( $sz == 0 )); then
            touch $DIR/file.$f || ( echo "touch ERROR" && exit 1 )
	   else
            dd if=/dev/zero of=$DIR/file.$f bs=1k count=$sz 2>/dev/null || ( echo "dd ERROR" && exit 1 )
	   fi
	   if (( $? != 0 )); then
		echo "ERROR $!"
           fi

	   ((TOTAL_FILES=$TOTAL_FILES+1))

        uindex=$(( `random256` % $NB_USERS ))
        uindex=$(( $uindex + 1 ))
        owner=`echo $USERS | cut -d " " -f $uindex`
        chown $owner:gpocre $DIR/file.$f

	    if (( $(($TOTAL_FILES % 1000)) == 0 )); then
		[[ -n "$now" ]] && last=$now
		now=`date +%s.%N`
		sec=`echo $now - $TIME_START | bc -l | xargs printf "%.2f"`
		if [[ $last != 0 ]]; then
			speed=`echo "1000/($now-$last)" | bc -l | xargs printf "%.2f"`
		else
			speed=`echo "1000/$sec" | bc -l | xargs printf "%.2f"`
		fi

		echo "$TOTAL_FILES files created in $sec s ($speed files/sec)"
	    fi
        done
    else
        for d in `seq 1 $SUBDIRS`; do
	    check_usage || exit 1
            mkdir -p $DIR/dir.$d
            mksubtree $DIR/dir.$d $(( $LVL + 1 ))
        done
    fi
}

while (( 1 )); do

     mksubtree $ROOT 1

done
