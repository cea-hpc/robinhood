#/!bin/sh

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
FILE_SZ_MB=2 # file size

TOTAL_FILES=0
TIME_START=`date +%s.%N`

function mksubtree
{
    local DIR=$1
    local LVL=$2
    local d
    local f

    if (( $LVL >= MAX_DEPTH )); then
        for f in `seq 1 $LEAVES`; do
           #echo "Creating file $DIR/file.$f..."
           touch $DIR/file.$f
	   if (( $? != 0 )); then
		echo "ERROR $!"
           fi
	   ((TOTAL_FILES=$TOTAL_FILES+1))
	    if (( $(($TOTAL_FILES % 1000)) == 0 )); then
		now=`date +%s.%N`
		sec=`echo $now - $TIME_START | bc -l`
		echo "$TOTAL_FILES files created in $sec s"
	    fi
        done
    else
        for d in `seq 1 $SUBDIRS`; do
            mkdir -p $DIR/dir.$d
            mksubtree $DIR/dir.$d $(( $LVL + 1 ))
        done
    fi
}

while (( 1 )); do

     mksubtree $ROOT 1

done
