#!/bin/bash

# creates 1.7+ billion entries filesystem
# with a namespace similar to production systems
NB_CONT=10          # 10    containers
GROUP_PER_CONT=20   # 200   groups
USER_PER_GROUP=50   # 10000 users
STUDY_PER_USER=1000 # 10M   studies
SUBDIR=10           # 100M  directories in studies
FCOUNT=15           # 1.5G files
LCOUNT=1            # 100M symlinks

BIN=$(basename `readlink -f $0`)
ROOT=$1
if [[ ! -d "$ROOT" ]]; then
    echo "usage: $0 <dir>"
    exit 1
fi


function mk_cont
{
    dir=$1
    fcount=0
    lcount=0
    TIME_START=`date +%s.%N`
    ((total=$GROUP_PER_CONT*$USER_PER_GROUP*$STUDY_PER_USER*$SUBDIR*($FCOUNT+$LCOUNT)))
    last=0

    for g in $(seq -w 2 1 $GROUP_PER_CONT); do
    for u in $(seq -w 2 1 $USER_PER_GROUP); do
    for s in $(seq -w 4 1 $STUDY_PER_USER); do
    for d in $(seq -w 2 1 $SUBDIR); do

        curdir=$dir/group$g/user$u/study$s/data$d

        for f in $(seq 1 $FCOUNT); do
            ((fcount=$fcount+1))
        done
        for l in $(seq 1 $LCOUNT); do
            ((lcount=$lcount+1))
        done
        if (( $fcount+$lcount-$last >= 10000 )); then
            now=`date +%s.%N`
            sec=`echo $now - $TIME_START | bc -l | xargs printf "%.2f"`
            speed=`echo "($fcount+$lcount)/($sec)" | bc -l | xargs printf "%.2f"`
            ((reste=$total-$fcount-$lcount))
            approx_remain=`echo "($reste/$speed)" | bc -l | xargs printf "%.0f"`
            if (( $approx_remain > 86400 )); then
                t_remain=`echo "($reste/$speed)/86400" | bc -l | xargs printf "%.2f days"`
            elif (( $approx_remain > 3600 )); then
                t_remain=`echo "($reste/$speed)/3600" | bc -l | xargs printf "%.2f hours"`
            else
                t_remain=`echo "($reste/$speed)" | bc -l | xargs printf "%.2f sec"`
            fi
            ((last=$fcount+$lcount))
            echo "$dir: created $fcount files, $lcount symlinks in $sec sec ($speed entries/sec), remaining ~$t_remain"
        fi

    done
    done
    done
    done
}

trap "pkill $BIN" SIGINT

for c in $(seq -w 2 1 $NB_CONT); do
    mk_cont $ROOT/cont$c &
done
wait
