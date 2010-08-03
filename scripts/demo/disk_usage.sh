#/!bin/sh

# -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=4:tabstop=4:

FS1=$1
FS2=$2

PERIOD=10

function df_line
{
    time=$1

    # get lustre OST usage
    DF1=`lfs df /mnt/lustre/ | grep OST | awk '{print $3}'`
    # get posix backend usage
    DF2=`df /mnt/backend | grep "/" | awk '{print $3}'`

    line=`echo $time $DF1 $DF2 | sed -e "s/ /;/g"`
    echo $line
}

TIME=0
echo "time;ost1;ost2;archive"
while ((1)); do
    df_line $TIME
    sleep $PERIOD
    TIME=$(( $TIME + $PERIOD ))
done
