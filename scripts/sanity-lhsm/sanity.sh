#/!bin/bash

# -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=4:tabstop=4:

function error
{
    echo "ERROR: $*"
    rh_cleanup
    exit 1
}

function check_cmd
{
    cmd=$1
    which $cmd >/dev/null 2>/dev/null || error "$cmd command is missing"
}

function rh_prereq_check
{
   check_cmd "mysqladmin"
   check_cmd "mysql_config"
   (/sbin/service mysqld status | grep running >/dev/null 2>/dev/null) \
        || (pgrep mysqld >/dev/null) || error "mysql daemon not running"
   check_cmd "mysql"
}

RH_DB="robinhood_sanity"
RH_CFG="rh-hsm.conf"
RH=../../src/robinhood/rh-hsm

function rh_setup
{
   LOGIN="robinhood"
   PASS="robinhood"
   rh_prereq_check

   # database setup
   mysqladmin create $RH_DB || error "Error creating robinhood DB"
   mysql $RH_DB > /dev/null << EOF
GRANT USAGE ON $RH_DB.* TO '$LOGIN'@'localhost' IDENTIFIED BY '$PASS' ;
GRANT ALL PRIVILEGES ON $RH_DB.* TO '$LOGIN'@'localhost' IDENTIFIED BY '$PASS' ;
FLUSH PRIVILEGES;
EOF
    [ $? -eq 0 ] || error "Error setting access rights for $LOGIN on db $RH_DB"

    mysql --user=$LOGIN --password=$PASS $RH_DB << EOF
quit
EOF
    [ $? -eq 0 ] || error "Error testing connection to database $RH_DB"
    echo "robinhood db setup successful"

    # changelog setup
    lctl --device lustre-MDT0000 changelog_register \
        || error "Cannot register changelog user"
    RH_CLID=$(tail -n 1 /proc/fs/lustre/mdd/lustre-MDT0000/changelog_users | awk '{print $1}')
    echo "changelog user id for robinhood is $RH_CLID"

    lctl set_param mdd.*.changelog_mask "CREAT UNLNK TRUNC TIME HSM SATTR" \
        || error "Error setting changelog mask"

    # initial cleanup
    lfs changelog_clear lustre $RH_CLID 0

    # generating config file
    sed -e "s/@RH_DB@/$RH_DB/" $RH_CFG.in | sed -e "s/@RH_CLID@/$RH_CLID/" > $RH_CFG \
        || error "Error creating robinhood config file"

    # initial scan (security)
    $RH -f $RH_CFG --scan --once || error "Error performing initial scan"
}

function rh_cleanup
{
    # drop database
    mysqladmin drop --force $RH_DB
    # deregister changelog client
    lfs changelog_clear lustre $RH_CLID 0
    lctl --device lustre-MDT0000 changelog_deregister $RH_CLID
}

function test1
{
    # create new files in lustre
    for i in $(seq 1 10); do
       dd if=/dev/zero of=/mnt/lustre/file.$i bs=1M count=10
    done
    sleep 1
    # read changelogs
    $RH -f $RH_CFG --readlog --once || error "Error reading events"
    # check their status in database
    nb_new=$($RH-report -f $RH_CFG --dump-status=new -P "/mnt/lustre" | tee /dev/tty \
         | grep file| wc -l)
    [ $nb_new -eq 10 ] || error "10 new files expected, $nb_new found"
    # now archive them
    $RH -f $RH_CFG --sync || error "Error archiving files"
    nb_migr=$($RH-report -f $RH_CFG --dump-status=archiving -P "/mnt/lustre" |\
              tee /dev/tty | grep file| wc -l)
    [ $nb_migr -eq 10 ] || error "10 files copy running expected, $nb_migr found"
    # wait for archiving operations to complete (timeout=30s)
    for timeo in $(seq 1 30); do
    	# check for HSM records (without clearing the log)
        hsm_cnt=$(lfs changelog lustre | grep HSM | wc -l)
        [ $hsm_cnt -ge 10 ] && break
        sleep 1
    done
    hsm_cnt=$(lfs changelog lustre | grep HSM | wc -l)
    [ $hsm_cnt -ge 10 ] || error "timeout reached, not enough HSM events"

    # 'HSM' events should have been raised
    $RH -f $RH_CFG --readlog --once || error "Error reading events"
    nb_done=$($RH-report -f $RH_CFG --dump-status=sync -P "/mnt/lustre" \
              | tee /dev/tty | grep file| wc -l)
    [ $nb_done -eq 10 ] || error "10 archived files expected, $nb_done found"

    # now purge archived entries
    $RH -f $RH_CFG --purge-fs=0 --ignore-policies || error "Error purging entries"

    nb_released=$(find /mnt/lustre/ -type f -exec lfs hsm_state {} \; | grep released | wc -l)
    [ $nb_released -eq 10 ] || error "10 released files expected, $nb_released found"

    sleep 1

    # now test hsm-remove
    rm -f /mnt/lustre/file.*
    sleep 1

    $RH -f $RH_CFG --readlog --once || error "Error reading events"
    $RH-report -f $RH_CFG --deferred-rm --csv
    sleep 2
    # launch entry removal in HSM
    $RH -f $RH_CFG --hsm-remove --once || error "Error removing entries"

}

rm -rf /mnt/lustre/*

rh_setup
echo "1- read chglog, archive, purge, remove"
test1
rh_cleanup

rm -rf /mnt/lustre/*
