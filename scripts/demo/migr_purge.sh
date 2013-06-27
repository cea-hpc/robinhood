#/!bin/sh

# -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=4:tabstop=4:

# 1) RH DB init: scan lustre FS
# 2) start logging disk usage
# 3) start posix copytool
# 4) start policy engine for monitoring disk usage and migrating files
# 5) start script for writing data
# 6) start policy engine for processing changelogs

ROOT=/mnt/lustre
RH=../../src/robinhood/robinhood

$RH -f rh.migr_purge.conf --scan --once -L events.log -l DEBUG

./disk_usage.sh > usage.csv &

hsm_posix_copytool --path=/mnt/backend  --verbose > ct.log 2> ct.log &

$RH -f rh.migr_purge.conf --migrate --purge -L rh.log -l DEBUG &

./write_data.sh /mnt/lustre &

$RH -f rh.migr_purge.conf --readlog -L events.log -l DEBUG &
