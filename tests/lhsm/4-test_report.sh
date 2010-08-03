#!/bin/sh

REPORT=../../src/robinhood/rbh-report

$REPORT --activity || exit 1
$REPORT --fsinfo || exit 1
$REPORT --userinfo=root || exit 1
$REPORT --groupinfo=root || exit 1
$REPORT --topdirs=2 || exit 1
$REPORT --topsize=2 || exit 1
$REPORT --toppurge=2 || exit 1
$REPORT --toprmdir=2 || exit 1
$REPORT --topusers=2 || exit 1
$REPORT --dump-all || exit 1
$REPORT --dump-user=root || exit 1
$REPORT --dump-group=root || exit 1
$REPORT --dump-ost=0 || exit 1

