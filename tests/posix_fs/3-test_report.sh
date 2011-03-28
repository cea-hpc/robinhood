#!/bin/sh

REPORT=../../src/robinhood/rbh-hsmlite-report

function test
{
	echo "Executing: $*"
	$* || exit 1
}

test $REPORT --activity $@
test $REPORT --fs-info $@
test $REPORT --class-info $@
test $REPORT --user-info=root $@
test $REPORT --group-info=root $@
test $REPORT --top-size=2 $@
test $REPORT --top-purge=2 $@
test $REPORT --top-users=2 $@
test $REPORT --dump-all --filter-path="/mnt/lustre/dir_A" $@
test $REPORT --dump-all --filter-class="default" $@
test $REPORT --dump-user=root $@
test $REPORT --dump-group=root $@
test $REPORT --dump-ost=0 $@
test $REPORT --dump-status=new $@
test $REPORT --dump-status=arch $@
test $REPORT --deferred-rm $@
