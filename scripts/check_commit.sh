#!/bin/bash

dir=$(dirname $(readlink -m $0))

opt=""

function check_file
{
	exec git diff $opt --format=email "$1" |
		$dir/checkpatch.pl --patch --no-tree -q -
}

if [ "$1" ]; then
    cmd="cat $1"
    opt="--cached"
else
    cmd="git status"
fi

rc=0
for f in $($cmd | grep -E 'added:|modified:|new file:' | cut -d ':' -f 2 |
           tr -d " " | egrep -E "\.[ch]$"); do
    check_file "$f" || rc=1
done

exit $rc
