#!/bin/bash

opt=""

function check_file
{
    # check perenthesing
    local said=0
    git diff $opt $1 | grep "^+" | grep -v "^+++" | sed -e 's/^+//' > /tmp/code.$$
    sed -e "s/( /(/g" -e "s/ )/)/g" /tmp/code.$$ > /tmp/indent.$$
    diff /tmp/code.$$ /tmp/indent.$$ > /tmp/diff.$$
    if (( $? )); then
        echo "WARNINGS in $f:" && said=1
        cat /tmp/diff.$$ | grep -E '^>|^<' | while read l; do
            echo "$l"| sed -e 's/^</this line: /' -e 's/^>/should be: /'
        done
        echo
    fi
    # check spaces at EOL
    while read l; do
        (($said == 0)) && echo "WARNINGS in $f:" && said=1
        # line
        echo $l
        #highlight
        echo $l | sed -e "s/[^ ]/ /g" -e "s/ \$/ ^space at EOL/g"
    done < <( grep -P "[\t ]\$" /tmp/code.$$ )
    (($said == 1 )) && echo
}

if [ "$1" ]; then
    cmd="cat $1"
    opt="--cached"
else
    cmd="git status"
fi
for f in $($cmd | grep -E 'added:|modified:|new file:' | cut -d ':' -f 2 | tr -d " "); do
    check_file $f
done

exit 0
first=0
while read l; do
    # line
    echo $l
    #highlight
    echo $l | sed -e "s/[^ ]/ /g" -e "s/ \$/ ^space at EOL/g"
done < <( grep -e "[\t ]\$" /tmp/code.$$ )

rm -f /tmp/code.$$ /tmp/indent.$$ /tmp/diff.$$
