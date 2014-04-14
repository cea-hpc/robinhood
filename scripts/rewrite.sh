#!/bin/bash

d=$(dirname $0)

function process_lines
{
    local l1
    local l2
    local l3
    local l4
    local ln
    local fi
    local num
    local repl

    while (( 1 )); do
        read l1 || break
        read l2 || break
        read l3 || break
        read l4
#        echo "l1: $l1"
#        echo "l2: $l2"
#        echo "l3: $l3"
#        echo "l4: $l4"

        #10703: FILE: src/common/lustre_tools.c:413:
        fi=$(echo "$l2" | awk '{print $3}' | cut -d ':' -f 1)
        num=$(echo "$l2" | awk '{print $3}' | cut -d ':' -f 2)
        ln=$(echo "$l3" | sed -e "s/^+//")
#        lbefore=$(sed "${num}q;d" $fi)
        sed -i -e "${num}s/ \* / */g" $fi
#        lafter=$(sed "${num}q;d" $fi)
        echo "$fi:$num" >&2
#        echo "AVANT: $lbefore" >&2
#        echo "APRES: $lafter" >&2
    done
}


#echo "summary of previous changes:" >&2
#git show $GIT_COMMIT^ src >&2

#echo "summary of current changes:" >&2
#git show $GIT_COMMIT src >&2

for f in $(git diff-tree --name-only --diff-filter=AMR --root -r --no-commit-id $GIT_COMMIT | grep '\.[chyl]$'); do
    git show $GIT_COMMIT $f > /tmp/patch || continue
    $d/checkpatch.pl --ignore CODE_INDENT,SPACE_BEFORE_TAB,LEADING_SPACE,PRINTF_L /tmp/patch | grep -A 2 "foo \* bar" | process_lines
    
    sed -i -e "s/( /(/g" -e "s/ )/)/g" $f
done
exit 0
