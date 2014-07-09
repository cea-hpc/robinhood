#!/bin/bash
#
# helper to convert robinhood 2.5 to robinhood 3.0  config file.
#

function error
{
    echo "$*"
    exit 1
}


if [ -z $1 ]; then
    echo "usage: $0 <cfg_file>"
    exit 1
fi
cfg=$1

tmp=$cfg.new
cp "$cfg" "$tmp" || error "failed to create temporary copy of $cfg"

old_blocks=("db_update_policy")
new_blocks=("db_update_params")

i=0
while [ -n "${old_blocks[$i]}" ]; do
    sed -i -e "s/\s*${old_blocks[$i]}\s*/${new_blocks[$i]}/i" $tmp
    ((i++))
done

echo "new config file created: $tmp"
