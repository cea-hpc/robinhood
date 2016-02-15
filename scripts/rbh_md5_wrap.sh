#!/bin/bash
output="$1"
path="$2"

if [ -z "$path" ]; then
	echo "usage: $(readlink -m $0) <previous_value> <path>" >&2
	exit 1
fi

dv=$(lfs data_version "$path")
md5=$(md5sum "$path" | awk '{print $1}')
# failed to get data version and md5
[[ -z "$dv" || -z "$md5" ]] && exit 1

if [ -n "$output" ]; then
	dv_old=$(echo "$output" | cut -d ':' -f 1)
	# if dv changed, recompute the checksum
	if [ "$dv" != "$dv_old" ]; then
		echo "$path: file data changed, recomputing checksum" >&2
		output=""
	fi
else
	echo "$path: no previous checksum" >&2
fi

if [ -z "$output" ]; then
	# no previous output (or data changed)
	# initial sum
	echo "$dv:$md5"
	exit 0
fi

# compare old and new checksums
md5_old=$(echo "$output" | cut -d ':' -f 2)
if [ "$md5" != "$md5_old" ]; then
	echo "$path: md5sum changed!" >&2
	exit 1
fi

echo "$path: md5sum OK: '$md5'" >&2
echo "$dv:$md5"
exit 0
