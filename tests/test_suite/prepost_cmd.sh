#!/bin/bash
mode=$1
file=$2
contents=$3

if [[ "$mode" == "append" ]]; then
	echo "$contents" >> $file
else
	echo "$contents" > $file
fi
