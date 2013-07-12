#!/bin/bash

action=$1
src=$2
dst=$3
hints=$4

touch $3

[ "$hints" = "fail" ] && exit 1
exit 0
