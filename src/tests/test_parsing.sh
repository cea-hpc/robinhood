#!/bin/bash

dir=$(dirname $0)
tstdir=$dir/tst.data

function error
{
    echo "ERROR: $*" >&2
    exit 1
}

function test_out
{
    grep "$1" $2 || error "pattern not found: $1"
}

$dir/test_parse $tstdir/bad.conf > /dev/null 2> /dev/null && error "Parsing should fail"
$dir/test_parse $tstdir/ok.conf > /dev/null || error "Parsing failed"

# check various parsing features (unicity, env variables, includes...)
out=/tmp/tst.$$
TEST_VAL=XYZ ./test_parse tst.data/test.conf > $out

test_out "block1.fs_path is defined and is unique" $out
test_out "block2.fs_path is defined and is not unique" $out 
test_out "block3.fs_path is not defined" $out
test_out "block4.env = XYZ" $out
test_out "block_inc1.include_var = 42" $out
test_out "block_inc2.var_env = XYZ" $out
test_out "block_inc3.subsubfile = yes" $out

rm -f $out
true
