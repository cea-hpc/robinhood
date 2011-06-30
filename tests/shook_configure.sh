#!/bin/sh

SHOOK_DIR=~/shook.git
export CFLAGS="-g -I$SHOOK_DIR/src/server -I$SHOOK_DIR/src/common"
export LDFLAGS="-L$SHOOK_DIR/src/server/.libs"
./configure --with-purpose=HSM_LITE
