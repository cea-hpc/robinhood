#!/bin/sh

SHOOK_DIR=/cea/local/home/leibovi/shook.git
export CFLAGS="-g -I$SHOOK_DIR/src/server -I$SHOOK_DIR/src/common"
export LDFLAGS="-L$SHOOK_DIR/src/server/.libs"
./configure --with-purpose=HSM_LITE
