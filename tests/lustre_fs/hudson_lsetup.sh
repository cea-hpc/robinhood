#!/bin/sh

HUDSON_DIR=$WORKSPACE

if [[ -d $HUDSON_DIR/Lustre ]]; then
	cd $HUDSON_DIR/Lustre/lustre/tests
	./llmount.sh
else
	echo "$HUDSON_DIR/Lustre: no such directory"
	exit 1
fi
