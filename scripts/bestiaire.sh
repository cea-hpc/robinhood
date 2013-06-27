#!/bin/sh
#
# create a filesystem with various object types etc...
#
# usage: bestiaire.sh <root>
#

TESTDIR=$1

if [ ! -d "$TESTDIR" ]; then
    echo "usage: $0 <root>"
    exit 1
fi

DIRNAME="directory"
FILENAME="file"
DEPTH=256
FILEPERDIR=10

#create files in root
for i in $(seq 1 $FILEPERDIR); do
    dd if=/dev/zero of="$TESTDIR/$FILENAME.$i" bs=1k count=$i || exit 1
done

# create directories (until DEPTH)
# create files at each level
d=0
curr="$TESTDIR"
while (( $d < $DEPTH )); do
    curr="$curr/$DIRNAME.$d"
    mkdir -p $curr || exit 1
    for i in $(seq 1 $FILEPERDIR); do
        dd if=/dev/zero of="$curr/$FILENAME.$d.$i" bs=1k count=$i || exit 1
    done
    ((d=$d+1))
done

# create various types
ln -s blablabla $TESTDIR/symlink || exit 1
ln -s $TESTDIR/$DIRNAME.0/file.0.1 $TESTDIR/$DIRNAME.0/symlink.0 || exit 1

ln $TESTDIR/$FILENAME.$i $TESTDIR/link.$i || exit 1
