#/!bin/sh

# -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=4:tabstop=4:

# this script fills a filesystem continuously
# while migration and purges are triggered
# by Policy Engine.

ROOT=$1

if [[ -z $ROOT ]]; then
    echo "Usage: $0 <path>";
    exit 1;
fi

MAX_DEPTH=4
SUBDIRS=30 # subdirs at each level
LEAVES=30 # nbr of files at lower level
FILE_SZ_MB=10 # file size

function mksubtree
{
    local DIR=$1
    local LVL=$2
    local d
    local f

    if (( $LVL >= $MAX_DEPTH )); then
        for f in `seq 1 $LEAVES`; do
           echo "Writing file $DIR/file.$f..."
           dd if=/dev/zero of=$DIR/file.$f bs=1M count=$FILE_SZ_MB 2> /dev/null
	       if (( $? != 0 )); then
		        echo "ERROR $!"
           fi
           # migrate file to backend
           echo "Archiving file $DIR/file.$f..."
           lfs hsm_archive $DIR/file.$f || echo "ERROR executing lfs hsm_archive $DIR/file.$f"
           sleep 2
        done

        df -h $DIR
        # release files if they are archived
        sleep 5
        echo "Releasing files in $DIR..."
        for f in $DIR/* ; do
            echo "release: $f"
            lfs hsm_release $f
        done
        sleep 2
        df -h $DIR
    else
        for d in `seq 1 $SUBDIRS`; do
            mkdir -p $DIR/dir.$d
            mksubtree $DIR/dir.$d $(( $LVL + 1 ))
        done
    fi
}

hsm_posix_copytool --path=/mnt/backend  --verbose > ct.log 2> ct.log &
mksubtree $ROOT 1
