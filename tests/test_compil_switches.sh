#!/bin/sh

purp_list=$*

if [[ -z $purp_list ]]; then
	echo "Usage: $0 <purpose list>"
	exit 1
fi

# count NB procs
NB_PROC=`cat /proc/cpuinfo | grep processor | tail -1 | cut -d ':' -f 2 | tr -d ' '`
((NB_PROC=$NB_PROC+1))

echo "Compilation using $NB_PROC processors"

ERRORS=""

for purp in $purp_list; do
for lustre in "--enable-lustre" "--disable-lustre"; do

# default per purpose and DB
config_cmd="./configure --with-purpose=$purp $lustre"

if [[ $purp = "LUSTRE_HSM" && $lustre = "--disable-lustre" ]]; then echo "skipping conflicting switches: $config_cmd"; continue; fi

echo "TEST: $config_cmd"

(CFLAGS="$CFLAGS_OPT" $config_cmd && make -j $NB_PROC ) 2>&1 | grep -v Werror | grep -v "unused variable" | grep -v "not used" | egrep -i 'error|warning' \
		&& ( echo FAILED; ERRORS="$ERRORS Error using compilation switches:$config_cmd\n" )

make clean 2>&1 >/dev/null

done
done

if [[ -n $ERRORS ]]; then
	echo "$ERRORS"
	exit 1
fi

echo "Building rpms"

ERRORS=""

for purp in $purp_list; do
for lustre in "--enable-lustre" "--disable-lustre"; do

# default per purpose and DB
config_cmd="./configure --with-purpose=$purp $lustre"

if [[ $purp = "LUSTRE_HSM" && $lustre = "--disable-lustre" ]]; then echo "skipping conflicting switches: $config_cmd"; continue; fi

echo "TEST: $config_cmd"

(CFLAGS="$CFLAGS_OPT" $config_cmd && make rpm ) 2>&1 | grep -v Werror | grep -v "unused variable" |  grep -v "not used" | egrep -i 'error|warning' \
		&& ( echo FAILED; ERRORS="$ERRORS Error using compilation switches:$config_cmd\n" )

make clean 2>&1 >/dev/null

done
done

if [[ -n $ERRORS ]]; then
	echo "$ERRORS"
	exit 1
fi

echo "Now testing advanced compilation switches"


# advanced switches

for purp in $purp_list; do
for lustre in "--enable-lustre" "--disable-lustre"; do
for fid in "--disable-fid-support" "--enable-fid-support"; do
for chglog in "--disable-changelogs" "--enable-changelogs"; do
for db in MYSQL SQLITE; do
for mdsstat in "--disable-mds-stat" "--enable-mds-stat"; do

config_cmd="./configure --with-db=$db --with-purpose=$purp $lustre $fid $chglog $mdsstat"

# skip conflicting options
if [[ $lustre = "--disable-lustre" && $fid = "--enable-fid-support" ]]; then echo "skipping conflicting switches: $config_cmd"; continue; fi
if [[ $lustre = "--disable-lustre" && $chglog = "--enable-changelogs" ]]; then echo "skipping conflicting switches: $config_cmd"; continue; fi
if [[ $lustre = "--disable-lustre" && $mdsstat = "--enable-mds-stat" ]]; then echo "skipping conflicting switches: $config_cmd"; continue; fi
if [[ $fid = "--disable-fid-support" && $chglog = "--enable-changelogs" ]]; then echo "skipping conflicting switches: $config_cmd"; continue; fi
if [[ $purp = "LUSTRE_HSM" && $lustre = "--disable-lustre" ]]; then echo "skipping conflicting switches: $config_cmd"; continue; fi
if [[ $purp = "LUSTRE_HSM" && $fid = "--disable-fid-support" ]]; then echo "skipping conflicting switches: $config_cmd"; continue; fi
if [[ $purp = "LUSTRE_HSM" && $chglog = "--disable-changelogs" ]]; then echo "skipping conflicting switches: $config_cmd"; continue; fi

echo "TEST: $config_cmd"

(CFLAGS="$CFLAGS_OPT" $config_cmd && make -j $NB_PROC ) 2>&1 | grep -v Werror | grep -v "unused variable" | grep -v "not used" | egrep -i 'error|warning' \
		&& ( echo FAILED; ERRORS="$ERRORS Error using compilation switches:$config_cmd\n" )

make clean 2>&1 >/dev/null

done
done
done
done
done
done

echo -e $ERRORS

