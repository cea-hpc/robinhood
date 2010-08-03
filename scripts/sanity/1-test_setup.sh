#!/bin/sh
service mysqld start
../create_db.sh << EOF
lustre
localhost
robinhood
robinhood
y

EOF

echo "Checking if cl1 is already registered..."
is_cl1=`grep cl1 /proc/{fs,sys}/{lnet,lustre}/mdd/*/changelog_users 2>/dev/null | wc -l`

if (( $is_cl1 != 1 )); then
	echo "No. Registering..."
	../enable_chglogs.sh
else
	echo "Yes"
fi

echo "Checking if copytool is already running..."
if (( `pgrep -f lhsmd_posix | wc -l` > 0 )); then
	echo "Already running"
else
	lhsmd_posix --hsm_root=/tmp --noshadow lustre &
fi
