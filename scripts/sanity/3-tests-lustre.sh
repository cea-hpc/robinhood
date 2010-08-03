FILE=/mnt/lustre/file_test$$

function test1
{
	echo "Writing file"
	dd if=/dev/zero of=$FILE bs=1M count=10 >/dev/null 2>/dev/null || echo "Error writing $FILE"
	echo "Archiving file"
	lfs hsm_archive $FILE || echo "Error archiving file $FILE"
	echo "Removing file"
	rm -f $FILE || echo "Error removing file $FILE"
}

DIRCOUNT=2

function test2
{
	for d in `seq 1 $DIRCOUNT`; do
		dir=/mnt/lustre/dir.$d
		mkdir -p $dir
		for f in `seq 1 15`; do
			dd if=/dev/zero of=$dir/file.$f bs=1M count=10
		done
		lfs hsm_archive $dir/* || echo "ERROR"
	done
	kill %1
}

function test2bis
{
	for d in `seq 1 $DIRCOUNT`; do
		dir=/mnt/lustre/dir.$d
		lfs hsm_release $dir/* || echo "ERROR"
	done
}

function test3
{
	FILE=/mnt/lustre/file.$$
	# try to release a file while it is opened
	dd if=/dev/zero of=$FILE bs=1M count=10
	lfs hsm_archive $FILE
	# wait for end of archive
	while (( `lfs hsm_state $FILE | grep archived | wc -l` != 1 )); do
		sleep 1;
	done
	# open the file
	perl -e "open F, \" < $FILE\"; sleep(5); my \$toto = <F>; if ( !\$toto) { print \"EOF\\n\" };" &
	echo "Process using file:"
	lsof $FILE
	lfs hsm_release $FILE && echo "ERROR: Should not allow releasing opened file"
	if (( `lfs hsm_state $FILE | grep released | wc -l` != 0 )); then
		echo "File is released: Test failed"
	fi
}


test1
test2
test2bis
test3
