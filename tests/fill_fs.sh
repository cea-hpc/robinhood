
ROOT=$1
COUNT=$2

if [[ -z "$ROOT" || -z "$COUNT" ]]; then
	echo "Usage: $0 <dir> <ino_count>"
	exit 1
fi

if [ ! -d $ROOT ]; then
	echo "Missing directory: $ROOT"
	exit 1
fi

ifree=`df -i $ROOT | tail -1 | awk '{ print $(NF-2) }'`
icnt=$(( $COUNT * 105 / 100 )) # add 5% for dirs
if (($ifree <= $icnt)); then
	ilimit=$(( $ifree * 100 / 105 ))
	echo "Not enough free inodes: setting limit to $ilimit"
	COUNT=$ilimit
fi

# if count < 100, no dir level
# if count < 10k, 1 single dir level
# if count > 10k, 2 dir levels

if (( $COUNT <= 100 )); then
	for f in `seq 1 $COUNT`; do touch $ROOT/file.$f; done;
elif (( $COUNT <= 10000 )); then
	fpd=$(( $COUNT/100 ))
	for d in `seq 1 100`; do
		echo  "$ROOT/dir.$d"
		mkdir -p "$ROOT/dir.$d" || exit 1
		for f in `seq 1 $fpd`; do
			touch "$ROOT/dir.$d/file.$f" || exit 1
		done
	done
else
	fpd=$(( $COUNT/10000 ))
	for d in `seq 1 95`; do for s in `seq 1 95`; do
		echo  "$ROOT/dir.$d/subdir.$s"
		mkdir -p $ROOT/dir.$d/subdir.$s || exit 1
		for f in `seq 1 100`; do
			touch $ROOT/dir.$d/subdir.$s/file.$f || exit 1
		done
	done; done
fi
