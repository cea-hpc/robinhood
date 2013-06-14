#/bin/sh

ROOT="/tmp/mnt.rbh_huge"
BKROOT="/tmp/backend"
RBH_OPT=""
DB=robinhood_test

XML="test_report.xml"
TMPXML_PREFIX="/tmp/report.xml.$$"
TMPERR_FILE="/tmp/err_str.$$"

TEMPLATE_DIR='../../doc/templates'

if [[ -z "$PURPOSE" || $PURPOSE = "TMP_FS_MGR" ]]; then
	is_backup=0
	RH="../../src/robinhood/robinhood $RBH_OPT"
	REPORT="../../src/robinhood/rbh-report $RBH_OPT"
	CMD=robinhood
	PURPOSE="TMP_FS_MGR"
elif [[ $PURPOSE = "BACKUP" ]]; then
	is_backup=1
	RH="../../src/robinhood/rbh-backup $RBH_OPT"
	REPORT="../../src/robinhood/rbh-backup-report $RBH_OPT"
	CMD=rbh-backup
fi

PROC=$CMD
CFG_SCRIPT="../../scripts/rbh-config"
CLEAN="rh_scan.log rh_migr.log rh_rm.log rh.pid rh_purge.log rh_report.log report.out rh_syntax.log"

SUMMARY="/tmp/test_${PROC}_summary.$$"

ERROR=0
RC=0
SKIP=0
SUCCES=0
DO_SKIP=0

function error_reset
{
	ERROR=0
	DO_SKIP=0
	cp /dev/null $TMPERR_FILE
}

function error
{
	echo "ERROR $@"
	((ERROR=$ERROR+1))

	if (($junit)); then
	 	grep -i error *.log >> $TMPERR_FILE
		echo "ERROR $@" >> $TMPERR_FILE
	fi
}

function set_skipped
{
	DO_SKIP=1
}

function clean_logs
{
	for f in $CLEAN; do
		if [ -s $f ]; then
			cp /dev/null $f
		fi
	done
}


function clean_db
{
	echo "Destroying any running instance of robinhood..."
	pkill robinhood
	pkill rbh-backup

	if [ -f rh.pid ]; then
		echo "killing remaining robinhood process..."
		kill `cat rh.pid`
		rm -f rh.pid
	fi

	sleep 1
	echo "Cleaning robinhood's DB..."
	$CFG_SCRIPT empty_db $DB > /dev/null
}

############################## TEST SECTION #########################

function scan_progress
{
	cfg=$1
	# wait for command to start
	sleep 1
	while pgrep $CMD >/dev/null ; do
		#$REPORT -f $cfg -i --csv | grep "Total"
		$REPORT -f $cfg -a --csv > /tmp/report.out
		entries=`grep entries_scanned /tmp/report.out | cut -d ',' -f 2 | tr -d ' '`
		[ -z $entries ] && entries=0
		speed=`grep scan_current_speed /tmp/report.out | cut -d ',' -f 2 | tr -d ' '`
		[ -z $speed ] && speed=0
		echo -ne "\r$entries entries scanned @ $speed entries/sec        "
		sleep 10
	done
	echo
}

function test_scan_report
{
	config_file=$1
	policy_str="$2"

	clean_logs

	echo "1-Scanning..."
	scan_progress ./cfg/$config_file &
	scan_t0=`date +%s.%N`
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	scan_t1=`date +%s.%N`

	# wait for progress function to end
	wait %1

	# get stats from log:
	compl_line=`grep "Full scan of $ROOT completed" rh_scan.log | cut -d '|' -f 2`
	echo $compl_line

	duration=`echo $compl_line | awk '{print $NF}' | sed -e "s/[ s]//g"`
	dur_1=`echo "$scan_t1 - $scan_t0" | bc -l`
	dur_1=`printf "%.2f" $dur_1`
	echo "Duration: scan=$duration, total=$dur_1"

	cp /dev/null rh_scan.log

	# testing second scan time
	echo "2-Second scan..."
	scan_progress ./cfg/$config_file &
	scan_t0=`date +%s.%N`
	$RH -f ./cfg/$config_file --scan -l DEBUG -L rh_scan.log  --once || error "scanning filesystem"
	scan_t1=`date +%s.%N`

	# wait for progress function to end
	wait %1

	# get stats from log:
	compl_line=`grep "Full scan of $ROOT completed" rh_scan.log | cut -d '|' -f 2`
	echo $compl_line

	duration_2=`echo $compl_line | awk '{print $NF}' | sed -e "s/[ s]//g"`
	dur_2=`echo "$scan_t1 - $scan_t0" | bc -l`
	dur_2=`printf "%.2f" $dur_2`
	echo "Duration: scan=$duration_2, total=$dur_2"

	echo "3-Compare with find -ls..."
	scan_t0=`date +%s.%N`
	find $ROOT -ls > /dev/null
	scan_t1=`date +%s.%N`
	dur_3=`echo "$scan_t1 - $scan_t0" | bc -l`
	dur_3=`printf "%.2f" $dur_3`
	echo "Duration: scan=$dur_3"

	echo "`date`; scan1: $duration, $dur_1 ; scan2: $duration_2, $dur_2; find: $dur_3" >> perf_history.log

	# duration of report commands:
	for opt in "--fs-info" "--class-info" "--user-info=root" "--group-info=root" "--top-dirs" "--top-size" "--top-purge" "--top-rmdir" "--top-users"; do
		report_t0=`date +%s.%N`
		$REPORT -f ./cfg/$config_file $opt > /dev/null
		report_t1=`date +%s.%N`
		diff=`echo "$report_t1 - $report_t0" | bc -l`
		diff_ms=`printf "%.3f" $diff`
		diff_s=`printf "%.0f" $diff`
		echo "Report time for $opt: $diff_ms sec"
		echo "`date`; report $opt: $diff_ms" >> perf_history.log
		(( $diff_s > 1 )) && echo "$opt is slow!"
	done

}

######################### END OF TEST FUNCTIONS #####################

only_test=""
quiet=0
junit=0

while getopts qj o
do	case "$o" in
	q)	quiet=1;;
	j)	junit=1;;
	[?])	print >&2 "Usage: $0 [-q] [-j] test_nbr ..."
		exit 1;;
	esac
done
shift $(($OPTIND-1))

if [[ -n "$1" ]]; then
	only_test=$1
fi

# initialize tmp files for XML report
function junit_init
{
	cp /dev/null $TMPXML_PREFIX.stderr
	cp /dev/null $TMPXML_PREFIX.stdout
	cp /dev/null $TMPXML_PREFIX.tc
}

# report a success for a test
function junit_report_success # (class, test_name, time)
{
	class="$1"
	name="$2"
	time="$3"

	# remove quotes in name
	name=`echo "$name" | sed -e 's/"//g'`

	echo "<testcase classname=\"$class\" name=\"$name\" time=\"$time\" />" >> $TMPXML_PREFIX.tc
}

# report a failure for a test
function junit_report_failure # (class, test_name, time, err_type)
{
	class="$1"
	name="$2"
	time="$3"
	err_type="$4"

	# remove quotes in name
	name=`echo "$name" | sed -e 's/"//g'`

	echo "<testcase classname=\"$class\" name=\"$name\" time=\"$time\">" >> $TMPXML_PREFIX.tc
	echo -n "<failure type=\"$err_type\"><![CDATA[" >> $TMPXML_PREFIX.tc
	cat $TMPERR_FILE	>> $TMPXML_PREFIX.tc
	echo "]]></failure>" 	>> $TMPXML_PREFIX.tc
	echo "</testcase>" 	>> $TMPXML_PREFIX.tc
}

function junit_write_xml # (time, nb_failure, tests)
{
	time=$1
	failure=$2
	tests=$3

	cp /dev/null $XML
#	echo "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" > $XML
	echo "<?xml version=\"1.0\" encoding=\"ISO8859-2\" ?>" > $XML
	echo "<testsuite name=\"robinhood.PosixTests\" errors=\"0\" failures=\"$failure\" tests=\"$tests\" time=\"$time\">" >> $XML
	cat $TMPXML_PREFIX.tc 		>> $XML
	echo -n "<system-out><![CDATA[" >> $XML
	cat $TMPXML_PREFIX.stdout 	>> $XML
	echo "]]></system-out>"		>> $XML
	echo -n "<system-err><![CDATA[" >> $XML
	cat $TMPXML_PREFIX.stderr 	>> $XML
	echo "]]></system-err>" 	>> $XML
	echo "</testsuite>"		>> $XML
}


function cleanup
{
	echo "cleanup..."
        if (( $quiet == 1 )); then
                clean_db | tee "rh_test.log" | egrep -i -e "OK|ERR|Fail|skip|pass"
        else
                clean_db
        fi
}

function run_test
{
	index=$1
	func=$2
	desc=$4
	shift

	index_clean=`echo $index | sed -e 's/[a-z]//'`

	if [[ -z $only_test || "$only_test" = "$index" || "$only_test" = "$index_clean" ]]; then
		cleanup
		echo
		echo "==== TEST #$index $func ($desc) ===="

		error_reset

		t0=`date "+%s.%N"`

		if (($junit == 1)); then
			# markup in log
			echo "==== TEST #$index $func ($desc) ====" >> $TMPXML_PREFIX.stdout
			echo "==== TEST #$index $func ($desc) ====" >> $TMPXML_PREFIX.stderr
			"$@" 2>> $TMPXML_PREFIX.stderr >> $TMPXML_PREFIX.stdout
		elif (( $quiet == 1 )); then
			"$@" 2>&1 > rh_test.log
			egrep -i -e "OK|ERR|Fail|skip|pass" rh_test.log
		else
			"$@"
		fi

		t1=`date "+%s.%N"`
		dur=`echo "($t1-$t0)" | bc -l`
		echo "duration: $dur sec"

		if (( $DO_SKIP )); then
			echo "(TEST #$index : skipped)" >> $SUMMARY
			SKIP=$(($SKIP+1))
		elif (( $ERROR > 0 )); then
			echo "TEST #$index : *FAILED*" >> $SUMMARY
			RC=$(($RC+1))
			if (( $junit )); then
				junit_report_failure "robinhood.$PURPOSE.Posix" "Test #$index: $desc" "$dur" "ERROR"
			fi
		else
			echo "TEST #$index : OK" >> $SUMMARY
			SUCCES=$(($SUCCES+1))
			if (( $junit )); then
				junit_report_success "robinhood.$PURPOSE.Posix" "Test #$index: $desc" "$dur"
			fi

		fi
	fi
}

# clear summary
cp /dev/null $SUMMARY

#init xml report
if (( $junit )); then
	junit_init
	tinit=`date "+%s.%N"`
fi


######### TEST LIST ###########

# syntax: run_test function  config	descr	args
# e.g.
# run_test 218	test_rmdir 	rmdir.conf  "rmdir policies"	16 32

run_test	1	test_scan_report common.conf "scan and reports on large FS"
run_test	2	test_scan_report innodb.conf "scan and reports on large FS (innodb)"



echo
echo "========== TEST SUMMARY ($PURPOSE) =========="
cat $SUMMARY
echo "============================================="

#init xml report
if (( $junit )); then
	tfinal=`date "+%s.%N"`
	dur=`echo "($tfinal-$tinit)" | bc -l`
	echo "total test duration: $dur sec"
	junit_write_xml "$dur" $RC $(( $RC + $SUCCES ))
	rm -f $TMPXML_PREFIX.stderr $TMPXML_PREFIX.stdout $TMPXML_PREFIX.tc
fi

rm -f $SUMMARY
if (( $RC > 0 )); then
	echo "$RC tests FAILED, $SUCCES successful, $SKIP skipped"
else
	echo "All tests passed ($SUCCES successful, $SKIP skipped)"
fi
rm -f $TMPERR_FILE
exit $RC
