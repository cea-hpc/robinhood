f=$1
script_dir=$(dirname $(readlink -m $0))

[ -f "$f" ] || exit 1

IGNORE_LIST="DEPRECATED_FUNCTION,INITIALISED_STATIC,GLOBAL_INITIALISERS,PRINTF_L,ASSIGN_IN_IF,C99_COMMENTS,USE_NEGATIVE_ERRNO,BRACES,NEW_TYPEDEFS"

# Kernel style:
#    -nbad -bap -nbc -bbo -hnl -br -brs -c33 -cd33 -ncdb -ce -ci4
#    -cli0 -d0 -di1 -nfc1 -i8 -ip0 -l80 -lp -npcs -nprs -npsl -sai
#    -saf -saw -ncs -nsc -sob -nfca -cp33 -ss -ts8 -il1


indent $f -o $f.new.c  \
-nbad -bap -nbc -bbo -hnl -br -brs -c0 -cd0 -ncdb -ce -ci4 \
-cli0 -d0 -di2 -nfc1 -i4 -nut -ip0 -l80 -lp -npcs -nprs -npsl -sai \
-saf -saw -ncs -nsc -sob -nfca -cp2 -ss -ts4 -il1 -T time_t -T uint64_t -T size_t -T global_config_t -T sm_info_def_t

$script_dir/checkpatch.pl --ignore $IGNORE_LIST --terse --show-types -f $f.new.c > $f.check.out

grep $f.new.c $f.check.out | while read l; do
	error=$(echo "$l" | cut -d ':' -f 4 | tr -d ' ')
	line=$(echo "$l" | cut -d ':' -f 2)

	case "$error" in
	POINTER_LOCATION)
		sed -i "$line s/\([a-z_]*\) \* \([a-z_]*\)/\1 *\2/g" $f.new.c
		;;
	SPACING)
		sed -i -e "$line s/( /(/g" -e "$line s/ )/)/g" -e "$line s/while(/while (/" $f.new.c
		;;
	RETURN_PARENTHESES)
		sed -i -e "$line s/return (\(.*\));/return \1;/" $f.new.c
		;;
	*)	
		echo "ignored error $error, line $line"
		;;
	esac
	
done

# mark remaining errors
$script_dir/checkpatch.pl --ignore $IGNORE_LIST --terse --show-types -f $f.new.c > $f.check.out
err=0
grep $f.new.c $f.check.out | while read l; do
	err=1
	error=$(echo "$l" | cut -d ':' -f 4 | tr -d ' ')
	line=$(echo "$l" | cut -d ':' -f 2)

	case "$error" in
	POINTER_LOCATION|SPACING|LONG_LINE)
		sed -i "$line s#^#// FIXME CHECKPATCH: #" $f.new.c
		;;
	*)	
		echo "ignored error $error, line $line"
		;;
	esac
done

[[ "$err" != "0" ]] && vimdiff $f.new.c $f
$script_dir/checkpatch.pl --ignore $IGNORE_LIST --show-types -f $f.new.c
