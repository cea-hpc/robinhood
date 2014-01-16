#!/bin/bash

cmd=$1
descr=$2
seealso=$3

if [[ -z $2 ]]; then
    echo "Missing command description" >&2
    exit 1
fi

# add name and descriptions
echo "NAME"
echo $(basename $cmd) "-" $descr
# removes special color characters
# change "Section: text" to "Section:\n  text"
# change "Section:" to "SECTION"
# change "USAGE" to "SYNOPSIS"
# Format option description
$cmd --help |  sed -r "s/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[m|K]//g" |
     sed -e "s#^\([A-Z][a-z _\-]*:\) \(.*\)#\1\n    \2#" |
     sed -e "s#^\([A-Z][a-z _\-]*\):#\U\1#" |
     sed -e "s#^USAGE\$#SYNOPSIS#" | perl -ne 'if (/^(\s+-[^\n]+)$/) { if ($opt == 1) {print "\n\n$1"} else { print "\n$1" }  $opt=1} elsif ($opt==1) {if ($_ =~ /^\b*([^\b]*)/) {print $1} else {print $_} $opt=0;} else { print $_ }'

if [[ -n $seealso ]]; then
    echo "SEE ALSO"
    echo "   $seealso"
fi
