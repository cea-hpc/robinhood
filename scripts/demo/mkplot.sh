#!/bin/sh

# convert output data
sed -e "s/;/ /g" usage.csv | grep -v ost | awk '{print $1" "$2" "$3" "$2+$3" "$4}' > usage.dat

# run gnuplot
gnuplot trace_usage.gp
