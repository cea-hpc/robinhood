set term png
set out "usage.png"
set xlabel "time(sec)"
set ylabel "space used(MB)"
set key left top
plot "usage.dat" using 1:($2/1024) title 'OST1' with lines, \
     "usage.dat" using 1:($3/1024) title 'OST2' with lines, \
     "usage.dat" using 1:($4/1024) title 'Lustre total (OST1+OST2)' with lines, \
     "usage.dat" using 1:($5/1024) title 'backend' with lines

