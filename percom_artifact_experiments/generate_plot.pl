set terminal pdf
set output "plot.pdf"
set ylabel "Time [s]" font ",14"
set xlabel "Number of workers" font ",14"
set xrange [2:20]
set for [i=2:20] xtics (0,i)
set key right top Right title 'Legend' box 3
plot 'plot2.dat' w line title "Centralized Mondrian" lt rgb "blue" lw 2, 'plot.dat' w linespoints title "Spark-based Mondrian" pt 1 ps 2 lt rgb "#009900" lw 2
set tics font ",14"
set title font ",14"
set key font ",14"
set title "Execution time comparison"
set autoscale fix
