# Copyright 2023 Unibg Seclab (https://seclab.unibg.it)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
