#!/bin/bash

function run {
	for approx in `echo 0.0 0.2 0.4 0.6 0.8 1.0`; do
		for nodes in `seq 1 23`; do
			pypy runsimulator.py  --approx $approx --nodes $nodes &
			PID=$!
		done
	done
	wait $PID
}

function myplot {
	OUTDIR=$1
	OUTNAME=$2
	gnuplot << EOF
	set term postscript eps color blacktext "Helvetica" 24
	set output "$OUTDIR/$OUTNAME.eps"
	set autoscale                        
	unset log                              
	unset label                            
	set xtic auto                          
	set ytic auto                          
	set title "CDF"
	set key inside right bottom vertical Right noreverse enhanced autotitles

	set xlabel "Time(s)"
	set ylabel "Fraction"
	set xtics border in scale 1,0.5 nomirror rotate by -45  offset character 0, 0, 0 font "Helvetica,20" 

	plot "$OUTDIR/res_D100_S0.0.res" using 2:1 title "0% SJF, D=100%" with linespoints, \
		"$OUTDIR/res_D100_S0.25.res" using 2:1 title "25% SJF, D=100%" with linespoints, \
		"$OUTDIR/res_D100_S0.5.res" using 2:1 title "50% SJF, D=100%" with linespoints, \
		"$OUTDIR/res_D100_S0.75.res" using 2:1 title "75% SJF, D=100%" with linespoints, \
		"$OUTDIR/res_D100_S1.0.res" using 2:1 title "100% SJF, D=100%" with linespoints
EOF
}

function measure {
	TRACE=$1
	OUTDIR=$2
	for sjf in `echo 0.0 0.25 0.5 0.75 1.0`; do
		#for approx in 1.0; do
		#for drop in 0 25 50 75 100; do
		for drop in 100; do
			OUTFILE=$OUTDIR/res_D$drop\_S$sjf
			#pypy runsimulator.py  --approx $approx --sjf $sjf -l $OUTFILE 
			pypy runsimulator.py -f $TRACE -d $drop -s $sjf -l $OUTFILE  #> $OUTFILE\.stats
			perl parse_hadoop_sim.pl $OUTFILE $OUTDIR
			python ../../repos/create-cdf-data.py $OUTFILE\.dat > $OUTFILE\.res
			mv history.html $OUTFILE\.html
			PID=$!
		done
	done
	myplot $OUTDIR var_sjf_d100
	convert $OUTDIR/var_sjf_d100.eps $OUTDIR/var_sjf_d100.png
	wait $PID
}



##time ru
time measure $*

