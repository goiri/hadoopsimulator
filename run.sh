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

function profile_entire_FB_trace {
	TRACE=$1
	NUM=$2
	OUTDIR=$3
	MYID=1
	MYPID=90
	for nodes in 9 18 36 72 144;do
		for sjf in `echo 0.0`; do
			for drop in 100; do
				OUTBASE=`echo $TRACE|sed 's/\//_/g'`
				OUTFILE=$OUTDIR/$OUTBASE"_"res_D$drop\_S$sjf\_N$nodes
				pypy runsimulator.py -f $TRACE -d $drop -s $sjf -l $OUTFILE -n $nodes & 
				MYPID=$!
				let MYID=$MYID+1
				if [ 0 -eq $((MYID%$NUM)) ];then
					wait $MYPID
				fi 
				#perl parse_hadoop_sim.pl $OUTFILE $OUTDIR
				#python ../../repos/create-cdf-data.py $OUTFILE\.dat > $OUTFILE\.res
				#mv history.html $OUTFILE\.html
			done
		done
	done
	#myplot $OUTDIR var_sjf_d100
	#convert $OUTDIR/var_sjf_d100.eps $OUTDIR/var_sjf_d100.png
	wait $MYPID
}

##time ru
#time measure $*
profile_entire_FB_trace $*


