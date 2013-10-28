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

function measure {
	TRACE=$1
	OUTDIR=$2
	for sjf in `echo 0.0 0.25 0.5 0.75 1.0`; do
		#for approx in 1.0; do
		for drop in 0 25 50 75 100; do
			OUTFILE=$OUTDIR/res_D$drop\_S$sjf
			#pypy runsimulator.py  --approx $approx --sjf $sjf -l $OUTFILE 
			pypy runsimulator.py -f $TRACE -d $drop -s $sjf -l $OUTFILE 
			perl parse_hadoop_sim.pl $OUTFILE > $OUTFILE\.dat
			python ../../repos/create-cdf-data.py $OUTFILE\.dat > $OUTFILE\.res
			PID=$!
		done
	done
	wait $PID
}

##time ru
time measure $*

