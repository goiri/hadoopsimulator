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

time run
