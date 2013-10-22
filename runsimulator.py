#!/usr/bin/env pypy

from optparse import OptionParser

import random
import sys

from simulator import Simulator
from node import Node
from job import Job

def unit_test():
	for i in range(0, 20):
		job = Job(nmaps=64, lmap=140, lmapapprox=60, nreds=1, lred=15, submit=i*150)
		job.approxAlgoMapVal = options.approx # Approximate 50% of the maps
		if random.random() < 0.5:
			job.priority = Job.Priority.VERY_HIGH
		jobId = simulator.addJob(job)
	
	for jobID in simulator.jobsQueue:
		myjob = simulator.jobs[jobID]
		print myjob.jobId, myjob.priority, myjob.submit

	sys.exit(0)

if __name__ == "__main__":
	# Parse options
	parser = OptionParser()
	parser.add_option("-n", "--nodes",                      dest="nodes",     type="int", default=32,            help="number of nodes")
	parser.add_option("-r", "--real",  action="store_true", dest="realistic",             default=False,         help="run a realistic simulation")
	parser.add_option("-l", "--log",                        dest="log",                   default=None,          help="specify log file")
	
	parser.add_option("-a", "--approx",                     dest="approx",  type="float", default=0.0, help="specify the approximation percentage")
	parser.add_option("-s", "--sjf",                        dest="sjf",     type="float", default=0.0, help="specify the percentage of newly submitted job using SJF scheduling")
	

	(options, args) = parser.parse_args()
	#options.realistic
	options.og = None
	# Initialize simulator
	simulator = Simulator(logfile=options.log)
	# Add 8 servers
	for i in range(0, options.nodes):
		simulator.nodes['sol%03d' % i] = Node('sol%03d' % i)
	'''
	# Set some servers to start sleeping
	for i in range(4, 8):
		simulator.nodes['sol%03d' % i].status = 'SLEEP'
	'''
	
	# Add jobs
	'''
	for i in range(0, 20):
		job = Job(nmaps=64, lmap=140, lmapapprox=60, nreds=1, lred=15, submit=i*200)
		job.approxAlgoMapVal = 0.0 # Approximate 50% of the maps
		jobId = simulator.addJob(job)
	'''
	#unit_test()
	for i in range(0, 20):
		job = Job(nmaps=64, lmap=140, lmapapprox=60, nreds=1, lred=15, submit=i*15)
		job.approxAlgoMapVal = options.approx # Approximate 50% of the maps
		if random.random() < options.sjf:
			job.priority = Constants.VERY_HIGH
		jobId = simulator.addJob(job)
	
	'''
	for i in range(0, 20):
		job = Job(nmaps=64, lmap=140, lmapapprox=60, nreds=1, lred=15, submit=i*200)
		job.approxDropMapVal = 0.1    # Drop once 50% of the maps are completed
		jobId = simulator.addJob(job)
	'''
	'''
	# Submit 20 jobs following a normal distribution
	for i in range(0, 20):
		submit = int(random.gauss(1500, 1000))
		if submit < 0:
			submit = 0
		job = Job(nmaps=64, lmap=140, lmapapprox=60, nreds=1, lred=15, submit=submit)
		job.approxAlgoMapVal = 0.5 # Approximate 50% of the maps
		jobId = simulator.addJob(job)
	'''
	'''
	# Submit one job
	job = Job(nmaps=66, lmap=140, lmapapprox=60, nreds=1, lred=15, submit=0)
	#job.approxAlgoMapVal = options.approx # Approximate 50% of the maps
	job.approxDropMapVal = 1.0-options.approx # Drop after X% of the maps
	jobId = simulator.addJob(job)
	'''
	
	'''
	simulator.addJob(Job(nmaps=16, lmap=100, nreds=1, lred=10))
	simulator.addJob(Job(nmaps=8, lmap=100, nreds=2))
	simulator.addJob(Job(nmaps=8, lmap=100, nreds=2))
	simulator.addJob(Job(nmaps=8, lmap=100, nreds=2))
	simulator.addJob(Job(nmaps=8, lmap=100, nreds=1))
	simulator.addJob(Job(nmaps=4, lmap=100, nreds=1))
	simulator.addJob(Job(nmaps=2, lmap=100, nreds=1))
	simulator.addJob(Job(nmaps=2, lmap=100, nreds=1))
	simulator.addJob(Job(nmaps=16, lmap=100, nreds=2, lred=100))
	'''
	
	#simulator.addJob(Job('job_0010', nmaps=16, lmap=100, nreds=1, lred=100, submit=500))
	
	# Start running simulator
	simulator.run()
	
	# Summary
	print 'Energy: %.1fWh' % (simulator.getEnergy()/3600.0)
	print 'Performance:', simulator.getPerformance(), len(simulator.jobs)
	print 'Quality: %.1f%%' % (simulator.getQuality()*100.0)
	
	# for approx in `echo 0 0.2 0.4 0.6 0.8 1.0`; do for nodes in `echo 1 2 4 6 10 14 18 22`; do energy=`pypy runsimulator.py  --approx $approx --nodes 1`; echo $approx $nodes $energy; done; done
	
