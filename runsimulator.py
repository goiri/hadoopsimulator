#!/usr/bin/env pypy

from optparse import OptionParser

import random
import sys

from simulator import Simulator
from node import Node
from job import Job
from workloadmanager import WorkloadManager

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
	parser.add_option('-l', "--log",                        dest="log",                      default=None,  help="specify the log file")
	
	parser.add_option('-n', "--nodes",                      dest="nodes",     type="int",    default=32,    help="specify the number of nodes")
	parser.add_option('-j', "--jobs",                       dest="jobs",      type="int",    default=20,    help="specify the number of jobs")
	parser.add_option('-g', "--gauss",                      dest="gauss",     type="float",  default=None,  help="specify the variance of the task length")
	
	parser.add_option('-a', "--approx",                     dest="approx",    type="float",  default=0.0,   help="specify the approximation percentage (0%)")
	parser.add_option('-d', "--drop",                       dest="drop",      type="float",  default=100.0, help="specify the approximation percentage (100%)")
	
	parser.add_option('-r', "--real",  action="store_true", dest="realistic",                default=False, help="run a realistic simulation")
	
	parser.add_option('-s', "--sjf",                        dest="sjf",       type="float",  default=0.0,   help="specify the percentage of newly submitted job using SJF scheduling")
	parser.add_option('-f', "--infile",                     dest="infile",    type="string", default="",    help="workload file")
	

	(options, args) = parser.parse_args()
	#options.realistic
	options.og = None
	# Initialize simulator
	simulator = Simulator(logfile=options.log)
	# Add servers
	for i in range(0, options.nodes):
		simulator.nodes['sol%03d' % i] = Node('sol%03d' % i)
	'''
	# Set some servers to start sleeping
	for i in range(4, 8):
		simulator.nodes['sol%03d' % i].status = 'SLEEP'
	'''
	
	# Test
	#unit_test()
	
	# Add jobs
	if len(options.infile) > 0:
		manager = WorkloadManager(options.infile)
		for job in manager.getJobs():
			if random.random() < options.sjf:
				job.priority = Constants.VERY_HIGH
			simulator.addJob(job)
		'''
		manager.initManager(options.infile)
		if options.sjf > 0.0:
			manager.applySJFPriority(options.sjf)
		manager.copyToSimulator(simulator);
		'''
	else:
		# Submit jobs
		for i in range(0, options.jobs):
			# Create the job
			job = Job(nmaps=64, lmap=140, lmapapprox=60, nreds=1, lred=15, submit=0)
			job.approxAlgoMapVal = options.approx # Approximate X% of the maps
			job.approxDropMapVal = options.drop   # Drop X% of the maps
			job.gauss = options.gauss # +/-%
			# Shortest job first policy, this is weird
			if random.random() < options.sjf:
				job.priority = Constants.VERY_HIGH
			jobId = simulator.addJob(job)
	
	# Start running simulator
	simulator.run()
	
	# Summary
	print 'Nodes:   %d'  %      len(simulator.nodes)
	print 'Energy:  %.1fWh'  % (simulator.getEnergy())
	print 'Perf:    %.1fs %d jobs' % (simulator.getPerformance(), len(simulator.jobs))
	print 'Quality: %.1f%%' % (simulator.getQuality())
	