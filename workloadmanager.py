import sys, random
from job import Job
from simulator import Simulator

'''
InitSimulator
'''	
class WorkloadManager:
        def __init__(self):
		self.jobQueue = []
		self.jobIdQueue = []

	def initSimulator(self, inFile):
		lineno=0
		with open(inFile, "r") as f:
			for line in f:
				if not line.startswith('#'):
					line = line.replace('\n', '')
					line = line.strip()
					if len(line)<=0:
						continue
					# Job(nmaps=64, lmap=140, lmapapprox=60, nreds=1, lred=15, submit=i*150, approx)
					splits = line.split()
					nmaps0 =      int(splits[0])
					lmap0 =       int(splits[1])
					lmapapprox0 = int(splits[2])
					nreds0 =      int(splits[3])
					lred0 =       int(splits[4])
					submit0 =     int(splits[5])
					approx0 =   float(splits[6])
					# Create job
					job = Job(nmaps=nmaps0, lmap=lmap0, lmapapprox=lmapapprox0, nreds=nreds0, lred=lred0, submit=submit0)
					job.approxAlgoMapVal = approx0 
					self.jobQueue.append(job)
					lineno+=1
		f.close()
		return lineno
	
	def applySJFPriority(self, rate):
		for job in self.jobQueue:
			if random.random() < rate: 
				job.priority = Job.Priority.VERY_HIGH

	def copyToSimulator(self, simulator):
		for job in self.jobQueue:
			jobId = simulator.addJob(job)
			self.jobIdQueue.append(jobId)

