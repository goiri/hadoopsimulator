#!/usr/bin/env pypy

from job import Job

class Scheduler:
	def __init__(self):
		# Jobs
		self.jobs = {}
		self.jobsQueue = []
		self.jobsDone = []
	
	'''
	Scheduling policies.
	Returns the order of 2 jobs according to the scheduling policy
	'''
	def schedulingPolicy(self, jobId1, jobId2):
		return self.schedulingFIFOprior(jobId1, jobId2)
		
		
	def schedulingFIFO(self, jobId1, jobId2):
		return self.jobs[jobId1].submit-self.jobs[jobId2].submit
	
	def schedulingFIFOprior(self, jobId1, jobId2):
		if self.jobs[jobId1].priority==self.jobs[jobId2].priority:
			return self.jobs[jobId1].submit-self.jobs[jobId2].submit
		else:
			return self.jobs[jobId2].priority-self.jobs[jobId1].priority
	
	def schedulingSJF(self, jobId1, jobId2):
		# TODO cheng
		# One approach can be to check length of the jobs in the queue
		# Sort it according to the length
		return self.jobs[jobId2].nmaps*self.jobs[jobId2].lmap - self.jobs[jobId1].nmaps*self.jobs[jobId1].lmap
	