#!/usr/bin/env pypy

from commons import isRealistic

import math
import random
if not isRealistic():
	random.seed(0)

from operator import attrgetter

from node import Node
from job import Job
from history import History
from history import HistoryViewer

from datetime import datetime

'''
Simulator.
'''
class Simulator:
	def __init__(self, logfile='history.log'):
		self.t = 0
		# Nodes
		self.nodes = {}
		# Jobs
		self.jobs = {}
		self.jobsQueue = []
		self.jobsDone = []
		# History
		self.logfile = logfile
		self.history = History(filename=self.logfile)
		# Job submission
		self.lastJobId = 1
		# Simulation
		self.maxTime = None
		# Id for jobs
		self.trackerId = datetime.now().strftime('%4Y%2m%2d%2H%2M')
		
		# Specify if the nodes are sent to sleep when there's no load
		self.nodeManagement = True
		
		# Outputs
		self.energy = None
	
	# Submit a job to run
	def addJob(self, job):
		# Assign automatic job id
		if job.jobId == None:
			while 'job_%s_%04d' % (self.trackerId, self.lastJobId) in self.jobs:
				self.lastJobId += 1 
			job.jobId = 'job_%s_%04d' % (self.trackerId, self.lastJobId)
		# Initialize tasks
		job.initTasks()
		
		# Save the information
		self.jobs[job.jobId] = job
		self.jobsQueue.append(job.jobId)
		
		# Sort the queue according to submission order
		#self.jobsQueue = sorted(self.jobsQueue, cmp=lambda jobId1, jobId2: self.jobs[jobId1].submit-self.jobs[jobId2].submit)
		self.jobsQueue = sorted(self.jobsQueue, cmp=lambda jobId1, jobId2: self.jobs[jobId1].submit-self.jobs[jobId2].submit if self.jobs[jobId1].priority==self.jobs[jobId2].priority else self.jobs[jobId2].priority-self.jobs[jobId1].priority)
		
		return job.jobId
	
	# Check if there is any idle node for reduces
	def getIdleNodeMap(self):
		for nodeId in sorted(self.nodes):
			node = self.nodes[nodeId]
			if node.status == 'ON' and len(node.maps) < node.numMaps:
				return node
		return None
	
	def getIdleNodesMap(self):
		ret = []
		for nodeId in sorted(self.nodes):
			node = self.nodes[nodeId]
			if node.status == 'ON' and len(node.maps) < node.numMaps:
				ret.append(node)
		return ret
	
	# Check if there is any idle node for reduces
	def getIdleNodeRed(self):
		for nodeId in sorted(self.nodes):
			node = self.nodes[nodeId]
			if node.status == 'ON' and len(node.reds) < node.numReds:
				return node
		return None
	
	def getIdleNodesRed(self):
		ret = []
		for nodeId in sorted(self.nodes):
			node = self.nodes[nodeId]
			if node.status == 'ON' and len(node.reds) < node.numReds:
				ret.append(node)
		return ret
	
	def getWakingNodes(self):
		ret = 0
		for nodeId in self.nodes:
			node = self.nodes[nodeId]
			if node.status.startswith('WAKING-'):
				ret += 1
		return ret
	
	# Get a queued map
	def getMapTask(self, approx=None):
		for jobId in self.jobsQueue:
			job = self.jobs[jobId]
			if self.t >= job.submit:
				mapTask = job.getMapTask(approx=approx)
				if mapTask != None:
					return mapTask
		return None
	
	# Get a queued reduce
	def getRedTask(self, approx=None):
		for jobId in self.jobsQueue:
			job = self.jobs[jobId]
			if self.t >= job.submit:
				redTask = job.getRedTask(approx=approx)
				if redTask != None:
					return redTask
		return None
	
	# Check if there is a map queued
	def mapQueued(self):
		ret = 0
		for jobId in self.jobsQueue:
			job = self.jobs[jobId]
			if self.t >= job.submit:
				ret += job.mapQueued()
		return ret
	
	# Check if the node is required: running job or providing data for a job
	def isNodeRequired(self, nodeId):
		node = self.nodes[nodeId]
		# Check if the node is in the covering subset (data) or is running
		if node.covering or node.isRunning():
			return True
		# Check if it has executed tasks from active tasks
		for jobId in self.jobsQueue:
			job = self.jobs[jobId]
			if job.isRunning() and nodeId in job.getNodes():
				return True
		return False
	
	# Check if there is a reduce queued
	def redQueued(self):
		ret = 0
		for jobId in self.jobsQueue:
			job = self.jobs[jobId]
			if self.t >= job.submit:
				ret += job.redQueued()
		return ret
	
	def getNodesUtilization(self):
		utilizations = []
		for nodeId in self.nodes:
			node = self.nodes[nodeId]
			if node.status == 'ON':
				utilization = 1.0*len(node.maps)/node.numMaps
				utilizations.append(utilization)
		return sum(utilizations)/len(utilizations) if len(utilizations)>0 else 1.0
	
	def getNodesRunning(self):
		ret = 0
		for nodeId in self.nodes:
			node = self.nodes[nodeId]
			if node.status == 'ON':
				ret += 1
		return ret
	
	# Energy in Wh
	def getEnergy(self):
		# J = Ws -> Wh
		return self.energy/3600.0
	
	# Average time to run per job in seconds
	def getPerformance(self):
		ret = None
		if len(self.jobs) > 0:
			ret = 0.0
			for jobId in self.jobs:
				job = self.jobs[jobId]
				ret += job.getFinish()
			ret = ret / len(self.jobs)
		return ret
	
	# Average quality per job in %
	def getQuality(self):
		ret = []
		for jobId in self.jobs:
			job = self.jobs[jobId]
			ret.append(job.getQuality())
		return sum(ret)/len(ret) if len(ret)>0 else 0.0
	
	# Run simulation
	def run(self):
		self.energy = 0.0
		
		# Log initial node status
		for nodeId in self.nodes:
			node = self.nodes[nodeId]
			self.history.logNodeStatus(self.t, node)
		
		# Iterate every X seconds
		while len(self.jobsQueue) > 0 and (self.maxTime == None or self.t < self.maxTime):
			# Run running tasks
			# =====================================================
			completedAttempts = []
			for node in self.nodes.values():
				completedAttempts += node.progress(1) # progress 1 second at a time
			
			# Mark completed maps
			completedJobs = []
			for attempt in completedAttempts:
				attempt.finish = self.t
				# Check if we finish the jobs
				completedJobs += attempt.getJob().completeAttempt(attempt)
				# Log
				self.history.logAttempt(attempt)
			
			for job in completedJobs:
				job.finish = self.t
				job.status = Job.Status.SUCCEEDED
				# Update queues
				self.jobsQueue.remove(job.jobId)
				self.jobsDone.append(job.jobId)
				# Log
				self.history.logJob(job)
			
			# Check which nodes are available to run tasks
			# =====================================================
			# Maps
			while self.mapQueued()>0 and self.getIdleNodeMap() != None:
				# Get a map that needs to be executed and assign it to a node
				idleNode = self.getIdleNodeMap()
				# TODO policy to decide when to approximate
				#mapAttempt = self.getMapTask(approx=True if self.getNodesUtilization() > 1.8 else False)
				mapAttempt = self.getMapTask()
				mapAttempt.start = self.t
				if mapAttempt.getJob().isMapDropping():
					mapAttempt.drop()
					mapAttempt.finish = self.t
					mapAttempt.approx = False
					completedJobs += mapAttempt.getJob().dropAttempt(mapAttempt)
					# Log
					self.history.logAttempt(mapAttempt)
				else:
					# Start running in a node
					idleNode.assignMap(mapAttempt)
			# Reduces
			while self.redQueued()>0 and self.getIdleNodeRed() != None:
				# Get a map that needs to be executed and assign it to a node
				idleNode = self.getIdleNodeRed()
				redAttempt = self.getRedTask()
				redAttempt.start = self.t
				if redAttempt.getJob().isRedDropping():
					redAttempt.drop()
					redAttempt.finish = self.t
					# Log
					self.history.logAttempt(redAttempt)
				else:
					idleNode.assignRed(redAttempt)
			
			# Node management
			# =====================================================
			# Check if we need less nodes. Idle nodes.
			if self.nodeManagement:
				lessNodes = 0
				lessNodes = min(len(self.getIdleNodesMap()), len(self.getIdleNodesRed()))
				# Check if we need more nodes. Size of the queues.
				moreNodes = 0
				if lessNodes == 0:
					moreNodesMaps = math.ceil(1.0*self.mapQueued() / 3) - self.getWakingNodes()
					moreNodesReds = math.ceil(self.redQueued() / 1) - self.getWakingNodes()
					moreNodes = max(moreNodesMaps, moreNodesReds, 0)
				# Change node status
				for node in self.nodes.values():
					if node.status == 'ON' and not self.isNodeRequired(node.nodeId) and lessNodes > 0:
						lessNodes -= 1
						seconds = node.timeSleep
						if isRealistic():
							seconds = random.gauss(seconds, 0.1*seconds) #+/-10%
						node.status = 'SLEEPING-%d' % seconds
						self.history.logNodeStatus(self.t, node)
					elif node.status == 'SLEEP' and moreNodes > 0:
						moreNodes -= 1
						seconds = node.timeWake
						if isRealistic():
							seconds = random.gauss(seconds, 0.1*seconds) #+/-10%
						node.status = 'WAKING-%d' % seconds
						self.history.logNodeStatus(self.t, node)
					# Transition status
					elif node.status.startswith('SLEEPING-'):
						seconds = int(node.status[len('SLEEPING-'):]) - 1
						if seconds <= 0:
							node.status = 'SLEEP'
							self.history.logNodeStatus(self.t, node)
						else:
							node.status = 'SLEEPING-%d' % seconds
					elif node.status.startswith('WAKING-'):
						seconds = int(node.status[len('WAKING-'):])   - 1
						if seconds <= 0:
							node.status = 'ON'
							self.history.logNodeStatus(self.t, node)
						else:
							node.status = 'WAKING-%d' % seconds
			# Account for power
			power = 0.0
			for node in self.nodes.values():
				power += node.getPower()
			self.history.logPower(self.t, power)
			
			self.energy += 1.0*power # s x W = J
			
			# Next period
			self.t += 1
		
		# Log final output
		if self.logfile != None:
			self.history.close()
			viewer = HistoryViewer(self.history.getFilename())
			viewer.generate()
