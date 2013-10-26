#!/usr/bin/env pypy

from job import Job

"""
Represents
"""
class Node:
	def __init__(self, nodeId=None):
		self.nodeId = nodeId
		self.status = 'ON' # ON -> SLEEPING-X -> SLEEP -> WAKING-X -> ON
		self.maps = [] # Attempts
		self.reds = [] # Attempts
		
		# If the node is in the covering subset
		self.covering = False
		
		# Slots
		self.numMaps = 3
		self.numReds = 1
		
		# Node description
		self.powerSleep = 3 # W
		self.powerIdle = 21 # W
		self.powerFull = 29 # W
		self.timeSleep = 7 # seconds
		self.timeWake =  7 # seconds
	
	'''
	Progress the execution of the node for a cycle.
	It returns the attempts that has finished.
	'''
	def progress(self, p):
		ret = []
		
		# Progress the execution of the tasks in this node
		# Maps
		for mapAttempt in list(self.maps):
			mapAttempt.progress(1)
			if mapAttempt.getJob().isMapDropping():
				mapAttempt.drop()
			# Check if the map is completed
			if mapAttempt.isCompleted():
				if mapAttempt.status != Job.Status.DROPPED:
					mapAttempt.status = Job.Status.SUCCEEDED
				self.maps.remove(mapAttempt)
				ret.append(mapAttempt)
		# Reduces
		for redAttempt in list(self.reds):
			# Check if the maps of this node are completed
			if redAttempt.getJob().isMapCompleted():
				redAttempt.progress(1)
				if redAttempt.getJob().isRedDropping():
					redAttempt.drop()
			# Check if the reduce is completed
			if redAttempt.isCompleted():
				if redAttempt.status != Job.Status.DROPPED:
					redAttempt.status = Job.Status.SUCCEEDED
				self.reds.remove(redAttempt)
				ret.append(redAttempt)
		return ret
	
	# Start running a map attempt
	def assignMap(self, attempt):
		self.maps.append(attempt)
		attempt.status = 'RUNNING'
		attempt.nodeId = self.nodeId
	
	# Start running a reduce attempt
	def assignRed(self, attempt):
		self.reds.append(attempt)
		attempt.status = 'RUNNING'
		attempt.nodeId = self.nodeId
	
	# Check if the node is running an attempt
	def isRunning(self):
		return (len(self.maps) + len(self.reds)) > 0
	
	
	def getPower(self):
		power = 0.0
		if self.status == 'ON':
			utilization = 1.0*(len(self.maps) + len(self.reds))/(self.numMaps + self.numReds)
			power += self.powerIdle + utilization*(self.powerFull - self.powerIdle)
		elif self.status.startswith('SLEEPING-'):
			power += self.powerIdle
		elif self.status == 'SLEEP':
			power += self.powerSleep
		elif self.status.startswith('WAKING-'):
			power += self.powerIdle
		return power
	
	def __str__(self):
		ret = self.nodeId
		for mapTask in self.maps:
			ret += ' ' + mapTask.attemptId
		for redTask in self.reds:
			ret += ' ' + redTask.attemptId
		return ret
