#!/usr/bin/env pypy

from commons import isRealistic

import random
if not isRealistic():
	random.seed(0)

class ID:
	@staticmethod
	def getId(id):
		return int(id.split('_')[-1])

"""
Represents a Hadoop attempt.
"""
class Attempt:
	def __init__(self, attemptId=None, task=None, seconds=10, approx=False):
		self.task = task
		self.attemptId = attemptId
		self.seconds = seconds
		self.approx = approx
		self.nodeId = None
		self.start = None
		self.finish = None
		self.status = 'QUEUED' # QUEUED -> RUNNING -> SUCCEEDED | DROPPED
	
	def progress(self, p):
		self.seconds -= p
	
	def drop(self):
		self.seconds = 0
		self.status = 'DROPPED'
	
	def isCompleted(self):
		return self.seconds <= 0
	
	def getJobId(self):
		return '_'.join(self.attemptId.split('_')[0:3]).replace('attempt_', 'job_')
	
	def getTaskId(self):
		return self.attemptId[:self.attemptId.rfind('_')].replace('attempt_', 'task_')
	
	def getId(self):
		return int(self.attemptId.split('_')[4])
	
	def isMap(self):
		#return self.getTask().isMap()
		return self.attemptId.rfind('_m_') >= 0
	
	def isRed(self):
		#return self.getTask().isRed()
		return self.attemptId.rfind('_r_') >= 0
	
	def isApprox(self):
		return self.approx
	
	def getTask(self):
		return self.task
	
	def getJob(self):
		return self.getTask().getJob()
	
	def __str__(self):
		return self.attemptId

"""
Represents a Hadoop task.
"""
class Task:
	def __init__(self, taskId=None, job=None, length=None, lengthapprox=None):
		self.job = job
		self.taskId = taskId
		self.length = length
		self.lengthapprox = length
		if lengthapprox != None:
			self.lengthapprox = lengthapprox
		self.gauss = None # Task length distribution in %
		self.attempts = {}
		self.nattempts = 0
		self.status = 'QUEUED' # Status: QUEUED -> RUNNING -> SUCCEEDED | DROPPED
	
	def isQueued(self):
		if len(self.attempts) == 0:
			return True
		else:
			for attempt in self.attempts.values():
				if attempt.status == 'QUEUED':
					return True
		return False
	
	def isMap(self):
		return self.taskId.rfind('_m_') >= 0
	
	def isRed(self):
		return self.taskId.rfind('_r_') >= 0
	
	def getAttempt(self, approx=False):
		if len(self.attempts) == 0:
			self.nattempts += 1
			attemptId = (self.taskId+'_%d' % self.nattempts).replace('task_', 'attempt_')
			seconds = self.length if not approx else self.lengthapprox
			if self.gauss != None:
				seconds = random.gauss(seconds, self.gauss/100.0*seconds)
				# Minimum task length
				if seconds < 3:
					seconds = 3
			attempt = Attempt(attemptId=attemptId, task=self, seconds=seconds, approx=approx)
			self.attempts[attempt.attemptId] = attempt
			return attempt
		else:
			for attempt in self.attempts.values():
				if attempt.status == 'QUEUED':
					return attempt
		return None
	
	# We drop this task before start
	def drop(self):
		if len(self.attempts) == 0:
			self.nattempts += 1
			attemptId = (self.taskId+'_%04d' % self.nattempts).replace('task_', 'attempt_')
			attempt = Attempt(attemptId=attemptId, task=self, seconds=0)
			attempt.status = 'DROPPED'
			self.attempts[attempt.attemptId] = attempt
		
	def getJob(self):
		return self.job

"""
Represents a Hadoop job.
"""
class Job:
	# Job priorities
	class Priority:
		VERY_HIGH = 5
		HIGH = 4
		NORMAL = 3
		LOW = 2
		VERY_LOW = 1
	
	def __init__(self, jobId=None, nmaps=16, lmap=60, lmapapprox=None, nreds=1, lred=30, lredapprox=None, submit=0):
		self.jobId = jobId
		self.nmaps = nmaps
		self.nreds = nreds
		self.lmap = lmap
		self.lred = lred
		self.lmapapprox = lmapapprox if lmapapprox != None else self.lmap
		self.lredapprox = lredapprox if lredapprox != None else self.lred
		self.submit = submit # Submission time
		
		self.priority = Job.Priority.NORMAL
		
		# Set queue execution state
		self.reset()
		
		# Algorithm
		self.approxAlgoMapMax = 0.0 # Max % => approxAlgoMapVal < approxAlgoMapMax
		self.approxAlgoMapVal = 0.0 # %
		self.approxAlgoRedMax = 0.0 # Max % => approxAlgoRedVal < approxAlgoRedMax
		self.approxAlgoRedVal = 0.0 # %
		
		# Dropping
		self.approxDropMapMax = 0.0 # Max % => approxAlgoMapVal < approxAlgoMapMax
		self.approxDropMapVal = 0.0 # %
		self.approxDropRedMax = 0.0 # Max % => approxAlgoRedVal < approxAlgoRedMax
		self.approxDropRedVal = 0.0 # %
		
		'''
		# Approximation
		self.approxMapProbability = 0.0
		self.approxRedProbability = 0.0
		# Dropping
		self.dropMapPercentage = 1.0
		self.dropRedPercentage = 1.0
		'''
	
	'''
	Reset the job running
	'''
	def reset(self):
		# Mark it as queued
		self.status = 'QUEUED'
		# Reset Tasks
		self.cmaps = 0
		self.creds = 0
		self.maps = {}
		self.reds = {}
	
	def initTasks(self):
		# Maps
		for nmap in range(0, self.nmaps):
			taskId = '%s_m_%06d' % (self.jobId.replace('job_', 'task_'), nmap+1)
			self.maps[taskId] = Task(taskId, self, self.lmap, self.lmapapprox)
		# Reduces
		for nred in range(0, self.nreds):
			taskId = '%s_r_%06d' % (self.jobId.replace('job_', 'task_'), nred+1)
			self.reds[taskId] = Task(taskId, self, self.lred, self.lredapprox)
	
	def getMapTask(self, approx=None):
		for mapTask in self.maps.values():
			if mapTask.isQueued():
				# We select the approximation according to a probability
				if approx == None:
					approx = False
					if random.random() < self.approxAlgoMapVal:
						approx = True
				return mapTask.getAttempt(approx=approx)
		return None
	
	def getRedTask(self, approx=None):
		if self.cmaps >= len(self.maps):
			for redTask in self.reds.values():
				if redTask.isQueued():
					# We select the approximation according to a probability
					if approx == None:
						approx = False
						if random.random() < self.approxAlgoRedVal:
							approx = True
					return redTask.getAttempt(approx=approx)
		return None
	
	def mapQueued(self):
		ret = 0
		for mapTask in self.maps.values():
			if mapTask.isQueued():
				ret += 1
		return ret

	def redQueued(self):
		ret = 0
		if self.isMapCompleted():
			for redTask in self.reds.values():
				if redTask.isQueued():
					ret += 1
		return ret
	
	def getStart(self):
		start = None
		for task in self.maps.values() + self.reds.values():
			for attempt in task.attempts.values():
				if start == None or start > attempt.start:
					start = attempt.start
		return start
	
	def getFinish(self):
		finish = None
		for task in self.maps.values() + self.reds.values():
			for attempt in task.attempts.values():
				if finish == None or finish < attempt.finish:
					finish = attempt.finish
		return finish
	
	# Check if all the maps are completed
	def isMapCompleted(self):
		return self.cmaps >= len(self.maps)
	
	# Check if the job is running
	def isRunning(self):
		return self.cmaps < len(self.maps) or self.creds < len(self.reds)
	
	# Get the list of nodes that have run this node
	def getNodes(self):
		ret = []
		for task in self.maps.values() + self.reds.values():
			for attempt in task.attempts.values():
				if attempt.nodeId != None and attempt.nodeId not in ret:
					ret.append(attempt.nodeId)
		return ret
	
	def getQuality(self):
		ret = 100.0
		total = 0
		approximations = 0
		for task in self.maps.values() + self.reds.values():
			for attempt in task.attempts.values():
				total += 1
				if attempt.approx or attempt.status == 'DROPPED':
					approximations += 1
		if total > 0:
			ret = 100.0 - 100.0*approximations/total
		return ret
	
	# Complete an attempt
	def completeAttempt(self, attempt, drop=False):
		ret = []
		# Map
		if attempt.isMap():
			for mapTask in self.maps.values():
				if mapTask.taskId == attempt.getTaskId():
					if drop:
						mapTask.status = 'DROPPED'
					else:
						mapTask.status = 'SUCCEEDED'
					self.cmaps += 1
		# Reduce
		else:
			for redTask in self.reds.values():
				if redTask.taskId == attempt.getTaskId():
					if drop:
						redTask.status = 'DROPPED'
					else:
						redTask.status = 'SUCCEEDED'
					self.creds += 1
			if self.creds >= len(self.reds):
				ret.append(self)
		return ret
	
	def isMapDropping(self):
		return 1.0*self.cmaps/len(self.maps) > self.approxDropMapVal
	
	def isRedDropping(self):
		return 1.0*self.creds/len(self.reds) > self.approxDropRedVal
	
	# Drop an attempt
	def dropAttempt(self, attempt):
		return self.completeAttempt(attempt, drop=True)
	
	# Add an attempt to the job
	def addAttempt(self, attempt):
		taskId = attempt.getTaskId()
		if attempt.isMap():
			if taskId not in self.maps:
				self.maps[taskId] = Task(taskId)
			self.maps[taskId].attempts[attempt.attemptId] = attempt
		if attempt.isRed():
			if taskId not in self.reds:
				self.reds[taskId] = Task(taskId)
			self.reds[taskId].attempts[attempt.attemptId] = attempt
				
