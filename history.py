#!/usr/bin/env pypy

from job import ID
from job import Job
from job import Task
from job import Attempt

from commons import timeStr

from math import floor, ceil
from operator import attrgetter
from operator import itemgetter

'''
Store a simulation history.
'''
class History:
	def __init__(self, filename='history.log'):
		self.filename = filename
		if self.filename != None:
			self.file = open(self.filename, 'w')

	def __del__(self):
		self.close()
		
	def close(self):
		if self.filename != None:
			self.file.close()
	
	def getFilename(self):
		return self.filename

	def logJob(self, job):
		if self.filename != None:
			self.file.write('Job JOBID="%s" JOB_STATUS="%s" SUBMIT_TIME="%d" START_TIME="%d" FINISH_TIME="%d" .\n' % (job.jobId, Job.Status.toString[job.status], job.submit, job.getStart(), job.getFinish()))

	def logTask(self, task):
		if self.filename != None:
			self.file.write('Task TASKID="%s" TASK_STATUS=%s" .\n' % (task.taskId, Job.Status.toString[task.status]))
	
	def logAttempt(self, attempt):
		if attempt.isMap():
			self.logMapAttempt(attempt)
		else:
			self.logReduceAttempt(attempt)
	
	def logMapAttempt(self, attempt):
		if self.filename != None:
			self.file.write('MapAttempt TASKID="%s" TASK_ATTEMPT_ID="%s" TASK_STATUS="%s" APPROXIMATED="%s" START_TIME="%d" FINISH_TIME="%d" HOSTNAME="%s" .\n' % (attempt.getTaskId(), attempt.attemptId, Job.Status.toString[attempt.status], str(attempt.approx), attempt.start, attempt.finish, attempt.nodeId))
	
	def logReduceAttempt(self, attempt):
		if self.filename != None:
			self.file.write('ReduceAttempt TASKID="%s" TASK_ATTEMPT_ID="%s" TASK_STATUS="%s" APPROXIMATED="%s" START_TIME="%d" FINISH_TIME="%d" HOSTNAME="%s" .\n' % (attempt.getTaskId(), attempt.attemptId, Job.Status.toString[attempt.status], str(attempt.approx), attempt.start, attempt.finish, attempt.nodeId))
		
	def logNodeStatus(self, t, node):
		if self.filename != None:
			self.file.write('Node HOSTNAME="%s" TIME="%d" STATUS="%s" .\n' % (node.nodeId, t, node.status))
		
	def logPower(self, t, power):
		if self.filename != None:
			self.file.write('Power TIME="%d" POWER="%d" .\n' % (t, power))

'''
Generate an HTML with the trace.
'''
class HistoryViewer:
	def __init__(self, filenamein='history.log', filenameout='history.html'):
		self.filenamein  = filenamein
		self.filenameout = filenameout
		if self.filenamein != None and self.filenameout != None:
			self.filein =  open(self.filenamein,  'r')
			self.fileout = open(self.filenameout, 'w')
			self.zoom = 0.5
			
			self.plotJobs = True
			self.plotTasks = False
			self.plotNodeTasks = False
			self.plotNodes = True
			self.plotNodesStatus = True
			self.plotPower = True
	
	def __del__(self):
		self.close()
	
	def close(self):
		if self.filenamein != None and self.filenameout != None:
			self.filein.close()
			self.fileout.close()
	
	def getDict(self, strings):
		ret = {}
		for keyvalue in strings:
			key, value = keyvalue.split('=')
			while value.startswith('"'):
				value = value[1:]
			while value.endswith('"'):
				value = value[:-1]
			ret[key] = value
		return ret
	
	def getTaskGraph(self, attempt):
		content = str(ID.getId(attempt.getTaskId()))
		content = ''
		out =  '<table  border="0" cellspacing="0" cellpadding="0" style="border:1px solid:black;">';
		out += '<tr height="5px">\n'; 
		out += '<td width="' + str(floor(1.0*                attempt.start *self.zoom)) + 'px" bgcolor="white"/>\n'
		out += '<td width="' + str(floor(1.0*(attempt.finish-attempt.start)*self.zoom)) + 'px" style="background-color:'+self.getTaskColor(attempt)+'; color:#FFFFFF; font-size:small" align="left">'+content+'</td>\n'
		out += '</tr>'
		out += '</table>\n'
		return out
	
	def getTaskGraphList(self, attempts):
		out =  '<table  border="0" cellspacing="0" cellpadding="0">'# style="border:1px solid black;">'
		#out += '<tr height="20px">\n'
		out += '<tr height="5px">\n'
		prev = 0
		for attempt in attempts:
			content = str(ID.getId(attempt.getTaskId()))
			content = ''
			out += '<td width="' + str(floor(1.0*(attempt.start -   prev)*self.zoom)) + 'px" bgcolor="white"/>\n'
			out += '<td width="' + str(floor(1.0*(attempt.finish-attempt.start)*self.zoom)) + 'px" style="background-color:'+self.getTaskColor(attempt)+'; color:#FFFFFF; font-size:small; border-color:black; border:1px solid:black; border-style: inset; border-width:1px;" align="left">'+content+'</td>\n'
			prev = attempt.finish
		out += "</tr>";
		out += "</table>\n";
		return out
	
	def getJobGraph(self, job):
		content = ''
		out =  '<table  border="0" cellspacing="0" cellpadding="0">';
		out += '<tr height="5px">\n'; 
		out += '<td width="' + str(floor(1.0*                job.submit *self.zoom)) + 'px" bgcolor="white"/>\n'
		out += '<td width="' + str(floor(1.0*(job.start-job.submit)*self.zoom)) + 'px" style="background-color:#0000FF; color:#FFFFFF; font-size:small; border-color:black; border:1px solid:black; border-style: inset; border-width:1px;" align="left">'+content+'</td>\n'
		out += '<td width="' + str(floor(1.0*(job.finish-job.start)*self.zoom)) + 'px" style="background-color:#00FF00; color:#FFFFFF; font-size:small; border-color:black; border:1px solid:black; border-style: inset; border-width:1px;" align="left">'+content+'</td>\n'
		out += '</tr>'
		out += '</table>\n'
		return out
	
	def getNodeStatusGraphList(self, nodeStatuses):
		out =  '<table  border="0" cellspacing="0" cellpadding="0">'# style="border:1px solid black;">'
		out += '<tr height="15px">\n'
		prevNodeStatus = nodeStatuses[0]
		for nodeStatus in nodeStatuses[1:]:
			out += '<td width="' + str(floor(1.0*(nodeStatus[0] - prevNodeStatus[0])*self.zoom)) + 'px" bgcolor="'+self.getNodeColor(prevNodeStatus[1])+'"/>\n'
			prevNodeStatus = nodeStatus
		out += "</tr>";
		out += "</table>\n";
		return out
	
	def getNodeColor(self, nodeStatus):
		if nodeStatus == 'ON':
			color = "#00FF00"
		elif nodeStatus == 'SLEEP':
			color = "#FFFFFF"
		elif nodeStatus.startswith('SLEEPING'):
			color = "#800000"
		elif nodeStatus.startswith('WAKING'):
			color = "#008000"
		elif nodeStatus.startswith('OFF'):
			color = "#000000"
		else:
			color = "#101010"
		return color
	
	def getTaskColor(self, attempt):
		if attempt.status == Job.Status.toString[Job.Status.DROPPED]:
			if attempt.approx:
				color = "#101010"
			else:
				color = "#000000"
		elif attempt.isMap() and attempt.approx:
			color = "#0000FF"
		elif attempt.isMap():
			color = "#000080"
		elif attempt.isRed() and attempt.approx:
			color = "#FF0000"
		elif attempt.isRed():
			color = "#800000"
		elif attempt.approx:
			color = "#00FF00"
		else:
			color = "#008000"
		return color
	
	'''
	Generate execution report from history file.
	'''
	def generate(self):
		if self.filenamein != None and self.filenameout != None:
			# Read all the information
			jobs = {}
			attempts = []
			nodes = {}
			power = []
			maxpower = 0.0
			for line in self.filein.readlines():
				if line.startswith('Job'):
					ret = self.getDict(line.split(' ')[1:-1])
					jobId = ret['JOBID']
					if jobId not in jobs:
						jobs[jobId] = Job(jobId=jobId)
					jobs[jobId].status = ret['JOB_STATUS']
					jobs[jobId].submit = int(ret['SUBMIT_TIME'])
					jobs[jobId].start =  int(ret['START_TIME'])
					jobs[jobId].finish = int(ret['FINISH_TIME'])
					'''
					job.nmapsapprox = 0
					job.nredsapprox = 0
					for attempt in attempts:
						if job.jobId == attempt.getJobId():
							if attempt.isMap():
								job.nmaps += 1
								if attempt.approx or attempt.status == Job.Status.DROPPED:
									job.nmapsapprox += 1
							if attempt.isMap():
								job.nreds += 1
								if attempt.approx or attempt.status == Job.Status.DROPPED:
									job.nredsapprox += 1
					job.quality = 1.0 - (1.0*job.nmapsapprox/job.nmaps)
					'''
				elif line.startswith('Task'):
					pass
				elif line.startswith('MapAttempt') or line.startswith('ReduceAttempt'):
					ret = self.getDict(line.split(' ')[1:-1])
					attempt = Attempt()
					attempt.attemptId = ret['TASK_ATTEMPT_ID']
					attempt.start =  int(ret['START_TIME'])
					attempt.finish = int(ret['FINISH_TIME'])
					attempt.approx = (ret['APPROXIMATED'] == 'True' or ret['APPROXIMATED'] == 'true')
					attempt.status = ret['TASK_STATUS']
					attempt.nodeId = ret['HOSTNAME']
					attempts.append(attempt)
					# Update job information
					jobId = attempt.getJobId()
					if jobId not in jobs:
						jobs[jobId] = Job(jobId=jobId)
					jobs[jobId].addAttempt(attempt)
				elif line.startswith('Node'):
					ret = self.getDict(line.split(' ')[1:-1])
					nodeId = ret['HOSTNAME']
					if nodeId not in nodes:
						nodes[nodeId] = []
					nodes[nodeId].append((int(ret['TIME']), ret['STATUS']))
				elif line.startswith('Power'):
					ret = self.getDict(line.split(' ')[1:-1])
					power.append((int(ret['TIME']), float(ret['POWER'])))
					maxpower = max(maxpower, float(ret['POWER']))
			
			# Node -> Attempts
			nodeAttempts = {}
			totalAttemptsApprox = 0
			totalAttemptsDropped = 0
			totalAttemptsPrecise = 0
			for attempt in attempts:
				if attempt.approx:
					totalAttemptsApprox += 1
				if attempt.status == Job.Status.DROPPED:
					totalAttemptsDropped += 1
				if attempt.status != Job.Status.DROPPED and not attempt.approx:
					totalAttemptsPrecise += 1
				if attempt.nodeId not in nodeAttempts:
					nodeAttempts[attempt.nodeId] = []
				nodeAttempts[attempt.nodeId].append(attempt)
			if 'None' in nodeAttempts:
				del nodeAttempts['None']
			
			totalJobTime = 0
			totalJobRunTime = 0
			totalQuality = 0.0
			for jobId in jobs:
				job = jobs[jobId]
				totalJobTime += job.finish - job.submit
				totalJobRunTime += job.finish - job.start
				totalQuality += job.getQuality()
			totalJobTime = totalJobTime/len(jobs)
			totalJobRunTime = totalJobRunTime/len(jobs)
			totalJobQuality = totalQuality/len(jobs)
			
			# Power
			power = sorted(power, key=itemgetter(0))
			totalenergy = 0.0
			for t, p in power:
				totalenergy += p
			totaltime = power[-1][0]
			
			# Generate output HTML
			self.fileout.write('<html>\n')
			self.fileout.write('<head>\n')
			self.fileout.write('<link rel="stylesheet" type="text/css" href="http://sol018:50030/static/hadoop.css">\n')
			self.fileout.write('<title>Execution profile</title>\n')
			self.fileout.write('</head>\n')
			self.fileout.write('<body>\n')
			
			# Summary
			self.fileout.write('<h1>Summary</h1>\n')
			self.fileout.write('<ul>\n')
			self.fileout.write('  <li>Jobs: %d</li>\n' % len(jobs))
			self.fileout.write('  <ul>\n')
			self.fileout.write('    <li>Average turn-around time: %.1fs</li>\n' % totalJobTime)
			self.fileout.write('    <li>Average runtime: %.1fs</li>\n' % totalJobRunTime)
			self.fileout.write('    <li>Average quality: %.1f%%</li>\n' % totalJobQuality)
			self.fileout.write('  </ul>\n')
			self.fileout.write('  <li>Attempts: %d</li>\n' % len(attempts))
			self.fileout.write('  <ul>\n')
			self.fileout.write('    <li>Precise: %d</li>\n' % totalAttemptsPrecise)
			self.fileout.write('    <li>Approximations: %d</li>\n' % totalAttemptsApprox)
			self.fileout.write('    <li>Dropped: %d</li>\n' % totalAttemptsDropped)
			self.fileout.write('  </ul>\n')
			self.fileout.write('  <li>Energy: %.1fWh</li>\n' % (totalenergy/3600.0))
			self.fileout.write('  <li>Efficiency (energy/quality): %.1f</li>\n' % ((totalenergy/3600.0)/totalJobQuality))
			self.fileout.write('  <li>Efficiency (energy<sup>2</sup>/quality): %.1f</li>\n' % (pow(totalenergy/3600.0, 2)/totalJobQuality))
			self.fileout.write('  <li>Efficiency (energy/quality<sup>2</sup>): %.1f</li>\n' % (totalenergy/3600.0/pow(totalJobQuality, 2)))
			self.fileout.write('  <li>Time: %s</li>\n' % timeStr(totaltime))
			self.fileout.write('</ul>\n')
			
			# Jobs
			if self.plotJobs:
				self.fileout.write('<h1>Jobs</h1>\n')
				self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
				self.fileout.write('<tr><th width="100px">Id</th><th width="100px">Quality</th><th width="100px">Time</th></tr>\n')
				for job in sorted(jobs.values(), key=attrgetter('finish')):
					self.fileout.write('<tr><td>%s</td><td align="right">%.1f%%</td><td align="right">%d+%ds &nbsp;</td><td>%s</td></tr>\n' %(job.jobId, job.getQuality(), job.start-job.submit, job.finish-job.start, self.getJobGraph(job)))
				self.fileout.write('</table>\n')
			
			# Tasks
			if self.plotTasks:
				self.fileout.write('<h1>Tasks</h1>\n')
				self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
				for attempt in sorted(attempts, key=attrgetter('attemptId')):
					self.fileout.write('<tr><td>'+self.getTaskGraph(attempt)+'</td></tr>\n')
				self.fileout.write('</table>\n')
			
			# Nodes -> Tasks
			if self.plotNodeTasks:
				self.fileout.write('<h1>Node &rarr; Tasks</h1>\n')
				self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
				for nodeId in sorted(nodeAttempts):
					self.fileout.write('<tr><td valign="top">'+nodeId+'</td>')
					self.fileout.write('<td>')
					self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
					for attempt in sorted(nodeAttempts[nodeId], key=attrgetter('start', 'attemptId')):
						self.fileout.write('<tr><td>'+self.getTaskGraph(attempt)+'</td></tr>\n')
					self.fileout.write('</table>\n')
					self.fileout.write('</td>')
					self.fileout.write('</tr>')
				self.fileout.write('</table>\n')
			
			# Nodes
			if self.plotNodes:
				self.fileout.write('<h1>Nodes</h1>\n')
				self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
				for nodeId in sorted(nodeAttempts):
					self.fileout.write('<tr><td valign="top">'+nodeId+'</td>')
					self.fileout.write('<td>')
					self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
					# Create slots
					slots = []
					for attempt in nodeAttempts[nodeId]:
						added = False
						for slot in slots:
							if slot[-1].finish <= attempt.start:
								slot.append(attempt)
								added = True
								break
						if not added:
							# Every slot is full, create a new one
							slots.append([attempt])
					# Draw slots
					for slot in slots:
						self.fileout.write('<tr><td>'+self.getTaskGraphList(slot)+'</td></tr>')
					self.fileout.write('</table>\n')
					self.fileout.write('</td>')
					self.fileout.write('</tr>')
				self.fileout.write('</table>\n')
			
			# Nodes ON/OFF
			if self.plotNodesStatus:
				self.fileout.write('<h1>Nodes status</h1>\n')
				self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
				for nodeId in sorted(nodes):
					self.fileout.write('<tr><td valign="top">'+nodeId+'</td>') # nodes[nodeId]
					self.fileout.write('<td>'+self.getNodeStatusGraphList(nodes[nodeId])+'</td></tr>')
				self.fileout.write('</table>\n')
			
			# Power
			if self.plotPower:
				# Compress data to fit into the screen
				auxpower = []
				granularity=int(ceil(power[-1][0] / 1000.0))
				for i in range(0, len(power)/granularity):
					avgpower = []
					for j in range(i*granularity, (i+1)*granularity):
						avgpower.append(power[j][1])
					avgpower = 1.0*sum(avgpower)/len(avgpower)
					auxpower.append((power[i*granularity][0], avgpower))
				power = auxpower
				# Draw
				self.fileout.write('<h1>Power</h1>\n')
				self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
				self.fileout.write('<tr>')
				for t, p in power:
					self.fileout.write('<td width="1px">')
					self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">')
					self.fileout.write('<tr height="%dpx"><td width="1px"/></tr>'  % (100.0*(maxpower-p)/maxpower))
					self.fileout.write('<tr height="%dpx"><td width="1px" style="background-color:#0000FF"/>' % (100.0*p/maxpower))
					self.fileout.write('</tr></table>')
					self.fileout.write('</td>\n')
				self.fileout.write('<tr>')
				self.fileout.write('</table>\n')
			
			self.fileout.write('</body>\n')
			self.fileout.write('</html>\n')
