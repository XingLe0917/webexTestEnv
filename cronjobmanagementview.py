from flask import jsonify, request
from view import app
import json
from common.wbxutil import wbxutil
from biz.CronjobManagement import listJobTemplate,addJobTemplate,deleteJobTemplate,listJobManagerInstance,shutdownJobmanagerInstance
from biz.CronjobManagement import startJobmanagerInstance,deleteJobmanagerInstance, listJobInstance,deleteJobInstance,addJobInstance, pauseJobInstance, resumeJobInstance

@app.route('/api/listjobtemplate', methods=['POST'])
def view_listJobTemplate():
	templatedict = listJobTemplate()
	return jsonify(templatedict)

@app.route('/api/addjobtemplate', methods=['POST'])
def view_addJobTemplate():
	try:
		templatedata = request.json["data"]
		addJobTemplate(templatedata)
		return {"result":"SUCCESS","msg":None}
	except Exception as e:
		return {"result": "FAILED", "msg":e.msg}

@app.route('/api/deletejobtemplate', methods=['POST'])
def view_deleteJobTemplate():
	templateid = request.json["templateid"]
	deleteJobTemplate(templateid)
	return {"result":"SUCCESS","msg":None}

@app.route('/api/listjobmanagerinstance', methods=['POST'])
def view_listJobManagerInstance():
	jsondata = request.json["data"].strip()
	paramdict = json.loads(jsondata)
	page_index = 0
	host_name = None
	if "page_index" in paramdict:
		page_index = paramdict["page_index"]
	if "host_name" in paramdict:
		host_name = paramdict["host_name"]
	jobManagerDict = listJobManagerInstance(host_name, page_index)
	return jsonify(jobManagerDict)

@app.route('/api/shutdownjobmanagerinstance', methods=['POST'])
def view_shutdownJobmanagerInstance():
	jsondata = request.json["data"].strip()
	paramdict = json.loads(jsondata)
	if "host_name" in paramdict:
		host_name = paramdict["host_name"]
		shutdownJobmanagerInstance(host_name)

@app.route('/api/startjobmanagerinstance', methods=['POST'])
def view_startJobmanagerInstance():
	paramdict = int(request.json["data"].strip())
	if "host_name" in paramdict:
		host_name = paramdict["host_name"]
		startJobmanagerInstance(host_name)

@app.route('/api/deletejobmanagerinstance', methods=['POST'])
def view_deleteJobmanagerInstance():
	jsondata = request.json["data"].strip()
	paramdict = json.loads(jsondata)
	if "host_name" in paramdict:
		host_name = paramdict["host_name"]
		deleteJobmanagerInstance(host_name)

@app.route('/api/listjobinstance', methods=['POST'])
def view_listJobInstance():
	host_name = request.json["host_name"].strip()
	jobInstanceList = listJobInstance(host_name)
	jobinstancedict = [jobinstancevo.to_dict() for jobinstancevo in jobInstanceList]
	return jsonify(jobinstancedict)

@app.route('/api/deletejobinstance', methods=['POST'])
def view_deleteJobInstance():
	jsondata = request.json["data"].strip()
	paramdict = json.loads(jsondata)
	if "jobid" in paramdict:
		deleteJobInstance(paramdict["jobid"])
	return {"result": "SUCCESS"}

@app.route('/api/addjobinstance', methods=['POST'])
def view_addJobInstance():
	jsondata = request.json["data"].strip()
	addJobInstance(jsondata)
	return {"result": "SUCCESS"}

@app.route('/api/pausejobinstance', methods=['POST'])
def view_pauseJobInstance():
	jsondata = request.json["data"].strip()
	paramdict = json.loads(jsondata)
	if "jobid" in paramdict:
		jobid = paramdict["jobid"]
	if "host_name" in paramdict:
		host_name = paramdict["host_name"]
	pauseJobInstance(jobid, host_name)
	return {"result": "SUCCESS"}

@app.route('/api/resumejobinstance', methods=['POST'])
def view_resumeJobInstance():
	jsondata = request.json["data"].strip()
	paramdict = json.loads(jsondata)
	if "jobid" in paramdict:
		jobid = paramdict["jobid"]
	if "host_name" in paramdict:
		host_name = paramdict["host_name"]
	pauseJobInstance(jobid, host_name)
	return {"result": "SUCCESS"}
