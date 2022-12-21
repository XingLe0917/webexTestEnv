import sys
import logging
from common.wbxexception import wbxexception
from common.wbxcache import getTaskFromCache,getLog
from common.wbxutil import wbxutil
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys
from common.wbxtask import wbxautotask
from biz.autotask.shareplexupgradetask import shareplexupgradetask
from biz.autotask.shareplexinstalltask import shareplexinstalltask
from biz.autotask.wbxdbcutover import wbxdbcutovertask
from biz.autotask.wbxdbcutovertask import wbxdbcutovertask
from biz.autotask.tahoebuildtask import tahoebuildtask
from biz.autotask.wbxdbcatask import dbcabuildtask
from biz.autotask.wbxlogmnrtask import wbxlogmnrtask
from biz.autotask.influxdbissuetask import influxdbtask
from biz.autotask.wbxtahoedbcutovertask import wbxtahoedbcutovertask
from biz.autotask.wbxdbaudittask import wbxdbaudittask
from biz.autotask.wbxcidbseperationtask import wbxcidbseperationtask
from biz.autotask.wbxora2pgtask import wbxora2pgtask

tasktype_definition={"SHAREPLEXUPGRADE_TASK":"biz.autotask.shareplexupgradetask.shareplexupgradetask",
                      "SHAREPLEXINSTALL_TASK":"biz.autotask.shareplexinstalltask.shareplexinstalltask",
                     "DBCUTOVER_TASK":"biz.autotask.wbxdbcutovertask.wbxdbcutovertask",
                     "CIDBSEP_TASK":"biz.autotask.wbxcidbseperationtask.wbxcidbseperationtask",
                     "TAHOEDBCUTOVER_TASK":"biz.autotask.wbxtahoedbcutovertask.wbxtahoedbcutovertask",
                     "TAHOEBUILD_TASK":"biz.autotask.tahoebuildtask.tahoebuildtask",
                     "DBCABUILD_TASK":"biz.autotask.wbxdbcatask.dbcabuildtask",
                     "INFLUXDB_ISSUE_TASK":"biz.autotask.influxdbissuetask.influxdbtask",
                     "LOGMNR_TASK":"biz.autotask.wbxlogmnrtask.wbxlogmnrtask",
                     "DBAUDIT_TASK": "biz.autotask.wbxdbaudittask.wbxdbaudittask",
                     "DATASYNCUP_TASK": "biz.autotask.wbxora2pgtask.wbxora2pgtask",}

logger = logging.getLogger("DBAMONITOR")

def biz_autotask_initialize(**kwargs):
    resDict = {"status": "SUCCEED", "errormsg": "", "joblist":None,"add_flag":"1"}
    try:
        if "task_type" not in kwargs:
            raise wbxexception("Do not find TASK_TYPE input parameter")
        taskclz = tasktype_definition[kwargs["task_type"]]
        idxsize = taskclz.rfind(".")
        modulepath = taskclz[0:idxsize]
        clzname = taskclz[idxsize + 1:]
        amodule = sys.modules[modulepath]
        daocls = getattr(amodule, clzname)
        autotask = daocls()
        if "self_heal" in kwargs and kwargs['self_heal'] == "1":
            res2 = autotask.get_last_self_heal_task(kwargs["task_type"],kwargs["db_name"],kwargs["host_name"],kwargs["splex_port"])
            last_vo = res2['last_vo']
            if last_vo and last_vo['status'] !='SUCCEED':
                logger.info("Do not need add autotask_initialize, kwargs={0}".format(kwargs))
                count1,count2 =autotask.update_job_lastmodifiedtime(last_vo['taskid'],last_vo['jobid'])
                logger.info("Update lastmodifiedtime done. wbxautotask_count={0},wbxautotaskjob_count={1} ".format(count1,count2))
                resDict['add_flag'] = "0"
                resDict['last_vo'] = last_vo
                return resDict
        taskvo = autotask.initialize(**kwargs)
        joblist = autotask.listTaskJobsByTaskid(taskvo.taskid)
        ls = []
        for job in joblist:
            ls.append(job.to_dict())
        resDict["joblist"] = ls
    except Exception as e:
        resDict["status"] = "FAILED"
        resDict["errormsg"] = str(e)
        logger.error("biz_autotask_initialize met error %s" % (str(e)), exc_info=e)
    return resDict

# def biz_autotask_preverify(taskid, jobid):
#     resDict = {"status": "SUCCEED", "resultmsg": "", "jobid": None}
#     try:
#         autotask = getTaskByTaskid(taskid)
#         if autotask is None:
#             raise wbxexception("DONOT get this task in db")
#         jobvo = autotask.getTaskJobByJobid(jobid)
#         if jobvo is None:
#             raise wbxexception("DONOT get this job in db")
#         autotask.startJob(jobid, autotask.preverify)
#         resDict["jobid"] = jobid
#     except Exception as e:
#         resDict["status"] = "FAILED"
#         resDict["resultmsg"] = str(e)
#     return resDict

def biz_autotask_preexecutejob(taskType,taskid,jobid):
    resDict = {"status": "SUCCEED", "resultmsg": "", "jobid":jobid}
    autotask = getTaskByTaskid(taskid)
    jobvo = autotask.getTaskJobByJobid(jobid)
    if jobvo is None:
        raise wbxexception("DONOT get this job with jobid=%s in db" % jobid)
    try:
        wbxautotask(taskid,taskType).updateJobStatus(jobid,"RUNNING")
    except Exception as e:
        resDict["status"]="FAILED"
        resDict["resultmsg"]=str(e)
    return resDict

def biz_autotask_executejob(taskid, jobid):
    resDict = {"status": "SUCCEED", "resultmsg": "", "jobid": None}
    try:
        autotask = getTaskByTaskid(taskid)
        jobvo = autotask.getTaskJobByJobid(jobid)
        if jobvo is None:
            raise wbxexception("DONOT get this job with jobid=%s in db" % jobid)
        if jobvo.execute_method == "SYNC":
            autotask.startJob(jobvo)
        elif jobvo.execute_method == "ASYNC":
            autotask.startJobAsync(jobvo)
        resDict["jobid"] = jobid
    except Exception as e:
        resDict["status"] = "FAILED"
        resDict["resultmsg"] = str(e)
        logger.error("biz_autotask_executejob(jobid=%s) met error %s" % (jobid, str(e)), exc_info=e)
    return resDict

def getTaskByTaskid(taskid):
    autotask = getTaskFromCache(taskid)
    if autotask is None:
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        # daomanager = daoManagerFactory.getDaoManager("RACAAWEB")
        dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        # dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            daomanager.startTransaction()
            taskvo = dao.getAutoTaskByTaskid(taskid)
            daomanager.commit()
            if taskvo is None:
                raise wbxexception("DONOT get this task in db")
            taskclz = tasktype_definition[taskvo.task_type]
            idxsize = taskclz.rfind(".")
            modulepath = taskclz[0:idxsize]
            clzname = taskclz[idxsize + 1:]
            amodule = sys.modules[modulepath]
            daocls = getattr(amodule, clzname)
            autotask = daocls(taskid)
            autotask.initialize(**taskvo.parameter)
        except Exception as e:
            daomanager.rollback()
            raise e
        finally:
            daomanager.close()
    return autotask


def biz_autotask_listJobsByTaskid(taskid):
    logger.info("biz_autotask_listJobsByTaskid(taskid=%s) start" % taskid)
    resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
    try:
        autotask = getTaskByTaskid(taskid)
        jobvoList = autotask.listTaskJobsByTaskid(taskid)
        jobvo = autotask.getTaskJobByTaskid(taskid)
        vo = {}
        dd = jobvo.to_dict()
        parameter = {}
        for key in dd:
            if key == 'taskid' or key == 'task_type' or key == 'createtime' or key == 'lastmodifiedtime':
                vo[key] = dd[key]
            else:
                parameter[key] = dd[key]
        vo['parameter'] = parameter
        resDict["task"] = vo

        jobvoList_new = []
        for job in jobvoList:
            jobvoList_new.append(job.to_dict())
        resDict["data"] = jobvoList_new

    except Exception as e:
        resDict["status"] = "FAILED"
        resDict["resultmsg"] = str(e)
        logger.error("biz_autotask_listJobsByTaskid(taskid=%s) met error %s" %(taskid, str(e)), exc_info = e)
    return dict(resDict)

def biz_autotask_listtasks(task_type):
    logger.info("biz_autotask_listtasks(task_type=%s) start" % task_type)
    resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = None
    try:
        daoManager = daoManagerFactory.getDefaultDaoManager()
        dao = daoManager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        daoManager.startTransaction()
        # if task_type == "DBCUTOVER_TASK":
        taskvolist = dao.getAutoTaskByTasktype(task_type)
        # else:
        #     taskvolist = dao.listAutoTaskByTasktype(task_type)
        daoManager.commit()
        ls = []
        for task in taskvolist:
            vo = {}
            dd = dict(task)
            # dd = task.to_dict()
            parameter = {}
            for key in dd:
                if key in ["taskid","task_type","createtime","lastmodifiedtime","status","createby"]:
                # if key=='taskid' or key=='task_type' or key=='createtime' or key=='lastmodifiedtime' or key=='status':
                  vo[key]=dd[key]
                else:
                    parameter[key]=dd[key]
            vo['parameter']=parameter
            ls.append(vo)
        resDict["data"] = ls
    except Exception as e:
        if daoManager is not None:
            daoManager.rollback()
        resDict["status"] = "FAILED"
        resDict["resultmsg"] = str(e)
        logger.error("biz_autotask_listtasks(task_type=%s) met error %s" % (task_type, str(e)), exc_info=e)
    finally:
        if daoManager is not None:
            daoManager.close()
    return resDict

def props(obj):
    pr = {}
    for name in dir(obj):
        value = getattr(obj, name)
        if not name.startswith('__') and not name.startswith('_') and not callable(value):
            pr[name] = value
    return pr

def biz_autotask_getjoblog(jobid):
    logger.info("biz_autotask_getjoblog(jobid=%s) start" % jobid)
    resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
    logs = getLog(jobid)
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    dao = daoManager.getDao(DaoKeys.DAO_AUTOTASKDAO)
    try:
        daoManager.startTransaction()
        jobvo = dao.getAutoTaskJobByJobid(jobid)
        jobstatus = ""
        if jobvo is not None:
            if wbxutil.isNoneString(logs):
                logs = "%s%s%s" % (jobvo.resultmsg1, jobvo.resultmsg2, jobvo.resultmsg3)
            jobstatus = jobvo.status
        daoManager.commit()
    except Exception as e:
        if daoManager is not None:
            daoManager.rollback()
        resDict["status"] = "FAILED"
        resDict["resultmsg"] = str(e)
        logger.error("biz_autotask_getjoblog(jobid=%s) met error %s" % (jobid, str(e)), exc_info=e)
    finally:
        if daoManager is not None:
            daoManager.close()

    resDict["data"] = logs
    resDict["jobstatus"] = jobstatus
    return resDict

def biz_autotask_exeoneclick(taskid):
    logger.info("biz_autotask_exeoneclick(taskid=%s) start" % taskid)
    resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
    task = biz_autotask_listJobsByTaskid(taskid)
    if task['status'] == 'SUCCEED':
        data = task['data']
        taskinfo=task['task']
        for job in data:
            jobid = job["jobid"]
            if job["status"] in ["PENDING" ,"FAILED"] :
                logger.info("check whether job: %s can oneclick" %jobid )
                isoneclick=job["isoneclick"] if "isoneclick" in job.keys() else False
                if isoneclick:
                    logger.info("step %s for jobid %s job start ..."%(job["process_order"],jobid))
                    try:
                        res=biz_autotask_preexecutejob(taskinfo["task_type"],taskid,jobid)
                        if res["status"] != "SUCCEED":
                            raise wbxexception(res["resultmsg"])
                        res = biz_autotask_executejob(taskid, jobid)
                        job_info = biz_autotask_getjoblog(jobid)
                        if job_info["jobstatus"] != "SUCCEED":
                            raise wbxexception(res["resultmsg"])
                        logger.info("step %s for jobid %s end with %s." % (job["process_order"], jobid,job_info["jobstatus"]))
                    except Exception as e:
                        resDict["status"] = "FAILED"
                        resDict["resultmsg"] = str(e)
                        logger.error("biz_autotask_exeoneclick(taskid=%s,jobid=%s) Error occurred with Error:%s" % (taskid,jobid,str(e)))
                        break
                else:
                    resDict["status"] = "FAILED"
                    resDict["resultmsg"] = "job: %s can not oneclick ,exit..." %jobid
                    logger.info("job: %s can not oneclick ,exit..." % jobid)
                    break
            else:
                logger.info("step %s with jobid %s job have done. Skip..." %(job["process_order"],job["job_action"]))
    else:
        resDict["status"] = "FAILED"
        resDict["resultmsg"] = "biz_autotask_listJobsByTaskid(taskid=%s) met error" % (taskid)
    return resDict

def biz_autotask_updjobstatus(taskid,jobid,status):
    resDict = {"status": "SUCCEED", "resultmsg": "", "jobid": None}
    try:
        autotask = getTaskByTaskid(taskid)
        autotask.updateJobStatus(jobid,status)
    except Exception as e:
        resDict["status"] = "FAILED"
        resDict["resultmsg"] = str(e)
        logger.error("biz_autotask_updjobstatus(jobid=%s) met error %s" % (jobid, str(e)), exc_info=e)
    return resDict

def biz_autotask_tahoebuild(taskid):
    logger.info("biz_autotask_tahoebuild(taskid=%s) start" % taskid)
    resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
    task = biz_autotask_listJobsByTaskid(taskid)
    if task['status'] =='SUCCEED':
        data = task['data']
        for job in data:
            jobid = job['jobid']
            if job['status'] == "PENDING" or job['status'] == "FAILED" :
                logger.info("step {0} {1} job start".format(job['process_order'],job['job_action']))
                res = None
                try:
                    res = biz_autotask_executejob(taskid, jobid)
                    job_info = biz_autotask_getjoblog(jobid)
                    if job_info["jobstatus"] != "SUCCEED":
                        resDict["status"] = "FAILED"
                        resDict["resultmsg"] = job_info["resultmsg"]
                        logger.error("{0} fail, tahoe build break. " .format(job['job_action']))
                        break
                except Exception as e:
                    logger.error("{0} fail, tahoe build break. " .format(job['job_action']))
                    resDict["status"] = "FAILED"
                    resDict["resultmsg"] = res["resultmsg"]
                    logger.error(str(e))
                    logger.error("biz_autotask_tahoebuild(taskid=%s) met error %s" % (taskid, resDict["resultmsg"]),
                                 exc_info=resDict["resultmsg"])
                    break
            else:
                logger.info("step {0} {1} job have done. Skip it.".format(job['process_order'],job['job_action']))
    else:
        resDict["status"] = "FAILED"
        resDict["resultmsg"] = "biz_autotask_listJobsByTaskid(taskid=%s) met error" %(taskid)
    return resDict

def getSelfHealingJobList():
    logger.info("Call API: getSelfHealingJobList")
    resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    dao = daoManager.getDao(DaoKeys.DAO_AUTOTASKDAO)
    try:
        daoManager.startTransaction()
        joblist = dao.getSelfHealingList()
        resDict['data'] = [dict(vo) for vo in joblist]
        daoManager.commit()
    except Exception as e:
        if daoManager is not None:
            daoManager.rollback()
        resDict["status"] = "FAILED"
        resDict["resultmsg"] = str(e)
        logger.error("getSelfHealingJobList met error %s" % (str(e)), exc_info=e)
    finally:
        if daoManager is not None:
            daoManager.close()
    return resDict

def get_DBMemoSize(db_name):
    res = {
        "status": "SUCCEED",
        "data": None
    }
    try:
        db_name=db_name.upper()
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        dbvo = daoManagerFactory.getDatabaseByDBName(db_name)
        if not dbvo:
            return res
        data = dbvo.getDBMemoSize()
        res["data"]=data
    except Exception as e:
        res["status"] = "FAILED"
        res["data"] = str(e)
    return res

