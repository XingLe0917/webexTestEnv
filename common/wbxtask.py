import uuid
import threading
from datetime import datetime
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys
from common.wbxexception import WbxDaoException, wbxexception
# from common.wbxthreadpool import autotaskpool
from common.wbxcache import getLog,removeLog, addTaskToCache, getTaskFromCache
from dao.vo.autotaskvo import wbxautotaskvo, wbxautotaskjobvo
import json
import logging
import threading

threadlocal = threading.local()
logger = logging.getLogger("DBAMONITOR")

# This is the parent class for all automation task
# If reload the task from db, then taskid is not None, but for new task, taskid is None
class wbxautotask:
    def __init__(self, taskid, taskType):
        if taskid is None:
            self._taskid = uuid.uuid4().hex
        else:
            self._taskid = taskid
        self._taskType = taskType

    def getTaskid(self):
        return self._taskid

    # repeattask means whether the same task can be scheduled multiple times
    # initialize() function of child class also do some precheck, but only check input parameter legality, but not check logic
    def initialize(self, repeattask = False, **kwargs):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDefaultDaoManager()
        dao = daoManager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        try:
            daoManager.startTransaction()
            taskvo = dao.getAutoTaskByTaskid(self._taskid)
            # parameter = json.dumps(kwargs)
            if taskvo is None:
                if not repeattask:
                    taskvo = dao.getAutoTaskByParameter(self._taskType, kwargs)
                    if taskvo is not None:
                        raise wbxexception("The same task already executed before")
                if "self_heal" in kwargs and kwargs['self_heal']:
                    taskvo = wbxautotaskvo(taskid=self._taskid, task_type=self._taskType, parameter=kwargs,self_heal=kwargs['self_heal'],createby=kwargs['createby'])
                else:
                    taskvo = wbxautotaskvo(taskid=self._taskid, task_type=self._taskType, parameter=kwargs,createby=kwargs['createby'])
                dao.addAutoTask(taskvo)
            daoManager.commit()
            addTaskToCache(self._taskid, self)
            return taskvo
        except Exception as e:
            daoManager.rollback()
            raise e
        finally:
            daoManager.close()

    def preverify(self,*args):
        pass

    def initializeFromDB(self, taskid):
        taskvo = self.getTaskByTaskid(taskid)
        if taskvo is None:
            raise wbxexception("Can not get the %s task log with taskid=%s" % (self._taskType, taskid))
        self._taskid = taskid
        self._taskType = taskvo.task_type
        self.initialize(**taskvo.parameter)
        return taskvo

    def getTaskByTaskid(self, taskid):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        try:
            daomanager.startTransaction()
            taskvo = dao.getAutoTaskByTaskid(taskid)
            daomanager.commit()
            return taskvo
        except Exception as e:
            daomanager.rollback()
        finally:
            daomanager.close()
        return None

    def listTaskJobsByTaskid(self, taskid):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        try:
            daomanager.startTransaction()
            jobvoList = dao.getAutoTaskJobByTaskid(taskid)
            daomanager.commit()
            return jobvoList
        except Exception as e:
            daomanager.rollback()
            raise e
        finally:
            daomanager.close()

    def getTaskJobByTaskid(self,taskid):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        try:
            daomanager.startTransaction()
            taskvo = dao.getAutoTaskByTaskid(taskid)
            daomanager.commit()
            return taskvo
        except Exception as e:
            daomanager.rollback()
            raise e
        finally:
            daomanager.close()

    def getTaskJobByJobid(self, jobid):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        try:
            daomanager.startTransaction()
            jobvo = dao.getAutoTaskJobByJobid(jobid)
            daomanager.commit()
            return jobvo
        except Exception as e:
            daomanager.rollback()
            raise e
        finally:
            daomanager.close()

    def getJobID(self):
        return uuid.uuid4().hex

    # the jobid may also be an id generated by other code
    def addJob(self, jobid = None, **kwargs):
        if jobid is None:
            jobid = self.getJobID()
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDefaultDaoManager()
        dao = daoManager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        try:
            jobvo = wbxautotaskjobvo(jobid=jobid,
                                     taskid=self._taskid,
                                     status="PENDING",
                                     parameter = kwargs)

            paramdict = {}
            if kwargs is not None and len(kwargs) > 0:
                for k,v in kwargs.items():
                    if k == "host_name":
                        jobvo.host_name = v
                    elif k == "splex_port":
                        jobvo.splex_port = v
                    elif k == "db_name":
                        jobvo.db_name = v
                    elif k == "job_action":
                        jobvo.job_action = v
                    elif k == "process_order":
                        jobvo.processorder = v
                    elif k == "execute_method":
                        jobvo.execute_method = v

            daoManager.startTransaction()
            dao.addAutoTaskJob(jobvo)
            daoManager.commit()
            return jobvo
        except Exception as e:
            daoManager.rollback()
            logger.error("wbxtask.addJob() with errormsg:%s"% e, exc_info = e)
            raise e
        finally:
            daoManager.close()

    # if the job log not in cache,then get from DB
    def getJobLog(self, jobid):
        resultmsg = getLog(jobid)
        if resultmsg is None:
            daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
            depotDaoManager = daoManagerFactory.getDefaultDaoManager()
            depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            try:
                depotDaoManager.startTransaction()
                taskvo = depotdbDao.geOperationTask(jobid)
                resultmsg="%s%s%s" %(taskvo.resultmsg1, taskvo.resultmsg2, taskvo.resultmsg3)
                depotDaoManager.commit()
            except Exception as e:
                depotDaoManager.rollback()
                raise e
            finally:
                depotDaoManager.close()
        return resultmsg

    def startJob(self, jobvo):
        func = getattr(self, jobvo.job_action)
        func(jobvo.jobid)

    def startJobAsync(self,jobvo):
        func = getattr(self, jobvo.job_action)
        t = threading.Thread(target=func, args=(jobvo.jobid,))
        t.start()

    def generateJobs(self):
        pass

    def updateJobStatus(self, jobid, status):
        logger.info("Update job status to be %s in depotdb with jobid=%s" %(status, jobid))
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        try:
            daomanager.startTransaction()
            jobvo = dao.getAutoTaskJobByJobid(jobid)
            if jobvo is not None:
                if jobvo.status in ('SUCCEED'):
                    raise wbxexception("This job has been executed successfully. Can not execute again")
                resultmsg = None
                if status in ('SUCCEED','FAILED','SKIP'):
                    jobvo.end_time = datetime.now()
                    # Do not remove below log, it will add the summarized line to the output log
                    logger.info("The automation task %s job %s %s" % (self._taskType, jobvo.job_action, status))
                    resultmsg = getLog(jobid)
                    threadlocal.current_jobid = None
                elif status == "RUNNING":
                    jobvo.start_time = datetime.now()
                    jobvo.resultmsg1=None
                    jobvo.resultmsg2=None
                    jobvo.resultmsg3=None
                    if jobvo.status in ("FAILED", "RUNNING"):
                        removeLog(jobid)
                    threadlocal.current_jobid = jobid

                if resultmsg is not None and resultmsg != "":
                    colwidth = 3900
                    resList = [resultmsg[x - colwidth:x] for x in range(colwidth, len(resultmsg) + colwidth, colwidth)]
                    logger.info("log resList:{0}".format(len(resList)))
                    jobvo.resultmsg1 = resList[0]
                    if len(resList) > 1:
                        jobvo.resultmsg2 = resList[1]
                    if len(resList) > 2:
                        jobvo.resultmsg3 = resList[-1]
                jobvo.status = status
            daomanager.commit()
            return jobvo
        except Exception as e:
            daomanager.rollback()
            raise e
        finally:
            daomanager.close()

    # For self-healing task, only when the last execution is successful, task with the same parameters can be added.
    def get_last_self_heal_task(self,task_type,db_name,host_name,splex_port):
        logger.info("Determine whether you need to add a task, task_type=%s,db_name=%s,host_name=%s,splex_port=%s" % (task_type,db_name,host_name,splex_port))
        resDict = {"status": "SUCCEED", "last_vo": None}
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = None
        try:
            daoManager = daoManagerFactory.getDefaultDaoManager()
            dao = daoManager.getDao(DaoKeys.DAO_AUTOTASKDAO)
            daoManager.startTransaction()
            taskvolist = dao.getlastjob(task_type,db_name,host_name,splex_port)
            if len(taskvolist) == 0 :
                return resDict
            else:
                resDict['last_vo'] = dict(taskvolist[0])
            daoManager.commit()
        except Exception as e:
            if daoManager is not None:
                daoManager.rollback()
                raise e
        return resDict

    def update_job_lastmodifiedtime(self,taskid,jobid):
        logger.info("update_job_lastmodifiedtime, taskid=%s,jobid=%s" % (taskid,jobid))
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = None
        try:
            daoManager = daoManagerFactory.getDefaultDaoManager()
            dao = daoManager.getDao(DaoKeys.DAO_AUTOTASKDAO)
            daoManager.startTransaction()
            count1 = dao.update_wbxautotask_lastmodifiedtime(taskid)
            count2 = dao.update_wbxautotaskjob_lastmodifiedtime(jobid)
            daoManager.commit()
            return count1,count2
        except Exception as e:
            if daoManager is not None:
                daoManager.rollback()




