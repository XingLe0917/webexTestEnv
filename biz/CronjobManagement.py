import logging

from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from dao.vo.cronjobmanagementvo import JobManagerInstanceVO, JobTemplateVO, JobInstanceVO
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from sqlalchemy.exc import IntegrityError
from common.wbxssh import wbxssh

logger = logging.getLogger("DBAMONITOR")
def listJobTemplate(job_name):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        templatelist = dao.listJobTemplate(job_name)
        templatedict = [templatevo.to_dict() for templatevo in templatelist]
        daoManager.commit()
        return templatedict
    except Exception as e:
        daoManager.rollback()
        logger.error("listJobTemplate error occurred", exc_info = e, stack_info = True)
    return None

def addJobTemplate(templatejson):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        templatevo = JobTemplateVO.loadFromJson(templatejson)
        if templatevo.appln_support_code is not None:
            for appln_support_code in templatevo.appln_support_code.split(","):
                if appln_support_code not in ('WEB','CONFIG','TEL','TEO','OPDB','MEDIATE','LOOKUP','MMP','TOOLS','MON_GRID','DIAGNS','CI','CSP','CALENDAR'):
                    raise wbxexception("invalid value of appln_support_code")
        if templatevo.job_level is not None:
            if templatevo.job_level not in ('SERVER', 'RAC', 'INSTANCE', 'DATABASE', 'SHAREPLEXPORT','SRC_SHAREPLEXPORT','TGT_SHAREPLEXPORT'):
                raise wbxexception("invalid value of job_level")
        if templatevo.application_type is not None:
            if templatevo.application_type not in ('PRI', 'GSB'):
                raise wbxexception("invalid value of application_type")
        if templatevo.job_type != "CRON":
            raise wbxexception("invalid value of job_type")

        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        dao.addJobTemplate(templatevo)
        dao.initializeAllJobManagerInstance()
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        logger.error("addJobTemplate error occurred", exc_info=e, stack_info=True)
        raise e

def deleteJobTemplate(templateid):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        templatevo = dao.getJobTemplate(templateid)
        if templatevo is not None:
            templatevo.status='DELETED'
        joblist = dao.listJobInstanceByTemplateid(templatevo.templateid)
        for job in joblist:
            job.status = "DELETED"
        dao.reloadJobManagerInstancebyTemplateid(templatevo.templateid)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        logger.error("addJobTemplate error occurred", exc_info=e, stack_info=True)
        raise e

def listJobManagerInstance(host_name):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        if not host_name:
            dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            jobmanagerdict = dao.listJobManagerInstance()
            daoManager.commit()
        else:
            dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
            jobmanagerList = dao.listJobManagerInstance(host_name)
            daoManager.commit()
            jobmanagerdict = []
            curtime = wbxutil.getcurrenttime(180)
            for jobManagervo in jobmanagerList:
                # print(jobManagervo.finalstatus)
                # if jobManagervo.lastupdatetime < curtime:
                #     jobManagervo.status='SHUTDOWN'
                jobManagervo.status = jobManagervo.finalstatus
                jobmanagerdict.append(jobManagervo.to_dict())
        return jobmanagerdict
    except Exception as e:
        daoManager.rollback()
        logger.error("listJobManagerInstance error occurred with error %s" % str(e), exc_info=e, stack_info=True)
        return None
    finally:
        daoManager.close()

def shutdownJobmanagerInstance(host_name):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        jobmanagerlist = dao.getJobManagerInstance(host_name)
        if len(jobmanagerlist) > 0:
            jobmanagervo = jobmanagerlist[0]
            jobmanagervo.status = "PRE_SHUTDOWN"
            jobmanagervo.opstatus = 1
        daoManager.commit()
        logger.info("shutdown metricAgent on server %s" % host_name)
    except Exception as e:
        daoManager.rollback()
        logger.error("shutdownJobmanagerInstance error occurred", exc_info=e, stack_info=True)
        raise e

def startJobmanagerInstance(host_name):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daomanager = daoManagerFactory.getDefaultDaoManager()
    try:
        # Add below code to avoid the case that start jobmanager several times
        try:
            daomanager.startTransaction()
            dao = daomanager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
            instancelist = dao.getJobManagerInstance(host_name)
            if len(instancelist) > 0:
                jobManagervo = instancelist[0]
                if jobManagervo.status == 'PRE_START':
                    raise wbxexception("Job Manager is already at STARTING status, Please wait 1 minutes and refresh page to check the status again")
                jobManagervo.status = "PRE_START"
                jobManagervo.opstatus = 0
            daomanager.commit()
        except Exception as e:
            daomanager.rollback()
            raise e

        server = daoManagerFactory.getServer(host_name)
        if server is not None:
            if server.getLoginpwd() is None:
                raise wbxexception("Can not get login user info from DepotDB")
            # ssh = wbxssh(server.host_name, server.ssh_port, server.loginuser.username, server.loginuser.pwd)
            server.connect()
            res = server.exec_command("ps aux | grep python | grep jobManager | grep -v grep | wc -l")
            if res.isdigit():
                ires = int(res)
                if ires > 0:
                    server.exec_command("ps aux | grep python | grep jobManager | grep -v grep | awk '{print $2}' | xargs kill -9")

                logger.info("The metricAgent is NOT running on server %s, start it" % host_name)
                # ssh.exec_command("rm /home/oracle/wbxjobmanager/biz/*.pyc; rm /home/oracle/wbxjobmanager/common/*.pyc")
                # ssh.exec_command("nohup python /home/oracle/wbxjobmanager/jobmanagerserver.py > /dev/null 1>&2 &")
                server.invoke_shell("nohup python /usr/local/wbxjobmanager/jobManagerAgent.py > /dev/null 2>&1 &")
    except Exception as e:
        logger.error("startJobmanagerInstance error occurred", exc_info=e, stack_info=True)
        raise e

def deleteJobmanagerInstance(host_name):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        jobmanagerlist = dao.getJobManagerInstance(host_name)
        jobInstanceList = dao.listJobInstance(host_name)
        for jobvo in jobInstanceList:
            dao.deleteJobInstance(jobvo)
        if len(jobmanagerlist) > 0:
            jobmanagervo = jobmanagerlist[0]
            dao.deleteJobManagerInstance(jobmanagervo)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        logger.error("deleteJobmanagerInstance error occurred", exc_info=e, stack_info=True)
        raise e

# Used for job manager instance monitor
def monitorFailedJobManagerInstance():
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        lastupdateTime = wbxutil.getcurrenttime(180)
        jobmanagerList = dao.listFailedJobManagerInstance(lastupdateTime)
        daoManager.commit()
        for jobmanagervo in jobmanagerList:
            host_name = jobmanagervo.host_name
            server = wbxdaomanagerfactory.getServer(host_name)
    except Exception as e:
        daoManager.rollback()
        logger.error("startJobmanagerInstance error occurred", exc_info=e, stack_info=True)
        raise e

def listJobInstance(host_name):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        jobInstanceList = dao.listJobInstance(host_name)
        daoManager.commit()
        instancedict = [instancevo.to_dict() for instancevo in jobInstanceList]
        return instancedict
    except Exception as e:
        daoManager.rollback()
        logger.error("listJobInstance error occurred", exc_info=e, stack_info=True)
        raise e

# Used for job instance monitor
def listFailedJobInstance(self):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        jobInstanceList = dao.listFailedJobInstance()
        daoManager.commit()
        return jobInstanceList
    except Exception as e:
        daoManager.rollback()
        logger.error("listFailedJobInstance error occurred", exc_info=e, stack_info=True)
        raise e

def deleteJobInstance(jobid):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        jobInstancevo = dao.getJobInstance(jobid)
        if jobInstancevo is not None:
            jobInstancevo.status="DELETED"
            jobmanagerlist = dao.getJobManagerInstance(jobInstancevo.host_name)
            if len(jobmanagerlist) > 0:
                jobmanagervo = jobmanagerlist[0]
                jobmanagervo.status = "PRE_RELOAD"
                jobmanagervo.opstatus = 1
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        logger.error("deleteJobInstance error occurred", exc_info=e, stack_info=True)
        raise e

def addJobInstance(jsondata):
    try:
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDefaultDaoManager()
        jobInstancevo = JobInstanceVO.loadFromJson(jsondata)
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        jobmanagerlist = dao.getJobManagerInstance(jobInstancevo.host_name)
        dao.addJobInstance(jobInstancevo)
        if len(jobmanagerlist) > 0:
            jobmanagervo = jobmanagerlist[0]
            jobmanagervo.status = "PRE_RELOAD"
            jobmanagervo.opstatus = 1
        daoManager.commit()
    except IntegrityError as e:
        daoManager.rollback()
        raise e
    except Exception as e:
        daoManager.rollback()
        logger.error("addJobInstance error occurred", exc_info=e, stack_info=True)
        raise e

def updateJobInstance(jsondata):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        jobInstancevo = JobInstanceVO.loadFromJson(jsondata)
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        jobvo = dao.getJobInstance(jobInstancevo.jobid)
        if jobvo is not None:
            jobvo.jobruntime = jobInstancevo.jobruntime
            jobvo.status = "RESCHEDULE"
            if jobvo.templateid is None:
                jobvo.jobname = jobInstancevo.jobname
                jobvo.commandstr = jobInstancevo.commandstr

            jobmanagerlist = dao.getJobManagerInstance(jobInstancevo.host_name)
            if len(jobmanagerlist) > 0:
                jobmanagervo = jobmanagerlist[0]
                jobmanagervo.status = "PRE_RELOAD"
                jobmanagervo.opstatus = 1
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        logger.error("addJobInstance error occurred", exc_info=e, stack_info=True)
        raise e

def pauseJobInstance(jobid):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        jobvo = dao.getJobInstance(jobid)
        jobvo.status = "PAUSE"
        jobmanagerlist = dao.getJobManagerInstance(jobvo.host_name)
        if len(jobmanagerlist) > 0:
            jobmanagervo = jobmanagerlist[0]
            jobmanagervo.status = "PRE_RELOAD"
            jobmanagervo.opstatus = 1
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        logger.error("pauseJobInstance error occurred", exc_info=e, stack_info=True)
        raise e

def resumeJobInstance(jobid):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_CRONJOBMANAGEMENTDAO)
        jobvo = dao.getJobInstance(jobid)
        jobvo.status = "RESUME"
        jobmanagerlist = dao.getJobManagerInstance(jobvo.host_name)
        if len(jobmanagerlist) > 0:
            jobmanagervo = jobmanagerlist[0]
            jobmanagervo.status = "PRE_RELOAD"
            jobmanagervo.opstatus = 1
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        logger.error("resumeJobInstance error occurred", exc_info=e, stack_info=True)
        raise e
