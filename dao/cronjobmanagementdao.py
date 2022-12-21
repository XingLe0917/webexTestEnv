from dao.wbxdaomanager import wbxdao
from dao.vo.cronjobmanagementvo import JobInstanceVO, JobManagerInstanceVO, JobTemplateVO
from datetime import datetime, timedelta
from common.wbxutil import wbxutil
from sqlalchemy import literal_column

from sqlalchemy import func, and_, text

class CronjobManagementDao(wbxdao):

    def listJobTemplate(self, job_name):
        session = self.getLocalSession()
        if not wbxutil.isNoneString(job_name):
            templateList = session.query(JobTemplateVO).filter(JobTemplateVO.jobname == job_name).all()
        else:
            templateList = session.query(JobTemplateVO).all()
        return templateList

    def addJobTemplate(self, jobTemplateVO):
        session = self.getLocalSession()
        session.add(jobTemplateVO)

    def getJobTemplate(self, templateid):
        session = self.getLocalSession()
        templatevo = session.query(JobTemplateVO).filter(JobTemplateVO.templateid == templateid).first()
        return templatevo

    def getJobTemplateByName(self, jobname):
        session = self.getLocalSession()
        templatevo = session.query(JobTemplateVO).filter(JobTemplateVO.jobname == jobname).first()
        return templatevo

    # def deleteJobTemplate(self, jobTemplatevo):
    #     session = self.getLocalSession()
    #     session.delete(jobTemplatevo)

    def listJobManagerInstance(self, host_name):
        # page_size = 20
        session = self.getLocalSession()
        if host_name is None or host_name == "":
            # jobManagerList = session.query(JobManagerInstanceVO).limit(page_size).offset((page_index-1)*page_size).all()
            jobManagerList = session.query(JobManagerInstanceVO).order_by(JobManagerInstanceVO.lastupdatetime).all()
        else:
            jobManagerList = session.query(JobManagerInstanceVO).filter(JobManagerInstanceVO.host_name.contains(host_name)).all()
        return jobManagerList

    # Used for job manager instance monitor
    def listFailedJobManagerInstance(self, lastupdateTime):
        session = self.getLocalSession()
        jobManagerList = session.query(JobManagerInstanceVO).filter(and_(JobManagerInstanceVO.lastupdatetime < lastupdateTime, JobManagerInstanceVO.status != "SHUTDOWN")).all()
        return jobManagerList

    def getJobManagerInstance(self, host_name):
        session = self.getLocalSession()
        jobManagerList = session.query(JobManagerInstanceVO).filter(JobManagerInstanceVO.host_name == host_name).all()
        return jobManagerList

    # When add or update or delete job tempalte, it need to notice all job manager instance to reload
    def initializeAllJobManagerInstance(self):
        session = self.getLocalSession()
        SQL = """update wbxjobmanagerinstance set status='PRE_INITIIALIZE',opstatus=1"""
        session.execute(SQL)

    def reloadJobManagerInstancebyTemplateid(self, templateid):
        session = self.getLocalSession()
        SQL = """UPDATE wbxjobmanagerinstance SET status='PRE_RELOAD',opstatus=1 WHERE host_name IN (SELECT host_name FROM wbxjobinstance WHERE templateid='%s')""" % templateid
        session.execute(SQL)

    def deleteJobManagerInstance(self, jobmanagervo):
        session = self.getLocalSession()
        session.delete(jobmanagervo)

    def listJobInstance(self, host_name):
        session = self.getLocalSession()
        jobList = session.query(JobInstanceVO).filter(and_(JobInstanceVO.host_name == host_name, JobInstanceVO.status != 'DELETED')).order_by(text("decode(status,'FAILED',0,1)")).all()
        return jobList

    def listJobInstanceByTemplateid(self, templateid):
        session = self.getLocalSession()
        jobList = session.query(JobInstanceVO).filter(JobInstanceVO.templateid == templateid).all()
        return jobList

    # Used for job instance monitor
    def listFailedJobInstance(self):
        session = self.getLocalSession()
        jobList = session.query(JobInstanceVO).filter(JobInstanceVO.status == "FAILED").all()
        return jobList

    def getJobInstance(self, jobid):
        session = self.getLocalSession()
        jobVO = session.query(JobInstanceVO).filter(JobInstanceVO.jobid == jobid).first()
        return jobVO

    # When deleted job manager instance, it need to delete all job instance
    def deleteJobInstance(self, jobInstanceVO):
        session = self.getLocalSession()
        session.delete(jobInstanceVO)

    def addJobInstance(self, jobInstanceVO):
        session = self.getLocalSession()
        session.add(jobInstanceVO)

    def startJobInstance(self, job_id):
        pass

    def stopJobInstance(self, job_id):
        pass


