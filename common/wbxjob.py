import logging
import time
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from apscheduler.executors.pool import ThreadPoolExecutor as APThreadPoolExecutor

from common.wbxexception import wbxexception
from common.wbxthreadpool import wbxthreadpool
from dao.vo.depotdbvo import wbxdatabasemanager
import json

from dao.wbxdaomanager import wbxdaomanagerfactory, wbxdaomanager, DaoKeys
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("DBAMONITOR")

class wbxjobmanager:
    jobmanager = None

    def __init__(self):
        executors = {'processpool': ProcessPoolExecutor(4)}
        job_default = {
            'coalesce': True,
            'max_instances': 10
        }
        self.scheduler = BackgroundScheduler(gconfig={"apscheduler.daemon":False}, executors=executors, job_default=job_default)
        self.jobdict = {}
        self.jobtimes = 0

    @staticmethod
    def getJobManager():
        if wbxjobmanager.jobmanager is None:
            wbxjobmanager.jobmanager = wbxjobmanager()
        return wbxjobmanager.jobmanager

    def start(self, poolsize):
        logger.info("wbxjobmanager.start(%d)" % poolsize)
        # self.poolsize = poolsize
        # self.threadpool = wbxthreadpool.getThreadPool()
        self.loadjobs()

    def loadjobs(self):
        if len(self.jobdict) < 1:
            daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
            daoManager = daoManagerFactory.getDefaultDaoManager()
            daoManager.startTransaction()
            jobDao = daoManager.getDao(DaoKeys.DAO_JOBMANAGERDAO)
            jobList = jobDao.searchJobs()
            daoManager.commit()
            for jobvo in jobList:
                logger.info(jobvo)
                if jobvo.job_type == "interval":
                    self.scheduler.add_job(func=self.dispatchjob, args=[jobvo.jobname], id=jobvo.jobid, trigger=jobvo.job_type,
                                       seconds=jobvo.frequency, start_date=jobvo.start_time, end_date=jobvo.end_time)
                elif jobvo.job_type == "cron":
                    jobargs = json.loads(jobvo.jobargs)
                    self.scheduler.add_job(func=self.dispatchjob, args=[jobvo.jobname], id=jobvo.jobid, trigger=jobvo.job_type,
                                           start_date=jobvo.start_time, end_date=jobvo.end_time, **jobargs)
                self.jobdict[jobvo.jobid] = jobvo
            self.scheduler.start()
        return self.jobdict.values()

    def mergejob(self, njobvo):
        jobid = njobvo.jobid
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDefaultDaoManager()
        jobDao = daoManager.getDao(DaoKeys.DAO_JOBMANAGERDAO)
        try:
            '''For getLoginFailedData job, it is generated according to requirement.
            If the password in DepotDB is not changed long times, it will generate two job for the db; so at here we need to avoid it;
            '''
            if njobvo.func=="getUserLoginFaileAuditdData":
                jobvo = jobDao.getJobByJobnameAndDBNames(njobvo.jobname, njobvo.dbnames)
                if jobvo is not None:
                    if njobvo.end_time < jobvo.start_time:
                        self.scheduler.reschedule_job(jobvo.jobid, trigger=jobvo.job_type, seconds=jobvo.frequency,
                                                  start_date=njobvo.start_time, end_date=njobvo.end_time)
                    return jobvo
            # This is used for update job
            if jobid in self.jobdict:
                jobvo = self.jobdict[jobid]
                # jobvo.jobid = njobvo.jobid
                jobvo.appln_support_code = njobvo.appln_support_code
                jobvo.dbnames = njobvo.dbnames
                jobvo.db_type = njobvo.db_type
                jobvo.job_type = njobvo.job_type
                # jobvo.func = njobvo.func
                jobvo.args = njobvo.args
                jobvo.start_time = njobvo.start_time
                jobvo.end_time = njobvo.end_time
                jobvo.frequency = njobvo.frequency
                jobvo.emailto = njobvo.emailto
                jobvo.sendtospark = njobvo.sendtospark
                jobvo = jobDao.getJobByJobname(njobvo.jobname)
                self.scheduler.reschedule_job(jobvo.jobid, trigger=jobvo.job_type, seconds=jobvo.frequency,
                                              start_date=jobvo.start_time, end_date=jobvo.end_time)
            # This is used for add a new job
            else:
                jobDao.addJob(njobvo)
                jobvo = jobDao.getJobByJobname(njobvo.jobname)
                self.scheduler.add_job(func=self.dispatchjob, args=[jobvo], id=jobvo.jobid, trigger=jobvo.job_type,
                                       seconds=jobvo.frequency, start_date=jobvo.start_time, end_date=jobvo.end_time)
            self.jobdict[jobvo.jobid] = jobvo
            return jobvo
        except Exception as e:
            daoManager.rollback()
            logger.error(e)
            raise wbxexception("Error occured")
        finally:
            daoManager.commit()


    def deletejob(self, jobid):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDefaultDaoManager()

        try:
            if jobid in self.jobdict:
                jobvo = self.jobdict[jobid]
                jobDao = daoManager.getDao(DaoKeys.DAO_JOBMANAGERDAO)
                jobDao.deleteJob(jobvo)
                self.scheduler.remove_job(jobvo.jobid)
            daoManager.commit()
            self.jobdict.pop(jobid)
        except Exception as e:
            daoManager.rollback()
            logger.error(e)
            raise wbxexception("Error occured")

    def dispatchjob(self, jobname):
        failedcount = 0
        # self.jobtimes = self.jobtimes + 1
        # if self.jobtimes > 1:
        #     return
        try:
            daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
            daoManager = daoManagerFactory.getDefaultDaoManager()
            jobDao = daoManager.getDao(DaoKeys.DAO_JOBMANAGERDAO)
            jobvo = jobDao.getJobByJobname(jobname)
            logger.info("%s.%s invoked with func=%s" % (self.__class__.__name__, "dispatchjob(jobvo)", jobvo.func))
            jobvo.runstatus = 'running'
            jobvo.laststarttime = datetime.now()
            daoManager.commit()

            dbidlist = self.getdbidlist(jobvo)
            # dbidlist = ["iddbormt0_RACINTH"]
            # Used for DEBUG only
            ffuture = None
            fcount = 0
            logger.info("func=%s, len(dbidlist)=%d" % (jobvo.func, len(dbidlist)))
            threadpool =  ThreadPoolExecutor(max_workers=5)
            try:
                if len(dbidlist) == 0:
                    ffuture = threadpool.submit(eval(jobvo.func), jobvo.emailto, jobvo.sendtospark)
                else:
                    futurelist = { dbid: threadpool.submit(eval(jobvo.func), dbid, jobvo.args, jobvo.emailto, jobvo.sendtospark) for dbid in dbidlist}
                    for dbid, future in futurelist.items():
                        try:
                            if not future.result(timeout=3*60):
                                failedcount = failedcount + 1
                        except Exception as e:
                            failedcount = failedcount + 1
                    if jobvo.reportfunc is not None:
                        ffuture = threadpool.submit(eval(jobvo.reportfunc), jobvo.emailto)
                if ffuture is not None:
                    if not ffuture.result(timeout=3 * 60):
                        failedcount = failedcount + 1
            finally:
                if threadpool is not None:
                    try:
                        threadpool.shutdown(wait=False)
                    except Exception as e:
                        pass
        except wbxexception as e:
            logger.error("The job %s has issue to start" % jobvo.jobname)
            logger.error(e)
        finally:
            print("final failedcount=%s" % failedcount)
            jobvo = jobDao.getJobByJobname(jobname)
            jobvo.runstatus = 'completed'
            jobvo.lastendtime = datetime.now()
            jobvo.failedcount = jobvo.failedcount + failedcount
            daoManager.commit()

    def getdbidlist(self, jobvo):
        dbidlist = []
        errorlist = []
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        if jobvo.dbnames is not None:
            pdbidlist = jobvo.dbnames.split(',')
            for dbid in pdbidlist:
                db = daoManagerFactory.getDatabaseByDBID(dbid)
                if db is not None:
                    dbidlist.append(dbid)
                else:
                    errorlist.append(dbid)

            if len(errorlist) > 0:
                raise wbxexception("the db %s does not exist" % errorlist)
        elif jobvo.appln_support_code is not None:
            dbtypelist = ["PROD", "BTS_PROD"]
            if jobvo.db_type is not None:
                dbtypelist = jobvo.db_type.split(",")

            if jobvo.appln_support_code == "ALL":
                dblist = daoManagerFactory.getAllDatabase().values()
                dbidlist = [db.getdbid() for db in dblist if db.db_type in dbtypelist]
            else:
                applnsupportcode = jobvo.appln_support_code
                appcodelist = applnsupportcode.split(",")

                for appcode in appcodelist:
                    dblist = daoManagerFactory.getDBListByAppCode(appcode)
                    if dblist is not None:
                        appcodedbidlist = [db.getdbid() for db in dblist if db.db_type in dbtypelist]
                        dbidlist.extend(appcodedbidlist)

            if len(errorlist) > 0:
                raise wbxexception("the appln_support_code %s does not exist" % errorlist)

        return dbidlist









