import logging
from dao.wbxdaomanager import wbxdaomanagerfactory
from common.wbxexception import wbxexception
from common.wbxtask import wbxautotask,threadlocal
from dao.wbxdaomanager import DaoKeys
import datetime
import time
from common.wbxutil import wbxutil
from common.wbxssh import wbxssh

logger = logging.getLogger("DBAMONITOR")

class wbxlogmnrtask(wbxautotask):
    def __init__(self,taskid = None):
        super(wbxlogmnrtask,self).__init__(taskid, "LOGMNR_TASK")
        self._logmnr_dbname = "BTGDIAGS"
        self._logmnr_dbtype = "BTS_PROD"

    def initialize(self, **kwargs):
        self._db_name = kwargs["db_name"].upper()
        self._db_type = kwargs["db_type"].upper()
        self._start_datetime = kwargs["start_datetime"]
        self._end_datetime = kwargs["end_datetime"]
        self._logmnr_tablename = self.getBackupTablename(self._db_name)

        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daomanagerfactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            rows = depotdbDao.getInstanceInfoByDBName(self._db_name, self._db_type)
            if len(rows)==0:
                raise wbxexception("%s %s does not find in depot db,pls double check" % (self._db_name, self._db_type))
            host_name = rows[0][2].host_name
            dbserver = daomanagerfactory.getServer(host_name)
            self._racdict = dbserver.getRacNodeDict()
            self._dbserver = self._racdict["node1"]
            rows = depotdbDao.getInstanceInfoByDBName(self._logmnr_dbname, self._logmnr_dbtype)
            if len(rows)==0:
                raise wbxexception("%s %s does not find in depot db,pls double check" % (self._logmnr_dbname, self._logmnr_dbtype))
            for wbxobjects in rows:
                try:
                    host_name = wbxobjects[2].host_name
                    logmnerserver = daomanagerfactory.getServer(host_name)
                    logger.info("check server %s connection" % logmnerserver.host_name)
                    logmnerserver.verifyConnection()
                    break
                except:
                    logger.info("server %s connection check failed, Try to switch other servers in the cluster" % logmnerserver.host_name)
                finally:
                    if logmnerserver:
                        logmnerserver.close()
            self._logserver = daomanagerfactory.getServer(host_name)

            logger.info("check server %s connection" % self._dbserver.host_name)
            self._dbserver.verifyConnection()
            logger.info("check server %s connection" % self._logserver.host_name)
            self._logserver.verifyConnection()

            kwargs["logmnr_tablename"]=self._logmnr_tablename
            taskvo = super(wbxlogmnrtask, self).initialize(**kwargs)
            self._restoretargetdir = "/staging/gates/logmnr/%s/%s" % (self._db_name.lower(),taskvo.taskid)

            jobList = self.listTaskJobsByTaskid(taskvo.taskid)
            if len(jobList) == 0:
                self.generateJobs()
        except Exception as e:
            depotDaoManager.rollback()
            raise e
        finally:
            depotDaoManager.close()
            if self._logserver:
                self._logserver.close()
            if self._dbserver:
                self._dbserver.close()
        return taskvo

    def getBackupTablename(self,db_name):
        timeformat = "%m%d%H%M%S"
        curtime = datetime.datetime.strftime(datetime.datetime.now(), timeformat)
        return "logmnr_%s_%s" % (db_name, curtime)

    def preverify(self, *args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("wbxdbcutover.executeOneStep(processid=%s)" % jobid)
        try:
            hasError=False
            logger.info("Check parameter with fromdate %s to enddate %s" %(self._start_datetime,self._end_datetime))
            timeformat = "%Y-%m-%d %H:%M:%S"
            fromdt = datetime.datetime.strptime(self._start_datetime, timeformat)
            todt = datetime.datetime.strptime(self._end_datetime, timeformat)
            timedelta = (todt - fromdt).total_seconds()
            if timedelta < 0:
                raise wbxexception("The end_datetime(%s) must greater than start_datetime(%s)" % (self._end_datetime, self._start_datetime))

            if timedelta > 10 * 60:
                raise wbxexception("It can not restore at most 10 minutes archive log each time. Please double check input start_datetime and end_datetime")

            lastdelta = (datetime.datetime.now() - fromdt).days
            if lastdelta > 29:
                raise wbxexception("This tool can only get latest 30 days archive log. Please double check input start_datetime")

            logger.info("Check whether scp is processing on host : %s" %(self._dbserver.host_name))
            cmd=" ps -ef|grep scp | grep %s | grep -wv grep | wc -l" %(self._restoretargetdir)
            logger.info("execute command %s on server %s" % (cmd, self._dbserver.host_name))
            rows = self._dbserver.exec_command(cmd)
            logger.info(rows)
            if int(rows) > 0:
                logger.error("scp process exist on server: %s" % (self._dbserver.host_name))
                hasError = True

            logger.info("Check whether logmnr server restore directory is empty")
            cmd="ls %s | wc -w" %(self._restoretargetdir)
            logger.info("execute command %s on server %s" % (cmd, self._logserver.host_name))
            rows = self._logserver.exec_command(cmd)
            logger.info(rows)
            if int(rows)>0:
                logger.error("under restore directory on logmnr server :%s has %s file or directory objects,pls double check." %(self._logserver.host_name,str(rows)))
                hasError=True

            if hasError:
                raise wbxexception("DBCUTOVER PreVerify jobid : %s end with Error" % (jobid))
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
            raise e
        finally:
            if self._logserver:
                self._logserver.close()
            if self._dbserver:
                self._dbserver.close()

    def executejob(self,*args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("wbxdbcutover.executeOneStep(jobid=%s)" % jobid)
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        daomanager.startTransaction()
        jobvo = dao.getAutoTaskJobByJobid(jobid)

        db_name = jobvo.db_name
        jobname = jobvo.parameter["stage"]
        commond = jobvo.parameter["command"]
        server = None
        parameter = ""
        if jobname == "archivelog_restore":
            server = self._dbserver
            parameter = """%s "%s" "%s" "%s" """ % (db_name.lower(), self._start_datetime, self._end_datetime,self._restoretargetdir)
        elif jobname == "archivelog_logmnr":
            server = self._logserver
            bk_tablename = jobvo.parameter["table_name"]
            parameter = """%s %s %s """ % (self._restoretargetdir, self._logmnr_dbname, bk_tablename)
        cmd = "sh %s %s" % (commond, parameter)
        logger.info("execute command %s on %s" % (cmd, server.host_name))
        wbxvo = wbxssh(server.host_name, server.ssh_port, server.login_user, server.login_pwd)
        try:
            wbxvo.connect()
            wbxvo.send(cmd)
            kargs = {}
            time.sleep(1)
            rows = ""
            while True:
                buff = wbxvo.recvs(**kargs)
                logger.info(buff.replace("‘", "'"))
                if buff:
                    rows += buff
                    if buff.strip().endswith(('$')):
                        if rows.find("WBXERROR") >= 0:
                            raise wbxexception("Error occurred with command %s" % (cmd))
                        break
            daomanager.commit()
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            daomanager.rollback()
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
            raise e
        finally:
            wbxvo.close()
            server.close()
            daomanager.close()

    def transferfile(self,*args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("wbxdbcutover.executeOneStep(processid=%s)" % jobid)
        haserror = False
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        wbxvo = wbxssh(self._dbserver.host_name, self._dbserver.ssh_port, self._dbserver.login_user,self._dbserver.login_pwd)
        try:
            daomanager.startTransaction()
            dao.getAutoTaskJobByJobid(jobid)
            cmd = "if [ ! -d /staging/gates/logmnr/%s ];then mkdir -p /staging/gates/logmnr/%s && chmod 777 /staging/gates/logmnr/%s;fi;" %(self._db_name.lower(),self._db_name.lower(),self._db_name.lower())
            logger.info("execute cmd %s on server %s" % (cmd, self._logserver.host_name))
            self._logserver.exec_command(cmd)
            cmd = "scp -r -P 22 %s %s@%s:/staging/gates/logmnr/%s" % (self._restoretargetdir, self._logserver.login_user, self._logserver.host_name,self._db_name.lower())
            logger.info("execute cmd %s on server %s" %(cmd,self._dbserver.host_name))
            res=""
            kargs = {"account":self._logserver.login_pwd,"password": self._logserver.login_pwd,"continue connecting":"yes"}

            wbxvo.connect()
            wbxvo.send(cmd)
            time.sleep(1)
            fromdt=datetime.datetime.now()
            step=1
            interval=20
            while True:
                buff = wbxvo.recvs(**kargs)
                todt = datetime.datetime.now()
                timedelta = (todt - fromdt).total_seconds()
                if int(timedelta)>interval*step:
                    logger.info(buff)
                    step += 1
                if buff:
                    res += buff
                    if buff.strip().endswith(('$')):
                        if res.find("100%")<0:
                            haserror=True
                        break
            if haserror:
                raise wbxexception("err has occured with cmd %s on server %s" %(cmd,self._dbserver.host_name))

            logger.info("Post verify begin...")
            cmd = "du -s %s |awk '{print $1}'|sed 's/[[:space:]]//g'" % (self._restoretargetdir)
            logger.info("check %s directory size on %s" %(self._restoretargetdir,self._dbserver.host_name))
            srcsize=wbxvo.exec_command(cmd)
            logger.info(srcsize)
            if wbxutil.isNoneString(srcsize):
                raise wbxexception("can not get directory size on host:%s" %(self._dbserver.host_name))
            logger.info("check %s directory size on %s" % (self._restoretargetdir, self._logserver.host_name))
            tgtsize = self._logserver.exec_command(cmd)
            logger.info(tgtsize)
            if wbxutil.isNoneString(tgtsize):
                raise wbxexception("can not get directory size on host:%s" %(self._logserver.host_name))
            if int(srcsize)!=int(tgtsize):
                raise wbxexception("directory size is different between %s:%s and %s:%s" % (self._dbserver.host_name,srcsize,self._logserver.host_name,tgtsize))

            daomanager.commit()
            logger.info("Post verify end with SUCCEED")
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            daomanager.rollback()
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
            raise e
        finally:
            self._dbserver.close()
            self._logserver.close()
            wbxvo.close()
            daomanager.close()

    def generateJobs(self):
        logger.info("generateCutoverStep(taskid=%s, db_name=%s)" % (self._taskid, self._db_name))

        self.addJob(host_name=self._dbserver.host_name, db_name=self._db_name, job_action="preverify",stage="preverify",
                    process_order=1, execute_method="SYNC",isoneclick=True)

        process_order=2
        self.addJob(host_name=self._dbserver.host_name, db_name=self._db_name, job_action="executejob",stage="archivelog_restore",
                    process_order=process_order, command="/staging/gates/archivelog_restore.sh",execute_method="SYNC",isoneclick=True)
        process_order+=1

        if self._dbserver._site_code != self._logserver._site_code:
            self.addJob(host_name=self._dbserver.host_name, db_name=self._db_name, job_action="transferfile",stage="transferfile",
                        process_order=process_order, execute_method="SYNC",isoneclick=True)
            process_order += 1

        self.addJob(host_name=self._logserver.host_name, db_name=self._logmnr_dbname, job_action="executejob",stage="archivelog_logmnr",
                    process_order=process_order,table_name=self._logmnr_tablename,command="/staging/gates/archivelog_logmnr.sh",
                    execute_method="SYNC",isoneclick=True)