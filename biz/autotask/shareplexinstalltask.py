import logging
from common.wbxtask import wbxautotask
from common.wbxcache import getTaskFromCache
from biz.dbmanagement.wbxdbshareplexport import wbxdbshareplexport
from biz.dbmanagement.wbxdbserver import wbxdbserver
from biz.dbmanagement.wbxdb import wbxdb
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys
from common.wbxexception import wbxexception
import os
from collections import OrderedDict

logger = logging.getLogger("DBAMONITOR")

class shareplexinstalltask(wbxautotask):
    def __init__(self, taskid = None):
        super(shareplexinstalltask,self).__init__(taskid, "SHAREPLEXINSTALL_TASK")
        self._host_name = None
        self._splex_port = None
        self._SP_SYS_PRODDIR = None
        self._splex_sid = None
        self._splex_version = None
        self._db_name = None
        self._spport = None
        self._newdb = None

    def initialize(self, **kwargs):
        self._host_name = kwargs["host_name"]
        self._splex_port = kwargs["splex_port"]
        self._splex_version = kwargs["splex_version"]
        self._splex_sid = kwargs["splex_sid"]
        self._db_name = kwargs["db_name"]
        root_dir = kwargs["root_dir"]
        self._SP_SYS_PRODDIR = root_dir + "/" + "shareplex%s" % self._splex_version.replace(".","")

        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        db = daomanagerfactory.getDatabaseByDBName(self._db_name)
        if db is not None:
            dbinstance = db.getInstanceByHostname(self._host_name)
            # This mean the db cutover case
            if dbinstance is None:
                self._newdb = wbxdb(self._db_name, self._splex_sid)
                self._newdb.initFromServer(self._host_name)
                userDict = db.getUserDict()
                for username, dbuser in userDict.items():
                    self._newdb.addUser(dbuser)
            # This mean install shareplex port on existed db case
            else:
                self._newdb = db
        # This means instlal sharpelex port on a new db case
        else:
            self._newdb = wbxdb(self._db_name, self._splex_sid)
            self._newdb.initFromServer(self._host_name)
        dbserver = self._newdb.getServer(self._host_name)
        if dbserver is None:
            raise wbxexception("Not get the server %s" % self._host_name)
        spport = dbserver.getShareplexPort(self._splex_port)
        if spport is not None:
            raise wbxexception("The shareplex port %s already exists on the server %s" % (self._splex_port, self._host_name))
        spport = wbxdbshareplexport(self._splex_port, dbserver)
        spport.addDatabase(self._newdb, self._splex_sid)
        dbserver.addShareplexPort(spport)

        row = self._newdb.getUserByUserName(spport.getShareplexUserName())
        if row is not None:
            raise wbxexception("The shareplex user %s already exists on the db %s, remove it before install port" % (spport.getShareplexUserName(), self._db_name))
        try:
            dbserver.connect()
            if not dbserver.isDirectory(root_dir):
                raise wbxexception("Inputted directory %s does not exist or not a directory" % root_dir)
            dbserver.checkEnviromentConfig()
        finally:
            dbserver.close()
        # Because shareplex port is necessary for each step, so the initialization should be in this function, can not in preverify
        spport.setEnviromentConfig(self._SP_SYS_PRODDIR, self._splex_sid)
        rows = self._newdb.getTablespace()
        for row in rows:
            if row[0] == "SPLEX_DATA":
                spport.setDataTablespace(row[0])
            elif row[0] in ('SPLEX_INDX', 'SPLEX_INDEX'):
                spport.setIndexTablespace(row[0])
            elif row[2] == "TEMPORARY":
                spport.setTempTablespace(row[0])
        taskvo = super(shareplexinstalltask, self).initialize(**kwargs)
        # If the task already created, but not exist in cache, so need to initialize again, add below check to avoid job generation again
        jobList = self.listTaskJobsByTaskid(self._taskid)
        if len(jobList) == 0:
            self.generateJobs()
        return taskvo

    def generateJobs(self):
        self.addJob(host_name=self._host_name, splex_port=self._splex_port, db_name=self._db_name, job_action="preverify",
                    process_order=1, execute_method="SYNC",prod_dir = self._SP_SYS_PRODDIR, splex_version=self._splex_version)
        self.addJob(host_name=self._host_name, splex_port=self._splex_port, db_name=self._db_name, job_action="installation",
                    process_order=2, execute_method="ASYNC",prod_dir = self._SP_SYS_PRODDIR, splex_version=self._splex_version)
        self.addJob(host_name=self._host_name, splex_port=self._splex_port, db_name=self._db_name, job_action="addmonitor",
                    process_order=3, execute_method="SYNC",prod_dir=self._SP_SYS_PRODDIR, splex_version=self._splex_version)

    def preverify(self, *args):
        jobid = args[0]
        try:
            logger.info("preverify for shareplex port with port=%s" % self._splex_port)
            self.updateJobStatus(jobid, "RUNNING")
            dbserver = self._newdb.getServer(self._host_name)
            if dbserver is None:
                raise wbxexception("Can not get server by host_name=%s" % self._host_name)
            spport = dbserver.getShareplexPort(self._splex_port)
            if spport is None:
                raise wbxexception("Can not get shareplex port instance with port=%s" % self._splex_port)
            spport.preverifyForNewPort(self._splex_version)
            try:
                dbserver.connect()
                if not dbserver.hasWritePrivilege("/tmp"):
                    raise wbxexception("The oracle user does not have write privilege to /tmp directory")
            finally:
                dbserver.close()
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
        finally:
            self._spport.getServer().close()

    def installation(self, *args):
        jobid = args[0]
        dbserver = None
        try:
            logger.info("installation shareplex port %s" % self._splex_port)
            logger.info("Update SHAREPLEXINSTALL_TASK job status to be RUNNING in depotdb with jobid=jobid")
            jobvo = self.updateJobStatus(jobid, "RUNNING")
            dbserver = self._newdb.getServer(self._host_name)
            if dbserver is None:
                raise wbxexception("Can not get server by host_name=%s" % self._host_name)
            spport = dbserver.getShareplexPort(self._splex_port)
            if spport is None:
                raise wbxexception("Can not get shareplex port instance with port=%s" % self._splex_port)

            dbserver.connect()
            if not spport.isBinaryInstalled(self._SP_SYS_PRODDIR):
                logger.info("The shareplex binary for version %s is not yet installed. Install it now" % self._splex_version)
                spport.getShareplexInstalledCount()
                spport.installShareplexBinary(self._splex_version)
            else:
                logger.info("The shareplex binary for version %s already exist" % self._splex_version)

            if not spport.isBinaryInstalled(self._SP_SYS_PRODDIR):
                raise wbxexception("Shareplex binary file installation failed, please check log")
            spport.addProfile(self._splex_sid)

            statusdbfile=spport.getVardir() + "/data/statusdb"
            if not dbserver.isFile(statusdbfile):
                logger.info("Executing ora_setup for the port %s" % self._splex_port)
                pwd = self._newdb.getSchemaPassword(spport.getShareplexUserName())
                if pwd is None:
                    pwd = "Tk07P#FBfT"

                data_tbs = spport.getDataTablespace()
                idx_tbs = spport.getIndexTablespace()
                temp_tbs = spport.getTempTablespace()
                asmsid = dbserver.getASMSid()
                args = ["n", "n", dbserver.getOracleHome(), self._splex_sid, "system", "sysnotallow", "y",
                        spport.getShareplexUserName(), pwd, pwd, "n", data_tbs, temp_tbs, idx_tbs, "y", "y", asmsid]

                spport.orasetup(self._splex_sid, *args)
            spport.addDefaultParameter()
            self.updateJobStatus(jobid,"SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
        finally:
            if dbserver is not None:
                dbserver.close()

    def addmonitor(self,*args):
        jobid = args[0]
        dbserver = None
        try:
            logger.info("addmonitor for shareplex port with port=%s" % self._splex_port)
            logger.info("Update job status to be RUNNING in depotdb")
            jobvo = self.updateJobStatus(jobid, "RUNNING")
            dbserver = self._newdb.getServer(self._host_name)
            if dbserver is None:
                raise wbxexception("Can not get server by host_name=%s" % self._host_name)

            spport = dbserver.getShareplexPort(self._splex_port)
            if spport is None:
                raise wbxexception("Can not get shareplex port instance with port=%s" % self._splex_port)
            dbserver.connect()
            spport.addcronjob()
            spport.registerIntoCRS()
            spport.addshsetport()

            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
            if dbserver is not None:
                dbserver.close()