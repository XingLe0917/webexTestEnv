import logging
from common.wbxtask import wbxautotask
from common.wbxcache import getTaskFromCache
from biz.dbmanagement.wbxdbshareplexport import wbxdbshareplexport
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys
from common.wbxexception import wbxexception
import threading
from collections import OrderedDict
from biz.dbmanagement.wbxdbuser import wbxdbuser

logger = logging.getLogger("DBAMONITOR")
#
# def biz_sharpelexupgrade_preverify(host_name, splex_port):
#     resDict = {"status": "SUCCESS", "resultmsg": "","jobid":None}
#     try:
#         task = shareplexupgradetask()
#         task.initialize(host_name=host_name, splex_port=splex_port)
#         jobid = task.addJob()
#         task.startJob(jobid, task.preverify)
#         resDict["jobid"] = jobid
#     except Exception as e:
#         resDict["status"] = "FAILED"
#         resDict["resultmsg"] = str(e)
#     return resDict
#
# def biz_shareplexupgrade_upgrade(taskid):
#     resDict = {"status":"SUCCESS","resultmsg":"","jobid":""}
#     try:
#         task = getTaskFromCache(taskid)
#         if task is None:
#             raise wbxexception("Not get this task in cache, please verify this task again")
#         jobid = task.addJob()
#         task.startJob(jobid, task.upgrade)
#         resDict["jobid"] = jobid
#     except Exception as e:
#         resDict["status"] = "FAILED"
#         resDict["resultmsg"] = str(e)
#     return resDict

class shareplexupgradetask(wbxautotask):
    def __init__(self, taskid = None):
        super(shareplexupgradetask,self).__init__(taskid, "SHAREPLEXUPGRADE_TASK")
        self._host_name = None
        self._splex_port = None
        self._spport = None
        self.install_files = {"9.2.1": "SharePlex-9.2.1-b39-ONEOFF-SPO3828-SPO17377-rhel-amd64-m64.tpm"}

    def initialize(self, **kwargs):
        self._host_name = kwargs["host_name"]
        self._splex_port = kwargs["splex_port"]
        self._old_version = kwargs["splex_old_version"]
        self._new_version = kwargs["splex_new_version"]
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        dbserver = daomanagerfactory.getServer(self._host_name)
        if dbserver is None:
            raise wbxexception("Do not find the server with host_name=%s" % self._host_name)
        spport = dbserver.getShareplexPort(self._splex_port)
        if spport is None:
            db = daomanagerfactory.getDatabaseByDBName("RACGMCT")
            dbuser = wbxdbuser(db.getDBName(), "splex19062")
            dbuser.setApplnSupportCode("mct")
            dbuser.setPassword("Tk07P#FBfT")
            db.addUser(dbuser)
            spport = wbxdbshareplexport(19062, dbserver,"Tk07P#FBfT")
            spport.addDatabase(db, "RACGMCT_SPLEX")
            dbserver.addShareplexPort(spport)
            # raise wbxexception("Do not find the shareplex port %s on the server %s" % (self._splex_port, self._host_name))
        taskvo = super(shareplexupgradetask, self).initialize(**kwargs)
        jobList = self.listTaskJobsByTaskid(self._taskid)
        if len(jobList) == 0:
            self.generateJobs()
        return taskvo

    def generateJobs(self):
        self.addJob(host_name=self._host_name, splex_port=self._splex_port, job_action="preverify", process_order=1, execute_method="SYNC")
        self.addJob(host_name=self._host_name, splex_port=self._splex_port, job_action="upgrade", process_order=2, execute_method="ASYNC")

    def preverify(self, *args):
        jobid = args[0]
        try:
            self.updateJobStatus(jobid, "RUNNING")
            logger.info("preverify start for shareplex upgrade port=%s" % self._splex_port)
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            dbserver = daomanagerfactory.getServer(hostname=self._host_name)
            if dbserver is None:
                raise wbxexception("Can not get server by host_name=%s" % self._host_name)
            self._spport = dbserver.getShareplexPort(self._splex_port)
            logger.info(("Get the server by host_name=%s" % self._host_name))
            if self._spport is None:
                raise wbxexception("Can not get shareplex port %s on server %s" % (self._splex_port, self._host_name))
            logger.info("Get the shareplex port %s on server %s" % (self._splex_port, self._host_name))
            # dbserver.verifyConnection()
            self._spport.getServer().connect()
            self._spport.preverifyForExistPort()
            self._spport.getShareplexInstalledCount()
            releasever = self._spport.getShareplexVersion()
            if releasever != "8.6.3":
                raise wbxexception("Current shareplex version is %s, but should be 8.6.3" % (releasever))
            vardirsize = self._spport.getVardirSize()
            if vardirsize > 2 * 1024 * 1024 * 1024 or vardirsize == -1:
                raise wbxexception("The vardir %s size is %s which exceed limitation" % (self._spport.SP_SYS_VARDIR, vardirsize))
            logger.info("The vardir %s size is %s"  % (self._spport.getVardir(), vardirsize))
            res = self._spport.qstatus()
            lines = res.splitlines()
            queuname = None
            for line in lines:
                line = line.strip()
                if line.find("Name:") >= 0:
                    queuname = line.split()[1]
                elif line.find("Backlog (messages)") >= 0:
                    backlogsize = line.split()[2]
                    if int(backlogsize) > 1 * 1024 * 1024 * 1024:
                        raise wbxexception("The queue %s has %s backlog which exceed limitation 1GB" % (queuname, backlogsize))
            logger.info("All queue backlog size is less than threshold 1G")
            self._spport.verifySplexuserPassword()
            logger.info("preverify(hostname=%s, port=%s) SUCCEED" % (self._host_name, self._splex_port))
            self._spport.preparecronjob()
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
        finally:
            self._spport.getServer().close()

    def upgrade(self, *args):
        jobid = args[0]
        statusdict = OrderedDict([("Install Binary File", None), ("Start Blackout", None), ("Stop Cronjob", None),
                                  ("Stop Shareplex service", None), ("Backup Vardir", None)])
        try:
            jobvo = self.updateJobStatus(jobid, "RUNNING")
            logger.info("upgrade shareplex port with port=%s" % self._splex_port)
            PRODDIR_NAME = "shareplex%s" % self._old_version.replace(".", "")
            NEW_PRODDIR_NAME = "shareplex%s" % self._new_version.replace(".", "")
            issucceed = False
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            dbserver = daomanagerfactory.getServer(jobvo.host_name)
            sp = dbserver.getShareplexPort(jobvo.splex_port)
            step = "Install Binary File"
            try:
                dbserver.connect()
                PROD_DIR = sp.getProddir()
                # call preverifyForExistPort() to initialize field value
                if PROD_DIR is None:
                    sp.preverifyForExistPort()
                    PROD_DIR = sp.getProddir()

                splex_user = sp.getShareplexUserName()
                dbList = sp.getDBList()
                serverList = None
                for dbname, db in dbList.items():
                    splex_sid = sp.getSplexsidByDBName(dbname)
                    key = "orasetup_for_sid_%s" % splex_sid
                    statusdict[key] = None
                    if serverList is None:
                        serverList = list(db.getServerNameList())
                statusdict["Change CRS service script file"] = None
                statusdict["Start Shareplex service"] = None
                statusdict["Replace cronjob for the port"] = None
                statusdict["Set shareplex parameter"] = None
                statusdict["Start Cronjob"] = None
                statusdict["Stop Blackout"] = None
                NEW_PROD_DIR = PROD_DIR.replace(PRODDIR_NAME, NEW_PRODDIR_NAME)
                NEW_SPLEX_BIN_DIR = NEW_PROD_DIR + "/bin"
                if not sp.isBinaryInstalled(NEW_PROD_DIR):
                    sp.installShareplexBinary(self._new_version)
                    if not sp.isBinaryInstalled(NEW_PROD_DIR):
                        raise wbxexception("Shareplex binary file installation failed, please check log")
                    statusdict[step] = True
                else:
                    statusdict.pop(step)
                step = "Start Blackout"

                for sname in serverList:
                    sserver = daomanagerfactory.getServer(sname)
                    sserver.startBlackout(8)

                statusdict[step] = True
                step = "Stop Cronjob"
                dbserver.stopService("crond")
                statusdict[step] = True
                step = "Stop Shareplex service"
                sp.stopShareplexService()
                statusdict[step] = True
                step = "Backup Vardir"
                sp.backupVardir()
                statusdict[step] = True
                for dbname, db in dbList.items():
                    splex_sid = sp.getSplexsidByDBName(dbname)
                    splex_pwd = db.getSchemaPassword(splex_user)
                    step = "orasetup_for_sid_%s" % splex_sid
                    sp.changeProfile(PRODDIR_NAME, NEW_PRODDIR_NAME,NEW_SPLEX_BIN_DIR)
                    # splex_user = sp.getShareplexUserName()
                    args = ['n', 'n', '', splex_sid, "system", "sysnotallow", "n", splex_user, splex_pwd, "n", "","", "", "y", "y", ""]
                    sp.orasetup(splex_sid, *args)
                    statusdict[step] = True

                step = "Change CRS service script file"
                sp.upgradeServiceConfig(PRODDIR_NAME, NEW_PRODDIR_NAME,NEW_SPLEX_BIN_DIR,serverList)
                statusdict[step] = True
                issucceed = True
            except wbxexception as e:
                logger.error(e)
                statusdict[step] = str(e)
            finally:
                try:
                    if issucceed:
                        step = "Set shareplex parameter"
                        paramdict = {"SP_OCT_TARGET_COMPATIBILITY": None,
                                     "SP_SYS_TARGET_COMPATIBILITY": "7",
                                     "SP_OCT_OLOG_USE_OCI": "0",
                                     "SP_OCT_OLOG_NO_DATA_DELAY": "5000000",
                                    "SP_OCT_DDL_UPDATE_CONFIG": "0"}
                        sp.changeparameter(paramdict)
                        statusdict[step] = True
                    if statusdict["Stop Shareplex service"]:
                        sp.startShareplexService()
                        statusdict["Start Shareplex service"] = True
                    if statusdict["Stop Cronjob"]:
                        dbserver.startService("crond")
                        statusdict["Start Cronjob"] = True
                    # If failed in above step, should not change crontab
                    if issucceed:
                        # It block other sessions to process shareplex upgrade. But it is ok, the concurrency request is not huge;
                        sp.uncommentcronjob()
                        statusdict["Replace cronjob for the port"] = True
                    if statusdict["Start Blackout"]:
                        dbserver.stopBlackout()
                        statusdict["Stop Blackout"] = True
                except Exception as e:
                    logger.error("Error ocurred at upgradeShareplex finally step: %s" % e)

            logger.info(
                "upgradeShareplex(host_name=%s, splex_port=%s, splex_oldversion=%s, splex_newversion=%s) end with status: %s" % (
                dbserver.getHostname(), sp.getPort(), self._old_version, self._new_version, statusdict))
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")


