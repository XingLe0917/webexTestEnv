import logging
import uuid
from datetime import datetime
import threading
import re
from collections import OrderedDict
from biz.dbmanagement.wbxdb import wbxdb
from biz.dbmanagement.wbxdbshareplexport import wbxdbshareplexport
from common.wbxexception import wbxexception
from concurrent.futures import ThreadPoolExecutor
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys
from common.wbxexception import WbxDaoException, wbxexception
from common.wbxcache import getTaskFromCache
from common.wbxtask import wbxautotask
from dao.vo.autotaskvo import wbxautotaskjobvo
from dao.vo.dbcutover import wbxdbcutovervo, wbxdbcutoverprocessvo, wbxdbcutoverspmappingvo

glock = threading.Lock()

logger = logging.getLogger("DBAMONITOR")

#Not Supported Case
#1. new and old db name is different
#2. only export part schemas
# def biz_dbcutover_listCutoveredDBs():
#     resDict = {"status": "SUCCEED", "errormsg": "", "data": None}
#     daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
#     daomanager = daomanagerfactory.getDefaultDaoManager()
#     cutoverdao = daomanager.getDao(DaoKeys.DAO_DBCUTOVERDAO)
#     try:
#         daomanager.startTransaction()
#         cutovervoList = cutoverdao.listAllCutoverDB()
#         resDict["data"] = [cutovervo.to_dict() for cutovervo in cutovervoList]
#     except Exception as e:
#         daomanager.rollback()
#         resDict["status"] = "FAILED"
#         resDict["errormsg"] = str(e)
#     finally:
#         daomanager.close()
#     return resDict
#
# # At this step we do not need to recontruct wbxdbcutover object
# def biz_dbcutover_listDBCutoverStep(taskid):
#     logger.info("listDBCutoverStep(taskid=%s)" % taskid)
#     resDict = {"status": "SUCCEED", "errormsg": "", "data": None}
#     daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
#     daomanager = daomanagerfactory.getDefaultDaoManager()
#     dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
#     try:
#         daomanager.startTransaction()
#         jobvolist = dao.getAutoTaskJobByTaskid(taskid)
#         resDict["data"] = [jobvo.to_dict() for jobvo in jobvolist if jobvo.job_action in ('PRECUTOVER','PREPARE','START','POST')]
#     except Exception as e:
#         daomanager.rollback()
#         resDict["status"] = "FAILED"
#         resDict["errormsg"] = str(e)
#     finally:
#         daomanager.close()
#     return resDict
#
# #when execute step, we need to reconstruct cutover object if not exist
# # why cutover object is contructed and cached at generation step time, but here it does not exist, such as ccp restart
# def biz_dbcutover_executeCutoverStep(jobid, taskid):
#     resDict = {"status": "SUCCEED", "errormsg": ""}
#     daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
#     daomanager = daoManagerFactory.getDefaultDaoManager()
#     dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
#     try:
#         daomanager.startTransaction()
#         jobvo = dao.getAutoTaskJobByJobid(jobid)
#         daomanager.commit()
#         if jobvo is None:
#             raise wbxexception("Not find this job in DB" % jobvo)
#         if jobvo.status == "SUCCESS":
#             raise wbxexception("This job is already SUCCEED, can not execute again")
#     except Exception as e:
#         daomanager.rollback()
#         resDict["status"] = "FAILED"
#         resDict["errormsg"] = str(e)
#         return resDict
#     finally:
#         daomanager.close()
#
#     try:
#         cutovertask = getTaskFromCache(taskid)
#         if cutovertask is None:
#             cutovertask = wbxdbcutovertask()
#             cutovertask.initializeFromDB(taskid)
#         cutovertask.startJob(jobid, cutovertask.executeOneStep)
#     except Exception as e:
#         resDict["status"] = "FAILED"
#         resDict["errormsg"] = str(e)
#     return resDict
#
# # If can not get cutover object, then need to reconstruct this object from preverify() fucntion again
# # For example, 1) call preverify, 2) restart ccp; 3) call generateCutoverStep()
# def biz_dbcutover_generateCutoverStep(taskid):
#     resDict = {"status":"SUCCEED","errormsg":"", "data":None}
#     try:
#         cutovertask = getTaskFromCache(taskid)
#         if cutovertask is None:
#             raise wbxexception("Not get this task in cache, please verify this task again")
#         stepList = cutovertask.generateCutoverStep()
#         resDict["data"] = stepList
#     except Exception as e:
#         resDict["status"] = "FAILED"
#         resDict["errormsg"] = str(e)
#     return resDict
#
# def biz_dbcutover_saveCutoverStep(taskid):
#     resDict = {"status": "SUCCEED", "errormsg": "", "data": None}
#     try:
#         if glock.acquire():
#             cutovertask = getTaskFromCache(taskid)
#             if cutovertask is None:
#                 raise wbxexception("Not find the task, execute preverify for this db cutover at first")
#             jobvoList = cutovertask.listTaskJobsByTaskid(taskid)
#             if len(jobvoList) > 1:
#                 raise wbxexception("This task jobs have been saved to DB")
#             stepList = cutovertask.saveCutoverStep()
#             resDict["data"] = stepList
#     except Exception as e:
#         resDict["status"] = "FAILED"
#         resDict["errormsg"] = str(e)
#     finally:
#         glock.release()
#     return resDict
#
# def biz_dbcutover_preverify(db_name, old_host_name, new_host_name):
#     resDict = {"status": "SUCCESS", "resultmsg": "", "jobid": None,"taskid":None}
#     try:
#         dbcutover = wbxdbcutovertask()
#         resDict["taskid"] = dbcutover.getTaskid()
#         dbcutover.initialize(db_name=db_name, old_host_name=old_host_name, new_host_name=new_host_name)
#         jobvo = dbcutover.addJob(job_action="PREVERIFY", db_name=db_name, old_host_name=old_host_name,new_host_name=new_host_name)
#         dbcutover.startJob(jobvo.jobid, dbcutover.preverify)
#         resDict["jobid"] = jobvo.jobid
#     except Exception as e:
#         resDict["status"] = "FAILED"
#         resDict["resultmsg"] = str(e)
#     return resDict


class wbxdbcutovertask(wbxautotask):
    def __init__(self,taskid = None):
        super(wbxdbcutovertask,self).__init__(taskid, "DBCUTOVER_TASK")
        self._old_host_name = None
        self._new_host_name = None
        self._db_name = None
        self._db_splex_sid = None
        self._dbinfo = None
        self._srcdbDict = {}
        self._tgtdbDict = {}
        self._serverDict = {}
        self._spportMapping = {}
        self._cutovervo = None
        self._cutoverStepList = []
        self._cutoverspmappingList = []
        # self._cutoverid = uuid.uuid4().hex
        self._isDataguard = False
        self._thread_pool = ThreadPoolExecutor(max_workers=5)
        self._lock = threading.Lock()
        self._isnew = True if taskid is None else False

    def getDBName(self):
        return self._db_name

    def initialize(self, **kwargs):
        self._old_host_name = kwargs["old_host_name"]
        self._new_host_name = kwargs["new_host_name"]
        self._db_name = kwargs["db_name"].upper()
        if self._db_name == "RACPSYT":
            self._db_splex_sid = "PSYTOOL_SPLEX"
        else:
            self._db_splex_sid="%s_SPLEX" % self._db_name
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        self._olddb = daomanagerfactory.getDatabaseByDBName(self._db_name)
        if self._olddb is None:
            raise wbxexception("Can not get database with db_name=%s" % self._db_name)
        logger.info("old db name %s exists" % self._db_name)
        for servername, dbserver in self._olddb.getServerDict().items():
            dbserver.verifyConnection()
        logger.info("old db servers ssh login verification passed")
        self._newdb = wbxdb(self._db_name, self._db_splex_sid)
        # self._newdb = wbxdb(self._db_name, "")
        self._serverDict.update(self._olddb.getServerDict())
        self._newdb.initFromServer(self._new_host_name)
        logger.info("new db servers ssh login verification passed")
        oldspportlist = self._olddb.getShareplexPortList()
        # for spport in oldspportlist:
        #     print(spport.getPort())
        server = self._newdb.getServer(None)
        newspportDict = server.getShareplexPortListFromCRS()
        for oldspport in oldspportlist:
            port = oldspport.getPort()
            try:
                # oldspport.getServer().connect()
                # if self._isnew:
                #     oldspport.preverifyForExistPort()
                for tgtdb in oldspport.getTgtDBList():
                    self._tgtdbDict[tgtdb.getDBName()] = tgtdb
                for srcdb in oldspport.getSrcDBList():
                    self._srcdbDict[srcdb.getDBName()] = srcdb

                if port in newspportDict:
                    new_host_name = newspportDict[port]
                    server = self._newdb.getServer(new_host_name)
                    spport = wbxdbshareplexport(port, server)
                    spport.addDatabase(self._newdb, self._db_splex_sid)
                    server.addShareplexPort(spport)
                    self._spportMapping[port] = {"port": port, "newport": spport, "oldport": oldspport}
                else:
                    raise wbxexception("The shareplex port %s on old db, but not exist on new db" % port)
            finally:
                pass
                # oldspport.getServer().close()

        logger.info("All shareplex ports on olddb are created on new db servers")
        taskvo = super(wbxdbcutovertask, self).initialize(**kwargs)
        jobList = self.listTaskJobsByTaskid(taskvo.taskid)
        if len(jobList) == 0:
            self.generateJobs()
        return taskvo

    def preverify(self, *args):
        jobid = args[0]
        try:
            jobvo = self.updateJobStatus(jobid, "RUNNING")
            # Check whether all shareplex port on old db are created on new db
            oldspportlist = self._olddb.getShareplexPortList()
            # server = self._newdb.getServer(None)
            # newspportDict = server.getShareplexPortListFromCRS()
            # for oldspport in oldspportlist:
            #     port = oldspport.getPort()
            #     oldspport.preverifyForExistPort()
            #     if port in newspportDict:
            #         new_host_name = newspportDict[port]
            #         server = self._newdb.getServer(new_host_name)
            #         spport = wbxdbshareplexport(port, server)
            #         spport.addDatabase(self._newdb, self._db_splex_sid)
            #         server.addShareplexPort(spport)
            #         self._spportMapping[port] = {"port": port, "newport": spport, "oldport": oldspport}
            #     else:
            #         raise wbxexception("The shareplex port %s on old db, but not exist on new db" % port)
            # logger.info("All shareplex ports on olddb are created on new db servers")

            # totalcount = len(oldspportlist)
            for spport in oldspportlist:
                try:
                    for tgtdb in spport.getTgtDBList():
                        tgtport = tgtdb.getShareplexPort(spport.getPort())
                        tgtserver = tgtport.getServer()
                        tgtserver.connect()
                        tgtport.preverifyForExistPort()
                        tgtserver.close()
                        self._tgtdbDict[tgtdb.getDBName()] = tgtdb
                    for srcdb in spport.getSrcDBList():
                        srcport = srcdb.getShareplexPort(spport.getPort())
                        srcserver = srcport.getServer()
                        logger.info("preverify shareplex port %s on source server %s" % (spport.getPort(), srcserver.getHostname()))
                        self._srcdbDict[srcdb.getDBName()] = srcdb
                        srcserver.connect()
                        srcport.preverifyForExistPort()
                        srcserver.close()
                except Exception as e:
                    logger.error("preverify shareplex port %s failed %s" % (spport.getPort(),e))
            logger.info("All sourcedb and target db exist and ssh login to the servers verification succeed")
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error("Error occured in wbxdbcutover.preverify(old_host_name=%s, new_host_name=%s, db_name=%s)" % (self._old_host_name, self._new_host_name, self._db_name), exc_info = e)
            self.updateJobStatus(jobid, "FAILED")

    # A database maybe both source and target of cutover db
    def getDatabaseBydbname(self, db_name):
        if db_name == self._db_name:
            return self._olddb
        if db_name in self._srcdbDict:
            return self._srcdbDict[db_name]
        if db_name in self._tgtdbDict:
            return self._tgtdbDict[db_name]
        return None

    # How to keep execute different steps in parallel and avoid execute the same step synchronally?
    # add lock in dbcutoverprocessvo
    def executeOneStep(self, *args):
        jobid = args[0]
        logger.info("wbxdbcutover.executeOneStep(%s)" % jobid)
        jobvo = self.updateJobStatus(jobid, "RUNNING")
        logger.info("wbxdbcutover.executeOneStep(processid=%s)" % jobid)
        if jobvo.status == "SUCCEED":
            raise wbxexception("This step(%s) has already completed" % jobid)
        try:
            resstr = ""
            if jobvo.parameter["server_type"] == "NEW":
                resstr = self._newdb.executeCutover(jobvo)
            elif jobvo.parameter["server_type"] == "OLD":
                resstr = self._olddb.executeCutover(jobvo)
            elif jobvo.parameter["server_type"] == "SRC":
                db = self._srcdbDict[jobvo.db_name]
                resstr = db.executeCutover(jobvo)

                #     for spmapping in self._cutoverspmappingList:
                #         if splex_port == spmapping.port:
                #             if processvo.server_type == "SRC":
                #                 params = "%s:%s:%s:%s:%s" % (
                #                 processvo.db_name, self._db_name, port, spmapping.old_host_name,
                #                 spmapping.new_host_name)
                #             else:
                #                 params = "%s:%s:%s:%s" % (
                #                 self._db_name, port, spmapping.old_host_name, spmapping.new_host_name)
                # if processvo.server_type == "NEW":
                #     resstr = self._newdb.executeCutover(processvo, params)
                # elif processvo.server_type == "OLD":
                #     resstr = self._olddb.executeCutover(processvo, params)
                # else:
                #     db_name = processvo.db_name
                #     if db_name in self._srcdbDict:
                #         db = self._srcdbDict[db_name]
                #         resstr = db.executeCutover(processvo, params)
                #     else:
                #         raise wbxexception("dbcutover.executeOneStep(db_name=%s) not exist in srcdbDict" % db_name)
            resList = resstr.splitlines()
            logger.info(resstr)
            jobvo.status = "SUCCEED"
            if len(resList) > 0:
                for line in resList:
                    if line.find("WBXERROR") >= 0:
                        jobvo.status = "FAILED"
                lastline = resList[-1]
                if lastline.find("SUCCEED") < 0:
                    jobvo.status = "FAILED"
            jobvo.starttime = datetime.now()
            self.updateJobStatus(jobid, jobvo.status)
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")

    # this function log is not in job log. because this is backend operation. not in job actions
    def initializeFromDB(self, taskid):
        taskvo = super(wbxdbcutovertask,self).initializeFromDB(taskid)
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daomanagerfactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        try:
            daomanager.startTransaction()
            self._db_name = taskvo.parameter["db_name"]
            self._old_host_name = taskvo.parameter["old_host_name"]
            self._new_host_name = taskvo.parameter["new_host_name"]
            self._olddb = daomanagerfactory.getDatabaseByDBName(self._db_name)
            self._newdb = wbxdb(self._db_name, self._olddb.getSplexSid())
            self._serverDict.update(self._olddb.getServerDict())
            self._newdb.initFromServer(self._new_host_name)

            oldspportlist = self._olddb.getShareplexPortList()
            server = self._newdb.getServer(None)
            newspportDict = server.getShareplexPortListFromCRS()
            for oldspport in oldspportlist:
                port = oldspport.getPort()
                if port in newspportDict:
                    new_host_name = newspportDict[port]
                    server = self._newdb.getServer(new_host_name)
                    spport = wbxdbshareplexport(port, server)
                    spport.addDatabase(self._newdb, self._db_splex_sid)
                    server.addShareplexPort(spport)

            for spport in oldspportlist:
                for tgtdb in spport.getTgtDBList():
                    tgtport = tgtdb.getShareplexPort(spport.getPort())
                    tgtserver = tgtport.getServer()
                    tgtserver.verifyConnection()
                    self._tgtdbDict[tgtdb.getDBName()] = tgtdb
                for srcdb in spport.getSrcDBList():
                    srcport = srcdb.getShareplexPort(spport.getPort())
                    srcserver = srcport.getServer()
                    srcserver.verifyConnection()
                    self._srcdbDict[srcdb.getDBName()] = srcdb

            self._cutoverStepList = dao.getAutoTaskJobByJobid(taskid)
            daomanager.commit()
        except Exception as e:
            daomanager.rollback()
        finally:
            daomanager.close()

    # This function is used for systool, opdb cutover. Not used for ccp page
    # def cutover(self, db_role, module, action):
    #     spportList = []
    #     if module == "OTHER":
    #         if db_role == "OLD":
    #             spportList = self._olddb.getShareplexPortList()
    #         elif db_role == "NEW":
    #             spportList = self._newdb.getShareplexPortList()
    #
    #         for spport in spportList:
    #             logger.warning("%s shareplexport %s" % (action, spport.getPort()))
    #         logger.warning("-------%s steps-----" % len(spportList))
    #         iscontinue=input("continue?")
    #         if iscontinue.upper() != "Y":
    #             return
    #
    #         if action == "START":
    #             for spport in spportList:
    #                 spport.start()
    #             # self._thread_pool.map(self.startShareplexService, spportList)
    #         elif action == "STOP":
    #             for spport in spportList:
    #                 spport.stop()
    #
    #         elif action == "SETUP":
    #             for spport in spportList:
    #                 spport.preverifyForExistPort()
    #             self._newdb.getTablespace()
    #             self._thread_pool.map(self.installOrasetup, spportList)
    #     else:
    #         cutoverStepList = self.getCutoverStep(db_role, module, action)
    #         for cutoverstep in cutoverStepList:
    #             logger.warning(cutoverstep)
    #         logger.warning("-------%s steps-----" % len(cutoverStepList))
    #         # iscontinue = input("continue?")
    #         # if iscontinue.upper() != "Y":
    #         #     return
    #         # for cutoverstep in cutoverStepList:
    #         #     self.executeOneStep(cutoverstep)
    #         self._thread_pool.map(self.executeOneStep, cutoverStepList)

    def getCutoverStepList(self):
        return self._cutoverStepList
    #
    # def startShareplexService(self, spport):
    #     try:
    #         spport.startShareplexService()
    #     except Exception as e:
    #         logger.error("Error occurred when execute startShareplexService %s on server %s with errormsg %s" % (spport.getPort(), spport.getServer().getHostname(), e), exc_info=e)
    #
    # def stopShareplexService(self, spport):
    #     port = spport.getPort()
    #     host_name = spport.getServer().getHostname()
    #     try:
    #         spport.stopShareplexService()
    #     except Exception as e:
    #         logger.error("Error occurred when execute startShareplexService %s on server %s with errormsg %s" % (
    #         port, host_name, e), exc_info=e)

    # Generate all steps according to formal cutover process;
    # But for different db cutover, it may just need part steps or need additional steps. it is controlled at execution time
    # After all steps are inserted into depotdb, then load steps from depotdb into app
    # A db cutover can be preverify multiple times and in parallel; But generateCutoverStep is not allowed to be executed in parallel
    # db cutover steps can not be re-generated without manual impact
    def generateJobs(self):
        logger.info("generateCutoverStep(taskid=%s, db_name=%s)" % (self._taskid, self._db_name))

        oldserver = self._olddb.getServer(None)
        newserver = self._newdb.getServer(None)
        self.addJob(host_name=oldserver.getHostname(), db_name=self._db_name,
                    job_action="preverify",
                    process_order=1, execute_method="SYNC")
        ########################### DB Precutover ##############
        self.addJob(host_name=oldserver.getHostname(), db_name=self._db_name,job_action="executeOneStep",
                    stage="PRECUTOVER",server_type="OLD", module="DB",
                    process_order=2, execute_method="SYNC")
        self.addJob(host_name=newserver.getHostname(), db_name=self._db_name,job_action="executeOneStep",
                    stage="PRECUTOVER", server_type="NEW", module="DB",
                    process_order=2, execute_method="SYNC")
        ########################### DB PREPARE ##############
        self.addJob(host_name=oldserver.getHostname(), db_name=self._db_name,job_action="executeOneStep",
                    stage="PREPARE", server_type="OLD", module="DB",
                    process_order=11, execute_method="SYNC")
        # no PREPARE step for new db
        # self.addJob(host_name=newserver.getHostname(), db_name=self._db_name,job_action="executeOneStep",
        #             stage="PREPARE", server_type="NEW", module="DB",
        #             process_order=12, execute_method="SYNC")
        ########################### DB START ##############
        self.addJob(host_name=oldserver.getHostname(), db_name=self._db_name,job_action="executeOneStep",
                    stage="START", server_type="OLD", module="DB",
                    process_order=13, execute_method="SYNC")
        self.addJob(host_name=newserver.getHostname(), db_name=self._db_name,job_action="executeOneStep",
                    stage="START", server_type="NEW", module="DB",
                    process_order=14, execute_method="SYNC")
        ########################### DB POST ##############
        self.addJob(host_name=oldserver.getHostname(), db_name=self._db_name,job_action="executeOneStep",
                    stage="POST", server_type="OLD", module="DB",
                    process_order=15, execute_method="SYNC")
        self.addJob(host_name=newserver.getHostname(), db_name=self._db_name,job_action="executeOneStep",
                    stage="POST", server_type="NEW", module="DB",
                    process_order=16, execute_method="SYNC")

        self.addJob(host_name=oldserver.getHostname(), db_name=self._db_name, job_action="executeOneStep",
                    stage="SETENVIROMENT", server_type="OLD", module="DB", opaction="START",
                    process_order=7, execute_method="SYNC")
        self.addJob(host_name=newserver.getHostname(), db_name=self._db_name, job_action="executeOneStep",
                    stage="SETENVIROMENT", server_type="NEW", module="DB", opaction="START",
                    process_order=7, execute_method="SYNC")

        self.addJob(host_name=oldserver.getHostname(), db_name=self._db_name, job_action="executeOneStep",
                    stage="SETENVIROMENT", server_type="OLD", module="DB", opaction="STOP",
                    process_order=20, execute_method="SYNC")
        self.addJob(host_name=newserver.getHostname(), db_name=self._db_name, job_action="executeOneStep",
                    stage="SETENVIROMENT", server_type="NEW", module="DB", opaction="STOP",
                    process_order=20, execute_method="SYNC")

        ########################### Shareplex parts ##############

        for port, mapping in self._spportMapping.items():
            oldport = mapping["oldport"]
            newport = mapping["newport"]
            old_host_name = oldport.getServer().getHostname()
            new_host_name = newport.getServer().getHostname()
            self.addJob(host_name=old_host_name, db_name=self._db_name,splex_port=port,job_action="executeOneStep",
                        stage="PRECUTOVER", server_type="OLD", module="SHAREPLEX",
                        old_host_name=old_host_name,new_host_name=new_host_name,
                        process_order=3, execute_method="SYNC")
            self.addJob(host_name=new_host_name, db_name=self._db_name,splex_port=port,job_action="executeOneStep",
                        stage="PRECUTOVER", server_type="NEW", module="SHAREPLEX",
                        old_host_name=old_host_name, new_host_name=new_host_name,
                        process_order=4, execute_method="SYNC")
            self.addJob(host_name=old_host_name, db_name=self._db_name, splex_port=port,job_action="executeOneStep",
                        stage="PREPARE", server_type="OLD", module="SHAREPLEX",
                        old_host_name=old_host_name, new_host_name=new_host_name,
                        process_order=9, execute_method="SYNC")
            self.addJob(host_name=new_host_name, db_name=self._db_name, splex_port=port,job_action="executeOneStep",
                        stage="PREPARE", server_type="NEW", module="SHAREPLEX",
                        old_host_name=old_host_name, new_host_name=new_host_name,
                        process_order=10, execute_method="SYNC")
            self.addJob(host_name=new_host_name, db_name=self._db_name, splex_port=port,job_action="executeOneStep",
                        stage="POST", server_type="NEW", module="SHAREPLEX",
                        old_host_name=old_host_name, new_host_name=new_host_name,
                        process_order=17, execute_method="SYNC")

            for srcdb in oldport.getSrcDBList():
                srcport = srcdb.getShareplexPort(port)
                if srcport is None:
                    continue
                self.addJob(host_name=srcport.getServer().getHostname(), db_name=srcdb.getDBName(), splex_port=port,
                            stage="PRECUTOVER", server_type="SRC", module="SHAREPLEX", job_action="executeOneStep",
                            old_host_name=old_host_name, new_host_name=new_host_name, tgt_db_name=self._db_name,
                            process_order=5, execute_method="SYNC")
                self.addJob(host_name=srcport.getServer().getHostname(), db_name=srcdb.getDBName(), splex_port=port,
                            stage="PREPARE1", server_type="SRC", module="SHAREPLEX",job_action="executeOneStep",
                            old_host_name=old_host_name, new_host_name=new_host_name,tgt_db_name=self._db_name,
                            process_order=6, execute_method="SYNC")
                self.addJob(host_name=srcport.getServer().getHostname(), db_name=srcdb.getDBName(), splex_port=port,
                            stage="PREPARE", server_type="SRC", module="SHAREPLEX",job_action="executeOneStep",
                            old_host_name=old_host_name, new_host_name=new_host_name, tgt_db_name=self._db_name,
                            process_order=8, execute_method="SYNC")
                self.addJob(host_name=srcport.getServer().getHostname(), db_name=srcdb.getDBName(), splex_port=port,
                            stage="ACTIVATEPOSTCONFIGFILE", server_type="SRC", module="SHAREPLEX", job_action="executeOneStep",
                            old_host_name=old_host_name, new_host_name=new_host_name, tgt_db_name=self._db_name,
                            process_order=21, execute_method="SYNC")
                self.addJob(host_name=srcport.getServer().getHostname(), db_name=srcdb.getDBName(), splex_port=port,
                            stage="REMOVEQUEUE", server_type="SRC", module="SHAREPLEX",
                            job_action="executeOneStep",
                            old_host_name=old_host_name, new_host_name=new_host_name, tgt_db_name=self._db_name,
                            process_order=22, execute_method="SYNC")

        self.addJob(host_name=newserver.getHostname(), db_name=self._db_name, job_action="executeOneStep",
                    stage="REGISTER", server_type="NEW", module="DB", application_type=self._olddb.getApplicationType(),
                    appln_support_code=self._olddb.getApplnSupportCode(),process_order=18, execute_method="SYNC")
        self.addJob(host_name=newserver.getHostname(), db_name=self._db_name, job_action="executeOneStep",
                    stage="POSTCUTOVER", server_type="NEW", module="DB",
                    process_order=19, execute_method="SYNC")

    def getDBInfo(self, host_name, db_name):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        db = daoManagerFactory.getDatabaseByDBName(db_name)
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            depotDaoManager.startTransaction()
            dbinfo = depotdbDao.getDBInfo(host_name, db_name)
            depotDaoManager.commit()
            if dbinfo is None:
                raise wbxexception("Can not get the database from depotdb with host_name=%s, db_name=%s" % (host_name, db_name))
            return dbinfo
        except Exception as e:
            depotDaoManager.rollback()
            raise e
        finally:
            depotDaoManager.close()

    def isDataguardEnabled(self):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDaoManager(self._db_name)
        dao = daomanager.getDao(DaoKeys.DAO_DBAUDITDAO)
        try:
            daomanager.startTransaction()
            self._isDataguard = dao.isDataguardEnabled()
            daomanager.commit()
        except Exception as e:
            daomanager.rollback()
            raise e
        finally:
            daomanager.close()

