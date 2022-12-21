from biz.wbxdbcutover import dbcutover_preverify, generateCutoverStep, saveCutoverStep,listCutoveredDBs, listDBCutoverStep, executeCutoverStep
import logging
import logging.config
import json
import threading
from datetime import datetime
from biz.esactivedatasearcher import getActiveDiskUsage, getActiveUserSessionUsage
from biz.ESDataReporter import get_osw_data_from_es, getDBListForDBHealth
from common.Config import Config
from dao.wbxdaomanager import wbxdaomanagerfactory
from biz.dbmanagement.wbxdb import wbxdb
from biz.dbmanagement.wbxdbserver import wbxdbserver
from biz.dbmanagement.wbxdbinstance import wbxdbinstance
from biz.dbmanagement.wbxdbuser import wbxdbuser
from biz.DepotDBResource import loadDepotDBInfo
from dao.depotdbdao import DepotDBDao
from dao.tablespacedao import TablespaceDao
from dao.jobmanagerdao import JobManagerDao
from dao.dbauditdao import DBAuditDao
from dao.shareplexmonitordao import ShareplexMonitorDao
from dao.configdbdao import ConfigDBDao
from dao.dbcutoverdao import DBCutoverDao
from dao.cronjobmanagementdao import CronjobManagementDao


local = threading.local()

def init():
    global logger
    config = Config.getConfig()
    logconfigfile = config.getLoggerConfigFile()
    logging.config.fileConfig(logconfigfile)
    logger = logging.getLogger("DBAMONITOR")

    depotdb = wbxdb("DEFAULT")
    depotdb.setApplnSupportCode("DEFAULT")
    config = Config.getConfig()
    (username, pwd, url) = config.getDepotConnectionurl()
    depotdb.setConnectionURL(url)
    dbuser = wbxdbuser(depotdb, username)
    dbuser.setPassword(pwd)
    depotdb.addUser(dbuser)
    daomanagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daomanagerFactory.addDatabase(depotdb, depotdb.getApplnSupportCode())
    loadDepotDBInfo()

from dao.vo.dbcutover import wbxdbcutoverprocessvo
import random
if __name__ == "__main__":
    init()
    # listCutoveredDBs()
    # resData = dbcutover_preverify("vadbormt011", "vadbormt030", "RACABWEB", "RACABWEB_SPLEX")
    # print(resData)
    # cutoverid = resData["cutoverid"]
    # resdict = generateCutoverStep(cutoverid)
    # print(resdict)
    # resdict=saveCutoverStep(cutoverid)
    # print(resdict)
    cutoverid = "6b9beb40bfed4b9e8b955934aae0b681"
    stepList = listDBCutoverStep(cutoverid)
    for processvo in stepList["data"]:
        executeCutoverStep(processvo["processid"], processvo["cutoverid"])
