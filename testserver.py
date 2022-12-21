import re
import time
import datetime
import sys


import logging.config
from common.Config import Config
# Do not remove this import, they will be initialize dynamically. otherwise, the project will start failed
from dao.depotdbdao import DepotDBDao
from dao.dbauditdao import DBAuditDao
from dao.shareplexmonitordao import ShareplexMonitorDao
from dao.configdbdao import ConfigDBDao
from dao.dbcutoverdao import DBCutoverDao
from dao.autotaskdao import autoTaskDao
from dao.cronjobmanagementdao import CronjobManagementDao
from dao.wbxdaomanager import wbxdaomanagerfactory
from biz.dbmanagement.wbxdb import wbxdb
from biz.dbmanagement.wbxdbuser import wbxdbuser
from dao.vo.depotdbvo import wbxschema
from biz.DepotDBResource import loadDepotDBInfo, addcronjoblog
from view import loadFlask
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from apscheduler.executors.pool import ThreadPoolExecutor as APThreadPoolExecutor
from collections import OrderedDict
import platform
from biz.KafkaMonitor import checkOneCronStatus

logger = None

def init():
    global logger
    config = Config.getConfig()
    logconfigfile = config.getLoggerConfigFile()
    logging.config.fileConfig(logconfigfile)
    logger = logging.getLogger("DBAMONITOR")
    logger.info(sys.version_info)
    logger.info(sys.modules)

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

def showres(jobvo):
    print("currenttime is %s" % jobvo.func)

def testdbHealthExamination():
    from biz.DBAuditDailyJob import dbHealthExamination
    # dbHealthExamination("txdbormt04_RACKTWEB", None, "zhiwliu@cisco.com","N")
    dbHealthExamination("sjdbth39_TSJ12", None, "zhiwliu@cisco.com","N")

def testShareplexupgrade():
    from biz.ShareplexUpgrade import precheckShareplex, upgradeShareplex
    host_name = "sjdbormt046"
    splex_port = "19062"
    splex_oldversion = "8.6.3"
    splex_newversion = "9.2.1"
    statuslist = precheckShareplex(host_name, splex_port, splex_oldversion)
    print(statuslist)
    # return
    upgradeShareplex(host_name, splex_port, splex_oldversion, splex_newversion)

def testShareplexMonitor():
    from biz.ShareplexMonitor import monitorCRByPort
    splex_port = "19009"
    host_name="txdbormt041.webex.com"
    monitorCRByPort(host_name, splex_port)

def testServer():
    from common.wbxssh import wbxssh
    server = wbxssh("sjgrcabt102.webex.com",22)

def testDBlinkMonitor():
    from biz.DBLinkMonitor import monitordblink
    # monitordblink("txdbormt04_RACKTWEB")
    # monitordblink("sjdbcfg1_CONFIGDB")
    # monitordblink("sjdbop_RACOPDB")
    # monitordblink("sjdblkup_SJLOOKUP")
    monitordblink("sjdbrpt_RACSJRPT")

def testCRMonitor():
    from biz.ShareplexMonitor import monitorCREnabled, monitorCRByPort
    # monitorCREnabled("abdblkup_ABLOOKUP")
    # monitorCREnabled("sjdblkup_SJLOOKUP")
    # monitorCREnabled("sjdbcfg1_CONFIGDB")
    monitorCREnabled("sjdbop_RACOPDB")
    # monitorCREnabled("sjdbrpt_RACSJRPT")
    # monitorCREnabled("sjdbth39_TSJ12")

def testDBPatchDeployment():
    from biz.DBAuditDailyJob import getdbpatchDeployment
    getdbpatchDeployment("txdbormt04_RACKTWEB")
    # getdbpatchDeployment("sjdbcfg1_CONFIGDB")

def testOthers():
    from biz.DBPatchJob import listshareplexmonitorresult
    sumlist = listshareplexmonitorresult()
    print(sumlist)

def testGetShareplexDelay():
    import json
    from biz.DBAuditJob import getConfigDBShareplexDelay
    dbResList = getConfigDBShareplexDelay("2020-03-31 00:00:00", "2020-03-31 08:00:00")
    print(json.dumps(dbResList))

def testGetMeetingDataDelay():
    import json
    from biz.DBAuditJob import getMeetingDataReplicationDelay
    dbResList = getMeetingDataReplicationDelay("AB", "2020-05-27 00:00:00", "2020-05-28 00:00:00")
    print(json.dumps(dbResList))


def testGenerateAWRReport():
    from biz.DBAuditJob import generateAWRReport, listDBInstanceName
    db_name = "RACKTWEB"
    res =listDBInstanceName(db_name)
    if res["status"] == "SUCCESS":
        instanceDict = res["data"]
        for instance_name, instance_number in instanceDict.items():
            generateAWRReport(db_name,"2020-05-05 07:40:18","2020-05-05 07:41:18",instance_number)

def testssh():
    from common.wbxssh import wbxssh
    host_name="txdbormt040"
    pwd="Rman$1357"
    server = wbxssh(host_name, 22, "oracle",pwd)
    try:
        server.connect()
        cmd = "sh /home/oracle/test.sh"
        server.exec_command(cmd)

    finally:
        server.close()


from common.wbxutil import wbxutil
import datetime
import sys
from biz.CronjobManagement import shutdownJobmanagerInstance, startJobmanagerInstance
if __name__ == "__main__":
    # print(sys.platform)
    init()
    checkOneCronStatus("sjdbormt046","stop")
    # shutdownJobmanagerInstance("rsdboradiag002")
    # startJobmanagerInstance("rsdboradiag002")
    # testGenerateAWRReport()
    # testGetShareplexDelay()
    # addcronjoblog(json.dumps(C_DATA))
    # testDBPatchDeployment()
    # testdbHealthExamination()
    # testCRMonitor()
    # testDBlinkMonitor()
    # testShareplexMonitor()
    # testShareplexupgrade()
    # testGetMeetingDataDelay()
    # testOthers()
    # upgradeShareplex(host_name, splex_port, action, splex_oldversion, splex_newversion):
    # action_list = ["start blackout", "stop cronjob", "stop port", "backup vardir", "Install binary", "execute ora_setup", "change service script file", "start port", "change cronjob", "start cronjob", "stop blackout"]
    # for action in action_list:
    #     upgradeShareplex(host_name,splex_port,action,splex_oldversion, splex_newversion)
    # from biz.DBAuditJob import userLoginVerification
    # userLoginVerification("sjdbormt01_RACVNMMP", "zhiwliu@cisco.com", "N")