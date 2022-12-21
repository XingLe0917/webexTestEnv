import time
import sys


import logging.config
from common.Config import Config
# Do not remove this import, they will be initialize dynamically. otherwise, the project will start failed
from dao.wbxdaomanager import wbxdaomanagerfactory
from biz.dbmanagement.wbxdb import wbxdb
from biz.dbmanagement.wbxdbuser import wbxdbuser
from biz.DepotDBResource import loadDepotDBInfo
from dao.depotdbdao import DepotDBDao
from dao.jobmanagerdao import JobManagerDao
from dao.dbauditdao import DBAuditDao
from dao.shareplexmonitordao import ShareplexMonitorDao
from dao.configdbdao import ConfigDBDao
from dao.dbcutoverdao import DBCutoverDao
from dao.autotaskdao import autoTaskDao
from dao.cronjobmanagementdao import CronjobManagementDao
from biz.autotask.wbxautotaskmanager import biz_autotask_initialize, biz_autotask_executejob, biz_autotask_listJobsByTaskid,biz_autotask_listtasks

logger = None

def init():
    global logger
    config = Config.getConfig()
    logconfigfile = config.getLoggerConfigFile()
    logging.config.fileConfig(logconfigfile)
    logger = logging.getLogger("DBAMONITOR")
    logger.info(sys.version_info)
    logger.info(sys.modules)

    # myhandler = wbxmem

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

def testShareplexupgrade():
    host_name = "rsdbmct1"
    port = 19062
    jobList = []

    # resDict = biz_autotask_initialize(task_type="SHAREPLEXUPGRADE_TASK", host_name=host_name, splex_port = port,splex_old_version="8.6.3",splex_new_version="9.2.1")
    # if resDict["status"] == "SUCCEED":
    #     jobList = resDict["joblist"]
    #     for jobvo in jobList:
    #         biz_autotask_executejob(jobvo.taskid, jobvo.jobid)

    # taskid = "3a62c077320f4e848b5841ead95d26bd"
    # resDict = biz_autotask_listJobsByTaskid(taskid)
    # for jobvo in resDict["data"]:
    #     if jobvo.status != 'SUCCEED':
    #         biz_autotask_executejob(jobvo.taskid, jobvo.jobid)

import json
def testShareplexInstallation():
    # print(json.dumps(biz_autotask_listtasks("SHAREPLEXINSTALL_TASK")))
    # resDict = biz_autotask_initialize(task_type="SHAREPLEXINSTALL_TASK", host_name="rsdbmct2", splex_port = 19062,
    #                         splex_version="9.2.1",db_name = "RACGMCT",splex_sid = "RACGMCT_SPLEX",root_dir = "/rsdbmct")
    resDict = biz_autotask_initialize(task_type="SHAREPLEXINSTALL_TASK", host_name="sjdbormt090", splex_port=19063,
                            splex_version="8.6.3",db_name = "RACOPDB",splex_sid = "RACOPDB_SPLEX",root_dir = "/sjdbormt")
    if resDict["status"] == "SUCCEED":
        jobList = resDict["joblist"]
        for jobvo in jobList:
            biz_autotask_executejob(jobvo.taskid, jobvo.jobid)

    taskid = "b527159db46a4247944eeae88d7c6de3"
    resDict = biz_autotask_listJobsByTaskid(taskid)
    for jobvo in resDict["data"]:
        if jobvo.status !='SUCCEED':
            biz_autotask_executejob(jobvo.taskid, jobvo.jobid)

def testTahoeBulid():
    # resDict = biz_autotask_initialize(task_type="TAHOEBUILD_TASK",
    #                                   base_host_name="tadbth392", base_db_name="tta39",
    #
    #                                   pri_host_name="sjdbth421", pri_db_name="tsjcomb6",
    #                                   gsb_host_name="tadbth391", gsb_db_name="ttacomb6",
    #
    #                                   pri_pool_name="tsj110",gsb_pool_name="tta110",
    #
    #                                   new_tahol_schema_name="tahoe110",
    #                                   port_for_configdb=17001,
    #                                   port_for_other=28989
    #                                   )
    #
    #
    # if resDict["status"] == "SUCCEED":
    #     jobList = resDict["joblist"]
    #     for jobvo in jobList:
    #         print(jobvo)


    # biz_autotask_executejob(taskid="b0e31a0108be462797478e4c2df9b7da", jobid='17bbf6615d8b447ca57ab02608ff1566')
    biz_autotask_executejob(taskid="a1df644ce7024cb094e525726bc521e5", jobid='8c63f66b87cf43ac946bf1e1049c1957')

def testInfluxdbIssue():
    # resDict = biz_autotask_initialize(task_type="INFLUXDB_ISSUE_TASK",
    #                                   db_name="RACVNCSP",
    #                                   host_name="sjdbormt011",
    #                                   instance_name= "racvncsp1",
    #                                   self_heal="1",
    #                                   describe="There is no influxdb data in last 5 mins"
    #                                   )
    # resDict = biz_autotask_initialize(task_type="TAHOEDBCUTOVER_TASK",pri_pool_name="TSJ125", gsb_pool_name="TTA125",new_pri_db_name="SJ1TELDB",port_from_configdb=17005,port_to_opdb=18125)

    # if resDict["status"] == "SUCCEED":
    #     jobList = resDict["joblist"]
    #     for jobvo in jobList:
    #         print(jobvo)
    biz_autotask_executejob(taskid="a1df644ce7024cb094e525726bc521e5", jobid='8c63f66b87cf43ac946bf1e1049c1957')

    # biz_autotask_executejob(taskid="2a7aad133a1d41b8a9e0f338175c9113", jobid='651fc53330794c2d9c9e8445d055994d')

if __name__ == "__main__":
    init()
    # testShareplexInstallation()
    # testShareplexupgrade()
    # testTahoeBulid()
    testTahoeBulid()
    print("wait time")

