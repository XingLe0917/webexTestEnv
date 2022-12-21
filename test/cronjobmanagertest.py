import unittest
import json
import sys

import logging.config
from common.Config import Config
from dao.depotdbdao import DepotDBDao
from dao.tablespacedao import TablespaceDao
from dao.jobmanagerdao import JobManagerDao
from dao.dbauditdao import DBAuditDao
from dao.shareplexmonitordao import ShareplexMonitorDao
from dao.configdbdao import ConfigDBDao
from dao.dbcutoverdao import DBCutoverDao
from dao.cronjobmanagementdao import CronjobManagementDao
from flask import jsonify

from biz.CronjobManagement import listJobTemplate, addJobTemplate, deleteJobTemplate, \
                listJobManagerInstance, \
                shutdownJobmanagerInstance
from biz.CronjobManagement import startJobmanagerInstance, deleteJobmanagerInstance, listJobInstance, \
                deleteJobInstance, addJobInstance, pauseJobInstance, resumeJobInstance, monitorFailedJobManagerInstance

from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.vo.depotdbvo import wbxdatabase
from dao.vo.depotdbvo import wbxserver
from dao.vo.depotdbvo import wbxschema
from biz.DepotDBResource import loadDepotDBInfo
from dao.vo.cronjobmanagementvo import JobInstanceVO

logger = None

class CronbJobManagementTest(unittest.TestCase):
    def setUp(self):
        global logger
        config = Config.getConfig()
        logconfigfile = config.getLoggerConfigFile()
        logging.config.fileConfig(logconfigfile)
        logger = logging.getLogger("DBAMONITOR")
        logger.info(sys.version_info)
        logger.info(sys.modules)

        depotdb = wbxdatabase()
        depotdb.appln_support_code = "DEFAULT"
        config = Config.getConfig()
        (username, pwd, url) = config.getDepotConnectionurl()
        depotdb.connectioninfo = url
        server = wbxserver()
        depotdb.addServer(server)
        schema = wbxschema(schema=username, password=pwd)
        depotdb.addSchema(schema)
        daomanagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanagerFactory.addDatabase("DEFAULT", depotdb)
        loadDepotDBInfo()

    # def test_listjobtemplate(self):
    #     templatelist = listJobTemplate("send alert email")
    #     for  templatevo in templatelist:
    #         print(templatevo["jobname"])

    # def test_deletejobtemplate(self):
    #     templatelist = listJobTemplate()
    #     for templatevo in templatelist:
    #         if templatevo["jobname"] == "clean_fra":
    #             vostr = json.dumps(templatevo)
    #             deleteJobTemplate(vostr)

    def test_listJobManagerInstance(self):
        jobmanagerlist =  listJobManagerInstance(host_name=None)
        for jobmanagervo in jobmanagerlist:
            print(json.dumps(jobmanagervo))
        obj = json.dumps(jobmanagerlist)
        self.assertTrue(len(jobmanagerlist) > 0)

    # def test_shutdownJobManagerInstance(self):
    #     shutdownJobmanagerInstance("txdbormt041")

    # def test_startJobManagerInstance(self):
    #     startJobmanagerInstance("sjdbormt047")

    # def test_deleteJobManagerInstance(self):
    #     deleteJobmanagerInstance("txdbormt013")

    # def test_monitorFailedJobManagerInstance(self):
    #     monitorFailedJobManagerInstance()

    # def test_listJobInstance(self):
    #     instancelist = listJobInstance('txdbormt013')
    #     for instancevo in instancelist:
    #         print(json.dumps(instancevo))

    # def test_pauseJobInstance(self):
    #     pauseJobInstance("93725F6469B23642E0534309FC0AD06A")

    # def test_resumeJobInstance(self):
    #     resumeJobInstance("93725F6469B23642E0534309FC0AD06A")

    # def test_deleteJobInstance(self):
    #     deleteJobInstance("937BE89A52AFD1FEE0534209FC0A267B")

    # def test_addJobInstance(self):
    #     jsondata = {"host_name":"txdbormt013", "jobname":"send alert email","job_type":"CRON", "commandstr":"/u00/app/admin/dbarea/bin/alert_mail_11g.sh racatweb3",
    #                 "jobruntime":'{"minute":"10,30,50"}',"status":"PENDING"}
    #     addJobInstance(jsondata)
    #     print("aa")

if __name__ == "__main__":
    unittest.main()
