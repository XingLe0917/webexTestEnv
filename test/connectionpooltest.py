import unittest
import json
import sys

import time
from dao.wbxdaomanager import DaoKeys
import logging.config
from common.Config import Config
from dao.depotdbdao import DepotDBDao
from dao.jobmanagerdao import JobManagerDao
from dao.dbauditdao import DBAuditDao
from dao.shareplexmonitordao import ShareplexMonitorDao
from dao.configdbdao import ConfigDBDao
from dao.dbcutoverdao import DBCutoverDao
from dao.cronjobmanagementdao import CronjobManagementDao
from dao.autotaskdao import autoTaskDao
from flask import jsonify
from biz.dbmanagement.wbxdb import wbxdb
from biz.dbmanagement.wbxdbuser import wbxdbuser
import threading

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

    def test_listJobManagerInstance(self):
        t1 = threading.Thread(target=self.listjob, name="thread1")
        t2 = threading.Thread(target=self.listjob, name="thread2")
        t3 = threading.Thread(target=self.listjob, name="thread3")
        t1.start()
        t2.start()
        t3.start()
        #pool overflow
        t4 = threading.Thread(target=self.listjob, name="thread4")
        t4.start()
        #pool full, should timeout
        t5 = threading.Thread(target=self.listjob, name="thread5")
        t5.start()
        t1.join()
        t2.join()
        #if no commit in listjob, no free connection, timeout too; If has commit in listjob, thread6 work well
        t6 = threading.Thread(target=self.listjob, name="thread6")
        t6.start()
        time.sleep(180)
        #if no commit, can not recycle connection, so timeout; otherwise connection already recycled, so work well
        t7 = threading.Thread(target=self.listjob, name="thread7")
        t7.start()
        print("end")

    def listjob(self):
        name = threading.current_thread().getName()
        print("start thread %s" % name)
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDefaultDaoManager()
        try:
            daoManager.startTransaction()
            dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            jobmanagerdict = dao.listJobManagerInstance()
            # if jobmanagerdict is not None:
            #     print(len(jobmanagerdict))
            print("executed SQL %s" % name)
            time.sleep(60)
            # daoManager.commit()
        except Exception as e:
            daoManager.rollback()
            logger.error("listJobManagerInstance error occurred with error %s" % str(e), exc_info=e, stack_info=True)
            return None
        # finally:
        #     daoManager.close()

if __name__ == "__main__":
    unittest.main()
