import sys

import logging.config
from common.Config import Config
# Do not remove this import, they will be initialize dynamically. otherwise, the project will start failed
from dao.depotdbdao import DepotDBDao
from dao.pgdepotdbdao import PGDepotDBDao
from dao.jobmanagerdao import JobManagerDao
from dao.dbauditdao import DBAuditDao
from dao.shareplexmonitordao import ShareplexMonitorDao
from dao.configdbdao import ConfigDBDao
from dao.dbcutoverdao import DBCutoverDao
from dao.ora2pgdao import ORA2PGDao
from dao.autotaskdao import autoTaskDao
from dao.cronjobmanagementdao import CronjobManagementDao
from dao.wbxdaomanager import wbxdaomanagerfactory
from biz.dbmanagement.wbxdb import wbxdb
from biz.dbmanagement.wbxdbserver import wbxdbserver
from biz.dbmanagement.wbxdbuser import wbxdbuser
from dao.vo.depotdbvo import wbxschema
from biz.DepotDBResource import loadDepotDBInfo, loadPGDepotDBInfo
from common.wbxjob import wbxjobmanager
from application import create_app

logger = None

def init():
    global logger
    config = Config.getConfig()
    logconfigfile = config.getLoggerConfigFile()
    logging.config.fileConfig(logconfigfile)
    logger = logging.getLogger("DBAMONITOR")

    daomanagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    config = Config.getConfig()
    (username, pwd, url) = config.getDepotConnectionurl()
    if url is not None:
        depotdb = wbxdb(config.getDepotdbname())
        depotdb.setApplnSupportCode("DEFAULT")
        depotdb.setDBVendor("Oracle")
        depotdb.setConnectionURL(url)
        dbuser = wbxdbuser(depotdb, username)
        dbuser.setPassword(pwd)
        depotdb.addUser(dbuser)
        daomanagerFactory.addDatabase(depotdb, depotdb.getApplnSupportCode())
        loadDepotDBInfo()

    (username, pwd, url) = config.getPGDepotConnectionurl()
    if url is not None:
        pgdepotdb = wbxdb(config.getPGDepotdbname())
        pgdepotdb.setApplnSupportCode("DEFAULT")
        pgdepotdb.setDBVendor("POSTGRESQL")
        pgdepotdb.setConnectionURL(url)
        dbuser = wbxdbuser(pgdepotdb, username)
        dbuser.setPassword(pwd)
        pgdepotdb.addUser(dbuser)
        daomanagerFactory.addDatabase(pgdepotdb, pgdepotdb.getApplnSupportCode())
        loadPGDepotDBInfo()

if __name__=='__main__':
    init()
    app = create_app()
    app.run(host="0.0.0.0", port=9000, debug=False)