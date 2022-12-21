import logging
import logging.config
import json
import threading
from datetime import datetime
from biz.esactivedatasearcher import getActiveDiskUsage, getActiveUserSessionUsage
from biz.ESDataReporter import get_osw_data_from_es, getDBListForDBHealth
from common.Config import Config
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.vo.depotdbvo import wbxdatabase
from dao.vo.depotdbvo import wbxserver
from dao.vo.depotdbvo import wbxschema
from biz.DepotDBResource import loadDepotDBInfo
from dao.depotdbdao import DepotDBDao
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

if __name__ == "__main__":
    init()
    # host_name = "lndbormt025"
    # db_name = "RACAIWEB"
    host_name = "vadbormt011"
    db_name = "RACABWEB"

    start_time = "2020-04-23 07:00:00"
    end_time = "2020-04-23 09:00:00"
    # print(json.dumps(getDBListForDBHealth()))
    # print(json.dumps(get_osw_data_from_es("iostat", host_name, db_name,start_time, end_time)))
    print(json.dumps(get_osw_data_from_es("usersession", host_name, db_name, start_time, end_time)))
    # print(json.dumps(get_osw_data_from_es("meminfo", host_name, db_name, start_time, end_time)))
    # print(json.dumps(get_osw_data_from_es("mpstat", host_name, db_name, start_time, end_time)))
    # print(json.dumps(get_osw_data_from_es("ifconfig", host_name, db_name, start_time, end_time)))
    # print(json.dumps(get_osw_data_from_es("traceoutmetric", host_name, db_name, start_time, end_time)))
    # print(json.dumps(get_osw_data_from_es("osload", host_name, db_name, start_time, end_time)))
    # print(json.dumps(get_osw_data_from_es("osprocess", host_name, db_name, start_time, end_time)))
    # print(json.dumps(get_osw_data_from_es("SJC02", "queuestatus", host_name, db_name, start_time, end_time)))
    # print(json.dumps(get_osw_data_from_es("tablespacemetric", host_name, db_name, start_time, end_time)))
    # resList = getActiveDiskUsage("sjdbormt046")
    # resList = getActiveUserSessionUsage("RACAVWEB")
    # resList = getTablespaceMetric(db_name, start_time,end_time, ["message.DB_TABLESPACE_TOTALSIZE","message.DB_TABLESPACE_USEDSIZE"])
    # print(json.dumps(resList))
    # response1 = getTop10NetworkInterfaceReadBytes("SJC02","sjdbormt046", datetime(2020,3,12,7,0,0), datetime(2020,3,5,4,0,0))
    # response2 = getMemoryTotalSize("SJC02","sjdbormt046", datetime(2020,3,4,1,0,0), datetime(2020,3,5,4,0,0))
    # response3 = getMemoryFreeSize("SJC02","sjdbormt046", datetime(2020,3,4,1,0,0), datetime(2020,3,5,4,0,0))
    # response4 = getCPUUsagePercentage("SJC02","sjdbormt046", datetime(2020,3,4,1,0,0), datetime(2020,3,5,4,0,0))
    # response5 = getTotalUserSessionCount("SJC02","sjdbormt046", "RACAVWEB", datetime(2020,3,4,1,0,0), datetime(2020,3,5,4,0,0))
    # response6 = getActiveUserSessionCount("SJC02","sjdbormt046", "RACAVWEB", datetime(2020,3,4,1,0,0), datetime(2020,3,5,4,0,0))
    # response7 = getSPCaptureProcessTXCount("SJC02","sjdbormt046", "RACAVWEB", datetime(2020,3,4,1,0,0), datetime(2020,3,5,4,0,0))


