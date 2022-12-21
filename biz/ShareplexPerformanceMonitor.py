import os
from common.wbxssh import wbxssh
import time, datetime
import logging
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from common.wbxcache import curcache
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from sqlalchemy import Table, Column, MetaData, String, DateTime, Integer, and_, create_engine
from sqlalchemy.exc import  DBAPIError, DatabaseError
from sqlalchemy.pool import NullPool

logger = logging.getLogger("DBAMONITOR")


def get_shareplex_process_cpu_consumption(db_name, starttime, endtime):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data = depotdbDao.getWbxsplexPerformanceMonitor(db_name, starttime, endtime)
        depotDaoManager.commit()
    except DatabaseError as e:
        logger.error("getWbxsplexPerformanceMonitor met error %s" % e)
        raise wbxexception(
            "Error ocurred when get info from wbxsplex_performance_monitor table in DepotDB with msg %s" % e)
    rstList = []
    if not data:
        return {
            "status": "FAIL",
            "shareplex_process_cpu_consumption": rstList
        }
    for row in data:
        rowdict = {"db_name": row[0], "host_name": row[1], "port": row[2], "process_type": row[3], "costtime": row[4],
                   "monitortime": row[5]}
        rstList.append(rowdict)
    return {
        "status": "SUCCCEED",
        "shareplex_process_cpu_consumption": rstList
    }

def get_replication_delay_time(db_name, starttime, endtime):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data = depotdbDao.getWbxsplexReplicationDelayTime(db_name, starttime, endtime)
        depotDaoManager.commit()
    except DatabaseError as e:
        logger.error("getWbxsplexPerformanceMonitor met error %s" % e)
        raise wbxexception(
            "Error ocurred when get info from wbxsplex_performance_monitor table in DepotDB with msg %s" % e)
    rstList = []
    if not data:
        return {
            "status": "FAIL",
            "replication_delay_time": rstList
        }
    for row in data:
        rowdict = {"src_host": row[0], "src_db": row[1], "port": row[2], "replication_to": row[3], "tgt_host": row[4], "tgt_db": row[5], "lastreptime": row[6], "montime": row[7]}
        rstList.append(rowdict)
    return {
        "status": "SUCCCEED",
        "replication_delay_time": rstList
    }
