import json
import logging
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from dao.vo.cronjobmanagementvo import JobManagerInstanceVO, JobTemplateVO, JobInstanceVO
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from sqlalchemy.exc import IntegrityError
from common.wbxssh import wbxssh
import cx_Oracle
from common.Config import Config
from sqlalchemy.exc import  DBAPIError, DatabaseError
import time

logger = logging.getLogger("DBAMONITOR")

metric_tablelist = ''
def get_metric_tablelist():
    global metric_tablelist
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        metric_tablelist = depotdbDao.getMetricList()
        if not metric_tablelist:
            status = "FAIL"
            errormsg = "No Data"
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"metric_tablelist": metric_tablelist,
            "status": status,
            "errormsg": errormsg}

def add_metric_setting(metric_name,job_name,warning_value,warning_channels,critical_value,critical_channels,operator,alerttype,db_name,instance_name,host_name,db_type):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        depotdbDao.addMetricSetting(metric_name,job_name,warning_value,warning_channels,critical_value,critical_channels,operator,alerttype,db_name,instance_name,host_name,db_type)
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        raise wbxexception(e)
    finally:
        depotDaoManager.close()

def update_metric_setting(metric_name,job_name,warning_value,warning_channels,critical_value,critical_channels,operator,alerttype,db_name,instance_name,host_name,db_type, thresholdid):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        depotdbDao.updateMetricSeeting(metric_name,job_name,warning_value,warning_channels,critical_value,critical_channels,operator,alerttype,db_name,instance_name,host_name,db_type, thresholdid)
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()

