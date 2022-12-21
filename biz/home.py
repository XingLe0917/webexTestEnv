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

def get_db_tablelist():
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        db_tablelist = depotdbDao.getDBList()
        if not db_tablelist:
            status = "FAIL"
            errormsg = "No Data"
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"db_tablelist": db_tablelist,
            "status": status,
            "errormsg": errormsg}


def get_host_tablelist():
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        host_tablelist = depotdbDao.getHostList()
        if not host_tablelist:
            status = "FAIL"
            errormsg = "No Data"
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"host_tablelist": host_tablelist,
            "status": status,
            "errormsg": errormsg}

def get_home_info():
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        task_tablelist = depotdbDao.getAutoTaskList()

        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"task_tablelist": task_tablelist,
            "status": status,
            "errormsg": errormsg}
