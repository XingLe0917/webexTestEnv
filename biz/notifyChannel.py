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

notify_tablelist = ''
def get_notify_tablelist():
    global notify_tablelist
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        notify_tablelist = depotdbDao.getNotifyList()
        if not notify_tablelist:
            status = "FAIL"
            errormsg = "No Data"
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"notify_tablelist": notify_tablelist,
            "status": status,
            "errormsg": errormsg}

def add_notify_channel(channel_name,channel_type,emails,teams):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        depotdbDao.addNotifyChannel(channel_name,channel_type,emails,teams)
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        raise wbxexception(e)
    finally:
        depotDaoManager.close()

def update_notify_channel(channel_name,channel_type,emails,teams,channel_id):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        depotdbDao.updateNotifyChannel(channel_name,channel_type,emails,teams,channel_id)
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
