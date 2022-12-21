import logging
import datetime
import base64
import uuid
import json
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from common.wbxexception import wbxexception
from common.wbxcache import curcache
from common.wbxutil import wbxutil
from common.wbxssh import wbxssh
from sqlalchemy.exc import  DBAPIError, DatabaseError
from dao.vo.autotaskvo import wbxautotaskvo
from collections import OrderedDict
import threading

logger = logging.getLogger("DBAMONITOR")

meetingDB_baseInfo = ''
def get_meetingDB_baseInfo():
    global meetingDB_baseInfo
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDaoManager('RACOPDB', 'test')
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        meetingDB_baseInfo = depotdbDao.getMeetingDBBaseInfo()
        if not meetingDB_baseInfo:
            status = "FAIL"
            errormsg = "failed to get meeting DB base info"
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"baseInfo": meetingDB_baseInfo,
            "status": status,
            "errormsg": errormsg}

meetingDB_hostNameList = ''
def get_meetingDB_dbName_by_hostName(host_name):
    global meetingDB_hostNameList
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDaoManager('RACOPDB', 'test')
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        meetingDB_hostNameList = depotdbDao.getMeetingDBDBNameByHostName(host_name)
        if not meetingDB_hostNameList:
            status = "FAIL"
            errormsg = "failed to get meeting DB dbschdma by dbname"
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"dbschdma": meetingDB_hostNameList,
            "status": status,
            "errormsg": errormsg}

meetingDB_tableList = ''
def get_meetingDB_tableList(db_name,host_name):
    global meetingDB_tableList
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDaoManager('RACOPDB', 'test')
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        meetingDB_tableList = depotdbDao.getMeetingDBTableList(db_name,host_name)
        if not meetingDB_tableList:
            status = "FAIL"
            errormsg = "failed to get meeting DB tableList"
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"tableList": meetingDB_tableList,
            "status": status,
            "errormsg": errormsg}

if __name__ == '__main__':
    result = get_meetingDB_baseInfo()
    print(result)


