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

channel_tablelist = ''
def get_channel_tablelist(src_appln_support_code,tgt_appln_support_code):
    global channel_tablelist
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        channel_tablelist = depotdbDao.getCannelTableListByTwoApplicationSupportCode(src_appln_support_code,tgt_appln_support_code)
        if not channel_tablelist:
            status = "FAIL"
            errormsg = "failed to get channel table list"
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"channel_tablelist": channel_tablelist,
            "status": status,
            "errormsg": errormsg}

replication_tablelist = ''
def get_replication_tablelist(table_name):
    global replication_tablelist
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        replication_tablelist = depotdbDao.getReplicationTableListByTableName(table_name)
        if not replication_tablelist:
            status = "FAIL"
            errormsg = "failed to get replication table list"
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"replication_tablelist": replication_tablelist,
            "status": status,
            "errormsg": errormsg}
