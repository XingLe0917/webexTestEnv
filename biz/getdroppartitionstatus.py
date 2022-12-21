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


def get_drop_partition_new_status(env):
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        drop_partition_status_list = depotdbDao.get_drop_partition_new_status(env)
        if not drop_partition_status_list:
            status = "FAIL"
            errormsg = "failed to get drop partition monitor list"
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"drop_partition_status_list": drop_partition_status_list,
            "status": status,
            "errormsg": errormsg}


def get_db_drop_partition_detail_status(db_name):
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        drop_partition_status_list = depotdbDao.get_drop_partition_status_by_dbname(db_name)
        if not drop_partition_status_list:
            status = "FAIL"
            errormsg = "failed to get %s drop partition monitor list" % db_name
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"drop_partition_status_list": drop_partition_status_list,
            "status": status,
            "errormsg": errormsg}


