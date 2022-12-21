import logging
import json
import base64
import threading
import uuid
from datetime import datetime
from requests.auth import HTTPBasicAuth
from common.wbxcache import getLog, removeLog
from common.wbxmail import wbxemailmessage, wbxemailtype, sendemail
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from sqlalchemy.exc import  DBAPIError, DatabaseError
from common.wbxexception import wbxexception
from common.wbxinfluxdb import wbxinfluxdb
from common.wbxtask import wbxautotask
from common.wbxtask import threadlocal
from datetime import datetime
from collections import OrderedDict
from cacheout import LRUCache
from common.wbxutil import wbxutil
from dao.vo.autotaskvo import wbxautotaskvo, wbxautotaskjobvo


logger = logging.getLogger("DBAMONITOR")


def get_homepage_db_version_count():
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to get_homepage_db_version_count...")
    data = {
        "name": "DB Version Count",
        "value": "db_version_count",
        "class": "grid-con-1",
        "total": 0,
        "data": []
    }
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        server_count_total = depotdbDao.get_homepage_server_info()
        data["total"] = server_count_total
        server_count_11g = depotdbDao.get_homepage_server_info("11g_db")
        data["data"].append({
            "value": server_count_11g,
            "name": "11g_db"
        })
        server_count_19c = depotdbDao.get_homepage_server_info("19c_db")
        data["data"].append({
            "value": server_count_19c,
            "name": "19c_db"
        })
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to get_homepage_server_count by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "data": data,
                "errormsg": errormsg}


def get_homepage_db_count():
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to get_homepage_db_count...")
    data = {
        "name": "DB Count",
        "value": "db_count",
        "class": "grid-con-2",
        "total": 0,
        "data": []
    }
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        db_count_total = depotdbDao.get_db_count_info()
        data["total"] = db_count_total
        for db_vendor in ["kafka_db", "oracle_db", "postgres_db", "mysql_db", "cassandra_db"]:
            db_count = depotdbDao.get_db_count_info(db_vendor)
            print(db_vendor, db_count)
            data["data"].append({
                "value": db_count,
                "name": db_vendor
            })
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to get_homepage_db_count by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "data": data,
                "errormsg": errormsg}


def get_homepage_db_type_count():
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to get_homepage_db_type_count...")
    data = {
        "name": "DB Type Count",
        "value": "db_type_count",
        "class": "grid-con-3",
        "total": 0,
        "data": []
    }
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        db_type_count_total = depotdbDao.get_db_count_info("oracle_db")
        data["total"] = db_type_count_total
        data["data"] = depotdbDao.get_db_type_count_info()
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to get_homepage_db_type_count by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "data": data,
                "errormsg": errormsg}


def get_shareplex_count():
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to get_shareplex_count...")
    data = {
        "name": "Splex Count",
        "value": "splex_count",
        "class": "grid-con-4",
        "total": 0,
        "data": []
    }
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data["data"] = depotdbDao.get_shareplex_count_info()
        data["total"] = depotdbDao.get_shareplex_count_total()
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to get_shareplex_count by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "data": data,
                "errormsg": errormsg}


def get_rencent_alert_info():
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to get_rencent_alert_info...")
    data = {"rencet_alert": []}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data["rencet_alert"] = depotdbDao.get_rencent_alert()
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to get_rencent_alert_info by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "data": data,
                "errormsg": errormsg}


def get_top_active_session_db_count():
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to get_top_active_session_db_count...")
    data = {"top_active_session_db": []}
    try:
        data["top_active_session_db"] = wbxinfluxdb().get_active_session_count()
    except Exception as e:
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to get_top_active_session_db_count by %s" % errormsg)
    finally:
        return {"status": status,
                "data": data,
                "errormsg": errormsg}
