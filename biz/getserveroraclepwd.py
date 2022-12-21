import os
from common.Config import Config
import base64
import logging
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from common.wbxcache import curcache
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from biz.dbmanagement.wbxdb import wbxdb
from sqlalchemy import Table, Column, MetaData, String, DateTime, Integer, and_, create_engine
from sqlalchemy.exc import  DBAPIError, DatabaseError
from sqlalchemy.pool import NullPool

logger = logging.getLogger("DBAMONITOR")


def get_db_metadata(data_type, data_value):
    if data_type not in ["host", "db", "schema"]:
        raise wbxexception("The data_type %s not support" % data_type)
    if data_type == "host":
        host_name = data_value.split(".")[0]
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            depotDaoManager.startTransaction()
            login_pwd = depotdbDao.getOracleUserPwdByHostname(host_name)
            if wbxutil.isNoneString(login_pwd):
                raise wbxexception("Can not get oracle user password on the server %s in DepotDB" % host_name)
            depotDaoManager.commit()
        except Exception as e:
            depotDaoManager.rollback()
            errormsg = "wbxdbcutoverserver.getOracleUserPwdByHostname(%s) with errormsg %s" % (host_name, e)
            logger.error(errormsg)
            raise wbxexception(errormsg)
        finally:
            depotDaoManager.close()
        return str(base64.b64encode(login_pwd.encode("utf-8")), "utf-8")
    elif data_type == "db":
        db_name = data_value.lower()
        if db_name == "auditdb":
            config = Config.getConfig()
            (username, pwd, url) = config.getDepotConnectionurl()
            return str(base64.b64encode(url.encode("utf-8")), "utf-8")
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            depotDaoManager.startTransaction()
            url = depotdbDao.getDBConnectionURL(db_name)
            if wbxutil.isNoneString(url):
                raise wbxexception("Can not get connection url on the db %s in DepotDB" % db_name)
            depotDaoManager.commit()
        except Exception as e:
            depotDaoManager.rollback()
            errormsg = "wbxdbcutoverserver.getDBConnectionURL(%s) with errormsg %s" % (db_name, e)
            logger.error(errormsg)
            raise wbxexception(errormsg)
        finally:
            depotDaoManager.close()
        return str(base64.b64encode(url.encode("utf-8")), "utf-8")


def getpoolinfobydbname(db_name):
    status = "SUCCEED"
    db_name = db_name.upper()
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        rst = depotdbDao.getpoolinfobydbNameorpoolName(db_name)
        if not rst:
            raise wbxexception("Can not get pool info on %s in DepotDB" % db_name)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        rst = "getpoolinfobydbname.getpoolinfobydbNameorpoolName(%s) with errormsg %s" % (db_name, e)
        logger.error(rst)
        status = "FAILED"
    finally:
        depotDaoManager.close()
    return {
        "status": status,
        "poolmeta": rst
    }
