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

splex_deny_user_list = ''
def get_splex_deny_user_list(curpage,pagesize):
    global splex_deny_user_list
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        splex_deny_user_list = depotdbDao.getSplexDenyUserList(curpage,pagesize)
        if not splex_deny_user_list:
            status = "FAIL"
            errormsg = "No Data"
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"splex_deny_user_list": splex_deny_user_list,
            "status": status,
            "errormsg": errormsg}
