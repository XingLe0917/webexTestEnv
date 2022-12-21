import logging
import threading
import re
import time
from common.wbxssh import wbxssh


from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")


def getSqlExecutionPlan(trim_host,db_name,sql_id,curpage, pagesize):
    logger.info("get sql execution plan start")
    status = "SUCCESS"
    errormsg = ""
    sql_execution_plan_list = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        sql_execution_plan_list = depotdbDao.getSqlExecutionPlan(trim_host,db_name,sql_id,curpage, pagesize)
        if not sql_execution_plan_list:
            status = "FAIL"
            errormsg = "No Data"

        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"sql_execution_plan_list": sql_execution_plan_list,
            "status": status,
            "errormsg": errormsg}

def getSqlExecutionPlanDetail(trim_host,db_name,sql_id):
    logger.info("get sql execution plan detail info")
    status = "SUCCESS"
    errormsg = ""
    sql_execution_plan_detail_list = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        sql_execution_plan_detail_list = depotdbDao.getSqlExecutionPlanDetail(trim_host,db_name,sql_id)
        if not sql_execution_plan_detail_list:
            status = "FAIL"
            errormsg = "No Data"

        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"sql_execution_plan_detail_list": sql_execution_plan_detail_list,
            "status": status,
            "errormsg": errormsg}