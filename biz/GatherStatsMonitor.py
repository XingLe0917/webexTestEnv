import logging
import threading
import re
import time
from common.wbxssh import wbxssh


from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")


def getGatherStatus(trim_host,db_name,schema_name,curpage, pagesize):
    logger.info("getGatherStatus start")
    # res = {"status": "SUCCESS", "errormsg": "", "data": None, "count":None}
    status = "SUCCESS"
    errormsg = ""
    gather_status_list = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        gather_status_list = depotdbDao.getGatherStats(trim_host,db_name,schema_name,curpage, pagesize)
        if not gather_status_list:
            status = "FAIL"
            errormsg = "No Data"

        # res['data'] = gather_status_list
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"gather_stats_job_list": gather_status_list,
            "status": status,
            "errormsg": errormsg}