import logging
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

def listjobs(search_type,job_name,curpage,pagesize):
    res = {
        "status": "SUCCEED",
        "data": None
    }
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    defaultDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        defaultDaoManager.startTransaction()
        rows = depotdbDao.listjobs(search_type,job_name,curpage,pagesize)
        defaultDaoManager.commit()
        res["data"] =  rows
    except Exception as e:
        logger.error("Error occurred in jobsMonitor.listjobs(db_name=%s)" % (job_name))
        defaultDaoManager.rollback()
        res["status"] = "FAILED"
    finally:
        defaultDaoManager.close()
    return res

def jobmonitor(job_name,datadate):
    res = {
        "status": "SUCCEED",
        "data": None
    }
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    defaultDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        defaultDaoManager.startTransaction()
        rows = depotdbDao.chatbotjobmonitor(job_name,datadate)
        defaultDaoManager.commit()
        res["data"] = rows
    except Exception as e:
        logger.error("Error occurred in jobsMonitor.listjobs(db_name=%s)" % (job_name))
        defaultDaoManager.rollback()
        res["status"] = "FAILED"
    finally:
        defaultDaoManager.close()
    return res