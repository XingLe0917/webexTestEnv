import logging
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")


def getdblicences(**kargs):
    res = {
        "status": "SUCCEED",
        "data": None
    }
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    opdtDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = opdtDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)

    try:
        opdtDaoManager.startTransaction()
        rows = depotdbDao.getdblicences(**kargs)
        opdtDaoManager.commit()
        res["data"] = rows
    except Exception as e:
        logger.error("Error occurred in getdblicences:%s" %str(e) )
        opdtDaoManager.rollback()
        res["status"] = "FAILED"
        res["data"] = str(e)
    finally:
        opdtDaoManager.close()

    return res


def getdbliclabelinfo():
    res = {
        "status": "SUCCEED",
        "data": None
    }
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    opdtDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = opdtDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)

    try:
        opdtDaoManager.startTransaction()
        rows = depotdbDao.getdbliclabelinfo()
        opdtDaoManager.commit()
        res["data"] = rows
    except Exception as e:
        logger.error("Error occurred in getdblicences:%s" % str(e))
        opdtDaoManager.rollback()
        res["status"] = "FAILED"
        res["data"] = str(e)
    finally:
        opdtDaoManager.close()

    return res