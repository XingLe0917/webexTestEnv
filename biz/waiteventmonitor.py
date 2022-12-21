import logging
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from common.wbxutil import wbxutil

logger = logging.getLogger("DBAMONITOR")

def listwaitevent(search_type, db_name):
    res = {
        "status": "SUCCEED",
        "data": None
    }
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    defaultDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        defaultDaoManager.startTransaction()
        rows = depotdbDao.listwaitevent(search_type, db_name)
        defaultDaoManager.commit()
        res["data"] = [row.to_dict() for row in rows]
    except Exception as e:
        logger.error("Error occurred in waiteventmonitor.listwaitevent(search_type=%s, db_name=%s)" % (search_type, db_name))
        defaultDaoManager.rollback()
        res["status"] = "FAILED"
    finally:
        defaultDaoManager.close()
    return res