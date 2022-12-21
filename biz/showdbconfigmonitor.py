import logging
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from common.wbxexception import wbxexception


logger = logging.getLogger("DBAMONITOR")


def show_db_config_monitor(db_name):
    if not db_name:
        raise wbxexception("show_db_config_monitor get db_name is null")
    status = "SUCCEED"
    errormsg = ""
    data_list = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data_list = depotdbDao.getdbconfigmonitordata(db_name)
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"data": data_list,
            "status": status,
            "errormsg": errormsg}

