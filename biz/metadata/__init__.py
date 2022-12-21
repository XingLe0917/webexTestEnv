import logging
from dao.wbxdaomanager import DaoKeys
from dao.wbxdaomanager import wbxdaomanagerfactory

logger = logging.getLogger("DBAMONITOR")

def get_session(key):
    daoManager = None
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    if key == DaoKeys.DAO_DEPOTDBDAO:
        daoManager = daoManagerFactory.getDefaultDaoManager()
    if key == DaoKeys.DAO_PGDEPOTDBDAO:
        daoManager = daoManagerFactory.getPGDefaultDaoManager()
    
    if daoManager is None:
        raise Exception("only Support depotdb and pg depotdb now!")

    dao = daoManager.getDao(key)

    daoManager.setLocalSession()
    return daoManager.getLocalSession()


def generate_insert_sql():
    #TODO
    ...


def generate_query_sql():
    #TODO
    ...


def generate_update_sql():
    #TODO
    ...


def generate_delete_sql():
    #TODO
    ...