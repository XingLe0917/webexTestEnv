import json
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

def get_db_parameter_list(db_name):
    pccp_parameter_list = []
    pccp_dba_hist_wr_control_list = []
    pccp_option_list = []
    pccp_dba_registry_list = []
    pccp_dba_autotask_client_list = []
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        pccp_parameter_list = depotdbDao.getPCCPParameterList(db_name)
        pccp_dba_hist_wr_control_list = depotdbDao.get_pccp_dba_hist_wr_control_list(db_name)
        pccp_option_list = depotdbDao.get_pccp_option_list(db_name)
        pccp_dba_registry_list = depotdbDao.get_pccp_dba_registry_list(db_name)
        pccp_dba_autotask_client_list = depotdbDao.get_pccp_dba_autotask_client_list(db_name)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"pccp_parameter_list": pccp_parameter_list,
            "pccp_dba_hist_wr_control_list": pccp_dba_hist_wr_control_list,
            "pccp_option_list": pccp_option_list,
            "pccp_dba_registry_list": pccp_dba_registry_list,
            "pccp_dba_autotask_client_list": pccp_dba_autotask_client_list,
            "status": status,
            "errormsg": errormsg}

def get_parameter_in_db_list(type,name):
    parameter_in_db_list = []
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        parameter_in_db_list = depotdbDao.get_parameter_in_db_list_list(type,name)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"parameter_in_db_list": parameter_in_db_list,
            "status": status,
            "errormsg": errormsg}

def get_failed_parameter_list():
    failed_parameter_list = []
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        failed_parameter_list = depotdbDao.get_failed_parameter_list()
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"failed_parameter_list": failed_parameter_list,
            "status": status,
            "errormsg": errormsg}

