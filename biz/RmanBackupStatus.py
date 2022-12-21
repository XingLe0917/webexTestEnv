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

logger = logging.getLogger("DBAMONITOR")

rman_backup_status_list = ''
def get_rman_backup_status_list():
    global rman_backup_status_list
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        rman_backup_status_list = depotdbDao.getRmanBackupStatusList()
        if not rman_backup_status_list:
            status = "FAIL"
            errormsg = "No Data"
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"rman_backup_status_list": rman_backup_status_list,
            "status": status,
            "errormsg": errormsg}

def check_rman_backup_status(host_name,db_name):
    cmd = None
    retcode = "SUCCEED"
    errormsg = ''
    db_name = db_name.upper()
    host_name = host_name.lower()
    daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daomanagerfactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    ha_name = depotdbDao.getinstanceNamebydbNameandhost(db_name, host_name)
    check_sql = '/staging/Scripts/oracle/11g/check_dd_backup_status/check_rman_status_4_one.sql'
    server = None
    try:
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        server = daomanagerfactory.getServer(host_name)
        server.connect()

        cmd = """
        . /home/oracle/.bash_profile
        db
        export ORACLE_SID=%s
        sqlplus / as sysdba << EOF 
        @%s
        exit
        EOF
        """ % (ha_name,check_sql)
        logger.info(cmd)
        cmdlog = server.exec_command(cmd)
        logger.info(cmdlog)

    except Exception as e:
        retcode = "FAILED"
        errormsg = e
        logger.error(e)
    finally:
        if server:
            server.close()
        if depotDaoManager:
            depotDaoManager.close()
    return {"status":retcode,"errormsg":errormsg}



