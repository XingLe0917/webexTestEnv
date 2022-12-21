import logging
import threading
import re
import time
from common.wbxssh import wbxssh


from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

def checkCRConfigStatus(host_name,port_number):
    logger.info("checkCRConfigStatus host_name=%s,port_number=%s" % (host_name,port_number))
    res = {"status": "SUCCESS", "errormsg": "", "msg": ""}
    jobserver_hostname = "sjgrcabt104"
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    server = None
    try:
        # server = daoManagerFactory.getServer(jobserver_hostname)
        server = wbxssh(jobserver_hostname, "22", "oracle", "Rman$1357")
        if server is not None:
            server.connect()
            cmd = "date"
            res1 = server.exec_command(cmd)
            logger.info(res1)
            cmd = ". /home/oracle/.bash_profile;python3 /home/oracle/projects/wbxdbaudit/dbaudit.py SHAREPLEXCRMONITOR_JOB %s %s" %(host_name,port_number)
            logger.info(cmd)
            server.exec_command(cmd)
    except Exception as e:
        raise e
    finally:
        if server is not None:
            server.close()
    if port_number == "":
        res['msg']= "The job started! Server:(%s),Port:(all ports)\n Please wait about 20s to get result." %(host_name)
    else:
        res[
            'msg'] = "The job started! Server:(%s),Port:(%s)\n Please wait about 20s to get result." % (host_name, port_number)
    return res


def getSingleCRconfigStatus(host_name,port_number):
    logger.info("getCRConfigStatus start")
    res = {"status": "SUCCESS", "errormsg": "", "data": None, "count": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        crconfig_status_list = depotdbDao.getSplexCRStatus(host_name,port_number)
        if not crconfig_status_list:
            status = "FAIL"
            errormsg = "No Data"

        res['data'] = crconfig_status_list
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()
    return res


def getCRConfigStatus():
    logger.info("getCRConfigStatus start")
    res = {"status": "SUCCESS", "errormsg": "", "data": None, "count":None}
    global crconfig_status_list
    host_name = ""
    port_number = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        crconfig_status_list = depotdbDao.getSplexCRStatus(host_name,port_number)
        if not crconfig_status_list:
            status = "FAIL"
            errormsg = "No Data"

        res['data'] = crconfig_status_list
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()
    return res


def getCRLogCount():
    logger.info("getCRLogCount start")
    res = {"status": "SUCCESS", "errormsg": "", "data": None, "count":None}

    global crcount_list
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        crcount_list = depotdbDao.getSplexCRLogCount()
        if not crcount_list:
            status = "FAIL"
            errormsg = "No Data"

        res['data'] = crcount_list
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()
    return res

def getCRLogCountHistory(host_name,db_name,splex_port):
    logger.info("getCRLogCountHistory start")
    res = {"status": "SUCCESS", "errormsg": "", "data": None, "count":None}

    global crcount_list
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        crcount_list = depotdbDao.getCRLogCountHistory(host_name,db_name,splex_port)
        if not crcount_list:
            status = "FAIL"
            errormsg = "No Data"

        res['data'] = crcount_list
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()
    return res

#add cr for shareplex to fixFailed case
def fixFailedCR(host_name,port_number):
    logger.info("checkSplexParams host_name=%s,port_number=%s" % (host_name, port_number))
    res = {"status": "SUCCESS", "errormsg": "", "msg": ""}
    jobserver_hostname = "sjgrcabt104"
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    db_name = ""
    splex_sid = ''
    password = ''
    try:
        depotDaoManager.startTransaction()
        failedserver= depotdbDao.getSplexCRFailed(host_name,port_number)
        if failedserver == None:
            res["status"]="FAILED"
            res["errormsg"]="The server %s is correct, no need to fix"%(host_name)
            return res
        db_name = failedserver[2]
        splex_sid = failedserver[7]
        password = failedserver[6]
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    server = None
    try:
        # server = daoManagerFactory.getServer(jobserver_hostname)
        # server = wbxssh(jobserver_hostname, "22", "oracle", "Rman$1357")
        server = wbxssh(host_name, "22", "oracle", password)
        if server is not None:
            server.connect()
            # cmd = "date"
            # res1 = server.exec_command(cmd)
            # logger.info(res1)
            cmd = ". /home/oracle/.bash_profile;sh /staging/gates/addcr_for_shareplex.sh %s %s EXECUTE %s" % (
            db_name,port_number,splex_sid)
            logger.info(cmd)
            server.exec_command(cmd)
    except Exception as e:
        raise e
    finally:
        if server is not None:
            server.close()
    if port_number == "":
        res[
            'msg'] = "The job started! Server:(%s),Port:(all ports)\n Please wait about 20s to get latest parameters." % (
            host_name)
    else:
        res[
            'msg'] = "The job started! Server:(%s),Port:(%s)\n Please wait about 20s to get latest parameters." % (
        host_name, port_number)
    return res


