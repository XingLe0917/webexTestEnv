import json
import logging
import threading
import re
import time
from common.wbxchatbot import wbxchatbot
from common.sshConnection import SSHConnection
from common.wbxexception import wbxexception
from common.wbxssh import wbxssh


from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

splex_params_list = ''
def getSplexParams(host_name,port,param,ismodified,curpage,pagesize):
    logger.info("CheckSplexParams host_name=%s, port=%s, param=%s, ismodified=%s, curpage=%s, pagesize=%s" % (host_name, port,param,ismodified,curpage,pagesize))
    res = {"status": "SUCCESS", "errormsg": "", "data": None, "count":None,"ports_num":"","ports_list":"",}

    global splex_params_list
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        splex_params_list = depotdbDao.getSplexParamsList(host_name,port,param,ismodified,curpage,pagesize)
        if not splex_params_list:
            status = "FAIL"
            errormsg = "No Data"
        res['data'] = [dict(vo) for vo in splex_params_list['paramslist']]
        res['count'] = splex_params_list['count']

        port_l = splex_params_list['ports_num']
        res['ports_num'] = len(port_l)
        res['ports_list'] = port_l

        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()
    return res

def addMonitorSplexParam(param_name):
    logger.info("Add Monitor Splex Param:%s" % (param_name))
    res = {"status": "SUCCESS", "errormsg": ""}

    param_ctgy = getParamCategoryByParam(param_name)
    if param_ctgy == "":
        res['status'] = "FAIL"
        res['errormsg'] = "Can't find param category"
        logger.info("Can't find Splex Param:%s in any Category, please check shareplex parameter:%s" % (param_name))
        return res
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        depotdbDao.addMonitorSplexParam(param_ctgy,param_name)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()
    return res

def getParamCategoryByParam(param_name):
    param_ctgy=''
    param_prefix = ["SP_ANL", "SP_CAP", "SP_OCT", "SP_DEQ",
                    "SP_CMP", "SP_OCF", "SP_COP", "SP_OSY", "SP_CPY",
                    "SP_XPT", "SP_IMP", "SP_SLG", "SP_OPO", "SP_OPX", "SP_QUE", "SP_ORD", "SP_SNMP", "SP_SYS"]
    param_dict = {
        'SP_ANL': 'analyze',
        'SP_OCT': 'capture',
        'SP_CAP': 'capture',
        'SP_DEQ': 'compare',
        'SP_CMP': 'compare',
        'SP_OCF': 'config',
        'SP_COP': 'cop',
        'SP_OSY': 'copy',
        'SP_CPY': 'copy',
        'SP_XPT': 'export',
        'SP_IMP': 'import',
        'SP_SLG': 'logging',
        'SP_OPO': 'post',
        'SP_OPX': 'post',
        'SP_QUE': 'queue',
        'SP_ORD': 'read',
        'SP_SNMP': 'SNMP',
        'SP_SYS': 'system',
    }

    for param in param_prefix:
        if param in param_name:
            print(param)
            param_ctgy = param_dict.get(param)
            logger.info("Find Splex Param:%s in Category:%s" % (param,param_ctgy))

    return param_ctgy

def removeMonitorSplexParam():
    pass

#check one server/port by optional
def checkSplexParams(host_name, port_number):
    logger.info("checkSplexParams host_name=%s,port_number=%s" % (host_name,port_number))
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
            cmd = ". /home/oracle/.bash_profile;python3 /home/oracle/projects/wbxdbaudit/dbaudit.py SHPLEXPARAMDETAIL_JOB %s %s" %(host_name,port_number)
            logger.info(cmd)
            server.exec_command(cmd)
    except Exception as e:
        raise e
    finally:
        if server is not None:
            server.close()
    if port_number == "":
        res['msg']= "The job started! Server:(%s),Port:(all ports)\n Please wait about 20s to get latest parameters." %(host_name)
    else:
        res[
            'msg'] = "The job started! Server:(%s),Port:(%s)\n Please wait about 20s to get latest parameters." % (host_name, port_number)
    return res

def getSplexParamsServerHostname():
    logger.info("get splex server hostname for fuzzy query")
    res = {"status": "SUCCESS", "errormsg": "", "data": None}

    # global splex_params_list
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        splex_server_hostname_list = depotdbDao.getShplexParamsServerHostname()
        if not splex_server_hostname_list:
            status = "FAIL"
            errormsg = "No Data"
        res['data']=splex_server_hostname_list
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()
    return res