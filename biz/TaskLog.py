import logging
import cx_Oracle

from common.wbxexception import wbxexception
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from cx_Oracle import DatabaseError

logger = logging.getLogger("DBAMONITOR")
def addTaskLog(job,host_name,port,parameters,status):
    logger.info("addTaskLog, job=%s host_name=%s port=%s parameters=%s status=%s"  % (job,host_name,port,parameters,status))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    result = {}
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        taskLs = depotdbDao.getTaskList(job,host_name,port)
        #status:  processing ,  finish , fail
        if len(taskLs)==0:
            depotdbDao.addTaskLog(job, host_name, port, parameters, status)
            result['opt'] = 'add'
        else:
            depotdbDao.updateTaskLog(job,host_name,port,parameters,status)
            result['opt'] = 'update'
        result['code'] = '0000'
        result['message'] = 'success'
        daoManager.commit()
        return result
    except Exception as e:
        daoManager.rollback()
        logger.error("addTaskLog error occurred", exc_info=e, stack_info=True)
    return None

def get_db_tns(db_name):
    # logger.info("get_db_tns, db_name=%s  " % (db_name))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    retn = {}
    try:
        daoManager.startTransaction()
        spDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rows = spDao.getDBTnsInfo(db_name)
        daoManager.commit()
        if len(rows) == 0:
            retn['code'] = 'fail'
            retn['message'] = " %s is invalid due to no tns info" % (db_name)
            return retn
        item = rows[0]
        trim_host = item['trim_host']
        listener_port = item['listener_port']
        service_name = "%s.webex.com" % item['service_name']
        value = '(DESCRIPTION ='
        if item['scan_ip1']:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                value, item['scan_ip1'], listener_port)
        if item['scan_ip2']:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                value, item['scan_ip2'], listener_port)
        if item['scan_ip3']:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                value, item['scan_ip3'], listener_port)
        value = '%s (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' % (
            value, service_name)
        retn['tns'] = value
        retn['trim_host'] = trim_host
        retn['code'] = 'success'
        return retn
    except Exception as e:
        daoManager.rollback()
        logger.error("get_db_tns error occurred", exc_info=e, stack_info=True)
        retn['code'] = 'fail'
        retn['message'] = str(e)
        return retn

def get_SQLResultByDB(db_name,sql):
    res = {"status": "SUCCESS", "errormsg": "","data":None}
    if "select" not in str(sql).lower():
        res["status"] = "FAILED"
        res["errormsg"] = "Only support select"
        return res
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    db = daoManagerFactory.getDatabaseByDBName(db_name)
    try:
        connUrl = db.getConnectionURL()
        logger.info(connUrl)
    except DatabaseError as e:
        logger.error("Can not getConnectionURL to db %s , %s" % (db_name,str(e)))
        raise wbxexception("Can not getConnectionURL to db %s" % (db_name))
    connect = cx_Oracle.connect("sys/sysnotallow@" + connUrl, mode=cx_Oracle.SYSDBA)
    try:
        cursor = connect.cursor()
        cursor.execute(sql)
        tables = cursor.fetchall()
        list = []
        col_name = cursor.description
        for row in tables:
            dict = {}
            for col in range(len(col_name)):
                key = col_name[col][0]
                value = row[col]
                dict[key] = value
            list.append(dict)
        res['data'] = list
    except Exception as e:
        raise wbxexception(str(e))
    return res