import logging
import uuid

from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

def setpgalertrule(**kargs):
    resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
    alert_type = kargs["alert_type"]
    alert_id = kargs.get("alert_id", "")
    host_name = kargs.get("host_name", "")
    db_name = kargs.get("db_name", "")
    splex_port = kargs.get("splex_port", "")
    alert_channel_type = kargs.get("alert_channel_type")
    alert_channel_value = kargs.get("alert_channel_value")
    comments = kargs.get("comments", "")
    is_public="1"
    if host_name or db_name or splex_port:
        is_public = "0"
    if '.webex.com' in host_name:
        host_name = str(host_name).split(".")[0]
    alert_title = alert_type
    if db_name:
        alert_title += "_" + db_name
    if host_name:
        alert_title += "_" + host_name
    if splex_port:
        alert_title += "_" + str(splex_port)

    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rulelist = dao.get_pg_alert_rule(alert_type,host_name,db_name,splex_port,alert_channel_type,alert_channel_value)
        if alert_id == "":
            logger.info("add new rule")
            if len(rulelist) == 0:
                alert_id = uuid.uuid4().hex
                dao.set_pg_alert_rule(alert_id,alert_type, host_name, db_name, splex_port, is_public, alert_channel_type,
                                      alert_channel_value, comments,alert_title)
            else:
                resDict["status"] = "FAILED"
                resDict["errormsg"] = "ERROR ! This rule already exists！"
        else:
            logger.info("update rule, alert_id=%s" %(alert_id))
            dao.update_pg_alert_rule(alert_id, host_name, db_name, splex_port, is_public, alert_channel_type,
                                  alert_channel_value, comments,alert_title)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        resDict["status"] = "FAILED"
        resDict["errormsg"] = str(e)
    return resDict

def getpgalertrule(alert_type):
    resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        alertrulelist = dao.list_pg_alert_rule(alert_type)
        resDict['data'] = [dict(vo) for vo in alertrulelist]
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        resDict["status"] = "FAILED"
        resDict["errormsg"] = str(e)
    return resDict

def deletepgalertrule(alert_id):
    resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        dao.delete_pg_alert_rule(alert_id)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        resDict["status"] = "FAILED"
        resDict["errormsg"] = str(e)
    return resDict

def getpgalertruletypes():
    resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = dao.get_pg_alert_rule_types()
        resDict['data'] = [dict(vo)['alert_type'] for vo in list]
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        resDict["status"] = "FAILED"
        resDict["errormsg"] = str(e)
    return resDict