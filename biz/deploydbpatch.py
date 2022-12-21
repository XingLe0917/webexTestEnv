import logging

from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

def getDbDeployChannel(src_db_name, target_schema_type,env):
    """
        get DB deploy shareplex channel info

        @params:
            :param src_db_name        : source db name
            :param target_schema_type        : target db schema type
            :param env      : db env

        @return: {"status", "SUCCESS/FAILED", "errormsg": "...", "data": None/[...]}
        """
    # logger.info("sentAlertToBot():: room_id=%s,to_person=%s,optional_flag=%s " % (room_id, to_person,optional_flag))
    logger.info("getDbDeployChannel, src_db_name=%s,target_schema_type=%s,env=%s" % (src_db_name, target_schema_type,env))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = dao.get_deploy_channel(str(src_db_name).upper(), str(target_schema_type).upper(),env)
        daoManager.commit()
        res['data'] = [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res



