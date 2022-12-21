import datetime
import logging
import uuid

from common.redisUtil import MyRedis
from common.sshConnection import SSHConnection
from common.wbxexception import wbxexception
from common.wbxssh import wbxssh
from dao.vo.alertvo import AlertVo
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

# def kafka_monitor(top):
#     r = MyRedis("sjgrcabt102.webex.com",6379,password='redispass').r.lrange('alertInfo', 0, top-1)
#     alerts = []
#     for item in r:
#         alerts.append(eval(item))
#     return alerts

def kafka_monitor(top):
    logger.info("kafka_monitor,top={0} " .format(top))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rows = dao.get_wbxmonitoralert2(top)
        list = [dict(vo) for vo in rows]
        res['data'] = list
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    finally:
        if daoManager is not None:
            daoManager.close()
    return res

def get_kafka_alert_threshold(metric_type,id):
    logger.info(
        "get_kafka_alert_threshold, metric_type=%s id=%s" % (metric_type,id))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        if id:
            list = depotdbDao.getkafka_monitor_threshold_by_id(id)
            return [vo.to_dict() for vo in list]
        list = depotdbDao.getkafka_monitor_threshold(metric_type)
        daoManager.commit()
        return [vo.to_dict() for vo in list]
    except Exception as e:
        daoManager.rollback()
        logger.error("get_kafka_alert_threshold error occurred", exc_info=e, stack_info=True)
    return None

def delete_alert_threshold(id):
    logger.info(
        "delete_alert_threshold, id=%s " % (id))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    result = {}
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        depotdbDao.delete_alert_threshold(id)
        result['code'] = '0000'
        result['message'] = 'success'
        daoManager.commit()
        return result
    except Exception as e:
        daoManager.rollback()
        logger.error("delete_alert_threshold error occurred", exc_info=e, stack_info=True)
    return None

def update_kafka_alert(alertVo,opt):
    logger.info("{} kafka alert ".format(opt))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    result = {}
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        if opt == 'add':
            depotdbDao.add_kafka_alert(alertVo)
        elif opt == 'update':
            depotdbDao.update_kafka_alert(alertVo)
        result['code'] = '0000'
        result['message'] = 'success'
        daoManager.commit()
        return result
    except Exception as e:
        daoManager.rollback()
        logger.error("add or update kafka alert error occurred", exc_info=e, stack_info=True)
    return None

def getCheckCronStatus():
    logger.info("getCheckCronStatus")
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    ls = []
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.getCheckCronStatus()
        daoManager.commit()
        for vo in list:
            item = dict(vo)
            if (item['status'] and "running" in item['status']) or item['db_agent_exist'] == "1":
                item['check_result'] = "1"
            else:
                item['check_result'] = "0"
            if item['status'] and "running" in item['status']:
                item['cron_status'] = "1"
            else:
                item['cron_status'] = "0"
            ls.append(item)
        return ls
    except Exception as e:
        daoManager.rollback()
        logger.error("getCheckCronStatus error occurred", exc_info=e, stack_info=True)
    return None

def checkOneCronStatus(host_name,opt):
    logger.info("checkOneCronStatus host_name=%s, opt=%s " % (host_name,opt))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    result = {}
    server = None
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        info = depotdbDao.getDBUserInfo(host_name)
        item = dict(info[0])
        is_exist_wbxjobinstance = item['is_exist_wbxjobinstance']
        server = daoManagerFactory.getServer(host_name)
        if server is not None:
            server.connect()
            # if server.login_user is None:
            #     raise wbxexception("Can not get login user info from DepotDB")
            # ssh = wbxssh(server.host_name, server.ssh_port, server.loginuser.username, server.loginuser.password)
            # ssh.connect()
            # conn = SSHConnection(host_name + '.webex.com', 22, item['username'], item['pwd'])
            if "start" == opt:
                # verify db exist
                cmd = "ps aux | grep -v grep | grep smon | wc -l"
                count = int(server.exec_command(cmd))
                # count = conn.exec_command(cmd)
                if count == 0:
                    result['code'] = '0002'
                    result['message'] = 'Sorry, DB not exist'
                    return result
                else:
                    cmd = "sudo /sbin/service crond restart"
                    r = server.exec_command(cmd)
                    # r = conn.exec_command(cmd)
                    logger.info("sudo /sbin/service crond restart ,host_name=%s,res=%s" %(host_name,r))
            cmd = '/sbin/service crond status'
            res = server.exec_command(cmd)
            resline = res.split("\n")
            if len(resline) > 1:
                for line in resline:
                    if "Active:" in line:
                        res = line.strip()
                        break
            logger.info("/sbin/service crond status ,host_name=%s,res=%s" % (host_name, res))
            logger.info("update table CRONJOBSTATUS, host_name=%s, status=%s,is_exist_wbxjobinstance=%s" % (
                host_name, res, is_exist_wbxjobinstance))
            depotdbDao.updateCronJobStatus(host_name, res, is_exist_wbxjobinstance)
            daoManager.commit()
            result['code'] = '0000'
            result['message'] = 'Success'
    except Exception as e:
        daoManager.rollback()
        result['code'] = '0001'
        result['message'] = 'Fail, please check it.'
        logger.error("checkOneCronStatus error occurred", exc_info=e, stack_info=True)
    finally:
        if server is not None:
            server.close()
    return result

if __name__ == "__main__":
    data = kafka_monitor(1)
    print(data)