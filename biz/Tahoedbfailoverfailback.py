import logging
import json
import base64
import threading
import uuid
from datetime import datetime
from requests.auth import HTTPBasicAuth
from common.wbxcache import getLog, removeLog
from common.wbxmail import wbxemailmessage, wbxemailtype, sendemail
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from sqlalchemy.exc import  DBAPIError, DatabaseError
from common.wbxexception import wbxexception
from common.wbxtask import wbxautotask
from common.wbxtask import threadlocal
from datetime import datetime
from collections import OrderedDict
from cacheout import LRUCache
from common.wbxutil import wbxutil
from dao.vo.autotaskvo import wbxautotaskvo, wbxautotaskjobvo
from dao.vo.depotdbvo import wbxmappinginfo

curcache = LRUCache(maxsize=1024)
TEO_ENV = "PROD"
status_map = {
    "all connected": "2",
    "pri connected": "1",
    "pri cut": "0",
    "unnormal": "-1"
}

logger = logging.getLogger("DBAMONITOR")


def tahoe_db_failover_first(**kwargs):
    db_name = kwargs["db_name"].upper()
    pool_name_list = kwargs["pool_name_list"]
    createby = kwargs["createby"]
    db_name = db_name.upper()
    status = "SUCCEED"
    errormsg = ""
    log_cache_id = "TAHOE_FAILOVER_FIRST_FROM_%s" % db_name
    threadlocal.current_jobid = log_cache_id
    sp = None
    try:
        sp = tahoedbpool.newInstance(pool_name_list, db_name)
        sp.stop_ha_service()
        sp.kill_application_connection("gsb")
    except Exception as e:
        status = "FAIL"
        errormsg = str(e)
    finally:
        if sp:
            record_autotask_log("TAHOEDB_FAILOVER", json.dumps(
                {"pool_name_list": pool_name_list, "db_name": db_name}),
                                createby, status, log_cache_id)
    return {
        "status": status,
        "errormsg": errormsg
    }


def tahoe_db_failover_second(**kwargs):
    db_name = kwargs["db_name"].upper()
    pool_name_list = kwargs["pool_name_list"]
    createby = kwargs["createby"]
    db_name = db_name.upper()
    status = "SUCCEED"
    errormsg = ""
    log_cache_id = "TAHOE_FAILOVER_SECOND_FROM_%s" % db_name
    threadlocal.current_jobid = log_cache_id
    sp = None
    try:
        sp = tahoedbpool.newInstance(pool_name_list, db_name)
        # sp.check_access()
        sp.kill_application_connection("pri")
        # record_automation("KILLPRIAPPCONNECTION", json.dumps({"pool_name": pool_name}))
    except Exception as e:
        status = "FAIL"
        errormsg = str(e)
    finally:
        if sp:
            record_autotask_log("TAHOEDB_FAILOVER", json.dumps(
                {"pool_name_list": pool_name_list, "db_name": db_name}),
                                createby, status, log_cache_id)
    return {
        "status": status,
        "errormsg": errormsg
    }


def tahoe_failback(**kwargs):
    db_name = kwargs["db_name"].upper()
    pool_name_list = kwargs["pool_name_list"]
    createby = kwargs["createby"]
    db_name = db_name.upper()
    status = "SUCCEED"
    errormsg = ""
    log_cache_id = "TAHOE_FAILBACK_TO_%s" % db_name
    threadlocal.current_jobid = log_cache_id
    sp = None
    try:
        sp = tahoedbpool.newInstance(pool_name_list, db_name)
        sp.failback()
        # record_automation("KILLPRIAPPCONNECTION", json.dumps({"pool_name": pool_name}))
    except Exception as e:
        status = "FAIL"
        errormsg = str(e)
    finally:
        if sp:
            record_autotask_log("TAHOEDB_FAILOVER", json.dumps(
                {"pool_name_list": pool_name_list, "db_name": db_name}),
                                createby, status, log_cache_id)
    return {
        "status": status,
        "errormsg": errormsg
    }


def check_tahoe_pool_status(**kwargs):
    pool_name_list = kwargs["pool_name_list"]
    db_name = kwargs["db_name"].upper()
    status = "SUCCEED"
    errormsg = ""
    result_dict = {}
    for pool_name in pool_name_list:
        pool_name = pool_name.upper()
        try:
            status_code = check_pool_status(db_name, pool_name)  # online: 1 ; offline: 0
        except Exception as e:
            status = "FAIL"
            errormsg = str(e)
            logger.error(errormsg)
            status_code = -1
        finally:
            result_dict.update({pool_name:status_code})
    result_dict.update({"status": status})
    return result_dict


def get_tahoedb_name_by_pool(**kwargs):
    pool_name_list = kwargs["pool_name_list"]
    status = "SUCCEED"
    rst_dict = {}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    for pool_name in pool_name_list:
        db_name = None
        try:
            pool_name = pool_name.upper()
            depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            depotDaoManager.startTransaction()
            db_name = depotdbDao.getDBNamebyPoolName(pool_name)
        except Exception as e:
            status = "FAIL"
            errormsg = str(e)
            logger.error(errormsg)
        finally:
            rst_dict.update({pool_name: db_name})
    rst_dict.update({"status": status})
    return rst_dict


def check_listener_status(db_name, pool_name_list):
    logger.info("Starting to check_listener_status for %s..." % db_name)
    db_name = db_name.upper()
    status = 1
    server = None
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    dbvo = daoManagerFactory.getDatabaseByDBName(db_name)
    for pool_name in pool_name_list:
        try:
            # pccp only support prod, if bts dbvo will be None, we will hard code to normal status
            # the operation will raise exception.
            tahoedb_status_cache_key = "TAHOE_STATUS_%s" % pool_name
            # only get real-time status when status cache is empty or expired
            if curcache.has(tahoedb_status_cache_key):
                status = curcache.get(tahoedb_status_cache_key)["status"]
            else:
                logger.info("Starting to get tahoedb status for %s..." % pool_name)
                _instance_dict = dbvo._instanceDict
                _server_list = []
                _host_list = []
                for hostitem, instanceitem in _instance_dict.items():
                    serveritem = instanceitem.getServer()
                    serveritem.verifyConnection()
                    _server_list.append(serveritem)
                    _host_list.append(hostitem)
                server = _server_list[0]

                # check local listener service status for all nodes one-time on one node
                for host in _host_list:
                    cmd = "source /home/oracle/.bash_profile; crsstat | grep ora.LISTENER.lsnr | grep %s | awk '{print $3}'" % host
                    resmsg = server.exec_command(cmd)
                    if "ONLINE" not in resmsg:
                        status = 0
                logger.info("Setting cache key %s : %s" % (tahoedb_status_cache_key, status))
                # set the status cache, the status has high priority of real-time status in case of frequently connection to server, in side of failover/failback operation, it will refresh per 3 mins
                curcache.add(tahoedb_status_cache_key, {"status": status}, ttl=3 * 60)
        except Exception as e:
            logger.error(e)
            status = 0
        finally:
            if server:
                server.close()
    return status


def check_pool_status(db_name, pool_name):
    status_map = {
        "all connected": "2",
        "pri connected": "1",
        "pri cut": "0",
        "unnormal": "-1"
    }
    db_name = db_name.upper()
    pool_name = pool_name.upper()
    tahoedb_status_cache_key = "TAHOE_STATUS_%s" % pool_name
    if curcache.has(tahoedb_status_cache_key):
        logger.info("CACHE %s : %s " % (tahoedb_status_cache_key, curcache.get(tahoedb_status_cache_key)))
        status = curcache.get(tahoedb_status_cache_key)["status"]
    else:
        logger.info("Starting to get tahoedb status for %s..." % pool_name)
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        dbvo = daoManagerFactory.getDatabaseByDBName(db_name)
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        status = status_map["unnormal"]
        try:
            depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            gsb_pool_name = depotdbDao.getGSBPoolByPoolname(pool_name)
            depotDaoManager.commit()
            _instance_dict = dbvo._instanceDict
            _server_list = []
            _host_list = []
            for hostitem, instanceitem in _instance_dict.items():
                serveritem = instanceitem.getServer()
                serveritem.verifyConnection()
                _server_list.append(serveritem)
                _host_list.append(hostitem)
            server = _server_list[0]
            server.connect()
            cmd = """
                    ps aux | grep lgwr | grep %s | awk -F ' ' '{print $NF}' | awk -F '_' '{print $NF}'
                    """ % db_name.lower()
            ha_name = server.exec_command(cmd)
            # print(ha_name)
            if not ha_name:
                logger.error("%s ha_name %s in server %s is null" % (pool_name, ha_name, server.host_name))
            ha_name = list(filter(None, ha_name.replace("$NF}'?", "").split("\n")))[0]
            cmd = """
. /home/oracle/.bash_profile
db
export ORACLE_SID=%s
sqlplus / as sysdba << EOF
select sum(case when instr(machine,'%s')>=0 then 1 else 0 end) pricount, sum(case when instr(machine,'%s')>=0 then 1 else 0 end) as gsbcount from gv\$session where machine like '%%%s%%' or machine like '%%%s%%' and lower(USERNAME)='%s';
exit;
EOF
""" % (ha_name, pool_name.lower(), gsb_pool_name.lower(), pool_name.lower(), gsb_pool_name.lower(), "tahoe")
            logger.info(cmd)
            rst = server.exec_command(cmd)
            # print(rst)
            pri_count, gsb_count = address_session_count(rst)
            if not pri_count.isdigit() or not gsb_count.isdigit():
                logger.error("pri_count %s or gsb_count %s is not digit" % (pri_count, gsb_count))
                status = status_map["unnormal"]
            if int(pri_count) > 0 and int(gsb_count) > 0:
                status = status_map["all connected"]
            elif int(pri_count) > 0 and int(gsb_count) == 0:
                status = status_map["pri connected"]
            else:
                status = status_map["pri cut"]
            logger.info("Setting cache key %s : %s" % (tahoedb_status_cache_key, status))
            curcache.add(tahoedb_status_cache_key, {"status": status}, ttl=3 * 60)
        except Exception as e:
            logger.info(e)
            status = status_map["unnormal"]
            if depotDaoManager:
                depotDaoManager.rollback()
        finally:
            if depotDaoManager:
                depotDaoManager.close()
            if server:
                server.close()
    return status


def address_session_count(rst):
    count_list = rst.split("\n")
    for i in range(0, len(count_list)):
        if "----------" in count_list[i]:
            if count_list[i + 1]:
                count_rst = count_list[i + 1].split()
                return count_rst[0], count_rst[1]
    return "0", "0"


class tahoedbpool:
    def __init__(self, db_name, primary_server, pool_name_list, pool_map):
        self.primary_server = primary_server
        self.pool_name_list = pool_name_list
        self.pool_map = pool_map
        self.pri_db_name = db_name
        self.schema = "tahoe"

    def close(self):
        for server in self.primary_server:
            server.close()

    @staticmethod
    def newInstance(pool_name_list, db_name):
        if not pool_name_list:
            raise wbxexception("pool_name can not be null")
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        pool_map = {}
        try:
            _pri_db = daoManagerFactory.getDatabaseByDBName(db_name)
            if not _pri_db:
                raise wbxexception("Can not get database with db_name=%s" % db_name)
            depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            for pool_name in pool_name_list:
                pool_name = pool_name.upper()
                depotdbDao.checkDBNameByPoolname(db_name, pool_name)
                gsb_pool_name = depotdbDao.getGSBPoolByPoolname(pool_name)
                pool_map.update({pool_name: gsb_pool_name})
            depotDaoManager.commit()
        except DatabaseError as e:
            logger.error(e)
            depotDaoManager.rollback()
            raise wbxexception(e)
        finally:
            depotDaoManager.close()
        primary_server = []
        _pri_instance_dict = _pri_db._instanceDict
        for hostitem, instanceitem in _pri_instance_dict.items():
            serveritem = instanceitem.getServer()
            serveritem.verifyConnection()
            primary_server.append(hostitem)
        # gsb_server = []
        # for host_item in gsb_host_name:
        #     server = wbxssh(host_item["host_name"], 22, "oracle", host_item["password"])
        #     try:
        #         server.connect()
        #     except Exception as e:
        #         raise wbxexception("cannot login the server %s with password in depot" % host_item["host_name"])
        #     gsb_server.append(server)
        sp = tahoedbpool(db_name, primary_server, pool_name_list, pool_map)
        return sp

    def stop_ha_service(self):
        logger.info("Starting to stop ha service...")
        server = None
        cmd = None
        retcode = "SUCCEED"
        host_name = self.primary_server[0]
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)
            cmd = ". /home/oracle/.bash_profile; crsstat | grep %s | grep %s | grep service" % (
            self.pri_db_name.lower(), host_name)
            rst = server.exec_command(cmd)
            for pool_name in self.pool_name_list:
                pool_name = pool_name.upper()
                logger.info("Starting to stop ha service for pool %s..." % pool_name)
                cmd = "source /home/oracle/.bash_profile; srvctl stop service -d %s -s %sha" % (self.pri_db_name.lower(), pool_name.lower())
                logger.info(cmd)
                # rst = server.exec_command(cmd)
                # logger.info(rst)
                #############time.sleep(5)
                cmd = "crsstat | grep '%sha' | awk '{print $3$4}'" % pool_name.lower()
                print(cmd)
                targetstate = server.exec_command(cmd)
                # for line in targetstate.split("\n"):
                    # if "OFFLINEOFFLINE" not in line:
                    #     raise wbxexception("Stop %s service on %s failed" % (pool_name, host_name))
                tahoedb_failback_cache_key = "TAHOE_STATUS_%s" % pool_name
                if curcache.has(tahoedb_failback_cache_key):
                    curcache.delete(tahoedb_failback_cache_key)
        except Exception as e:
            logger.error(e)
        finally:
            if server:
                server.close()
        if retcode == "FAILED":
            raise wbxexception("Failed to stop_ha_service on db {0} ({1})".format(self.pri_db_name, host_name))

    def failback(self):
        logger.info("Starting to start ha service...")
        server = None
        cmd = None
        retcode = "SUCCEED"
        host_name = self.primary_server[0]
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)
            cmd = ". /home/oracle/.bash_profile; crsstat | grep %s | grep %s | grep service" % (
                self.pri_db_name.lower(), host_name)
            rst = server.exec_command(cmd)

            for pool_name in self.pool_name_list:
                pool_name = pool_name.upper()
                logger.info("Starting to start ha service for pool %s..." % pool_name)
                cmd = "source /home/oracle/.bash_profile; srvctl start service -d %s -s %sha" % (
                self.pri_db_name.lower(), pool_name.lower())
                logger.info(cmd)
                # rst = server.exec_command(cmd)
                # logger.info(rst)
                #############time.sleep(5)
                cmd = "crsstat | grep '%sha' | awk '{print $3$4}'" % pool_name.lower()
                print(cmd)
                targetstate = server.exec_command(cmd)
                # for line in targetstate.split("\n"):
                #     if "ONLINEONLINE" not in line:
                #         raise wbxexception("Start %s service on %s failed" % (pool_name, host_name))
                tahoedb_failback_cache_key = "TAHOE_STATUS_%s" % pool_name
                if curcache.has(tahoedb_failback_cache_key):
                    curcache.delete(tahoedb_failback_cache_key)
        except Exception as e:
            logger.error(e)
        finally:
            if server:
                server.close()
        if retcode == "FAILED":
            raise wbxexception("Failed to start_ha_service on db {0} ({1})".format(self.pri_db_name, host_name))

    def kill_application_connection(self, db_type):
        logger.info("Starting to kill application connection...")
        cmd = None
        retcode = "SUCCEED"
        server = None
        host_name = self.primary_server[0]
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)
            cmd = """
ps aux | grep lgwr | grep %s | awk '{print %s}' | awk -F '_' '{print %s}'
""" % (self.pri_db_name.lower(), "$NF", "$NF")
            print(cmd)
            for pool_name in self.pool_name_list:
                pool_name = pool_name.upper()
                logger.info("starting to get kill connections command for pool %s" % pool_name)
                machine_like = pool_name if db_type == "gsb" else self.pool_map[pool_name]
                cmd = """
localsid=`ps -ef|grep ora_smon|grep -i %s|grep -v grep |awk '{{print $NF}}'|awk -F '_' '{{print $NF}}'`
. /home/oracle/.bash_profile
db
export ORACLE_SID=$localsid
sqlplus / as sysdba << EOF 
SET pagesize 0 linesize 1000 feedback off heading off echo off serveroutput on;
select 'alter system kill session ''' ||sid || ',' || serial# || ''' immediate;' from %s where USERNAME='%s' and regexp_like(machine,'^%s[[:alpha:]]');
exit;
EOF
""" % (self.pri_db_name, "gv\$session", self.schema.upper(), machine_like.lower())
                logger.info(cmd)
                cmd_list = server.exec_command(cmd)
                print(cmd_list)
                exec_cmd_list = self.address_cmd_list(cmd_list)
                if not exec_cmd_list:
                    raise wbxexception("Failed to get session from %s in %s" % (pool_name, server.host_name))
                cmd_str = ""
                for cmd in exec_cmd_list:
                    cmd_str = cmd_str + cmd + "\n"
                cmd_str += "exit;"
                for server_item in self.primary_server:
                    logger.info("starting to kill connections for pool %s on server %s" % (pool_name, server_item))
                    daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
                    _server = daomanagerfactory.getServer(server_item)
                    CMD = """
localsid=`ps -ef|grep ora_smon|grep -i %s|grep -v grep |awk '{{print $NF}}'|awk -F '_' '{{print $NF}}'`
. /home/oracle/.bash_profile
db
export ORACLE_SID=$localsid
sqlplus / as sysdba << EOF 
%s
EOF
""" % (self.pri_db_name, cmd_str)
                    logger.info(CMD)
                    # _server.exec_command(CMD)
                    tahoedb_failback_cache_key = "TAHOE_STATUS_%s" % pool_name
                    if curcache.has(tahoedb_failback_cache_key):
                        curcache.delete(tahoedb_failback_cache_key)
        except Exception as e:
            retcode = "FAILED"
            logger.error(e)
        finally:
            if server:
                server.close()
        if retcode == "FAILED":
            raise wbxexception(
                "Failed to kill_application_connection on db {0}".format(self.pri_db_name))


    def address_cmd_list(self, cmd_list):
        cmds = cmd_list.split("\n")
        rst_list = []
        for cmd_item in cmds:
            if "alter system kill session" in cmd_item:
                rst_list.append(cmd_item.split("SQL>")[-1])
        return rst_list

    def check_access(self):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDefaultDaoManager()
        dao = daoManager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        try:
            daoManager.startTransaction()
            # parameter = json.dumps(kwargs)
            taskvo = dao.getAutoTaskByParameter("KILLGSBAPPCONNECTION", json.dumps({"pool_name": self.pool_name}))
            print(taskvo)
            if taskvo is None:
                raise wbxexception("The first execution not executed before")
            create_time = taskvo.lastmodifiedtime
            now_time = datetime.now()
            durn = (now_time - create_time).seconds
            if int(durn) <= 1800:
                raise wbxexception("The duration time is shorter than 30 min")
            if int(durn) >= 86400:
                raise wbxexception("The duration time is over than 1 day")
            daoManager.commit()
        except Exception as e:
            daoManager.rollback()
            raise e
        finally:
            daoManager.close()


def record_autotask_log(taskType, args_str, createby, status, log_cache_id):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    dao = daoManager.getDao(DaoKeys.DAO_AUTOTASKDAO)
    try:
        daoManager.startTransaction()
        taskid = uuid.uuid4().hex
        taskvo = dao.getAutoTaskByTaskid(taskid)
        # parameter = json.dumps(kwargs)
        if taskvo is None:
            taskvo = wbxautotaskvo(taskid=taskid, task_type=taskType, parameter=args_str, createby=createby)
            dao.addAutoTask(taskvo)
        jobid = uuid.uuid4().hex
        log = getLog(log_cache_id)
        resultmsg1 = ""
        resultmsg2 = ""
        resultmsg3 = ""
        if log is not None and log != "":
            colwidth = 3900
            resList = [log[x - colwidth:x] for x in range(colwidth, len(log) + colwidth, colwidth)]
            resultmsg1 = str(resList[0])
            if len(resList) > 1:
                resultmsg2 = str(resList[1])
            else:
                resultmsg2 = ''
            if len(resList) > 2:
                resultmsg3 = str(resList[-1])
            else:
                resultmsg3 = ''
        jobvo = dao.getAutoTaskJobByJobid(jobid)
        if jobvo is None:
            jobvo = wbxautotaskjobvo(jobid=jobid,
                                     taskid=taskid,
                                     status=status,
                                     parameter = args_str,
                                     resultmsg1=resultmsg1,
                                     resultmsg2=resultmsg2,
                                     resultmsg3=resultmsg3)
            dao.addAutoTaskJob(jobvo)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        logger.info(str(e))
    finally:
        daoManager.close()
        removeLog(log_cache_id)


def generate_cmd_in_db(cmd, db_name, redirectfile=None):
    _tmpcmd = ""
    if redirectfile:
        _tmpcmd = "| tee -a %s" % redirectfile
    _cmd = """
localsid=`ps -ef|grep ora_smon|grep -i %s|grep -v grep |awk '{{print $NF}}'|awk -F '_' '{{print $NF}}'`
. /home/oracle/.bash_profile
db
export ORACLE_SID=$localsid
sqlplus -S / as sysdba << EOF %s
SET pagesize 0 linesize 1000 feedback off heading off echo off serveroutput on;
%s
exit;
EOF""" % (db_name.lower(), _tmpcmd, cmd)
    return _cmd

