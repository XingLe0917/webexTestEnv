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

curcache = LRUCache(maxsize=1024)
job_status_map = {
    "OKOKOK": "SUCCEED",
    "ERROR": "FAILED"
}
TEO_ENV = "PROD"

logger = logging.getLogger("DBAMONITOR")


def get_teodb_failover_list():
    logger.info("Starting to get_teodb_failover_list...")
    status = "SUCCEED"
    errormsg = ""
    data = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data = depotdbDao.get_teodb_failover_list()
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        status = "FAIL"
        errormsg = str(e)
        logger.error("failed to get_teodb_failover_list by %s" % errormsg)
    finally:
        depotDaoManager.close()
    return {"status": status,
            "data": data,
            "errormsg": errormsg}


def get_teodb_failover_detail(taskid, jobid):
    logger.info("Starting to get_teodb_failover_detail (taskid=%s, jobid=%s)..." % (taskid, jobid))
    status = "SUCCEED"
    errormsg = ""
    data = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data = depotdbDao.get_teodb_failover_detail_log(taskid, jobid)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        status = "FAIL"
        errormsg = str(e)
        logger.error("failed to get_teodb_failover_detail(taskid=%s, jobid=%s) by %s" % (taskid, jobid, errormsg))
    finally:
        depotDaoManager.close()
    return {"status": status,
            "data": data,
            "errormsg": errormsg}


def check_teodb_status(db_name):
    db_name = db_name.upper()
    status = "OKOKOK"
    errormsg = ""
    status_code = 0
    try:
        listener_status = check_listener_status(db_name)  # online: 1 ; offline: 0
        if listener_status != 1:
            status_code = -1
            connection_count = check_connection_status(db_name)
            logger.info("connection_count:%s" % connection_count)
            if int(connection_count) > 5000:
                status_code = 0
    except Exception as e:
        status = "ERROR"
        errormsg = str(e)
        logger.error(errormsg)
        status_code = -1

    return {
        "errorCode": status,
        "status" : status_code,
        "errorMessage": errormsg
    }


def check_connection_status(db_name):
    logger.info("Starting to check_connection_status for %s..." % db_name)
    db_name = db_name.upper()
    connection_count = 0
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDaoManager(db_name)
    dao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
    try:
        daoManager.startTransaction()
        connection_count = dao.getteodbmachineconnection()
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        raise e
    finally:
        daoManager.close()
    return connection_count


def check_listener_status(db_name):
    logger.info("Starting to check_listener_status for %s..." % db_name)
    db_name = db_name.upper()
    status = 1
    server = None
    try:
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        dbvo = daoManagerFactory.getDatabaseByDBName(db_name)
        # pccp only support prod, if bts dbvo will be None, we will hard code to normal status
        # the operation will raise exception.
        if db_name in ("RACBTRPT", "BTRSRPT", "RACFRRPT", "RACAMRPT") or not dbvo:
            return 1
        teodb_status_cache_key = "TEODB_STATUS_%s" % db_name
        # only get real-time status when status cache is empty or expired
        if curcache.has(teodb_status_cache_key):
            status = curcache.get(teodb_status_cache_key)["status"]
        else:
            logger.info("Starting to get teodb status for %s..." % db_name)
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
            logger.info("Setting cache key %s : %s" % (teodb_status_cache_key, status))
            # set the status cache, the status has high priority of real-time status in case of frequently connection to server, in side of failover/failback operation, it will refresh per 3 mins
            curcache.add(teodb_status_cache_key, {"status": status}, ttl=3 * 60)
        # if failback cache is set, we will kill inactive session every 30 mins in cache alive time
        teodb_failback_cache_key = "TEODB_FAILBACK_%s" % db_name
        if curcache.has(teodb_failback_cache_key):
            logger.info("CACHE %s : %s " % (teodb_failback_cache_key, curcache.get(teodb_failback_cache_key)))
            starttime = curcache.get(teodb_failback_cache_key)["starttime"]
            last_execute_time = curcache.get(teodb_failback_cache_key)["last_execute_time"]
            total_seconds = (wbxutil.getcurrenttime() -
                             datetime.strptime(last_execute_time, "%Y-%m-%d %H:%M:%S")).total_seconds()
            # if internal arrived, we will kill inactive session and reset the failback cache, keeping the starttime and db_name, update the last_execute_time, set ttl into left seconds
            if total_seconds >= 30 * 60:
                kill_inactive_session(db_name)
                left_seconds = 24 * 60 * 60 - \
                               (wbxutil.getcurrenttime() - datetime.strptime(starttime, "%Y-%m-%d %H:%M:%S")).total_seconds()
                curcache.delete(teodb_failback_cache_key)
                logger.info("Setting cache key %s" % teodb_failback_cache_key)
                curcache.add(teodb_failback_cache_key, {"db_name": db_name, "starttime": starttime,
                                                        "last_execute_time": wbxutil.gettimestr()}, ttl=left_seconds)
    except Exception as e:
        logger.error(e)
        status = 0
    finally:
        if server:
            server.close()
    return status


def kill_inactive_session(db_name):
    logger.info("Starting to kill inactive session on db %s ..." % db_name)
    daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
    server = None
    try:
        dbvo = daomanagerfactory.getDatabaseByDBName(db_name)
        if not dbvo:
            logger.error("Can not get database with db_name=%s" % db_name)
        _pri_instance_dict = dbvo._instanceDict
        logger.info("pri db name %s exists" % db_name)
        server_list = []
        host_list = []
        for hostitem, instanceitem in _pri_instance_dict.items():
            serveritem = instanceitem.getServer()
            serveritem.verifyConnection()
            server_list.append(serveritem)
            host_list.append(hostitem)
        file_name = "/u00/app/admin/dbarea/bin/kill_session.sh"
        for server in server_list:
            if not server.isFile(file_name):
                logger.error("there is no file %s on server %s" % (file_name, server.hostname))
            cmd = """
localsid=`ps -ef|grep ora_smon|grep -i %s|grep -v grep |awk '{{print $NF}}'|awk -F '_' '{{print $NF}}'`
. /home/oracle/.bash_profile
sh %s $localsid""" % (db_name.lower(), file_name)
            logger.info("execute %s on %s" % (cmd, server.hostname))
            if TEO_ENV == "PROD":
                server.exec_command(cmd)
            server.close()
    except Exception as e:
        logger.error("Error occurred in kill_inactive_session:%s" % str(e))
    finally:
        if server:
            server.close()
    logger.info("Kill inactive session on db %s ended." % db_name)


def failover(**kwargs):
    status = "OKOKOK"
    errormsg = ""
    sp = None
    pri_db_name = kwargs["pri_db_name"].upper()
    gsb_db_name = kwargs["gsb_db_name"].upper()
    createby = kwargs["createby"]
    log_cache_id = "TEODB_FAILOVER_FROM_%s_TO_%s" % (pri_db_name, gsb_db_name)
    threadlocal.current_jobid = log_cache_id
    try:
        # check local listener service on pri/gsb works fine, then the failover allowed
        if check_listener_status(pri_db_name) != 1 or check_listener_status(gsb_db_name) != 1:
            raise wbxexception("the listener service on {0} or {1} is abnormal!".format(pri_db_name, gsb_db_name))
        sp = teodboperation().newInstance(pri_db_name, gsb_db_name)
        sp.stop_listener_service()
        # As Doris's requirements, since Jan 2022, load balance policy changed,we donot need to kill application connetcions mannually, the connections will failover automatically
        # sp.kill_application_connection()
        # the status has changed after failover, clear the status cache, it will be filled when checking
        teodb_status_cache_key = "TEODB_STATUS_%s" % pri_db_name
        logger.info("Clearing cache key %s" % teodb_status_cache_key)
        if curcache.has(teodb_status_cache_key):
            curcache.delete(teodb_status_cache_key)
        # the teodb_failback_cache_key is for timely killing inactive session, which should not keep running after new failover executed
        teodb_failback_cache_key = "TEODB_FAILBACK_%s" % pri_db_name
        if curcache.has(teodb_failback_cache_key):
            curcache.delete(teodb_failback_cache_key)
    except Exception as e:
        status = "ERROR"
        errormsg = str(e)
        logger.error(errormsg)
    finally:
        if sp:
            record_autotask_log("TEODB_FAILOVER", json.dumps({"pri_db_name":pri_db_name, "gsb_db_name": gsb_db_name, "operation_time": wbxutil.gettimestr()}), createby, job_status_map[status], log_cache_id)
    return {
        "errorCode": status,
        "errorMessage": errormsg
    }

def failback(**kwargs):
    pri_db_name = kwargs["pri_db_name"].upper()
    gsb_db_name = kwargs["gsb_db_name"].upper()
    createby = kwargs["createby"]
    log_cache_id = "TEODB_FAILOVER_FROM_%s_TO_%s" % (pri_db_name, gsb_db_name)
    threadlocal.current_jobid = log_cache_id
    sp = None
    status = "OKOKOK"
    errormsg = ""
    try:
        # check local listener service on pri is down while gsb's works fine, then the failback allowed
        if check_listener_status(pri_db_name) == 1 or check_listener_status(gsb_db_name) != 1:
            raise wbxexception("the listener service on {0} or {1} is abnormal!".format(pri_db_name, gsb_db_name))
        sp = teodboperation().newInstance(pri_db_name, gsb_db_name)
        sp.start_listener_service()
        # the listener service status will be changed after failback, clean the status cache, it will be filled when checking
        teodb_status_cache_key = "TEODB_STATUS_%s" % pri_db_name
        logger.info("Clearing cache key %s" % teodb_status_cache_key)
        if curcache.has(teodb_status_cache_key):
            curcache.delete(teodb_status_cache_key)
        # the cache is used to kill inactive session every 30 mins in 24 h after failback
        teodb_failback_cache_key = "TEODB_FAILBACK_%s" % pri_db_name
        if curcache.has(teodb_failback_cache_key):
            curcache.delete(teodb_failback_cache_key)
        logger.info("Setting cache key %s" % teodb_failback_cache_key)
        curcache.add(teodb_failback_cache_key, {"db_name": pri_db_name, "starttime": wbxutil.gettimestr(),
                                                "last_execute_time": wbxutil.gettimestr()}, ttl=24 * 60 * 60)
    except Exception as e:
        status = "ERROR"
        errormsg = str(e)
        logger.error(errormsg)
    finally:
        if sp :
            record_autotask_log("TEODB_FAILBACK", json.dumps({"pri_db_name":pri_db_name, "gsb_db_name": gsb_db_name, "operation_time": wbxutil.gettimestr()}), createby,  job_status_map[status], log_cache_id)
    return {
        "errorCode": status,
        "errorMessage": errormsg
    }


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


class teodboperation:
    def __init__(self, pri_db_name=None, gsb_db_name=None, pri_host=[], gsb_host=[]):
        self.pri_db_name = pri_db_name
        self.gsb_db_name = gsb_db_name
        self._pri_host = pri_host
        self._gsb_host = gsb_host
        self.tem_kill_connection_cmd_file = "/tmp/TEOFAILOVER_KILL_CONNECTION_CMD.sql"
        self.tem_kill_connection_cmd_filename = "TEOFAILOVER_KILL_CONNECTION_CMD.sql"

    def newInstance(self, pri_db_name, gsb_db_name):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        try:
            # check pri db exists
            _pri_db = daoManagerFactory.getDatabaseByDBName(pri_db_name)
            if not _pri_db:
                raise wbxexception("Can not get database with db_name=%s" % pri_db_name)
            # check pri hosts login
            _pri_instance_dict = _pri_db._instanceDict
            logger.info("pri db name %s exists" % pri_db_name)
            _pri_host = []
            for hostitem, instanceitem in _pri_instance_dict.items():
                serveritem = instanceitem.getServer()
                serveritem.verifyConnection()
                _pri_host.append(hostitem)
            logger.info("pri db servers %s ssh login verification passed" % _pri_host)
            _pri_host.sort()
            # check gsb db exists
            _gsb_db = daoManagerFactory.getDatabaseByDBName(gsb_db_name)
            if not _gsb_db:
                raise wbxexception("Can not get database with db_name=%s" % gsb_db_name)
            _gsb_instance_dict = _gsb_db._instanceDict
            logger.info("gsb db name %s exists" % gsb_db_name)
            # check gsb hosts login
            _gsb_host = []
            for hostitem, instanceitem in _gsb_instance_dict.items():
                serveritem = instanceitem.getServer()
                serveritem.verifyConnection()
                _gsb_host.append(hostitem)
            logger.info("gsb db servers %s ssh login verification passed" % _gsb_host)
            _gsb_host.sort()
        except Exception as e:
            raise e
        sp = teodboperation(pri_db_name, gsb_db_name, _pri_host, _gsb_host)
        return sp

    def stop_listener_service(self):
        logger.info("Starting to stop listener service...")
        server = None
        cmd = None
        retcode = "SUCCEED"
        host_name = self._pri_host[0]
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)

            cmd = "source /home/oracle/.bash_profile; srvctl stop listener -listener LISTENER"
            logger.info("%s on server %s" % (cmd, host_name))
            if TEO_ENV == "PROD":
                resmsg = server.exec_command(cmd, async_log=True)
                for host in self._pri_host:
                    cmd = "source /home/oracle/.bash_profile; crsstat | grep ora.LISTENER.lsnr | grep %s | awk '{print $3}'" % host
                    logger.info("%s on server %s" % (cmd, host_name))
                    resmsg = server.exec_command(cmd)
                    if "OFFLINE" not in resmsg:
                        raise wbxexception("Failed to stop Listener on %s" % host)
            logger.info("Stop listener service end.")
        except Exception as e:
            retcode = "FAILED"
            logger.error(e)
        finally:
            if server:
                server.close()
        if retcode == "FAILED":
            raise wbxexception("Failed to stop_listener_service on db {0} ({1})".format(self.pri_db_name, host_name))

    def kill_application_connection(self):
        logger.info("Starting to kill application connection...")
        cmd = None
        retcode = "SUCCEED"
        already_send_email = False
        for host_name in self._pri_host:
            server = None
            try:
                daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
                server = daomanagerfactory.getServer(host_name)

                if not already_send_email and TEO_ENV == "PROD":
                    _cmd = "select machine ,count(*) from %s where type='USER' and schemaname in ('BOSSRPT','XXRPTH') group by machine;" % "gv\$session"
                    cmd = generate_cmd_in_db(_cmd, self.pri_db_name)
                    data = server.exec_command(cmd)
                    # send email on connection status before failover
                    self.send_pre_failover_machine_connection(_cmd, data, host_name)
                    already_send_email = True
                cmd = """
                echo "" > %s
                """ % (self.tem_kill_connection_cmd_file)
                server.exec_command(cmd)
                cmd = "select 'alter system kill session ''' ||sid || ',' || serial# || ''' immediate;' from %s where machine not like '%s%%' and type='USER' and schemaname in ('BOSSRPT','XXRPTH');" % ("gv\$session", host_name.lower()[:-1])
                cmd = generate_cmd_in_db(cmd, self.pri_db_name, self.tem_kill_connection_cmd_file)
                logger.info(cmd)
                cmd_list = server.exec_command(cmd)
                exec_cmd_list = self.address_cmd_list(cmd_list)
                if not exec_cmd_list:
                    raise wbxexception("Error occurred with command %s" % (cmd))
                cmd = """
cd /tmp"""
                cmd_str = "@%s" % self.tem_kill_connection_cmd_filename
                cmd += generate_cmd_in_db(cmd_str, self.pri_db_name)
                logger.info(cmd)
                if TEO_ENV == "PROD":
                    kill_conn = server.exec_command(cmd)
                logger.info("Kill application connection end.")
            except Exception as e:
                retcode = "FAILED"
                logger.error(e)
            finally:
                if server:
                    server.close()
            if retcode == "FAILED":
                raise wbxexception(
                    "Failed to kill_application_connection on db {0} ({1})".format(self.pri_db_name, host_name))

    def address_cmd_list(self, cmd_list):
        cmds = cmd_list.split("\n")
        rst_list = []
        for cmd_item in cmds:
            if "alter system kill session" in cmd_item:
                rst_list.append("alter system kill session" + cmd_item.split("alter system kill session")[-1])
        rst_list = list(set(rst_list))
        return rst_list

    def send_pre_failover_machine_connection(self, cmd, machine_connection, hostname):
        logger.info("send mail on machine connections on server %s before teodb failover(from %s to %s) start." % (hostname, self.pri_db_name, self.gsb_db_name))

        if not machine_connection:
            return True

        emailcontent = """

Hi dba:

Here are all machine connections collected: 

%s \n
        """ % machine_connection
        emailcontent += "========================================================================\n"
        emailcontent += """
The cmd is %s""" % cmd

        emailtitle = "Report: machine connection before TEODB failover from {0} to {1}".format(
            self.pri_db_name, self.gsb_db_name)
        emailmsg = wbxemailmessage(emailtitle, emailcontent, receiver="cwopsdba@cisco.com")
        sendemail(emailmsg)
        logger.info("send mail on machine connections on server %s before teodb failover(from %s to %s) end." % (hostname, self.pri_db_name, self.gsb_db_name))

    def start_listener_service(self):
        logger.info("Starting to start listener service...")
        server = None
        cmd = None
        retcode = "SUCCEED"
        host_name = self._pri_host[0]
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)

            cmd = "source /home/oracle/.bash_profile; crsctl start resource ora.LISTENER.lsnr -unsupported"
            logger.info("%s on server %s" % (cmd, host_name))
            if TEO_ENV == "PROD":
                resmsg = server.exec_command(cmd)
            for host in self._pri_host:
                cmd = "source /home/oracle/.bash_profile; crsstat | grep ora.LISTENER.lsnr | grep %s | awk '{print $3}'" % host
                logger.info("%s on server %s" % (cmd, host_name))
                resmsg = server.exec_command(cmd)
                if "ONLINE" not in resmsg:
                    raise wbxexception("Failed to start Listener on %s" % host)
            logger.info("Start listener service end.")
        except Exception as e:
            retcode = "FAILED"
            logger.error(e)
        finally:
            if server:
                server.close()
        if retcode == "FAILED":
            raise wbxexception(e)

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

