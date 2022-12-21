import logging
import datetime
import base64
import uuid
import json
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from common.wbxexception import wbxexception
from common.wbxcache import curcache
from common.wbxutil import wbxutil
from common.wbxssh import wbxssh
from sqlalchemy.exc import  DBAPIError, DatabaseError
from dao.vo.autotaskvo import wbxautotaskvo
from collections import OrderedDict
import threading

logger = logging.getLogger("DBAMONITOR")


def get_telegraf_monitor_list():
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        telegraf_list = depotdbDao.getTelegrafMonList()
        if not telegraf_list:
            status = "FAIL"
            errormsg = "failed to get telegraf monitor list"
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"telegraf_list": telegraf_list,
            "status": status,
            "errormsg": errormsg}


def telegraf_action(host_name, action_type):
    status = "SUCCEED"
    errormsg = ""
    host_name = host_name.split(".")[0]
    if action_type not in ["start", "check", "install"]:
        return {
            "status": "FAIL",
            "errormsg": "the action %s not support" % action_type
        }
    if action_type == "start":
        sp = None
        try:
            sp = wbxtelegraf.newInstance(host_name)
            sp.login()
            sp.start_telegraf()
        except Exception as e:
            errormsg = str(e)
            status = "FAIL"
        finally:
            if sp is not None:
                sp.close()
        return {
            "status": status,
            "errormsg": errormsg
        }
    elif action_type == "check":
        sp = None
        try:
            sp = wbxtelegraf.newInstance(host_name)
            sp.login()
            sp.check_telegraf()
        except Exception as e:
            errormsg = str(e)
            status = "FAIL"
        finally:
            if sp is not None:
                sp.close()
        return {
            "status": status,
            "errormsg": errormsg
        }
    elif action_type == "install":
        sp = None
        try:
            sp = wbxtelegraf.newInstance(host_name)
            sp.login()
            sp.install_telegraf()
        except Exception as e:
            errormsg = str(e)
            status = "FAIL"
        finally:
            if sp is not None:
                sp.close()
        return {
            "status": status,
            "errormsg": errormsg
        }


class wbxtelegraf:
    def __init__(self, server):
        self.host_name = server.host_name
        self.server = server
        self.log_file = "/var/log/telegraf/telegraf.log"
        self.install_script = "/staging/gates/telegraf/install_telegraf.sh"

    def close(self):
        self.server.close()

    def login(self):
        self.server.connect()

    @staticmethod
    def newInstance(host_name):
        if not host_name:
            raise wbxexception("host_name can not be null")
        host_name = host_name.split(".")[0]

        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            depotDaoManager.startTransaction()
            ssh_pwd = depotdbDao.getOracleUserPwdByHostname(host_name)
            site_code = depotdbDao.getSitecodeByHostname(host_name)
            depotDaoManager.commit()
        except DatabaseError as e:
            logger.error("getUserPasswordByHostname getSitecodeByHostname met error %s" % e)
            raise wbxexception(
                "Error ocurred when get oracle user password and sitecode on the server %s in DepotDB with msg %s" % (
                    host_name, e))
        if wbxutil.isNoneString(ssh_pwd) or wbxutil.isNoneString(site_code):
            raise wbxexception(
                "Can not get oracle user password and site_code on the server %s in DepotDB" % host_name)

        # servervo = daoManagerFactory.getServer(host_name)
        # if servervo is None:
        #     raise wbxexception("can not get server info with hostname %s" % host_name)
        # ssh_port = servervo.ssh_port
        server = wbxssh(host_name, 22, "oracle", ssh_pwd)
        try:
            server.connect()
        except Exception as e:
            raise wbxexception("cannot login the server %s with password in depot" % host_name)
        sp = wbxtelegraf(server)
        return sp

    def start_telegraf(self):
        self.check_telegraf_installed()
        cmd = "sudo service telegraf start"
        self.server.exec_command(cmd)
        self.check_telegraf()

    def check_telegraf(self):
        try:
            status = 1
            error_time = ""
            error_content = ""
            try:
                self.check_telegraf_installed()
            except Exception as e:
                status = 2
                record_check(self.host_name, status, error_time, error_content)
                raise wbxexception(e)
            try:
                self.check_telegraf_running()
            except Exception as e:
                status = 0
                record_check(self.host_name, status, error_time, error_content)
                raise wbxexception(e)
            cmd = "date '+%Y-%m-%d %H:%M:%S'"
            checknow = self.server.exec_command(cmd)
            checknow = checknow.split("\n")[0]
            checknow = wbxutil.convertStringtoDateTime(checknow)
            checktime_list = []
            for i in range(0, 5):
                delta = datetime.timedelta(minutes=i)
                checktime = checknow - delta
                checktime_list.append(checktime.strftime("%Y-%m-%dT%H:%M"))
            for checktime in checktime_list:
                cmd = "sudo cat %s | grep %s | grep E!" % (self.log_file, checktime)
                error_log = self.server.exec_command(cmd)
                if error_log:
                    error_log.replace(" ", "").replace("\n", "")
                    error_time = error_log.split(" E! ")[0].strip().replace("T", " ").split("Z")[0]
                    error_content = error_log.split(" E! ")[-1].strip()
                    status = 3
                    record_check(self.host_name, status, error_time, error_content)
                    raise wbxexception(error_time + error_content)
            record_check(self.host_name, status, error_time, error_content)
        except Exception as e:
            raise wbxexception(e)

    def install_telegraf(self):
        if not self.server.isFile(self.install_script):
            raise wbxexception("the telegraf install script %s not exist on server %s" % (self.install_script, self.host_name))
        cmd = ". /home/oracle/.11g_db; sh %s" % self.install_script
        record_automation("INSTALLTELEGRAF_TASK", json.dumps(
            {"host_name": self.host_name}))
        self.server.execute_command(cmd)
        self.check_telegraf()

    def check_telegraf_installed(self):
        cmd = "sudo service telegraf status"
        is_existed = self.server.exec_command(cmd)
        if "unrecognized service" in is_existed:
            raise wbxexception("there is no telegraf service on server %s" % self.host_name)

    def check_telegraf_running(self):
        cmd = "sudo service telegraf status"
        is_running = self.server.exec_command(cmd)
        if "OK" not in is_running:
            raise wbxexception("telegraf service is stopped on server %s" % self.host_name)


def record_check(host_name, status, error_time, error_content):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        depotdbDao.inserttelegrafstatus(host_name, status, error_time, error_content)
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        raise wbxexception(e)
    finally:
        depotDaoManager.close()

def record_automation(taskType, args_str):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    dao = daoManager.getDao(DaoKeys.DAO_AUTOTASKDAO)
    try:
        daoManager.startTransaction()
        taskid = uuid.uuid4().hex
        taskvo = dao.getAutoTaskByTaskid(taskid)
        # parameter = json.dumps(kwargs)
        if taskvo is None:
            taskvo = wbxautotaskvo(taskid=taskid, task_type=taskType, parameter=args_str)
            dao.addAutoTask(taskvo)
        daoManager.commit()
        return taskvo
    except Exception as e:
        daoManager.rollback()
        raise e
    finally:
        daoManager.close()


if __name__ == '__main__':
    sp = wbxtelegraf.newInstance("rsdboradiag002")
    sp.check_telegraf_installed()


