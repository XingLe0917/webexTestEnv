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


def install_jobmanager(host_name):
    status = "SUCCEED"
    errormsg = ""
    sp = None
    try:
        sp = wbxjobmanager.newInstance(host_name)
        sp.login()
        sp.installjobmanager()
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


class wbxjobmanager:
    def __init__(self, server):
        self.host_name = server.host_name
        self.server = server
        self.install_script = "/staging/gates/install_dbmetricagent.sh"

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
            depotDaoManager.commit()
        except DatabaseError as e:
            logger.error("getUserPasswordByHostname getSitecodeByHostname met error %s" % str(e))
            raise wbxexception(
                "Error ocurred when get oracle user password on the server %s in DepotDB with msg %s" % (
                    host_name, str(e)))
        if wbxutil.isNoneString(ssh_pwd):
            raise wbxexception(
                "Can not get oracle user password on the server %s in DepotDB" % host_name)

        servervo = daoManagerFactory.getServer(host_name)
        if servervo is None:
            raise wbxexception("can not get server info with hostname %s" % host_name)
        ssh_port = servervo.ssh_port
        server = wbxssh(host_name, ssh_port, "oracle", ssh_pwd)
        try:
            server.connect()
        except Exception as e:
            raise wbxexception("cannot login the server %s with password in depot" % host_name)
        sp = wbxjobmanager(server)
        return sp


    def installjobmanager(self):
        if not self.server.isFile(self.install_script):
            raise wbxexception("the jobmanager install script %s not exist on server %s" % (self.install_script, self.host_name))
        cmd = ". /home/oracle/.11g_db; sh %s" % self.install_script
        self.server.execute_command(cmd)
        record_automation("INSTALLJOBMANAGER_TASK", json.dumps(
            {"host_name": self.host_name}))


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
    sp = wbxjobmanager.newInstance("rsdboradiag002")


