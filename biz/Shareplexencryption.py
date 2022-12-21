import logging
import requests
import base64
import json
import uuid
import datetime
from requests.auth import HTTPBasicAuth
from common.wbxssh import wbxssh
from common.wbxtask import wbxautotask
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from sqlalchemy.exc import  DBAPIError, DatabaseError
from common.wbxexception import wbxexception
from common.wbxcache import curcache
from common.wbxutil import wbxutil
from dao.vo.autotaskvo import wbxautotaskvo
from collections import OrderedDict
import threading

logger = logging.getLogger("DBAMONITOR")


def precheck_shareplex_encryption(src_host, port):
    status = "SUCCEED"
    sp = None
    errormsg = ""
    try:
        sp = splexencryption.newInstance(src_host, port)
        sp.login()
        sp.check_shareplex_info_valid()
    except Exception as e:
        status = "FAIL"
        errormsg = str(e)
    finally:
        if sp is not None:
            sp.close()
    return {"status": status,
            "errormsg": errormsg}


def excute_shareplex_encryption(src_host, port):
    status = "SUCCEED"
    sp = None
    errormsg = ""
    logmsg = []
    try:
        sp = splexencryption.newInstance(src_host, port)
        sp.login()
        sp.excute_src_encryption()
        sp.excute_tgt_encryption()
        logmsg = sp.logmsg
    except Exception as e:
        status = "FAIL"
        errormsg = str(e)
    finally:
        if sp is not None:
            sp.close()
    return {"status": status,
            "errormsg": errormsg,
            "logmsg": logmsg}



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



class splexencryption:

    def __init__(self, src_server, tgt_server, port):
        self.src_host_name = [item.host_name for item in src_server]
        self.tgt_host_name = [item.host_name for item in tgt_server]
        self.port = port
        self.logmsg = []
        self.src_server = src_server
        self.tgt_server = tgt_server
        self.key_value = "%s33E283116B1165BD049DC94181C96CE76ED9AAC3AFD495636650780A9AB" % port if len(port) == 5 else "0%s33E283116B1165BD049DC94181C96CE76ED9AAC3AFD495636650780A9AB" % port

    def close(self):
        for src_server in self.src_server:
            src_server.close()
        for tgt_server in self.src_server:
            tgt_server.close()

    def login(self):
        for src_server in self.src_server:
              src_server.connect()
        for tgt_server in self.src_server:
              tgt_server.connect()

    @staticmethod
    def newInstance(src_host, port):
        param = { "src_host": src_host, "port": port }
        for k, v in param.items():
            if not v:
                raise wbxexception("%s can not be null" % k)
        src_host = src_host.split(".")[0]
        key = "%s_%s_splexAES" % (src_host, port)
        sp = curcache.get(key)
        if sp is None:
            ssh_user = "oracle"
            daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
            depotDaoManager = daoManagerFactory.getDefaultDaoManager()
            depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            try:
                depotDaoManager.startTransaction()
                src_host_list, tgt_host_list = depotdbDao.getsrctgthostname(src_host, port)
                logger.info("src_host: %s" % src_host_list)
                logger.info("tgt_host: %s" % tgt_host_list)
                src_host_dict = {}
                tgt_host_dict = {}
                if not src_host_list or not tgt_host_list:
                    raise wbxexception("get src_host %s and tgt_host %s" % (src_host_list, tgt_host_list))
                for host in src_host_list:
                    src_ssh_pwd = depotdbDao.getOracleUserPwdByHostname(host)
                    src_host_dict.update({
                        host: src_ssh_pwd
                    })
                for host in tgt_host_list:
                    tgt_ssh_pwd = depotdbDao.getOracleUserPwdByHostname(host)
                    tgt_host_dict.update({
                        host: tgt_ssh_pwd
                    })
                depotDaoManager.commit()
            except DatabaseError as e:
                logger.error("getOracleUserPwdByHostname met error %s" % str(e))
                depotDaoManager.rollback()
                raise wbxexception(
                    "getOracleUserPwdByHostname met error %s" % str(e))
            finally:
                depotDaoManager.close()
            for k, v in src_host_dict.items():
                if wbxutil.isNoneString(v):
                    raise wbxexception("Can not get oracle user password on the server %s in DepotDB" % k)
            for k, v in tgt_host_dict.items():
                if wbxutil.isNoneString(v):
                    raise wbxexception("Can not get oracle user password on the server %s in DepotDB" % k)
            src_server = []
            tgt_server = []
            for k, v in src_host_dict.items():
                srcserver = wbxssh(k, 22, ssh_user, v)
                try:
                    srcserver.verifyConnection()
                except Exception as e:
                    raise wbxexception("cannot login the server %s with password in depot" % k)
                src_server.append(srcserver)
            for k, v in tgt_host_dict.items():
                tgtserver = wbxssh(k, 22, ssh_user, v)
                try:
                    tgtserver.verifyConnection()
                except Exception as e:
                    raise wbxexception("cannot login the server %s with password in depot" % k)
                tgt_server.append(tgtserver)
            sp = splexencryption(src_server, tgt_server, port)
            curcache.set(key, sp)
        return sp

    def check_shareplex_info_valid(self):
        for type, server_list in {"src": self.src_server, "tgt": self.tgt_server}.items():
            for server in server_list:
                cmd = ". /home/oracle/.bash_profile; crsstat | grep -c 'shareplex%s '" % self.port
                port_num = server.exec_command(cmd)
                port_num = int(port_num.split("\n")[0])
                if port_num < 1:
                    raise wbxexception("Cannot find the shareplex port %s on server %s" % (self.port, server.host_name))
                cmd = ". /home/oracle/.bash_profile; crsstat | grep 'shareplex%s ' | sed 's| ||g'" % self.port
                targetstate = server.exec_command(cmd)
                if "ONLINEONLINE" not in targetstate:
                    raise wbxexception("Shareplex port %s on %s taget state is %s" % (self.port, server.host_name, targetstate))

    def check_enable_aes_info(self, aes_info):
        aes_list = aes_info.split("\n")
        for item in aes_list:
            if not item:
                continue
            aes_item = list(filter(None, item.split("  ")))
            if int(aes_item[1]) > 0:
                return True
        return False


    def address_show_info(self, show_info):
        show_list = show_info.split("\n")
        rst_dict_list = []
        for server_tuple in show_list:
            if not server_tuple:
                continue
            server_tuple = list(filter(None, server_tuple.split("  ")))
            rst_dict = {
                "Process": server_tuple[0],
                "Source": server_tuple[1],
                "Target": server_tuple[2],
                "State": server_tuple[3],
            }
            if server_tuple[3] != "Running":
                    raise wbxexception(
                        "the process(%s) of shareplex port %s is not running" % (rst_dict, self.port))
            if rst_dict["Process"] == "Export":
                rst_dict["queuename"] = rst_dict["Source"]
            elif rst_dict["Process"] == "Import":
                rst_dict["queuename"] = rst_dict["Source"].split("%s-vip-")[-1]
            rst_dict_list.append(rst_dict)
        return rst_dict_list

    def excute_src_encryption(self):
        process_type = "export"
        for srcserver in self.src_server:
            cmd = "cat /etc/oraport | grep %s | awk -F ':' '{print $NF}'" % self.port
            shareplex_bin_dir = srcserver.exec_command(cmd)
            shareplex_bin_dir = shareplex_bin_dir.split("\n")[0]
            cmd = """
            source %s/.profile_%s;  %s/sp_ctrl << EOF
            set encryption key %s
            set param SP_XPT_ENABLE_AES 1
            stop export
            start export

            exit
            EOF
            """ % (shareplex_bin_dir, self.port, shareplex_bin_dir, self.key_value)
            # print(cmd)
            rst = srcserver.exec_command(cmd)
            logger.info("%s set export encryption successfully!" % srcserver.host_name)
            self.logmsg.append({
                "host_name": srcserver.host_name,
                "logmsg": "set export encryption successfully!"
            })

    def excute_tgt_encryption(self):
        process_type = "import"
        for tgtserver in self.tgt_server:
            cmd = "cat /etc/oraport | grep %s | awk -F ':' '{print $NF}'" % self.port
            shareplex_bin_dir = tgtserver.exec_command(cmd)
            shareplex_bin_dir = shareplex_bin_dir.split("\n")[0]
            cmd = """
                    source %s/.profile_%s;  %s/sp_ctrl << EOF
                    set encryption key %s
                    set param SP_IMP_ENABLE_AES 1
                    stop import
                    start import

                    exit
                    EOF
                    """ % (shareplex_bin_dir, self.port, shareplex_bin_dir, self.key_value)
            rst = tgtserver.exec_command(cmd)

            logger.info("%s set import encryption successfully!" % tgtserver.host_name)
            self.logmsg.append({
                "host_name": tgtserver.host_name,
                "logmsg": "set import encryption successfully!"
            })

if __name__ == '__main__':
    precheck_shareplex_encryption()


