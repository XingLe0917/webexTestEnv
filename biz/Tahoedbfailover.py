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
from datetime import datetime
from dao.vo.autotaskvo import wbxautotaskvo
from collections import OrderedDict
import threading

logger = logging.getLogger("DBAMONITOR")


def stop_ha_service_and_kill_gsb_connection(pool_name):
    sp = None
    status = "SUCCEED"
    errormsg = ""
    try:
        sp = tahoedbpool.newInstance(pool_name)
        sp.stop_ha_service()
        record_automation("STOPTAHOEHASERVICE", json.dumps({"pool_name": pool_name}))
        sp.kill_application_connection("gsb")
        record_automation("KILLGSBAPPCONNECTION", json.dumps({"pool_name": pool_name}))
    except Exception as e:
        status = "FAIL"
        errormsg = str(e)
    finally:
        if sp is not None:
            sp.close()

    return {
        "status": status,
        "errormsg": errormsg
    }


# def check_all_tahoe_db_status(pool_name_list):
#     if not pool_name_list:
#     sp = None
#     status = "SUCCEED"
#     errormsg = ""
#     status_code = "0"
#     try:
#         sp = tahoedbpool.newInstance(pool_name)
#         status_code = sp.check_db_status()
#     except Exception as e:
#         status = "FAIL"
#         errormsg = str(e)
#         logger.error(errormsg)
#         status_code = "-1"
#     finally:
#         if sp is not None:
#             sp.close()
#
#     return {
#         "status": status,
#         "status_code": status_code
#     }


def check_tahoe_db_status(pool_name):
    sp = None
    status = "SUCCEED"
    errormsg = ""
    status_code = "0"
    try:
        sp = tahoedbpool.newInstance(pool_name)
        status_code = sp.check_db_status()
    except Exception as e:
        status = "FAIL"
        errormsg = str(e)
        logger.error(errormsg)
        status_code = "-1"
    finally:
        if sp is not None:
            sp.close()

    return {
        "status": status,
        "status_code": status_code
    }


def kill_primary_application(pool_name):
    sp = None
    status = "SUCCEED"
    errormsg = ""
    try:
        sp = tahoedbpool.newInstance(pool_name)
        sp.check_access()
        sp.kill_application_connection("pri")
        # record_automation("KILLPRIAPPCONNECTION", json.dumps({"pool_name": pool_name}))
    except Exception as e:
        status = "FAIL"
        errormsg = str(e)
    finally:
        if sp is not None:
            sp.close()

    return {
        "status": status,
        "errormsg": errormsg
    }


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


class tahoedbpool:

    def __init__(self, pri_db_name, primary_server, gsb_db_name, pool_name, gsb_pool_name, schema):
        self.primary_server = primary_server
        self.gsb_db_name = gsb_db_name
        self.pool_name = pool_name
        self.schema = schema
        self.gsb_pool_name = gsb_pool_name
        self.primary_host_list = [item.host_name for item in primary_server]
        self.pri_db_name = pri_db_name

    def close(self):
        for server in self.primary_server:
            server.close()

    @staticmethod
    def newInstance(pool_name):
        if not pool_name:
            raise wbxexception("pool_name can not be null")
        pool_name = pool_name.lower()
        key = "tahoedbfailover_%s" % (pool_name)
        sp = curcache.get(key)
        if sp is None:
            daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
            depotDaoManager = daoManagerFactory.getDefaultDaoManager()
            depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            try:
                depotDaoManager.startTransaction()
                primary_db_name, gsb_db_name, gsb_pool_name, schema = depotdbDao.getDBNameByPoolname(pool_name)
                if not primary_db_name or not gsb_db_name:
                    raise wbxexception("getDBNameByPoolname %s get None" % pool_name)
                primary_host_name = depotdbDao.getHostNameOraclePwdByDBname(primary_db_name)
                if not primary_host_name:
                    raise wbxexception("getHostNameOraclePwdByDBname %s get None" % primary_db_name)
                # gsb_host_name = depotdbDao.getHostNameOraclePwdByDBname(gsb_db_name)
                # if not gsb_host_name:
                #     raise wbxexception("getHostNameOraclePwdByDBname %s get None" % gsb_db_name)
                depotDaoManager.commit()
            except DatabaseError as e:
                logger.error(e)
                depotDaoManager.rollback()
                raise wbxexception(e)
            finally:
                depotDaoManager.close()
            primary_server = []
            for host_item in primary_host_name:
                server = wbxssh(host_item["host_name"], 22, "oracle", host_item["password"])
                try:
                    server.connect()
                except Exception as e:
                    raise wbxexception("cannot login the server %s with password in depot" % host_item["host_name"])
                primary_server.append(server)
            # gsb_server = []
            # for host_item in gsb_host_name:
            #     server = wbxssh(host_item["host_name"], 22, "oracle", host_item["password"])
            #     try:
            #         server.connect()
            #     except Exception as e:
            #         raise wbxexception("cannot login the server %s with password in depot" % host_item["host_name"])
            #     gsb_server.append(server)
            sp = tahoedbpool(primary_db_name, primary_server, gsb_db_name, pool_name, gsb_pool_name, schema)
            curcache.set(key, sp)
        return sp

    def address_cmd_list(self, cmd_list):
        cmds = cmd_list.split("\n")
        rst_list = []
        for cmd_item in cmds:
            if "alter system kill session" in cmd_item:
                rst_list.append(cmd_item.split("SQL>")[-1])
        return rst_list

    def address_session_count(self, rst):
        count_list = rst.split("\n")
        for i in range(0, len(count_list)):
            if "----------" in count_list[i]:
                if count_list[i + 1]:
                    count_rst = count_list[i + 1].split()
                    return count_rst[0], count_rst[1]
        return "0", "0"

    def stop_ha_service(self):
        primary_node1 = self.primary_server[0]
        primary_node1.connect()
        cmd = ". /home/oracle/.bash_profile; crsstat | grep %s | grep %s | grep service" % (self.pri_db_name.lower(), primary_node1.host_name)
        rst = primary_node1.exec_command(cmd)
        db_name = rst.split()[0].split("ora.")[-1].split(".%sha" % self.pool_name)[0]
        # cmd = "srvctl stop service -d %s -s %s" % (db_name, self.pool_name)
        # print(cmd)
        # rst = primary_node1.exec_command(cmd)
        cmd = "source /home/oracle/.bash_profile; srvctl stop service -d %s -s %sha" % (self.pri_db_name.lower(), self.pool_name.lower())
        print(cmd)
        rst = primary_node1.exec_command(cmd)
        print(rst)
        #############time.sleep(5)
        cmd = "crsstat | grep '%sha' | awk '{print $3$4}'" % self.pool_name.lower()
        print(cmd)
        targetstate = primary_node1.exec_command(cmd)
        for line in targetstate.split("\n"):
            if "OFFLINEOFFLINE" not in line:
                raise wbxexception("Stop service on %s failed" % primary_node1.host_name)

    def kill_application_connection(self, db_type):
        machine_like = self.gsb_pool_name if db_type == "gsb" else self.pool_name
        server = self.primary_server[0]
        server.connect()
        cmd = """
        ps aux | grep lgwr | grep %s | awk '{print %s}' | awk -F '_' '{print %s}'
        """ % (self.pri_db_name.lower(), "$NF", "$NF")
        print(cmd)
        ha_name = server.exec_command(cmd)
        print(ha_name)
        ha_name = ha_name.split("\n")[0]
        # ha_name = ha_name.split("_")[-1]
        if not ha_name:
            raise wbxexception("%s ha_name %s in server %s is null" % (self.pool_name, ha_name, server.host_name))
        cmd = """
        . /home/oracle/.bash_profile
        db
        export ORACLE_SID=%s
        sqlplus / as sysdba << EOF 
        SET pagesize 0 linesize 1000 feedback off heading off echo off serveroutput on;
        select 'alter system kill session ''' ||sid || ',' || serial# || ''' immediate;' from %s where USERNAME='%s' and regexp_like(machine,'^%s[[:alpha:]]');
        exit;
        EOF
        """ % (ha_name, "gv\$session", self.schema.upper(), machine_like.lower())
        print(cmd)
        cmd_list = server.exec_command(cmd)
        exec_cmd_list = self.address_cmd_list(cmd_list)
        if not exec_cmd_list:
            raise wbxexception("Failed to get session from %s in %s" % (self.pool_name, server.host_name))
        server.close()
        cmd_str = ""
        for cmd in exec_cmd_list:
            cmd_str = cmd_str + cmd + "\n"
        cmd_str += "exit;"
        cmd = """
        . /home/oracle/.bash_profile
        db
        export ORACLE_SID=%s
        sqlplus / as sysdba << EOF 
        %s
        EOF
        """ % ("HANAMEEXAM", cmd_str)
        for server_item in self.primary_server:
            print(server_item.host_name)
            CMD = cmd
            server_item.connect()
            ha_cmd = """
                    ps aux | grep lgwr | grep %s | awk '{print $NF}' | awk -F _ '{print $NF}'
                    """ % self.pri_db_name.lower()
            ha_name = server_item.exec_command(ha_cmd)
            ha_name = ha_name.split("\n")[0]
            CMD = CMD.replace("HANAMEEXAM", ha_name)
            print(CMD)
            server_item.exec_command(CMD)
            server_item.close()

    def check_db_status(self):
        status_map = {
            "all connected": "2",
            "pri connected": "1",
            "pri cut": "0",
            "unnormal": "-1"
        }

        try:
            server = self.primary_server[0]
            server.connect()
            cmd = """
                    ps aux | grep lgwr | grep %s | awk -F ' ' '{print $NF}' | awk -F '_' '{print $NF}'
                    """ % self.pri_db_name.lower()
            ha_name = server.exec_command(cmd)
            # print(ha_name)
            if not ha_name:
                logger.error("%s ha_name %s in server %s is null" % (self.pool_name, ha_name, server.host_name))
            ha_name = list(filter(None, ha_name.replace("$NF}'?", "").split("\n")))[0]
            cmd = """
                . /home/oracle/.bash_profile
                db
                export ORACLE_SID=%s
                sqlplus / as sysdba << EOF
                select sum(case when instr(machine,'%s')>=0 then 1 else 0 end) pricount, sum(case when instr(machine,'%s')>=0 then 1 else 0 end) as gsbcount from gv\$session where machine like '%%%s%%' or machine like '%%%s%%' and lower(USERNAME)='%s';
                exit;
                EOF
                """ % (ha_name, self.pool_name.lower(), self.gsb_pool_name.lower(), self.pool_name.lower(), self.gsb_pool_name.lower(), self.schema.lower())
            # print(cmd)
            rst = server.exec_command(cmd)
        finally:
            if server is not None:
                server.close()
        # print(rst)
        pri_count, gsb_count = self.address_session_count(rst)
        if not pri_count.isdigit() or not gsb_count.isdigit():
            logger.error("pri_count %s or gsb_count %s is not digit" % (pri_count, gsb_count))
            return status_map["unnormal"]
        if int(pri_count) > 0 and int(gsb_count) > 0:
            return status_map["all connected"]
        elif int(pri_count) > 0 and int(gsb_count) == 0:
            return status_map["pri connected"]
        else:
            return status_map["pri cut"]
        return status_map["unnormal"]

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


