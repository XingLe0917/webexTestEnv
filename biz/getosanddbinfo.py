import os
from common.Config import Config
import base64
import logging
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from common.wbxcache import curcache
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from biz.dbmanagement.wbxdb import wbxdb
from sqlalchemy import Table, Column, MetaData, String, DateTime, Integer, and_, create_engine
from sqlalchemy.exc import  DBAPIError, DatabaseError
from sqlalchemy.pool import NullPool

logger = logging.getLogger("DBAMONITOR")


def get_os_and_db_info_by_host_name(host_name):
    status = "SUCCESS"
    result_dict = {
        "os_info": [],
        "db_info": []
    }
    errormsg = ""
    try:
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        server = daomanagerfactory.getServer(host_name)
        if not server:
            raise wbxexception("can not find the server with host_name=%s in depot db" % host_name)
        racdict = server.getRacNodeDict()
        for server_item in racdict.values():
            result_dict["os_info"].append(get_os_info(server_item.getHostname()))

        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        defaultDaoManager = daomanagerfactory.getDefaultDaoManager()
        depotdao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        db_dict = depotdao.gethosttopologybyhostname(host_name)
        db_list = list(set([ item["db_name"] for item in db_dict ]))
        print('------------------', db_list)
        for db_name in db_list:
            result_dict["db_info"].append(get_db_info(db_name, host_name))

    except Exception as e:
        logger.error(e)
        raise wbxexception(e)
    finally:
        if server:
            server.close()
        if depotdao:
            depotdao.close()
    return {
        "status": status,
        "errormsg": errormsg,
        "data": result_dict
    }


def get_os_and_db_info_by_db_name(db_name):
    status = "SUCCESS"
    result_dict = {
        "os_info": [],
        "db_info": []
    }
    errormsg = ""
    try:
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        db = daomanagerfactory.getDatabaseByDBName(db_name)
        if db is None:
            raise wbxexception("Can not get database with db_name=%s" % db_name)
        for servername, dbserver in db.getServerDict().items():
            result_dict["os_info"].append(get_os_info(servername))
    except Exception as e:
        logger.error(e)
        raise wbxexception(e)
    return {
        "status": status,
        "errormsg": errormsg,
        "data": result_dict
    }


def get_os_info(host_name):
    rst_dict = {
            "host_name": host_name,
            "transparent_hugepage": "",
            "hugepage_size": "",
            "MemTotal": "",
            "MemFree": "",
            "MemAvailable": "",
            "physical_cpu": "",
            "cpu_cores": ""
        }
    try:
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        server = daomanagerfactory.getServer(host_name)
        server.connect()
        ################ get transparent_hugepage #########################
        cmd = "cat /sys/kernel/mm/transparent_hugepage/enabled | awk -F '[' '{print$NF}' | awk -F ']' '{print $1}'"
        logger.info("%s on server %s" % (cmd, host_name))
        transparent_hugepage = server.exec_command(cmd, async_log=True)
        if transparent_hugepage.find("WBXERROR") >= 0:
            raise wbxexception("Error occurred with command %s" % (cmd))
        transparent_hugepage = transparent_hugepage.split("\n")[0]
        ############# get hugepage size #########################
        cmd = "cat /proc/meminfo | grep -i HugePagesize|sed 's/[[:space:]]//g'| awk -F ':' '{print $NF}'"
        logger.info("%s on server %s" % (cmd, host_name))
        hugepage_size = server.exec_command(cmd, async_log=True)
        if hugepage_size.find("WBXERROR") >= 0:
            raise wbxexception("Error occurred with command %s" % (cmd))
        hugepage_size = hugepage_size.split("\n")[0]
        ############# get memory size #########################
        cmd = "cat /proc/meminfo | grep -i MemTotal|sed 's/[[:space:]]//g'| awk -F ':' '{print $NF}'"
        logger.info("%s on server %s" % (cmd, host_name))
        MemTotal = server.exec_command(cmd, async_log=True)
        if MemTotal.find("WBXERROR") >= 0:
            raise wbxexception("Error occurred with command %s" % (cmd))
        MemTotal = MemTotal.split("\n")[0]

        cmd = "cat /proc/meminfo | grep -i MemFree|sed 's/[[:space:]]//g'| awk -F ':' '{print $NF}'"
        logger.info("%s on server %s" % (cmd, host_name))
        MemFree = server.exec_command(cmd, async_log=True)
        if MemFree.find("WBXERROR") >= 0:
            raise wbxexception("Error occurred with command %s" % (cmd))
        MemFree = MemFree.split("\n")[0]

        cmd = "cat /proc/meminfo | grep -i MemAvailable|sed 's/[[:space:]]//g'| awk -F ':' '{print $NF}'"
        logger.info("%s on server %s" % (cmd, host_name))
        MemAvailable = server.exec_command(cmd, async_log=True)
        if MemAvailable.find("WBXERROR") >= 0:
            raise wbxexception("Error occurred with command %s" % (cmd))
        MemAvailable = MemAvailable.split("\n")[0]
        ############# get cpu count #########################
        cmd = "cat /proc/cpuinfo | grep ^'physical id' | awk -F: '{print $2}' | sed 's/[[:space:]]//g' | sort | uniq | wc -l"
        logger.info("%s on server %s" % (cmd, host_name))
        physical_cpu = server.exec_command(cmd, async_log=True)
        if physical_cpu.find("WBXERROR") >= 0:
            raise wbxexception("Error occurred with command %s" % (cmd))
        physical_cpu = physical_cpu.split("\n")[0]

        cmd = "cat /proc/cpuinfo | grep 'cpu cores' |uniq| awk -F: '{print $2}' | sed 's/[[:space:]]//g'"
        logger.info("%s on server %s" % (cmd, host_name))
        cpu_cores = server.exec_command(cmd, async_log=True)
        if cpu_cores.find("WBXERROR") >= 0:
            raise wbxexception("Error occurred with command %s" % (cmd))
        cpu_cores = cpu_cores.split("\n")[0]
        rst_dict = {
            "host_name": host_name,
            "transparent_hugepage": transparent_hugepage,
            "hugepage_size": hugepage_size,
            "MemTotal": MemTotal,
            "MemFree": MemFree,
            "MemAvailable": MemAvailable,
            "physical_cpu": physical_cpu,
            "cpu_cores": cpu_cores
        }
    except Exception as e:
        logger.error(e)
        raise wbxexception(e)
    finally:
        if server:
            server.close()
    return rst_dict


# def get_db_info(db_name, host_name):
#     rst_dict = {
#         "db_name": db_name,
#         "max"
#     }
#     try:
#         daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
#         daoManager = daomanagerfactory.getDaoManager(db_name)

