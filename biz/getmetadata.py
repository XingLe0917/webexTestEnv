import os
from common.Config import Config
import base64
import logging
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from common.wbxinfluxdb import wbxinfluxdb
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from common.wbxchatbot import wbxchatbot

logger = logging.getLogger("DBAMONITOR")


def get_metadata(data_type, data_value):
    status = "SUCCESS"
    result_dict = {
        "os_info": [],
    }
    errormsg = ""
    if data_type not in ["host", "db"]:
        raise wbxexception("The data_type %s not support" % data_type)
    if data_type == "host":
        host_name = data_value.split(".")[0].lower()
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            depotDaoManager.startTransaction()
            host_topology = depotdbDao.gethosttopologybyhostnameanddbconfig(host_name)
            if not host_topology:
                raise wbxexception("Can not get host and instance info on the host %s in DepotDB" % host_name)
            result_dict.update({
                "host_topology": host_topology
            })
            ############# get hugepage size, memory total size, memory available size #########################
            influx_db_obj = wbxinfluxdb()
            mem_dict = influx_db_obj.get_os_info_for_chatbot()
            ############# get core cpu, physical cpu #########################
            cpu_info = depotdbDao.get_cpu_info_for_chatbot()
            host_list = depotdbDao.get_rac_host_list(host_name)
            for host in host_list:
                item_dict = {"host_name": host}
                item_dict.update(mem_dict[host])
                item_dict.update(cpu_info[host])
                result_dict["os_info"].append(item_dict)
            depotDaoManager.commit()
            result_dict = address_host_chatbot_content(result_dict)
        except Exception as e:
            depotDaoManager.rollback()
            errormsg = "getmetadata.gethostinfo(%s) with errormsg %s" % (host_name, str(e))
            logger.error(errormsg)
            status = "FAIL"
        finally:
            if depotDaoManager:
                depotDaoManager.close()
    elif data_type == "db":
        db_name = data_value.upper()
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            depotDaoManager.startTransaction()
            db_host_topology = depotdbDao.getdbhostnameinstancenameanddbconfig(db_name)
            if not db_host_topology:
                raise wbxexception("Can not get host and instance info on the db %s in DepotDB" % db_name)
            result_dict.update({
                "db_host_topology": db_host_topology
            })
            db_type = depotdbDao.getdbapplnsupportcode(db_name)
            if not db_type:
                raise wbxexception("Can not get db type on the db %s in DepotDB" % db_name)
            port_list = ["there is no port info in depot!"]
            # if db_type.upper() in ["WEB", "TEL"]:
            port_list = depotdbDao.getshareplexportbydb(db_name)
            if not port_list:
                port_list = ["there is no port info in depot!"]
            result_dict.update({
                "shareplex_port": port_list
            })
            ############# get hugepage size, memory total size, memory available size #########################
            influx_db_obj = wbxinfluxdb()
            mem_dict = influx_db_obj.get_os_info_for_chatbot()
            ############# get core cpu, physical cpu #########################
            cpu_info = depotdbDao.get_cpu_info_for_chatbot()
            host_list = list(set([item["host_name"] for item in db_host_topology]))
            for host in host_list:
                item_dict = {"host_name": host}
                item_dict.update(mem_dict[host])
                item_dict.update(cpu_info[host])
                result_dict["os_info"].append(item_dict)
            failoverdb_info = depotdbDao.get_db_failoverdb(db_name)
            result_dict.update({
                "failoverdb_info": dict(failoverdb_info[0]) if failoverdb_info else ""
            })
            depotDaoManager.commit()
            result_dict = address_db_chatbot_content(result_dict)
        except Exception as e:
            depotDaoManager.rollback()
            errormsg = "getmetadata.getdbinfo(%s) with errormsg %s" % (db_name, str(e))
            logger.error(errormsg)
            status = "FAIL"
        finally:
            depotDaoManager.close()
    return {
        "status": status,
        "errormsg": errormsg,
        "data": result_dict
    }


def address_host_chatbot_content(dict):
    content_string_dict = {
        "host_topology": "",
        "os_info": ""
    }
    host_topology_data = []
    host_topology_title = ["db_name", "host_name", "instance_name", "pga_aggregate_target", "sga_max_size", "sga_target"]
    for num, item in enumerate(dict["host_topology"]):
        host_topology_data.append([item["db_name"], item["host_name"], item["instance_name"], item["pga_aggregate_target"], item["sga_max_size"], item["sga_target"]])
    content_string_dict["host_topology"] += wbxchatbot().address_alert_list(host_topology_title, host_topology_data)

    os_info_title = ["host_name", "core_cpu", "physical_cpu", "huge_page_size", "mem_available", "mem_total"]
    os_info_data = []
    for num, item in enumerate(dict["os_info"]):
        os_info_data.append([item["host_name"], item["core_cpu"], item["physical_cpu"], item["huge_page_size"], item["mem_available"],
                             item["mem_total"]])
    content_string_dict["os_info"] += wbxchatbot().address_alert_list(os_info_title, os_info_data)
    return content_string_dict


def address_db_chatbot_content(dict):
    content_string_dict = {
        "db_host_topology": "",
        "shareplex_port": "",
        "os_info": "",
        "failoverdb_info":""
    }
    db_topology_data = []
    db_topology_title = ["db_name", "host_name", "instance_name", "pga_aggregate_target", "sga_max_size", "sga_target"]
    for num, item in enumerate(dict["db_host_topology"]):
        db_topology_data.append([item["db_name"], item["host_name"], item["instance_name"], item["pga_aggregate_target"], item["sga_max_size"], item["sga_target"]])
    content_string_dict["db_host_topology"] += wbxchatbot().address_alert_list(db_topology_title, db_topology_data)

    os_info_title = ["host_name", "core_cpu", "physical_cpu", "huge_page_size", "mem_available", "mem_total"]
    os_info_data = []
    for num, item in enumerate(dict["os_info"]):
        os_info_data.append([item["host_name"], item["core_cpu"], item["physical_cpu"], item["huge_page_size"], item["mem_available"],
                             item["mem_total"]])
    content_string_dict["os_info"] += wbxchatbot().address_alert_list(os_info_title, os_info_data)

    content_string_dict["shareplex_port"] = dict["shareplex_port"]
    content_string_dict["failoverdb_info"] = dict["failoverdb_info"]

    return content_string_dict


def get_all_dc_name():
    status = "SUCCEED"
    site_code_list = []
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        site_code_list = depotdbDao.get_all_site_code()
        if not site_code_list:
            raise wbxexception("Can not get all the oracle site_code in DepotDB" )
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = "getmetadata.get_all_site_code with errormsg %s" % str(e)
        logger.error(errormsg)
        status = "FAIL"
    finally:
        depotDaoManager.close()
    return {
        "status": status,
        "errormsg": errormsg,
        "data": site_code_list
    }

def get_all_server_info():
    status = "SUCCEED"
    site_code_list = []
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        server_info_list = depotdbDao.get_all_server_info()
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = "getmetadata.get_all_site_code with errormsg %s" % str(e)
        logger.error(errormsg)
        status = "FAIL"
    finally:
        depotDaoManager.close()
    return {
        "status": status,
        "data": server_info_list
    }
