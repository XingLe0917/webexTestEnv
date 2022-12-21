import logging
import re

from common.wbxchatbot import wbxchatbot
from common.wbxexception import wbxexception
from dao.vo.depotdbvo import wbxserver, wbxdatabase
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

def get_db_info(db_name, host_name):
    logger.info(
        "get_db_info, db_name=%s host_name=%s" % (db_name, host_name))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.get_db_info(db_name, host_name)
        daoManager.commit()
        return [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        logger.error("get_db_info error occurred", exc_info=e, stack_info=True)
    return None

def get_rac_info(db_name, host_name):
    logger.info(
        "get_rac_info, db_name=%s, host_name=%s" % (db_name,host_name))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.get_rac_info(db_name, host_name)
        daoManager.commit()
        return [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        logger.error("get_rac_info error occurred", exc_info=e, stack_info=True)
    return None

def get_depot_manage_info(db_name, host_name):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    logger.info(
        "get_depot_manage_info, db_name=%s host_name=%s" % (db_name, host_name))
    if not db_name and not host_name:
        res["status"] = "FAILED"
        res["errormsg"] = "please give db_name or host_name"
    else:
        data = {}
        data['rac_info'] = get_rac_info(db_name, host_name)
        data['db_info'] = get_db_info(db_name, host_name)
        res["data"] = data
    return res

def get_depot_manage_user_info(db_name,trim_host):
    logger.info(
        "get_depot_manage_user_info, db_name=%s,trim_host=%s" % (db_name,trim_host))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.get_depot_manage_user_info(db_name,trim_host)
        daoManager.commit()
        return [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        logger.error("get_depot_manage_user_info error occurred", exc_info=e, stack_info=True)
    return None

def get_depot_manage_splexplex_info(db_name):
    logger.info(
        "get_depot_manage_splexplex_info, db_name=%s" % (db_name))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.get_depot_manage_splexplex_info(db_name)
        daoManager.commit()
        return [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        logger.error("get_depot_manage_splexplex_info error occurred", exc_info=e, stack_info=True)
    return None

def depot_manage_reload(host_name,type):
    logger.info(
        "depot_manage_reload, host_name=%s, type=%s " % (host_name,type))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    res = {"status": "SUCCESS", "errormsg": ""}
    trim_host = ""
    nodes_list = []
    server_inst_name_list = []
    server = None
    try :
        try:
            server = daoManagerFactory.getServer(host_name)
            server.connect()
            nodes = server.exec_command(". /home/oracle/.bash_profile;olsnodes")
            _inst_name = server.exec_command(
                ". /home/oracle/.bash_profile;ps -ef |grep _smon_ |grep -v grep |grep -vi ASM |awk '{print $NF}' |awk -F \"_\" '{print $NF}'")
            nodes_list = []
            if "olsnodes" not in nodes:
                nodes_list = nodes.split('\n')
            server_inst_name_list = _inst_name.split('\n')
            scan_name = server.exec_command(
                ". /home/oracle/.bash_profile;srvctl config scan  | grep ^\"SCAN name\" | awk '{print $3}' | awk -F\. '{print $1}' | sed \"s/,//g\"")
        except Exception as e:
            raise wbxexception("WBXERROR: Error occurred:depot_manage_reload, e=%s" % (str(e)))

        depot_host_list = []
        try:
            daoManager.startTransaction()
            dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            server_info = dao.get_host_info_by_hostname(host_name)
            if scan_name:
                ll = dao.get_host_info_by_scanname(scan_name)
                depot_host_list = [dict(vo)['host_name'] for vo in ll]
            daoManager.commit()
            if len(server_info) > 0:
                trim_host = dict(server_info[0])['trim_host']
        except Exception as e:
            daoManager.rollback()
            raise wbxexception("get host info from depotDB error occurred with error %s" % str(e))

        if "reload" == type:
            ## For remove node case
            logger.info("Check if need to remove node from deportDB")
            for deport_host_name in depot_host_list:
                if deport_host_name not in nodes_list:
                    logger.info("{0} not in olsnodes.".format(deport_host_name))
                    try:
                        daoManager.startTransaction()
                        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                        dao.delete_host_info(deport_host_name)
                        dao.delete_instance_info(deport_host_name)
                        daoManager.commit()
                    except Exception as e:
                        daoManager.rollback()
                        logger.error("delete instance_info/host_info error occurred with error %s" % str(e), exc_info=e,
                                     stack_info=True)

        if "reload" == type:
            ## For added server case
            logger.info("Check if need to add node into deportDB")
            for node_host_name in nodes_list:
                if node_host_name and node_host_name not in depot_host_list:
                    try:
                        daoManager.startTransaction()
                        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                        hostInfoVo = get_server_info(node_host_name)
                        dao.insert_host_info(hostInfoVo)
                        daoManager.commit()
                    except Exception as e:
                        daoManager.rollback()
                        raise wbxexception("insert host_info error occurred with error %s" % str(e))

        for inst_name in server_inst_name_list:
            cmd = """
                                . /home/oracle/.bash_profile
                                db
                                export ORACLE_SID=%s
                                sqlplus / as sysdba << EOF 
                                set linesize 1000;
                                select gd.name||','||gi.instance_name||','||gi.host_name||',isFlag' from gv\$instance gi, v\$database gd;
                                exit;
                                EOF
                                """ % (inst_name)
            rest_list = server.exec_command(cmd)
            for res_info in str(rest_list).split("\n"):
                if "isFlag" in res_info:
                    db_name = res_info.split(",")[0]
                    instance_name = res_info.split(",")[1]
                    hostName = str(res_info.split(",")[2]).split(".")[0]
                    rows = None
                    try:
                        daoManager.startTransaction()
                        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                        rows = dao.isExistDeport(instance_name, hostName)
                        daoManager.commit()
                    except Exception as e:
                        daoManager.rollback()
                        raise wbxexception("isExistDeport error occurred with error %s" % str(e))
                    if len(rows) == 0:
                        # ins_map = {}
                        # try:
                        #     ins_map = get_instance_by_db(db_name)
                        # except Exception as e:
                        #     raise wbxexception("get instance_by_db error occurred with error %s" % str(e))
                        #
                        # if instance_name in ins_map and hostName in ins_map[instance_name]:
                        #     logger.info(
                        #         "Find the target not in instance_info table. instance_name={0},host_name={1},db_name={2}".format(
                        #             instance_name, hostName, db_name))
                        #     try:
                        #         daoManager.startTransaction()
                        #         dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                        #         dao.insert_instance_info(trim_host,hostName,db_name,instance_name)
                        #         daoManager.commit()
                        #     except Exception as e:
                        #         daoManager.rollback()
                        #         raise wbxexception("insert instance_info error occurred with error %s" % str(e))
                        logger.info(
                            "Find the target not in instance_info table. instance_name={0},host_name={1},db_name={2}".format(
                                instance_name, hostName, db_name))
                        try:
                            daoManager.startTransaction()
                            dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                            dao.insert_instance_info(trim_host, hostName, db_name, instance_name)
                            daoManager.commit()
                        except Exception as e:
                            daoManager.rollback()
                            raise wbxexception("insert instance_info error occurred with error %s" % str(e))

                        else:
                            logger.info(
                                "Skip it. instance_name={0},host_name={1},db_name={2}".format(instance_name, hostName,
                                                                                              db_name))

        deport_inst_list = []
        try:
            daoManager.startTransaction()
            dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            ins = dao.get_inst_list(host_name)
            deport_inst_list = [dict(vo)['instance_name'] for vo in ins]
            daoManager.commit()
        except Exception as e:
            daoManager.rollback()
            raise wbxexception("get_inst_list error occurred with error %s" % str(e))

        if "reload" == type:
            for deport_inst in deport_inst_list:
                if deport_inst not in server_inst_name_list:
                    logger.info(
                        "Found the target that needs to be deleted from instance_info table. host_name={0},instance_name={1}".format(
                            host_name, deport_inst))
                    try:
                        daoManager.startTransaction()
                        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                        dao.delete_instance_info_by(host_name,deport_inst)
                        daoManager.commit()
                    except Exception as e:
                        daoManager.rollback()
                        raise wbxexception("delete instance_info error occurred with error %s" % str(e))

    except Exception as e:
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
        return res
    return res

def depot_manage_add_rac(host_name):
    logger.info(
        "depot_manage_add_rac, host_name=%s, " % (host_name))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        server_info = dao.get_host_info_by_hostname(host_name)
        if len(server_info)== 0:
            hostInfoVo = get_server_info(host_name)
            dao.insert_host_info(hostInfoVo)
        else:
            res["status"] = "FAILED"
            res["errormsg"] = "The RAC info has already exists. No need to add again. Please check it."
    except Exception as e:
        daoManager.rollback()
        logger.error("insert host_info error occurred with error %s" % str(e), exc_info=e,
                     stack_info=True)
    return res

def get_server_info(host_name):
    logger.info(
        "get_server_info, host_name=%s, " % (host_name))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        server = daoManagerFactory.getServer(host_name)
        server.connect()
        _inst_name = server.exec_command(". /home/oracle/.bash_profile;ps -ef |grep _smon_ |grep -v grep |grep -vi ASM |awk '{print $NF}' |awk -F \"_\" '{print $NF}'")
        logger.info("_inst_name={0}" .format(_inst_name))
        scan_port = server.exec_command(
            ". /home/oracle/.bash_profile;srvctl config scan_listener | grep TCP: | awk -F: '{print $NF}' | head -n 1")
        logger.debug("scan_port={0}".format(scan_port))
        scan_ip1 = server.exec_command(
            ". /home/oracle/.bash_profile;srvctl config scan | grep -E 'scan1|SCAN 1' | awk '{print $NF}' | awk -F: '{print $NF}' |sed 's/[[:space:]]//g'")
        logger.debug("scan_ip1={0}".format(scan_ip1))
        scan_ip2 = server.exec_command(
            ". /home/oracle/.bash_profile;srvctl config scan | grep -E 'scan1|SCAN 2' | awk '{print $NF}' | awk -F: '{print $NF}' |sed 's/[[:space:]]//g'")
        logger.debug("scan_ip2={0}".format(scan_ip2))
        scan_ip3 = server.exec_command(
            ". /home/oracle/.bash_profile;srvctl config scan | grep -E 'scan1|SCAN 3' | awk '{print $NF}' | awk -F: '{print $NF}' |sed 's/[[:space:]]//g'")
        logger.debug("scan_ip3={0}".format(scan_ip3))
        scan_name = server.exec_command(
            ". /home/oracle/.bash_profile;srvctl config scan  | grep ^\"SCAN name\" | awk '{print $3}' | awk -F\. '{print $1}' | sed \"s/,//g\"")
        logger.debug("scan_name={0}".format(scan_name))

        comments = server.exec_command("cat /etc/redhat-release")
        logger.debug("comments={0}".format(comments))
        ssh_port = server.exec_command(
            "sudo cat /etc/ssh/sshd_config | grep Port | grep -v GatewayPorts | grep -Eo '[0-9]+'")
        logger.debug("ssh_port={0}".format(ssh_port))
        cpu_model = str(server.exec_command("cat /proc/cpuinfo | grep ^'model name' | head -n 1")).split(":")[1]
        logger.debug("cpu_model={0}".format(cpu_model))
        cpu_cores = server.exec_command(
            "cat /proc/cpuinfo | grep ^'siblings' | head -n 1 | awk -F: '{print $2}'| sed 's/[[:space:]]//g'")
        logger.debug("cpu_cores={0}".format(cpu_cores))
        physical_cpu = server.exec_command(
            "cat /proc/cpuinfo | grep ^'physical id' |  awk -F: '{print $2}' | sed 's/[[:space:]]//g' | sort | uniq | wc -l")
        logger.debug("physical_cpu={0}".format(physical_cpu))
        kernel_release = server.exec_command("uname -r")
        logger.debug("kernel_release={0}".format(kernel_release))
        processor = server.exec_command("uname -p")
        logger.debug("processor={0}".format(processor))
        hardware_platform = server.exec_command("uname -i")
        logger.debug("hardware_platform={0}".format(hardware_platform))
        host_ip = server.exec_command(
            "nslookup %s | grep ^Address:| grep -v '#'| awk -F: '{print $2}'| sed 's/[[:space:]]//g'" % (
                host_name))
        logger.debug("host_ip={0}".format(host_ip))
        host_vip_name = "{0}-vip".format(host_name)
        logger.debug("host_vip_name={0}".format(host_vip_name))
        host_vip_ip = server.exec_command(
            "nslookup %s | grep ^Address: | grep -v '#' | awk -F: '{print $2}' | sed 's/[[:space:]]//g'" % (
                host_vip_name))
        logger.debug("host_vip_ip={0}".format(host_vip_ip))
        priv_name = "{0}-HAIP1".format(host_name)
        priv_ip = server.exec_command("sudo cat /etc/hosts | grep %s | awk '{print $1}'" % (priv_name))
    except Exception as e:
        raise wbxexception("WBXERROR: Error occurred:depot_manage_reload, e=%s" % (str(e)))

    trim_host=""
    lc_code= ""
    site_code=""
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        server_info = dao.get_host_info_by_hostname(host_name)
        daoManager.commit()
        if len(server_info) > 0:
            trim_host = dict(server_info[0])['trim_host']
            lc_code = dict(server_info[0])['lc_code']
            logger.debug("trim_host={0}".format(trim_host))
            logger.debug("lc_code={0}".format(lc_code))
    except Exception as e:
        daoManager.rollback()
        logger.error("get host info from depotDB error occurred with error %s" % str(e), exc_info=e, stack_info=True)

    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        site_code = dict(dao.get_site_code(lc_code)[0])['site_code']
        logger.debug("site_code={0}".format(site_code))
    except Exception as e:
        daoManager.rollback()
        logger.error("get_site_code error occurred with error %s" % str(e), exc_info=e,
                     stack_info=True)

    hostInfoVo = wbxserver
    hostInfoVo.trim_host = trim_host
    hostInfoVo.host_name = host_name
    hostInfoVo.domain = "webex.com"
    hostInfoVo.site_code = site_code
    hostInfoVo.host_ip = host_ip
    hostInfoVo.vip_name = host_vip_name
    hostInfoVo.vip_ip = host_vip_ip
    hostInfoVo.priv_name = priv_name
    hostInfoVo.priv_ip = priv_ip
    hostInfoVo.scan_name = scan_name
    hostInfoVo.scan_ip1 = scan_ip1
    hostInfoVo.scan_ip2 = scan_ip2
    hostInfoVo.scan_ip3 = scan_ip3
    hostInfoVo.os_type_code = "R-L"
    hostInfoVo.processor = processor
    hostInfoVo.kernel_release = kernel_release
    hostInfoVo.hardware_platform = hardware_platform
    hostInfoVo.physical_cpu = physical_cpu
    hostInfoVo.cores = cpu_cores
    hostInfoVo.cpu_model = cpu_model
    hostInfoVo.flag_node_virtual = "N"
    hostInfoVo.comments = comments
    hostInfoVo.lc_code = lc_code
    hostInfoVo.ssh_port = scan_port
    return vars(hostInfoVo)

def depot_manage_add_DB(host_name,db_name,db_type,application_type,appln_support_code,web_domain,ismonitor,wbx_cluster):
    logger.info(
        "depot_manage_add_DB, host_name=%s,db_name=%s,db_type=%s,application_type=%s,appln_support_code=%s,web_domain=%s,ismonitor=%s,wbx_cluster=%s " % (
        host_name, db_name, db_type, application_type, appln_support_code, web_domain, ismonitor,wbx_cluster))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        db_info = dao.get_database_info(db_name)
        if len(db_info) > 0:
            res["status"] = "FAILED"
            res["errormsg"] = "The database_info has already exists. No need to add again. Please check it."
            return res
    except Exception as e:
        daoManager.rollback()

    trim_host = ""
    _inst_name = ""
    db_version = ""
    service_name = ""
    listener_port = ""
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)

        server_info = dao.get_host_info_by_hostname(host_name)
        daoManager.commit()
        if len(server_info) > 0:
            trim_host = dict(server_info[0])['trim_host']
    except Exception as e:
        daoManager.rollback()
        logger.error("get host info from depotDB error occurred with error %s" % str(e), exc_info=e, stack_info=True)

    try:
        server = daoManagerFactory.getServer(host_name)
        try:
            server.connect()
        except Exception as e:
           res["status"] = "FAILED"
           res["errormsg"] = "Error occurred:server.connect, host_name=%s, e=%s" % (host_name,str(e))
           logger.error("Error occurred:server.connect, host_name=%s, e=%s" % (host_name,str(e)))
           return res

        db_home = server.exec_command("cat /etc/oratab | grep %s* |awk '{print $1}' |awk -F : '{print $2}'" %(db_name))
        cmd = '''
        . /home/oracle/.bash_profile;ps -ef |grep _smon_%s* |grep -v grep |grep -vi ASM |awk '{print $NF}' |  awk -F: '{print $NF}'|awk -F _ '{print $NF}'
        ''' %(str(db_name).lower())
        _inst_name = server.exec_command(cmd)
        logger.info("_inst_name={0}".format(_inst_name))
        logger.info("db_home={0}".format(db_home))
        cmd = """
                                    . /home/oracle/.bash_profile
                                    db
                                    export ORACLE_SID=%s
                                    sqlplus / as sysdba << EOF 
                                    select version||',isFlag' from v\$instance;
                                    exit;
                                    EOF
                                    """ % (_inst_name)
        rest_list = server.exec_command(cmd)
        for res_info in str(rest_list).split("\n"):
            if "isFlag" in res_info:
                db_version = res_info.split(",")[0]
        logger.info("db_version={0}".format(db_version))
        cmd = ". /home/oracle/.bash_profile;srvctl config database -d %s" %(db_name)
        rest_list = server.exec_command(cmd)
        for res_info in str(rest_list).split("\n"):
            if "Services" in res_info:
                service_name = str(res_info.split(":")[1]).strip().split(",")[0]
        logger.info("service_name={0}".format(service_name))
        cmd = """
                                                    . /home/oracle/.bash_profile
                                                    db
                                                    export ORACLE_SID=%s
                                                    sqlplus / as sysdba << EOF 
                                                    set linesize 1000;
                                                    show parameter local_listener;
                                                    exit;
                                                    EOF
                                                    """ % (_inst_name)
        rest_list = server.exec_command(cmd)
        # print(rest_list)
        for res_info in str(rest_list).split("\n"):
            if "PORT" in res_info:
                port_value = res_info.strip()
                listener_port = re.search(r'PORT=([0-9]+)',port_value).groups()[0]
        logger.info("listener_port={0}".format(listener_port))
    except Exception as e:
        raise wbxexception("WBXERROR: Error occurred:depot_manage_add_DB, e=%s" % (str(e)))

    databaseVo = wbxdatabase
    databaseVo.db_name = db_name
    databaseVo.trim_host = trim_host
    databaseVo.db_vendor = "Oracle"
    databaseVo.db_version = db_version
    databaseVo.db_type = db_type
    databaseVo.application_type = application_type
    databaseVo.appln_support_code = appln_support_code
    databaseVo.db_home = db_home
    databaseVo.service_name = service_name
    databaseVo.listener_port = listener_port
    databaseVo.monitor = ismonitor
    databaseVo.web_domain = web_domain
    databaseVo.contents = ""
    databaseVo.wbx_cluster = wbx_cluster

    logger.info("insert database_info,trim_host=%s, host_name=%s, db_name=%s, " % (trim_host, host_name, db_name))
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        dao.insert_database_info(databaseVo)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        logger.error("insert database_info error occurred with error %s" % str(e), exc_info=e,
                     stack_info=True)
        res["status"] = "FAILED"
        res["errormsg"] = "Error occurred:insert database_info, e=%s" % (str(e))
        return res

    res_2 = depot_manage_reload(host_name, "add")
    logger.info(res_2)
    return res

def parse_config(line,src_host_name,src_splex_sid):
    res = {"status": "SUCCESS", "errormsg": "","data":None}
    tgt_host_name = ""
    tgt_splex_sid = ""
    re2 = re.match(r'(.*)\*(.*)-vip(.*)@', line)
    if re2:
        tgt_host_name = re2.groups()[1]
        if "-" in tgt_host_name:
            tgt_host_name = tgt_host_name.split("-")[0]
    re4 = re.match(r'(.*)@o.(.*)', line)
    if re4:
        tgt_splex_sid = re4.groups()[1].upper()

    item = {}
    item['src_host_name'] = src_host_name
    item['tgt_host_name'] = tgt_host_name
    item['src_splex_sid'] = src_splex_sid
    item['tgt_splex_sid'] = tgt_splex_sid
    if not tgt_host_name or not tgt_splex_sid:
        res["status"] = "FAILED"
        res["errormsg"] = "Missing essential information,  tgt_host_name={1},tgt_splex_sid={1} , line={2}" .format(tgt_host_name,tgt_splex_sid,line)
        return res

    item['replication_to'] = "auto"
    item['src_db'] = ""
    item['tgt_db'] = ""

    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    server = daoManagerFactory.getServer(src_host_name)
    try:
        cmd4 = """. /home/oracle/.bash_profile
                   db
                   sqlplus system/sysnotallow@%s << EOF
                   set linesize 1000;
                   select name||',isFlag' from v\$database;
                   exit;
                    EOF
                    """ % (
            src_splex_sid)

        src_db_str_2 = server.exec_command(cmd4)
        for src_db_str_item in str(src_db_str_2).split("\n"):
            if "isFlag" in src_db_str_item:
                item['src_db'] = src_db_str_item.split(",")[0]
                break
        if not item['src_db']:
            res["status"] = "FAILED"
            res["errormsg"] = "Do not find src_db , src_host_name={0}, src_splex_sid={1}, line={2}".format(
                src_host_name, src_splex_sid,line)
            return res

    except Exception as e:
        logger.error(str(e))
        res["status"] = "FAILED"
        res["errormsg"] = "Error occurred, e=%s" % (str(e))
        return res

    tgt_server = daoManagerFactory.getServer(tgt_host_name)
    if not tgt_server:
        res["status"] = "FAILED"
        res["errormsg"] = "Do not find this server, host_name={0}, line={1}" .format(tgt_host_name,line)
        return res
    tgt_server.connect()
    try:
        cmd6 = '''
              . /home/oracle/.bash_profile
              db
              sqlplus system/sysnotallow@%s << EOF
              select name||',isFlag' from v\$database;
              exit;
              EOF
            ''' % (
            tgt_splex_sid)
        tgt_db_str_2 = tgt_server.exec_command(cmd6)
        for tgt_db_str_item in str(tgt_db_str_2).split("\n"):
            if "isFlag" in tgt_db_str_item:
                item['tgt_db'] = tgt_db_str_item.split(",")[0]
        if not item['tgt_db']:
            err = "Do not find tgt_db , tgt_host_name={0}, tgt_splex_sid={1}, line={2}".format(
                tgt_host_name, tgt_splex_sid,line)
            logger.error(err)
            res["status"] = "FAILED"
            res["errormsg"] = err
            return res

    except Exception as e:
        logger.error(str(e), exc_info=e)
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    res['data'] = item
    return res

def depot_manage_reload_shareplex(src_host_name,port):
    logger.info("depot_manage_reload_shareplex, src_host_name=%s, port=%s " % (src_host_name,port))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    server = daoManagerFactory.getServer(src_host_name)
    daoManager = daoManagerFactory.getDefaultDaoManager()
    if server == None:
        res["status"] = "FAILED"
        res["errormsg"] = "Error occurred, do not find this server, src_host_name=%s " % (src_host_name)
        return res
    try:
        server.connect()
    except Exception as e:
        logger.error(str(e))
        res["status"] = "FAILED"
        res["errormsg"] = "Error occurred, src_host_name=%s, e=%s," % (src_host_name,str(e))
        return res
    splex_list = []
    try:
        cmd = "ps -ef | grep sp_cop | grep -v grep | grep %s | awk '{print $8}' | sed -n '1p'" % port
        logger.info(cmd)
        pre_str = server.exec_command(cmd)
        logger.info(pre_str)
        if pre_str == "" or "/" not in pre_str:
            res["status"] = "FAILED"
            res["errormsg"] = "Error occurred, do not find sp_cop, port=%s,host_name=%s, %s" % (port, src_host_name,pre_str)
            return res
        pre = str(pre_str).split('/')[1]
        cmd1 = "cat /" + pre + "/vardir_" + str(
            port) + "/data/statusdb | grep \"active from\" | awk -F\\\" '{print $2}'"
        logger.info(cmd1)
        active_config_file = server.exec_command(cmd1)
        logger.info("active_config_file={0}" .format(active_config_file))
        if not active_config_file:
            res["status"] = "FAILED"
            res["errormsg"] = "Do not find active config file, src_host_name={0},port={1}" .format(src_host_name,port)
            return res
        active_config_filename_path = "/{0}/vardir_{1}/config/{2}".format(pre, port, active_config_file)
        logger.info("active_config_filename_path={0}" .format(active_config_filename_path))

        cmd2 = "cat " + active_config_filename_path + " | grep -i Datasource"
        Datasource_line = server.exec_command(cmd2)
        logger.info("***********************************************************")
        logger.info(Datasource_line)
        if not Datasource_line:
            res["status"] = "FAILED"
            res["errormsg"] = "Error occurred, Datasource not found, src_host_name=%s," % (src_host_name)
            return res

        re1 = re.match(r'Datasource:o.(.*)', Datasource_line, re.IGNORECASE)
        src_splex_sid = re1.groups()[0].upper()
        if not src_splex_sid:
            res["status"] = "FAILED"
            res["errormsg"] = "Error occurred, src_splex_sid not found, src_host_name=%s," % (src_host_name)
            return res
        cmd3 = "cat " + active_config_filename_path +" | grep -vE '#|^$|^!' | grep -vi Datasource | grep -vi SPLEX_MONITOR_ADB"
        # logger.info(cmd3)
        active_config_file_content_1 = server.exec_command(cmd3)
        cmd4 = "cat " + active_config_filename_path + " | grep -vE '#|^$|^!' | grep -vi Datasource | grep -i SPLEX_MONITOR_ADB"
        # logger.info(cmd4)
        active_config_file_content_2 = server.exec_command(cmd4)

        cmd5 = "cat "+ active_config_filename_path +" | grep -vE '#' | grep '!' | grep -vi '!KEY' | wc -l"
        partition_count = server.exec_command(cmd5)
        logger.info("partition_count={0}".format(partition_count))
        partion_list = None
        if int(partition_count) > 0:
            logger.info("parse partitioning file")
            partition_res = parse_partition_content(src_host_name,port)
            if partition_res["status"] != "SUCCESS":
                res["status"] = "FAILED"
                res["errormsg"] = "Error occurred, parse partitioning file, src_host_name=%s, e=%s" % (src_host_name,partition_res['errormsg'])
                return res
            partion_list = partition_res['data']

        active_config_file_content = active_config_file_content_1 + "\n" + active_config_file_content_2
        lines = active_config_file_content.split("\n")
        routing_map = {}
        routing_map_schema = {}
        for line in lines:
            logger.info(line)
            line = ' '.join(line.split()).strip()
            if len(line) > 0:
                if "(" in line:
                    new_list = []
                    for split in line.split(" "):
                        if "(" not in split:
                            new_list.append(split)
                    line = " ".join(str(i) for i in new_list)
                    logger.info("--->" + line)

                line_list = line.split(" ")
                if "+" in line or "!kafka" in line or len(line_list)<3:
                    logger.info("***[Unsupported]***  {0} " .format(line))
                    job = wbxchatbot()
                    content = "### There is a config file that cannot be parsed on shareplex config file." + "\n"
                    content += "\tsrc_host_name\tport\tfile path" + "\t\t\t\t\t\t\t\t\t\t\t\t\t\ttext" + "\n"
                    text = """\t%s\t\t%s\t%s\t\t%s""" % (src_host_name, port, active_config_filename_path, line)
                    content += text + "\n"
                    job.alert_msg_to_dbabot_by_roomId(content,
                                                      "Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5")
                else:
                    if len(line_list) > 3:
                        line = ' '.join([line_list[0], line_list[1], line_list[-1]])
                        logger.info("--->" + line)

                    routing = line.split(" ")[-1]
                    if "!" in routing:
                        route_name = line.split(" ")[-1].split("!")[1].strip()
                        new_routing = ""
                        for partion in partion_list:
                            Scheme = ""
                            Name = ""
                            Route = ""
                            Condition = ""
                            if "Scheme" in partion:
                                Scheme = partion['Scheme']
                            if "Name" in partion:
                                Name = partion['Name']
                            if "Route" in partion:
                                Route = partion['Route']
                            if "Condition" in partion:
                                Condition = partion['Condition']
                            if route_name == Scheme or route_name == Condition:
                                new_routing = Route
                        if new_routing =="":
                            res["status"] = "FAILED"
                            res["errormsg"] = "Do not find this partition , route_name={0}, src_host_name={1} ,line={2}".format(route_name, src_host_name,line)
                            return res
                        line = ' '.join([line_list[0], line_list[-2], new_routing])
                    routing = line.split(" ")[-1]
                    item = {}
                    src_schema = line.split(" ")[0].split(".")[0].lower()
                    tgt_schema = line.split(" ")[1].split(".")[0].lower()
                    if not src_schema or not tgt_schema:
                        res["status"] = "FAILED"
                        res["errormsg"] = "Do not find src_schema or tgt_schema, src_schema={0}, tgt_schema={1}".format(
                            src_schema, tgt_schema)
                        return res
                    qname = ""
                    re2 = re.match(r'(.*)SPLEX_MONITOR_ADB_(.*)', line.split(" ")[0], re.IGNORECASE)
                    if re2:
                        qname = re2.groups()[1].lower()
                    item['src_schema'] = src_schema
                    item['tgt_schema'] = tgt_schema
                    item['qname'] = qname
                    if routing not in routing_map:
                        logger.info("parse config line")
                        parse_res = parse_config(line, src_host_name, src_splex_sid)
                        if parse_res['status'] != "SUCCESS":
                            res["status"] = "FAILED"
                            res["errormsg"] = parse_res['errormsg']
                            return res
                        routing_map[routing] = parse_res
                        data_m = routing_map[routing]['data']
                        key = data_m['tgt_host_name'] + "-" + data_m['tgt_splex_sid']
                        if "splex" not in src_schema and "splex" not in tgt_schema:
                            if key not in routing_map_schema:
                                schema_list = []
                                schema = {}
                                schema['src_schema'] = src_schema
                                schema['tgt_schema'] = tgt_schema
                                schema_list.append(schema)
                                routing_map_schema[key] = schema_list
                            else:
                                schema_list = routing_map_schema[key]
                                schema = {}
                                schema['src_schema'] = src_schema
                                schema['tgt_schema'] = tgt_schema
                                if schema not in schema_list:
                                    schema_list.append(schema)
                                    routing_map_schema[key] = schema_list
                    else:
                        logging.info("skip parse")

                    data = routing_map[routing]['data']

                    item['src_host_name'] = data['src_host_name']
                    item['tgt_host_name'] = data['tgt_host_name']
                    item['src_splex_sid'] = data['src_splex_sid']
                    item['tgt_splex_sid'] = data['tgt_splex_sid']
                    item['replication_to'] = data['replication_to']
                    item['src_db'] = data['src_db']
                    item['tgt_db'] = data['tgt_db']
                    if item not in splex_list:
                        logger.info(item)
                        splex_list.append(item)

        logger.info("***********************************************************")
        return_splex_list = []
        return_splex_list_key = []
        for info in splex_list:
            logger.info("origin:{0}" .format(info))
            add_info_list = []
            if "splex" not in info['src_schema'] and "splex" not in info['tgt_schema']:
                add_info_list.append(info)
            else:
                routing_key = info['tgt_host_name'] + "-" + info['tgt_splex_sid']
                if routing_key in routing_map_schema:
                    schema_list = routing_map_schema[routing_key]
                    for schema in schema_list:
                        info_new = {}
                        info_new['src_schema'] = schema['src_schema']
                        info_new['tgt_schema'] = schema['tgt_schema']
                        info_new['qname'] = info['qname']
                        info_new['src_host_name'] = info['src_host_name']
                        info_new['tgt_host_name'] = info['tgt_host_name']
                        info_new['replication_to'] = info['replication_to']
                        info_new['src_db'] = info['src_db']
                        info_new['tgt_db'] = info['tgt_db']
                        info_new['src_splex_sid'] = info['src_splex_sid']
                        info_new['tgt_splex_sid'] = info['tgt_splex_sid']
                        add_info_list.append(info_new)
                else:
                    logging.info("{0} not exist before, skip".format(routing_key))
            for add in add_info_list:
                add_key = add['src_host_name'] + "_" + add['tgt_host_name'] + "_" + add['src_db'] + "_" + add[
                    'tgt_db'] + "_" + add["src_splex_sid"] + "_" + add["tgt_splex_sid"] + "_" + add[
                              'src_schema'] + "_" + add['tgt_schema']
                if add['replication_to']:
                    add_key += "_" + add['replication_to']
                if add['qname']:
                    add_key += "_" + add['qname']
                if add_key not in return_splex_list_key:
                    try:
                        daoManager.startTransaction()
                        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                        ls = dao.get_replication_to(add['src_db'],add['tgt_db'],add['src_schema'],add['tgt_schema'])
                        if len(ls) >0:
                            add['replication_to'] = str(dict(ls[0])['replication_to'])
                        daoManager.commit()
                    except Exception as e:
                        daoManager.rollback()
                        logger.error(str(e), exc_info=e)
                        res["status"] = "FAILED"
                        res["errormsg"] = str(e)
                        return res
                    logger.info("now:{0}" .format(add))
                    return_splex_list.append(add)
                    return_splex_list_key.append(add_key)

        if len(return_splex_list)>0:
            src_db = return_splex_list[0]['src_db']
            try:
                daoManager.startTransaction()
                dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                logger.info("delete shareplex_info start, src_db={0},src_host_name={1},port={2} ".format(src_db,src_host_name,port))
                dao.delete_shareplex_info(src_db,src_host_name,port)
                daoManager.commit()
            except Exception as e:
                daoManager.rollback()
                logger.error(str(e), exc_info=e)
                res["status"] = "FAILED"
                res["errormsg"] = str(e)
                return res
            logger.info("delete shareplex_info done")

    except Exception as e:
        logger.error(str(e), exc_info=e)
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res

    logger.info("************************* final result, count={0} **********************************".format(len(return_splex_list)))
    #For add case
    for item in return_splex_list:
        logger.info(item)
        try:
            daoManager.startTransaction()
            dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            dao.insertShareplex_info(src_host_name, item['src_db'], port, item['replication_to'],
                                     item['tgt_host_name'],
                                     item['tgt_db'], item['qname'], src_splex_sid, item['tgt_splex_sid'],
                                     item['src_schema'], item['tgt_schema'])
            daoManager.commit()
        except Exception as e:
            daoManager.rollback()
            logger.error(str(e), exc_info=e)
            res["status"] = "FAILED"
            res["errormsg"] = str(e)
            return res
    return res

def depot_manage_add_user(password,db_name,trim_host,user_name,appln_support_code,schematype):
    logger.info(
        "depot_manage_add_user, password=%s,db_name=%s,trim_host=%s,user_name=%s,appln_support_code=%s,schematype=%s" % (password,db_name,trim_host,user_name,appln_support_code,schematype))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        applist = depotdbDao.getApplnList(trim_host, db_name, user_name)
        if len(applist) == 0:
            depotdbDao.insertUserPwdInfo(password, trim_host, db_name, user_name, appln_support_code,schematype)
        else:
            errormsg = "The user info has already exists. No need to add again."
            logger.info(errormsg)
            res = {"status": "FAILED", "errormsg": errormsg}
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        logger.error("depot_manage_add_user error occurred", exc_info=e, stack_info=True)
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    return res

def get_depot_manage_instance(db_name):
    logger.info(
        "get_depot_manage_instance, db_name=%s" % (db_name))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        info = dao.get_depot_manage_instance(db_name)
        res['data'] = [dict(vo) for vo in info]
        daoManager.commit()
        return res
    except Exception as e:
        daoManager.rollback()

def get_depot_manage_pool_info(db_name):
    logger.info(
        "get_depot_manage_instance, db_name=%s" % (db_name))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        info = dao.get_depot_manage_pool_info(db_name)
        res['data'] = [dict(vo) for vo in info]
        daoManager.commit()
        return res
    except Exception as e:
        daoManager.rollback()

def add_depot_manage_pool_info(trim_host,db_name,appln_support_code,pool_name,schema,service_name):
    logger.info(
        "add_depot_manage_pool_info, trim_host=%s,db_name=%s,appln_support_code=%s,pool_name=%s,schema=%s" % (
        trim_host, db_name, appln_support_code, pool_name, schema))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        dao.insertAppln_mapping_info(trim_host, db_name, appln_support_code, pool_name, schema,service_name)
        daoManager.commit()
        return res
    except Exception as e:
        daoManager.rollback()
        raise wbxexception(str(e))

def parse_partition_content(src_host_name,port):
    res = {"status": "SUCCESS", "errormsg": "","data":None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    server = daoManagerFactory.getServer(src_host_name)
    # partion_map = {}
    partion_list = []
    try:
        cmd6 = "ps -ef | grep sp_cop | grep -v grep | grep %s | awk '{print $8}' | sed -n '1p'" % (port)
        logger.info(cmd6)
        res6 = server.exec_command(cmd6)
        re6 = re.match(r'(.*).app-modules(.*)', res6)
        bin_path = ""
        if re6:
            bin_path = "%sbin" % (str(re6.groups()[0]))
        if bin_path == "":
            res["status"] = "FAILED"
            res["errormsg"] = "Error occurred, find Shareplex bin Dir, port={0}, host_name={1}".format(port,src_host_name)
            return res

        cmd = """
                               source %s/.profile_%s;cd %s;
                               ./sp_ctrl << EOF
                               view partitions all
                               EOF
                               """ % (bin_path, port, bin_path)
        content = server.exec_command(cmd)
        logger.info("view partitions all:")
        logger.info(content)
        key_list = []
        contents = content.split('\n')
        for line in contents:
            reg1 = re.match(r'(.*)Scheme(.*)', line)
            reg2 = re.match(r'(.*)Route(.*)', line)
            reg = re.match(r'(.*)@o.(.*)', line)
            if reg1 or reg2:
                key_list = ' '.join(line.split('    ')).strip().split()
            if reg:
                value_list = [x.strip() for x in line.split('    ') if x.strip() != '']
                partion_map = {}
                for index, value in enumerate(value_list):
                    partion_map[key_list[index]] = value
                partion_list.append(partion_map)
        res['data'] = partion_list
    except Exception as e:
        res["status"] = "FAILED"
        res["errormsg"] = "Error occurred,parse partition , host_name={0}, e={1}, ".format(src_host_name, str(e))
        return res
    return res

def get_instance_by_db(db_name):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    try:
        daoManager = daoManagerFactory.getDaoManager(db_name)
    except Exception as e:
        raise wbxexception(str(e))
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        instances = dao.get_instance()
        daoManager.commit()
        map = {}
        for info in instances:
            item = dict(info)
            instance_name = item['instance_name']
            host_name = str(item['host_name']).split(".")[0]
            if instance_name not in map:
                map[instance_name] = []
                map[instance_name].append(host_name)
            else:
                new_ls = map[instance_name]
                new_ls.append(host_name)
                map[instance_name] = new_ls
        return map
    except Exception as e:
        daoManager.rollback()
        raise wbxexception(str(e))

def get_DB_connection_endpoint(db_type,appln_support_code,web_domain,schema,db_name):
    logger.info(
        "get_DB_connection_endpoint, db_type=%s, appln_support_code=%s, web_domain=%s, schema=%s,db_name=%s " % (db_type,appln_support_code,web_domain,schema,db_name))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        info = dao.get_connection_endpoint(db_type,appln_support_code,web_domain,schema,db_name)
        daoManager.commit()
        return dict(info)
    except Exception as e:
        daoManager.rollback()
        raise wbxexception(str(e))

def get_pgdb_info(**kwargs):
    logger.info("get_pgdb_info, kwargs=%s" % (kwargs))
    res = {"status": "SUCCESS", "errormsg": ""}
    release_number = kwargs.get("release_number", "")
    service_name = kwargs.get("release_number", "")
    service_env = kwargs.get("service_env", "")
    service_dc = kwargs.get("service_dc", "")
    service_cluster = kwargs.get("service_cluster", "")
    app_code = kwargs.get("app_code", "")
    app_type = kwargs.get("app_type", "")

    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        # dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        # dao.add_alertdetail(wbxmonitoralertdetailVo)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)

if __name__ == "__main__":
    host_name1 = 'tbormt014'
    host_name = str(host_name1).split(".")[0]
    print(host_name)