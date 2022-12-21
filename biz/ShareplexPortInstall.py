import logging
import requests
import traceback
import json
import uuid
import datetime
from common.wbxssh import wbxssh
from common.wbxtask import wbxautotask
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from sqlalchemy.exc import DBAPIError, DatabaseError
from common.wbxexception import wbxexception
from common.wbxcache import curcache
from common.wbxutil import wbxutil
from dao.vo.autotaskvo import wbxautotaskvo
from collections import OrderedDict
import threading

logger = logging.getLogger("DBAMONITOR")
BINARY_FILE_NAME = "SharePlex-10.0.0-b107-r56750-ONEOFF-SPO-20916-20929b-15335d-3895-17174-20982-rhel-amd64-m64.tpm"


def setup_shareplex_port(host_name, port, datasource, createby):
    sp = None
    status = "SUCCEED"
    errormsg = ""
    host_name = host_name.split(".")[0]
    try:
        sp = shareplexport.newInstance(host_name, port, datasource)
        sp.login()
        taskid = record_automation("SHAREPLEXPORTINSTALL_TASK",
                                   json.dumps({"host_name": host_name, "port": port, "datasource": datasource}), createby)
        sp.taskid = taskid
        record_shareplexportinstall_log(taskid, host_name, datasource, port, "EXECUTE_INSTALL", "RUNNING", "")
        record_shareplexportinstall_log(taskid, host_name, datasource, port, "SHAREPLEX_INSTALL", "RUNNING", "")
        sp = shareplexport.newInstance(host_name, port, datasource)
        sp.taskid = taskid
        sp.login()
        sp.execute_install()
    except Exception as e:
        status = "FAIL"
        errormsg = str(e)
        if sp is not None:
            sp.logstr.append("%s\n%s" % (errormsg, traceback.format_exc()))
    finally:
        if sp is not None:
            sp.close()
        if sp is not None and sp.taskid:
            record_shareplexportinstall_log(sp.taskid, host_name, datasource, port, "EXECUTE_INSTALL", status,
                                            "\n".join(sp.logstr))
            record_shareplexportinstall_log(sp.taskid, host_name, datasource, port, "SHAREPLEX_INSTALL", status, "")
    return {"status": status,
            "errormsg": errormsg}


def get_shareplex_port_install_history_list():
    status = "SUCCEED"
    errormsg = ""
    data = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data = depotdbDao.get_shareplex_port_install_history()
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        status = "FAIL"
        errormsg = str(e)
    finally:
        depotDaoManager.close()
    return {"status": status,
            "data": data,
            "errormsg": errormsg}


def get_shareplex_port_install_detail(taskid):
    if not taskid:
        return {
            "status": "FAIL",
            "logstr": [],
            "errormsg": "taskid cannot be null"
        }
    status = "SUCCEED"
    errormsg = ""
    logstr = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        logstr = depotdbDao.get_shareplex_port_install_detail_log(taskid)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        status = "FAIL"
        errormsg = str(e)
    finally:
        depotDaoManager.close()
    return {"status": status,
            "logstr": logstr,
            "errormsg": errormsg}


def preverify_shareplex_port_install(host_name, port, datasource):
    sp = None
    status = "SUCCEED"
    errormsg = ""
    host_name = host_name.split(".")[0]
    try:
        sp = shareplexport.newInstance(host_name, port, datasource)
    except Exception as e:
        status = "FAIL"
        errormsg = "".join(str(e).split("\n"))
    finally:
        if sp is not None:
            sp.close()
    return {"status": status,
            "errormsg": errormsg}


# def execute_shareplex_port_install(host_name, port, datasource):
#     sp = None
#     status = "SUCCEED"
#     errormsg = ""
#     try:
#         sp = shareplexport.newInstance(host_name, port, datasource)
#         # sp.taskid = "dda42f1a031c4200868add017225cae4"
#         sp.checkpreverified()
#         record_shareplexportinstall_log(sp.taskid, host_name, datasource, port, "EXECUTE_INSTALL", "RUNNING", "")
#         sp.login()
#         sp.execute_install()
#     except Exception as e:
#         status = "FAIL"
#         errormsg = str(e)
#         sp.logstr.append(errormsg)
#     finally:
#         if sp is not None:
#             sp.close()
#         print(sp.logstr)
#         record_shareplexportinstall_log(sp.taskid, host_name, datasource, port, "EXECUTE_INSTALL", status, "\n".join(sp.logstr))
#         record_shareplexportinstall_log(sp.taskid, host_name, datasource, port, "SHAREPLEX_INSTALL", status, "")
#     return {"status": status,
#             "errormsg": errormsg}


def record_automation(taskType, args_str, createby):
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
        daoManager.commit()
        return taskvo.taskid
    except Exception as e:
        daoManager.rollback()
        raise e
    finally:
        daoManager.close()


def record_shareplexportinstall_log(taskid, host_name, datasource, port, process_type, status, logstr):
    logstr = "\n".join(logstr) if isinstance(logstr, list) else logstr
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        oldlog = depotdbDao.getshareplexportlog(taskid, process_type)
        if oldlog:
            depotdbDao.updateshareplexportlog(taskid, process_type, status, logstr)
        else:
            depotdbDao.insertshareplexportlog(taskid, host_name, datasource, port, process_type, status, logstr)
        depotDaoManager.commit()
    except DatabaseError as e:
        logger.error(e)
        depotDaoManager.rollback()
        raise wbxexception(e)
    finally:
        depotDaoManager.close()


class shareplexport:

    def __init__(self, server, port, datasource):
        self.server = server
        self.port = port
        self.datasource = datasource
        self.host_name = server.host_name
        self.taskid = ""
        self.logstr = []
        self.binary_file = BINARY_FILE_NAME
        self.port_install_script = "port_setup_10_automation.sh" # /staging/Scripts/oracle/port_setup/
        self.NEW_SP_SYS_PRODDIR = "SHAREPLEXNFS/shareplex10"

    def close(self):
        self.server.close()

    def login(self):
        self.server.connect()

    @staticmethod
    def newInstance(host_name, port, datasource):
        host_name = host_name.split(".")[0].lower()
        if not host_name:
            raise wbxexception("Hostname cannot be null")
        if not port or not port.isdigit():
            raise wbxexception("Port %s format is not allowed" % port)
        datasource = datasource.split("_")[0]
        if not datasource:
            raise wbxexception("Datasource cannot be null")
        key = "shareplexportinstall_%s_%s_%s" % (host_name, port, datasource)
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        server = None
        try:
            server = daoManagerFactory.getServer(host_name)
            server.verifyConnection()
            logger.info("server {0} ssh login verification passed".format(host_name))
            server.connect()
            cmd = "source /home/oracle/.bash_profile; crsstat | grep shareplex{0} | wc -l".format(port)
            logger.info(cmd)
            splex_port_count = server.exec_command(cmd)
            splex_port_count = splex_port_count.split("\n")[0]
            if int(splex_port_count) > 0:
                raise wbxexception("the port {0} has been running on server {1}".format(port, host_name))
            cmd = "source /home/oracle/.bash_profile; cat /etc/oratab | grep {0} | wc -l".format(datasource)
            logger.info(cmd)
            splex_datasource_count = server.exec_command(cmd)
            splex_datasource_count = splex_datasource_count.split("\n")[0]
            if int(splex_datasource_count) == 0:
                raise wbxexception("the datasource {0} not existed on server {1}".format(datasource, host_name))
            cmd = "df -h | grep splex | wc -l"
            logger.info(cmd)
            splex_nfs_count = server.exec_command(cmd)
            splex_nfs_count = splex_nfs_count.split("\n")[0]
            if int(splex_nfs_count) == 0:
                raise wbxexception("the shareplex nfs volume not existed on server {0}".format(host_name))
            cmd = "ls /staging/Software/Oracle/Software/Shareplex/{0} | wc -l".format(BINARY_FILE_NAME)
            logger.info(cmd)
            binary_file_count = server.exec_command(cmd)
            binary_file_count = binary_file_count.split("\n")[0]
            if int(binary_file_count) == 0:
                raise wbxexception("the shareplex binary file not existed on server {0}".format(host_name))
            cmd = "ls /staging/Scripts/oracle/port_setup/port_setup_10_automation.sh | wc -l".format(BINARY_FILE_NAME)
            logger.info(cmd)
            automation_file_count = server.exec_command(cmd)
            automation_file_count = automation_file_count.split("\n")[0]
            if int(automation_file_count) == 0:
                raise wbxexception("the shareplex port setup automation file not existed on server {0}".format(host_name))
        except DatabaseError as e:
            logger.error(e)
            raise wbxexception(e)
        finally:
            if server:
                server.close()
        sp = shareplexport(server, port, datasource)
        sp.logstr = []
        return sp

    def checkpreverified(self):
        if not self.taskid:
            raise wbxexception("Need preverify first")
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            depotDaoManager.startTransaction()
            depotdbDao.getshareplexportlog(self.taskid, "SHAREPLEX_INSTALL")
            preverify_status = depotdbDao.getshareplexportlog(self.taskid, "PREVERIFY")
            if preverify_status != "SUCCEED":
                raise wbxexception("Need preverify successfully first")
            depotDaoManager.commit()
        except DatabaseError as e:
            logger.error(e)
            depotDaoManager.rollback()
            raise wbxexception(e)
        finally:
            depotDaoManager.close()

    def execute_install(self):
        self.getShareplexInstalledCount()
        self.get_sys_prod_dir()
        self.install_binary_file()
        self.install_shareplex_port()

    def get_sys_prod_dir(self):
        self.logstr.append("======= GET SYS PROD DIR =======")
        logger.info("======= GET SYS PROD DIR =======")
        if self.server.host_name in ["frdbormt011", "frdbormt012"]:
            self.NEW_SP_SYS_PRODDIR = "/frdbormt01/shareplex10"
            return True
        cmd = "sudo grep nfs /etc/fstab | grep splex"
        fs_tab_data = self.server.exec_command(cmd)
        if not fs_tab_data:
            raise wbxexception("the sys prod nfs not exist")
        self.NEW_SP_SYS_PRODDIR = self.NEW_SP_SYS_PRODDIR.replace("SHAREPLEXNFS", address_fstab_data(fs_tab_data))
        if not self.server.isDirectory("/" + self.NEW_SP_SYS_PRODDIR.split("/")[1]):
            cmd = """
            sudo mkdir %s
            sudo chmod -R 775 %s
            sudo mount %s
            """ % ("/" + self.NEW_SP_SYS_PRODDIR.split("/")[1], "/" + self.NEW_SP_SYS_PRODDIR.split("/")[1], "/" + self.NEW_SP_SYS_PRODDIR.split("/")[1])
            self.server.exec_command(cmd)
        if not self.server.isDirectory(self.NEW_SP_SYS_PRODDIR) and self.server.isDirectory("/" + self.NEW_SP_SYS_PRODDIR.split("/")[1]):
            cmd = """
            sudo chmod -R 755 %s
            sudo mkdir %s
            sudo chmod -R 755 %s
            """ % ("/" + self.NEW_SP_SYS_PRODDIR.split("/")[1], self.NEW_SP_SYS_PRODDIR, self.NEW_SP_SYS_PRODDIR)
            self.server.exec_command(cmd)
        if not self.server.isDirectory(self.NEW_SP_SYS_PRODDIR):
            raise wbxexception("the sys prod dir not exist")

    def install_binary_file(self):
        self.logstr.append("======= INSTALL BINARY FILE =======")
        logger.info("======= INSTALL BINARY FILE =======")
        if self.isBinaryInstalled():
            self.logstr.append("Binary already installed")
            logger.info("Binary already installed")
            return True
        res = self.server.exec_command("sudo mkdir %s" % self.NEW_SP_SYS_PRODDIR)
        res = self.server.exec_command("sudo chown -R oracle:oinstall %s" % self.NEW_SP_SYS_PRODDIR)

        tpmfile = "/tmp/%s" % self.binary_file
        if not self.server.isFile(tpmfile):
            cmd = "cp -f /staging/Software/Oracle/Software/Shareplex/%s /tmp/" % self.binary_file
            res = self.server.exec_command(cmd)
            cmd = "chmod 775 %s" % tpmfile
            res = self.server.exec_command(cmd)
            if not self.server.isFile(tpmfile):
                raise wbxexception("The file does not exist %s" % tpmfile)

            cmd = "cksum %s | awk '{print $1}'" % tpmfile
            res = self.server.exec_command(cmd)
            if int(res) != 1914959346:
                raise wbxexception("The cksum value of %s is not 4138614057" % tpmfile)

        self.DEFAULT_VARDIR = "/tmp/shareplex_vardir"
        if self.server.isDirectory(self.DEFAULT_VARDIR):
            # remove installation temp dir
            cmd = "rm -rf %s" % self.DEFAULT_VARDIR
            res = self.server.exec_command(cmd)
        print('ooooooooooooooooooooooooooooooo')
        cmd = "sh /tmp/%s" % self.binary_file
        print(cmd)
        args = []
        if self.version_count > 0:
            args.append(str(self.version_count + 1))
        args.append(self.NEW_SP_SYS_PRODDIR)
        args.append(self.DEFAULT_VARDIR)
        args.append("1")
        args.append("2100")
        args.append("yes")
        args.append("yes")
        args.append("DZHCEZ8VJ8V54WPGAJ2NL73N8SQVZR6Z7B")
        args.append("CISCO SYSTEMS INC")
        logger.info("%s\n%s" % (cmd, args))
        res = self.server.exec_command(cmd, 300, True, *args)
        # kargs = {"product directory location": self.NEW_SP_SYS_PRODDIR,
        #          "variable data directory location": self.DEFAULT_VARDIR,
        #          "10. racdba": "1",
        #          "[2100]": "2100",
        #          "Proceed with installation" : "yes",
        #          "10.0.0 license" : "yes",
        #          "License key": "DZHCEZ8VJ8V54WPGAJ2NL73N8SQVZR6Z7B",
        #          "customer name associated with this license key": "CISCO SYSTEMS INC"}
        # print(kargs)
        # self.server.send(cmd)
        # while True:
        #     res = self.server.recvs(**kargs)
        #     print(res)
        #     if res.strip().endswith(('$')):
        #         break
        self.logstr.append(res)
        logger.info("installshareplexbinary end host_name=%s, splex_port=%s" % (self.host_name, self.port))
        self.logstr.append("installshareplexbinary end host_name=%s, splex_port=%s" % (self.host_name, self.port))

    def isBinaryInstalled(self):
        cmd = "ls -al %s/data/param-defaults 2>/dev/null | awk '{print $9}'" % self.NEW_SP_SYS_PRODDIR
        res = self.server.exec_command(cmd)
        return True if not wbxutil.isNoneString(res) else False

    def getShareplexInstalledCount(self):
        installfile = "/home/oracle/.shareplex/install.conf"
        if self.server.isFile(installfile):
            res = self.server.exec_command("""cat %s | grep "\/" | awk -F: '{print $1}' | wc -l""" % installfile)
            self.version_count = int(res)
        else:
            self.version_count = 0
        logger.info("version_count=%s on host %s splex_port %s" % (self.version_count, self.host_name, self.port))
        self.logstr.append("installshareplexbinary end host_name=%s, splex_port=%s" % (self.host_name, self.port))

    def install_shareplex_port(self):
        self.logstr.append("======= INSTALL SHAREPLEX PORT %s =======" % self.port)
        logger.info("======= INSTALL SHAREPLEX PORT %s =======" % self.port)
        if not self.server.isFile("/staging/Scripts/oracle/port_setup/%s" % self.port_install_script):
            raise wbxexception("the shareplex port install script %s not exist" % self.port_install_script)

        tpmfile = "/tmp/%s" % self.port_install_script
        if self.server.isFile(tpmfile):
            cmd = "rm -rf %s" % tpmfile
            logger.info("Removing old %s" % tpmfile)
            res = self.server.exec_command(cmd)
        cmd = "cp -f /staging/Scripts/oracle/port_setup/%s /tmp/" % self.port_install_script
        res = self.server.exec_command(cmd)
        cmd = "chmod 775 %s" % tpmfile
        res = self.server.exec_command(cmd)
        if not self.server.isFile(tpmfile):
            raise wbxexception("The file does not exist %s" % tpmfile)

        print("""
        {INPUTVAR_PORT}|%s
        {INPUTVAR_DATASOURCE}|%s
        {INPUTVAR_PROD_DIR}|%s""" % (self.port, self.datasource, self.NEW_SP_SYS_PRODDIR))
        cmd = """
        source /home/oracle/.bash_profile
        sed -i "s|{INPUTVAR_PORT}|%s|g" %s
        sed -i "s|{INPUTVAR_DATASOURCE}|%s|g" %s
        sed -i "s|{INPUTVAR_PROD_DIR}|%s|g" %s
        sed -i "s|{INPUTVAR_SCRIPT_DIR}|%s|g" %s
        sh %s
        """ % (self.port, tpmfile, self.datasource, tpmfile, self.NEW_SP_SYS_PRODDIR, tpmfile, "/staging/Scripts/oracle/port_setup", tpmfile, tpmfile)
        res = self.server.exec_command(cmd, 300)
        self.logstr.append(res)
        if "Exiting..." in res:
            raise wbxexception("Failed to setup the shareplex port!!")


def address_fstab_data(data):
    data_list = data.split("\n")
    volume_list = []
    #10.252.5.165:/sjdbcfg_new	/sjdbcfg	nfs	rw,bg,nointr,rsize=32768,wsize=32768,tcp,actimeo=0,vers=3,timeo=600,nolock	0	0
    for rst_item in data_list:
        if not rst_item or rst_item[0] == "#":
            continue
        mount_info = rst_item.replace("\t", " ").split(" nfs ")[0].split(" ")
        # disk_info = rst_item.replace("\t", " ").split(" nfs ")[-1].split(" ")
        mount_info = list(filter(None, mount_info))
        # disk_info = list(filter(None, disk_info))
    return mount_info[-1]


if __name__ == '__main__':
    sp = shareplexport(wbxssh("sjdbormt0152", 22, "oracle", "uB9d%LVD4Wq"), "70014", "BTTH_SPLEX")
    sp.execute_install()



