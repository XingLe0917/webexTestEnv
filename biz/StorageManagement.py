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


def create_nfs_volume(datacenter, size_gb, vol_name, client_list, notification_user, mount_dir):
    status = "SUCCEED"
    sp = None
    rst_dict = {}
    do_flag = False
    i = 0
    host_ip_list = []
    for host_name in client_list:
        i += 1
        try:
            sp, host_ip = wbxnfsstorageclient.newInstance(host_name, vol_name, mount_dir)
            sp.login()
            sp.check_mount_info_valid()
            host_ip_list.append(host_ip)
            if i == len(client_list):
                do_flag = True
            if do_flag:
                res, res_bool = nfsautomation(datacenter, vol_name).create_volume(size_gb, host_ip_list, notification_user)
                if not res_bool:
                    raise wbxexception(res)
                # record_automation("NFSCREATEVOLUME_TASK", json.dumps({"datacenter": datacenter, "size_gb": size_gb, "vol_name": vol_name, "client_list": client_list, "notification_user": notification_user, "mount_dir": mount_dir}))
        except Exception as e:
            rst_dict[host_name] = str(e)
            status = "FAIL"
        finally:
            if sp is not None:
                sp.close()
    return {"status": status,
            "rst_dict": rst_dict}


def add_nfs_client(datacenter, mount_ip, vol_name, client_list, notification_user, vol_type):
    sp = None
    status = "SUCCEED"
    rst_dict = {}
    do_flag = False
    for host_name in client_list:
        try:
            sp = wbxnfsstorageclient.newInstance(host_name, vol_name, None)
            sp.login()
            sp.check_mount_info_valid()
            if not do_flag:
                res, res_bool = nfsautomation(datacenter, vol_name).add_client(mount_ip, client_list, notification_user)
                if not res_bool:
                    raise wbxexception(res)
                record_automation("NFSADDCLIENT_TASK", json.dumps({"datacenter": datacenter, "mount_ip": mount_ip, "vol_name": vol_name, "client_list": client_list, "notification_user": notification_user}))
        except Exception as e:
            rst_dict[host_name] = str(e)
            status = "FAIL"
        finally:
            if sp is not None:
                sp.close()

    return {"status": status,
            "rst_dict": rst_dict}


def resize_nfs_volume(datacenter, additional_size, mount_ip, vol_name, notification_user):
    res, res_bool = nfsautomation(datacenter, vol_name).resize_volume(additional_size, mount_ip, notification_user)
    if not res_bool:
        logger.error(res)
        return {"status": "FAIL", "errormsg": str(res)}
    record_automation("NFSRESIZEVOLUME_TASK", json.dumps({"datacenter": datacenter, "additional_size": additional_size, "mount_ip": mount_ip, "vol_name": vol_name, "notification_user": notification_user}))
    return {"status": "SUCCEED", "errormsg": ""}


def mount_nfs(mount_ip, client_list, vol_name, mount_dir):
    status = "SUCCEED"
    rst_dict = {}
    sp = None
    vol_name = vol_name[1:] if vol_name[0] == "/" else vol_name
    for host_name in client_list:
        try:
            sp, host_ip = wbxnfsstorageclient.newInstance(host_name, vol_name, mount_dir)
            sp.login()
            sp.mount_ip = mount_ip
            sp.check_mount_info_valid()
            sp.mount_nfs_volume_to_server()
            record_automation("NFSRESIZEVOLUME_TASK", json.dumps({"mount_ip": mount_ip, "client_list": client_list, "vol_name": vol_name, "mount_dir": mount_dir}))
            rst_dict[host_name] = "%s:/%s %s nfs rw,bg,hard,nointr,rsize=32768,wsize=32768,tcp,vers=3,timeo=600,nolock 0 0" % (mount_ip, vol_name, mount_dir)
        except Exception as e:
            rst_dict[host_name] = str(e)
            status = "FAIL"
        finally:
            if sp is not None:
                sp.close()
    return {"status": status,
            "rst_dict": rst_dict}


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


def address_fstab_data(data):
    data_list = data.split("\n")
    volume_list = []
    #10.252.5.165:/sjdbcfg_new	/sjdbcfg	nfs	rw,bg,nointr,rsize=32768,wsize=32768,tcp,actimeo=0,vers=3,timeo=600,nolock	0	0
    for rst_item in data_list:
        if not rst_item or rst_item[0] == "#":
            continue
        mount_info = rst_item.replace("\t", " ").split(" nfs ")[0].split(" ")
        disk_info = rst_item.replace("\t", " ").split(" nfs ")[-1].split(" ")
        mount_info = list(filter(None, mount_info))
        disk_info = list(filter(None, disk_info))
        volume_list.append(mount_info[0])
    return volume_list


class wbxnfsstorageclient:

    def __init__(self, server, vol_name, mount_dir):
        self.host_name = server.host_name
        self.vol_name = vol_name
        self.mount_dir = mount_dir
        self.mount_ip = None
        self.nfs_config_file = "/etc/fstab"
        self.server = server
        # self.vol_type == "shareplex"

    def close(self):
        self.server.close()

    def login(self):
        self.server.connect()

    @staticmethod
    def newInstance(host_name, vol_name, mount_dir):
        if not host_name:
            raise wbxexception("host_name can not be null")
        host_name = host_name.split(".")[0]
        key = "%s_%s_%s" % (host_name, vol_name, mount_dir)
        sp = curcache.get(key)
        host_ip = None
        if sp is None:
            ssh_user = "oracle"
            daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
            depotDaoManager = daoManagerFactory.getDefaultDaoManager()
            depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            try:
                depotDaoManager.startTransaction()
                ssh_pwd = depotdbDao.getOracleUserPwdByHostname(host_name)
                host_ip = depotdbDao.getHostIPByHostname(host_name)
                host_ip = ".".join(host_ip.split(".")[0:-1]) + ".0/24"
                depotDaoManager.commit()
            except DatabaseError as e:
                logger.error("getUserPasswordByHostname getSitecodeByHostname met error %s" % e)
                raise wbxexception(
                    "Error ocurred when get oracle user password and sitecode on the server %s in DepotDB with msg %s" % (
                        host_name, e))

            # servervo = daoManagerFactory.getServer(host_name)
            # if servervo is None:
            #     raise wbxexception("can not get server info with hostname %s" % host_name)
            # ssh_port = servervo.ssh_port
            server = wbxssh(host_name, 22, ssh_user, ssh_pwd)
            try:
                server.connect()
            except Exception as e:
                raise wbxexception("cannot login the server %s with password in depot" % host_name)
            sp = wbxnfsstorageclient(server, vol_name, mount_dir)
            curcache.set(key, sp)
        return sp, host_ip

    def check_mount_info_valid(self):
        if self.server.isDirectory(self.mount_dir) and not self.server.isEmpty(self.mount_dir):
            raise wbxexception("mount_dir %s is not empty or is mounted" % self.mount_dir)
        if not self.server.isFile(self.nfs_config_file):
            raise wbxexception("nfs config file %s not exist" % self.nfs_config_file)
        if not self.mount_ip:
            return True
        cmd = "sudo grep nfs %s" % self.nfs_config_file
        fs_tab_data = self.server.exec_command(cmd)
        fs_tab_list = address_fstab_data(fs_tab_data)
        if "%s:%s" % (self.mount_ip, self.vol_name) in fs_tab_list:
            raise wbxexception(
                "%s:%s exist in %s %s" % (self.mount_ip, self.vol_name, self.host_name, self.nfs_config_file))

    def mount_nfs_volume_to_server(self):
        if not self.server.isDirectory(self.mount_dir):
            cmd = """
            sudo mkdir %s
            sudo chmod -R 777 %s
            """ % (self.mount_dir, self.mount_dir)
            self.server.exec_command(cmd)
        if not self.server.isDirectory(self.mount_dir) or not self.server.isEmpty(self.mount_dir):
            raise wbxexception("mount_dir %s is not valid" % self.mount_dir)
        cmd = """
        sudo sudo su -c "echo '%s:%s %s nfs rw,bg,hard,nointr,rsize=32768,wsize=32768,tcp,vers=3,timeo=600,nolock 0 0' >> %s"
        """ % (self.mount_ip, "/" + self.vol_name, self.mount_dir, self.nfs_config_file)
        self.server.exec_command(cmd)
        cmd = "sudo sudo su -c 'mount %s'" % self.mount_dir
        rst = self.server.exec_command(cmd)
        cmd = "df -h | grep -i '%s'" % self.mount_dir
        rst = self.server.exec_command(cmd)
        if self.mount_dir not in rst:
            raise wbxexception("the shareplex nfs volume is not in df on %s" % self.server.host_name)


class nfsautomation(object):
    def __init__(self, datacenter, vol_name):
        self.vol_name = vol_name
        self.datacenter = datacenter
        self.api_url = "https://csgcc.prv.webex.com/v2/jobs/"
        self.api_usr = "dba.api_j2"
        self.api_pwd = b'REIxQTJBQ0JGRDU1MkY1Mw=='

    def post_data_to_api(self, param):
        r = requests.post(self.api_url,
                          json=param,
                          auth=HTTPBasicAuth(self.api_usr, str(base64.b64decode(self.api_pwd), "utf-8")))
        return r.text, r.status_code

    def add_client(self, mount_ip, client_list, notification_user):
        clients_str_list = []
        for client_item in client_list:
            clients_str_list.append("{ \"name\": \"%s.webex.com\"}" % client_item)
        post_body = {
            "appId": "240",
            "appVersion": "1.0",
            "name": "cdot_AddClient_0%s" % datetime.datetime.now().strftime('%Y%m%d%H%M%S'),
            "environmentId": "67",
            "jobs": [
                {
                    "tierId": "241",
                    "parameters": {
                        "appParams": [
                            {
                                "name": "storage_type",
                                "value": "nfs-io1"
                            },
                            {
                                "name": "datacenter",
                                "value": self.datacenter
                            },
                            {
                                "name": "mount_path",
                                "value": "%s:/%s" % (mount_ip, self.vol_name)
                            },
                            {
                                "name": "clients",
                                "value": "[" + ",".join(clients_str_list) + "]"
                            },
                            {
                                "name": "notifications",
                                "value": "[{ \"type\": \"Email\", \"emailIds\": [ \"%s@cisco.com\"]}]" % notification_user
                            }
                        ]
                    }
                }
            ],
            "responseJobJaxbNotRequired": True
        }
        response_text, response_code = self.post_data_to_api(post_body)
        if response_code == 200:
            return "", True
        else:
            return response_text, False

    def create_volume(self, size_gb, client_list, notification_user):
        client_list = list(set(client_list))
        post_body = {
        "appId":248,
        "appVersion":"1.0",
        "name":"26adb3f9-545c-4162-fd8dgddddddf17ddddd1-d3ddd111d1fddd12dddd2ddfgf3dd3ddsdddf2d2dgsffg74%s" % datetime.datetime.now().strftime('%Y%m%d%H%M%S'),
        "environmentId":"67",
        "jobs":[
           {
              "tierId":"249",
              "parameters":{
                 "appParams":[
                    {
                       "name":"datacenter",
                       "value":self.datacenter.upper()
                    },
                    {
                       "name":"size_gb",
                       "value": size_gb
                    },
                    {
                       "name":"app_name",
                       "value":"general"
                    },
                    {
                       "name":"storage_type",
                       "value":"nfs-io1"
                    },


                    {
                       "name":"vol_name",
                       "value":self.vol_name
                    },
                    {
                       "name":"clients",
                           "value":"[{ \"name\": \"%s\"}] " % client_list[0]
                    },
                    {
                       "name":"notifications",
                       "value":"[{ \"type\": \"Email\", \"emailIds\": [ \"%s@cisco.com\" ]}]" % notification_user
                    }
                 ]
              }
           }
        ],
        "responseJobJaxbNotRequired":True
    }
        print(post_body)
        response_text, response_code = self.post_data_to_api(post_body)
        if response_code == 200:
            return "", True
        else:
            return response_text, False
        # return "", False

    def resize_volume(self, additional_size, mount_ip, notification_user):
        post_body = {
            "appId": 244,
            "appVersion": "1.0",
            "name": "33545569-7779-4055-bdea-52322296334d8%s" % datetime.datetime.now().strftime('%Y%m%d%H%M%S'),
            "environmentId": "67",
            "jobs": [
                {
                    "tierId": "245",
                    "parameters": {
                        "appParams": [
                            {
                                "name": "datacenter",
                                "value": self.datacenter
                            },
                            {
                                "name": "additional_size",
                                "value": additional_size
                            },
                            {
                                "name": "cdot_lif_ip",
                                "value": mount_ip
                            },
                            {
                                "name": "vol_name",
                                "value": self.vol_name
                            },
                            {
                                "name": "notifications",
                                "value": "[{ \"type\": \"Email\", \"emailIds\": [ \"%s@cisco.com\"]}]" % notification_user
                            }
                        ]
                    }
                }
            ],
            "responseJobJaxbNotRequired": True
        }
        response_text, response_code = self.post_data_to_api(post_body)
        if response_code == 200:
            return "", True
        else:
            return response_text, False
