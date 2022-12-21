import logging
import urllib
from os import listdir, system
import codecs

from common.sshConnection import SSHConnection
from common.wbxexception import wbxexception
from common.wbxssh import wbxssh
from dao.wbxdaomanager import wbxdaomanagerfactory

logger = logging.getLogger("DBAMONITOR")

def getAwrreport2():
    logger.info("getAwrreport")
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    # daoManager = daoManagerFactory.getDefaultDaoManager()
    cmd = 'cd /usr/local/nginx/html/awrreport;ls | grep awrreport_'
    server = daoManagerFactory.getServer("sjgrcabt102")
    result = {}
    if server is not None:
        if server.loginuser is None:
            raise wbxexception("Can not get login user info from DepotDB")
        ssh = wbxssh(server.host_name, server.ssh_port, server.loginuser.username, server.loginuser.password)
        ssh.connect()
        res = ssh.exec_command(cmd)
        result['code'] = '0000'
        result['message'] = 'Success'
        result['file'] = str(res)
    return result

def getReport(type,env):
    logger.info("getReport, type={0},env={1}" .format(type,env))
    result = {}
    path = ""
    if "china" == env:
        path = "/usr/share/nginx/html/"
    else:
        path = "/usr/local/nginx/html/"
    if type == "awr":
        path += "awrreport/"
    elif type == "ash":
        path += "ashreport/"
    logger.info("path={0}" .format(path))
    files = listdir(path)
    list = []
    if files:
        for file_name in files:
            if file_name.startswith("awrreport_") or file_name.startswith("ashreport_"):
                list.append(file_name)
    result['code'] = '0000'
    result['message'] = 'Success'
    result['files'] = list
    return result

def getOneReportHtml(filename,type,env):
    logger.info("getOneReportHtml, filename=%s type=%s, env=%s" % (filename, type,env))
    result = {}
    if "china" == env:
        path = "/usr/share/nginx/html/"
    else:
        path = "/usr/local/nginx/html/"
    if type == "awr":
        path += "awrreport/"
    elif type == "ash":
        path += "ashreport/"
    file = path + filename
    logger.info("file=%s" %(file))
    content = open(file, 'r')
    result['code'] = '0000'
    result['message'] = 'Success'
    result['res'] = content.read()
    return result

if __name__ == "__main__":
    result = getOneReportHtml("awrreport_RACBIWEB_2_2020-04-30-04-59-00_2020-04-30-05-29-00_5566_5569.html","awr","china")
    print(result)