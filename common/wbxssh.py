import datetime
import logging
import paramiko
from paramiko import SSHClient
import socket
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
import re
# from gevent.socket import wait_read

import time

LINUX_LINE_SEPARATOR="\n"
LINUX_PATH_SEPARATOR="/"

logger = logging.getLogger("DBAMONITOR")

class wbxssh(object):
    def __init__(self, host_name, ssh_port, login_user, login_pwd):
        self.host_name = host_name
        self.ssh_port = ssh_port
        self.login_user = login_user
        self.login_pwd = login_pwd
        self.client = None
        self.channel = None
        self.isconnected = False

    def getHostName(self):
        return self.host_name

    def getLoginuser(self):
        return self.login_user

    def setLoginpwd(self,login_pwd):
        self.login_pwd = login_pwd

    def getLoginpwd(self):
        return self.login_pwd

    def connect(self):
        self.client = SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            hostip="{0}".format(self.host_name) if re.match(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$", self.host_name) else "{0}.webex.com".format(self.host_name)
            self.client.connect("%s" % hostip, port=self.ssh_port, username=self.login_user, password=self.login_pwd)
            self.isconnected = True
        except (paramiko.BadHostKeyException, paramiko.BadAuthenticationType, paramiko.SSHException, socket.error) as e:
            raise wbxexception(e)

    def verifyConnection(self):
        try:
            self.connect()
            res = self.exec_command("pwd")
            if res == "":
                raise wbxexception("Can not login to server %s" % self.host_name)
            else:
                logger.info("Login to %s succeed" % self.host_name)
        except Exception as e:
            logger.error("Verify connection to %s failed with error: %s" % (self.host_name, e))
            raise e
        finally:
            self.close()

    def send(self, cmd):
        if self.channel is None:
            self.channel = self.client.invoke_shell()
        newline = '\r'
        line_buffer = ''
        self.channel.send(cmd + line_buffer + newline)

    def recv(self):
        res = ""
        if self.channel is not None:
            while True:
                buffer = self.channel.recv(1024).decode('utf-8')
                if len(buffer) == 0:
                    break
                res = "%s%s" % (res,buffer)
        return buffer

    def recvs(self,**kargs):
        buffer=None
        if self.channel is not None:
            buffer = self.channel.recv(1024).decode('utf-8','replace')
            for key in kargs.keys():
                if buffer.upper().find(key.upper())>=0:
                    self.send(kargs[key])
        return buffer

    def invoke_shell(self,cmd):
        logger.info(cmd)
        try:
            if not self.isconnected:
                self.connect()
            channel=self.client.invoke_shell()
            channel.send(cmd + "\n")
        except paramiko.SSHException as e:
            raise wbxexception("Error ocurred when executing command %s with error msg %s" % (cmd, e))

    def exec_command(self, cmd, timeout = 0, async_log = False, *args):
        # logger.info(cmd)
        try:
            if not self.isconnected:
                self.connect()
            stdin, stdout, stderr = self.client.exec_command(cmd)
            if len(args) > 0:
                time.sleep(3)
                for arg in args:
                    stdin.write(arg + "\n")
                    stdin.flush()
            if timeout > 0:
                stdout.channel.settimeout(timeout)
            res=""
            if async_log:
                while True:
                    # wait_read(stdout.channel.fileno())
                    curmsg = stdout.channel.recv(1024)
                    curerr = stderr.channel.recv(1024)
                    if not len(curmsg):
                        break

                    curmsg = curmsg.decode('utf-8')
                    curerr = curerr.decode('utf-8')
                    res="%s%s%s" %(res,curmsg, curerr)
                    logger.info((curmsg + curerr).strip())
                if stdout.channel.recv_exit_status():
                    curerr = stderr.read().decode('utf-8')
                    logger.info(curerr)
                    res = "%s%s" %(res,curerr)
                    # callback(data, *callbackargs)
            else:
                bres, berr = stdout.read(), stderr.read()
                res = bres.decode('utf-8').strip(LINUX_LINE_SEPARATOR)
                err = berr.decode('utf-8').strip(LINUX_LINE_SEPARATOR)
                res_code = stdout.channel.recv_exit_status()  # exiting code for executing
                if res_code != 0:
                    if err != "":
                        res = "%s\n%s" %(res,err)
            return res
        # except paramiko.SSHException as e:
        except Exception as e:
            logger.error("error occurred when executing %s on server %s" % (cmd, self.host_name), exc_info = e)
            raise wbxexception("Error ocurred when executing command %s on server %s with error msg %s" % (cmd, self.host_name, e))

    def execute_command(self, cmd, timeout = 0, async_log = False, *args):
        try:
            if not self.isconnected:
                self.connect()
            stdin, stdout, stderr = self.client.exec_command(cmd)
            if len(args) > 0:
                time.sleep(3)
                for arg in args:
                    stdin.write(arg + "\n")
                    stdin.flush()
            if timeout > 0:
                stdout.channel.settimeout(timeout)
            res=""
            if async_log:
                while True:
                    # wait_read(stdout.channel.fileno())
                    curmsg = stdout.channel.recv(1024)
                    curerr = stderr.channel.recv(1024)
                    if not len(curmsg):
                        break

                    curmsg = curmsg.decode('utf-8')
                    curerr = curerr.decode('utf-8')
                    res="%s%s%s" %(res,curmsg, curerr)
                    logger.info(curmsg + curerr)
                    # callback(data, *callbackargs)
            else:
                bres, berr = stdout.read(), stderr.read()
                res = bres.decode('utf-8').strip(LINUX_LINE_SEPARATOR)
                err = berr.decode('utf-8').strip(LINUX_LINE_SEPARATOR)
                res_code = stdout.channel.recv_exit_status()  # exiting code for executing
                if res_code != 0:
                    raise wbxexception("%s\n%s" %(res,err))
            return res
        # except paramiko.SSHException as e:
        except Exception as e:
            logger.error("error occurred when executing %s on server %s" % (cmd, self.host_name), exc_info = e)
            raise wbxexception("Error ocurred when executing command %s on server %s with error msg %s" % (cmd, self.host_name, e))

    def close(self):
        if self.channel is not None:
            self.channel.close()
        if self.client is not None:
            self.client.close()
        if self.isconnected:
            self.isconnected=False

    def stopService(self, servicename):
        logger.info("stopService servicename=%s on server %s" % (servicename, self.host_name))
        self.exec_command("sudo service %s stop" % servicename, timeout=60)

    def startService(self, servicename):
        logger.info("startService servicename=%s on server %s" % (servicename, self.host_name))
        self.exec_command("sudo service %s start" % servicename, timeout=60)

    def startBlackout(self, hours):
        cmd = "ps aux | grep emagent | grep perl | grep -v grep | awk '{print $11}' | cut -d '/' -f 1,2,3,4,5,6,7,8"
        emctl_base = self.exec_command(cmd)
        prog_emctl = "%s/bin/emctl" % emctl_base
        if self.isFile(prog_emctl):
            cmd = "%s start blackout forAutomationTool -nodeLevel  -d %s:00" % (prog_emctl, hours)
            logger.info("startBlackout cmd=%s on server %s" % (cmd, self.host_name))
            self.exec_command(cmd)
        else:
            raise wbxexception("Does not find emctl command when start blackout: %s" % prog_emctl)

    def stopBlackout(self):
        cmd = "ps aux | grep emagent | grep perl | grep -v grep | awk '{print $11}' | cut -d '/' -f 1,2,3,4,5,6,7,8"
        emctl_base = self.exec_command(cmd)
        prog_emctl = "%s/bin/emctl" % emctl_base

        if self.isFile(prog_emctl):
            cmd = "%s stop blackout forAutomationTool" % (prog_emctl)
            logger.info("stopBlackout on server %s cmd=%s" % (self.host_name, cmd))
            self.exec_command(cmd)
        else:
            raise wbxexception("Does not find emctl command when stop blackout: %s" % prog_emctl)

    def isFile(self, filepath):
        cmd = "if [ -f %s ]; then echo 'Y'; else echo 'N'; fi" % filepath
        res = self.exec_command(cmd)
        return True if res == "Y" else False


    def isEmpty(self, pathname):
        cmd = "if [ -z `ls -A %s` ]; then echo 'Y'; else echo 'N'; fi" % pathname
        res = self.exec_command(cmd)
        if res.split("\n")[0] == "Y":
            return True
        else:
            return False

    def isDirectory(self, pathname):
        cmd = "if [ -d %s ]; then echo 'Y'; else echo 'N'; fi" % pathname
        res = self.exec_command(cmd)
        return True if res == "Y" else False

    def isNullDirectory(self, pathname):
        cmd = '''if [ "`ls -A %s`" == "" ]; then echo "Y"; else echo "N"; fi''' % pathname
        res = self.exec_command(cmd)
        return True if res == "Y" else False

    def hasWritePrivilege(self, pathname):
        cmd = "if [ -w %s ]; then echo 'Y'; else echo 'N'; fi" % pathname
        res = self.exec_command(cmd)
        return True if res == "Y" else False

    def removeFile(self, file_path):
        cmd = "rm -f %s" % file_path
        self.exec_command(cmd)

    def installDBpatch(self, releaseNumber, rpmfilename):
        tgtdir = "/home/oracle"
        rpmfile = "%s/%s" % (tgtdir, rpmfilename)
        if not self.isFile(rpmfile):
            rpmurl = "http://repo.webex.com/prod/DBpatch/noarch/%s" % rpmfilename
            cmd = "sudo wget -P %s %s" % (tgtdir, rpmurl)
            self.exec_command(cmd)
            if not self.isFile(rpmfile):
                raise wbxexception("Can not download rpm file %s" % rpmurl)

        res = self.exec_command("sudo rpm -qa | grep %s | grep -v grep" % releaseNumber)
        if not wbxutil.isNoneString(res):
            self.exec_command("sudo rpm -e %s" % res)
        self.exec_command("sudo rpm -ivh %s" % rpmfile)
        installdir = "/tmp/%s" % releaseNumber
        if not self.isDirectory(installdir):
            raise wbxexception("After installation, can not get the installation directory %s" % installdir)

if __name__ == "__main__":
    host_name="tadbth392"
    login_user="oracle"
    login_pwd="ylh04Kri#JI"
    dblinkName = "TO_BASELINE_TAHOE"
    directory_name = "EXPDP_DIR"
    wbxssh = wbxssh(host_name, 22, login_user, login_pwd)

    a = wbxutil.getcurrenttime()
    b = datetime.datetime.strftime(a, "%Y-%m-%d_%H-%M-%S")

    try:
        wbxssh.connect()
    except Exception as e:
        print(e)
    cmd ="ps -ef | grep smon"
    # cmd = ". /home/oracle/.bash_profile; export ORACLE_SID=ttacomb62; impdp system/sysnotallow network_link=%s  directory=%s  schemas=test CONTENT=METADATA_ONLY EXCLUDE=STATISTICS PARALLEL=8 logfile=impdptest_%s.log" % (
    # dblinkName, directory_name, b)
    cmd =". /home/oracle/.bash_profile; export ORACLE_SID=ttacomb62; impdp system/sysnotallow network_link=TO_CONFIGDBHA_AUTO tables=test.WBXBRIDGECONNECTION,test.WBXCARRIER,test.WBXCARRIEREGRESSPOINT,test.WBXCOUNTRY,test.WBXPROFILEROUTETABLEMAP,test.WBXROUTETABLEITEM,test.WBXSITECONFIG,test.WBXSITEEXTINFO,TEST.WBXTELECALLBACKGROUP,TEST.WBXTELECALLBACKGROUPITEM,TEST.WBXSITEBASE,TEST.WBXSITEBASECONFIG,TEST.WBXSITESPECIFICPARAMS,TEST.WBXSITEBASEMAP,TEST.WBXPCNPASSCODERANGE,test.WBXGLOBALUSEDPASSCODE,test.WBXDOMAINCONFSGTMAPPING,test.WBXCONNECTIONGROUPUCRESET,test.WBXUCREDNSSRV,test.WBXUCREPOOL,test.WBXUCRESET,test.WBXTELCOUNTRYREGION,wbx11.WBXBILLINGGROUP,wbx11.WBXBILLINGGROUPCONFIG,wbx11.WBXBILLINGGROUPUSER,test.WBXEGRESSCALLROUTINGPROFILE ,test.WBXROUTINGPROFILETYPE ,test.WBXSITEROUTINGPROFILE ,test.WBXDESELECTEDCALLBACKCOUNTRY content=DATA_ONLY cluster=N parallel=8 DIRECTORY=%s REMAP_SCHEMA=wbx11:test&" %(directory_name)
    # cmd = ". /home/oracle/.bash_profile;export ORACLE_SID=ttacomb62; "
    print(cmd)
    # res = wbxssh.exec_command(cmd)
    # print(res)
