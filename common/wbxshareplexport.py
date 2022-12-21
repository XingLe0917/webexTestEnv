import os
from common.wbxssh import wbxssh
import time, datetime
import logging
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from common.wbxcache import curcache
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from sqlalchemy import Table, Column, MetaData, String, DateTime, Integer, and_, create_engine
from sqlalchemy.exc import  DBAPIError, DatabaseError
from sqlalchemy.pool import NullPool

logger = logging.getLogger("DBAMONITOR")


class wbxshareplexport:

    def __init__(self, server, splex_port):
        self.host_name = server.host_name
        self.splex_port = splex_port
        self.server = server
        self.splexuser = "splex%s" % splex_port
        self.service_name="shareplex%s" % self.splex_port
        self.profile_name = ".profile_%s" % self.splex_port
        self.profile_link_name=".profile_u%s" % self.splex_port
        self.upgradeStatus = False
        self.install_files={"9.2.1":"SharePlex-9.2.1-b39-ONEOFF-SPO3828-SPO17377-rhel-amd64-m64.tpm"}
        self.reload = False

        self.installStatus = False
        self.script_dir = "/staging/Scripts/oracle/port_setup"
        self.host_vip = "%s-vip" % server.host_name

    @staticmethod
    def newInstance(host_name, splex_port):

        if wbxutil.isNoneString(host_name):
            raise wbxexception("host_name can not be null")
        if isinstance(splex_port, String) and wbxutil.isNoneString(splex_port):
            raise wbxexception("splex_port can not be null")

        host_name = host_name.split(".")[0]
        key = "%s_%s" % (host_name, splex_port)
        sp = curcache.get(key)
        if sp is None or sp.reload:
            ssh_user = "oracle"
            daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
            depotDaoManager = daoManagerFactory.getDefaultDaoManager()
            depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            try:
                depotDaoManager.startTransaction()
                ssh_pwd = depotdbDao.getOracleUserPwdByHostname(host_name)
                depotDaoManager.commit()
            except DatabaseError as e:
                logger.error("getUserPasswordByHostname met error %s" % e)
                raise wbxexception(
                    "Error ocurred when get oracle user password on the server %s in DepotDB with msg %s" % (
                    host_name, e))
            if wbxutil.isNoneString(ssh_pwd):
                raise wbxexception("Can not get oracle user password on the server %s in DepotDB" % host_name)

            servervo = daoManagerFactory.getServer(host_name)
            if servervo is None:
                raise wbxexception("can not get server info with hostname %s" % host_name)
            ssh_port = servervo.ssh_port
            server = wbxssh(host_name, ssh_port, ssh_user, ssh_pwd)
            sp = wbxshareplexport(server, splex_port)
            curcache.set(key, sp)
        return sp

    def login(self):
        self.server.connect()

    def close(self):
        self.server.close()

    def setReload(self, reload):
        self.reload = reload

    def setstatus(self, isrunning):
        self.upgradeStatus = isrunning

    def isUpgrading(self):
        return self.upgradeStatus

    def startBlackout(self, hour):
        self.server.startBlackout(self.serverlist, hour)

    def stopBlackout(self):
        self.server.stopBlackout(self.serverlist)

    def isPortExist(self):
        cmd = "ps aux | grep sp_cop | grep %s | grep -v grep | wc -l"% self.splex_port
        res = self.server.exec_command(cmd)
        if int(res) != 1:
            raise wbxexception("The shareplex port %s does not exist or is not running on server %s" % (self.splex_port, self.host_name))

        cmd = "cat /etc/oraport | grep -v ^# | grep ^%s | awk -F: '{print $2}'" % self.splex_port
        self.SPLEX_BIN_DIR  = self.server.exec_command(cmd)
        if self.SPLEX_BIN_DIR  == "":
            raise wbxexception("Do not get the shareplex port from /etc/oraport file")
        if not self.server.isDirectory(self.SPLEX_BIN_DIR):
            raise wbxexception("%s is not a directory" % self.SPLEX_BIN_DIR)
        # if self.SPLEX_BIN_DIR.find(PRODDIR_NAME) < 0:
        #     raise wbxexception("The splex bin dir %s does not include standard name format %s" % (self.SPLEX_BIN_DIR, PRODDIR_NAME))

        self.SPLEX_PROFILE = "%s/%s" % (self.SPLEX_BIN_DIR, self.profile_name)
        if not self.server.isFile(self.SPLEX_PROFILE):
            raise wbxexception('The profile %s does not exist' % self.SPLEX_PROFILE)

        cmd = ". %s; echo ORACLE_SID:$ORACLE_SID\;SP_SYS_VARDIR:$SP_SYS_VARDIR\;SP_SYS_PRODDIR:$SP_SYS_PRODDIR" % (self.SPLEX_PROFILE)
        res = self.server.exec_command(cmd)
        itemdict = {proitem.split(":")[0]: proitem.split(":")[1] for proitem in res.split(";")}
        self.ORACLE_SID = itemdict["ORACLE_SID"]
        self.SP_SYS_VARDIR = itemdict["SP_SYS_VARDIR"]
        self.SP_SYS_PRODDIR = itemdict["SP_SYS_PRODDIR"]
        self.PRODDIR_NAME = os.path.basename(self.SP_SYS_PRODDIR)
        self.paramdb = "%s/data/paramdb" % self.SP_SYS_VARDIR
        connectionfile = "%s/data/connections.yaml" % self.SP_SYS_VARDIR
        res = self.server.exec_command(r"""cat %s | tr '\n' ' ' | sed "s/o\./\n/g" | grep -i splex%s | awk '{print $1}' | awk -F: '{print $1}' """ % (connectionfile, self.splex_port))
        self.dbsiddict = {}
        if not wbxutil.isNoneString(res):
            splex_sids = res.splitlines()
            for splex_sid in splex_sids:
                self.dbsiddict[splex_sid] = ""

        logger.info("SP_SYS_PRODDIR=%s, SP_SYS_VARDIR=%s, dbsids=%s on host %s splex_port %s" %(self.SP_SYS_PRODDIR, self.SP_SYS_VARDIR, res, self.host_name, self.splex_port))

    def getShareplexInstalledCount(self):
        installfile = "/home/oracle/.shareplex/install.conf"
        if self.server.isFile(installfile):
            res = self.server.exec_command("""cat %s | grep "\/" | awk -F: '{print $1}' | wc -l""" % installfile)
            self.version_count = int(res)
        else:
            self.version_count = 0
        logger.info("version_count=%s on host %s splex_port %s" % (self.version_count, self.host_name, self.splex_port))

    def checkEnviromentConfig(self):
        logger.info("checkEnviromentConfig start host_name=%s, splex_port=%s" %(self.host_name, self.splex_port))
        db_file = "/home/oracle/.11g_db"
        grid_file = "/home/oracle/.11g_grid"
        if not self.server.isFile(db_file):
            raise wbxexception("%s does not exist" % db_file)
        if not self.server.isFile(grid_file):
            raise wbxexception("%s does not exist" % grid_file)
        cmd = ". /home/oracle/.11g_grid; echo $ORACLE_HOME"
        self.GRID_HOME = self.server.exec_command(cmd)
        cmd = ". /home/oracle/.11g_db; echo $ORACLE_HOME"
        self.DB_HOME = self.server.exec_command(cmd)
        cmd = ". /home/oracle/.11g_grid; olsnodes"
        res = self.server.exec_command(cmd)
        self.serverlist = res.splitlines()
        for host_name in self.serverlist:
            cmd = """ ssh %s echo "TRUE" """ % host_name
            res = self.server.exec_command(cmd)
            if res.find("TRUE") == -1:
                raise wbxexception("Can not login to %s from current server %s with non-password login" % (host_name, self.host_name))
        logger.info("serverlist=%s" % self.serverlist)

        self.GRID_PROFILE_FILE = "%s/crs/profile/shareplex%s.cap" % (self.GRID_HOME, self.splex_port)
        self.GRID_SCRIPT_FILE = "%s/crs/script/splex_action_%s.sh" % (self.GRID_HOME, self.splex_port)
        if not self.server.isFile(self.GRID_PROFILE_FILE):
            raise wbxexception("%s does not exist" % self.GRID_PROFILE_FILE)
        if not self.server.isFile(self.GRID_SCRIPT_FILE):
            raise wbxexception("%s does not exist" % self.GRID_SCRIPT_FILE)

        cmd = ". /home/oracle/.11g_grid; crsstat | grep %s" % self.service_name
        res = self.server.exec_command(cmd)
        if wbxutil.isNoneString(res):
            raise wbxexception("The service %s does not exist in CRS" % self.service_name)
        else:
            svcstatus = res.split()
            if svcstatus[2] != "ONLINE" and svcstatus[3] != "ONLINE":
                raise wbxexception(
                    "The service %s in CRS is not ONLINE status, please double check" % (self.service_name))

    def getSplexuserPassword(self):
        logger.info("getSplexuserPassword start host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))
        daoManagerFactory  = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        userlist = []
        try:
            depotDaoManager.startTransaction()
            userlist = depotdbDao.getSplexuserPasswordByPort(self.host_name, self.splex_port)
            depotDaoManager.commit()
        except DatabaseError as e:
            logger.error("getUserPasswordByHostname met error %s" % e)

        for userinfo in userlist:
            trim_host = userinfo[0]
            db_name = userinfo[1]
            pwd = userinfo[3]
            splex_sid = userinfo[4]
            logger.info("verify user %s password in db %s" % (self.splexuser, splex_sid))
            if splex_sid in self.dbsiddict:
                self.dbsiddict[splex_sid] = pwd
            else:
                raise wbxexception("The dbsid %s does not in connections.yaml file" % splex_sid)

            db = daoManagerFactory.getDatabaseByDBID(db_name)
            try:
                connectionurl = db.getConnectionURL()
                engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (self.splexuser, pwd, connectionurl),
                                       poolclass=NullPool, echo=False)
                connect = engine.connect()
                res = connect.execute("select sysdate as curtime from dual")
                res.close()
                connect.connection.commit()
            except DatabaseError as e:
                raise wbxexception("Can not login to db %s with splex user %s password in DepotDB" % (db_name, self.splexuser))

        # for splex_sid, pwd in self.dbsiddict.items():
        #     if wbxutil.isNoneString(pwd):
        #         raise wbxexception("Can not get user %s password in %s from DepotDB" % (self.splexuser, splex_sid))
        logger.info("getSplexuserPassword end host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))

    def getShareplexVersion(self):
        logger.info("getShareplexVersion start host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))
        cmd = ". %s; cd %s; echo version | ./sp_ctrl" % (self.SPLEX_PROFILE, self.SPLEX_BIN_DIR)
        res = self.server.exec_command(cmd)
        lines = res.splitlines()
        for line in lines:
            if line.find("SharePlex Version") >= 0:
                releaseversion = line.split('=')[1].strip().replace("\.","")
                return releaseversion
        return None

    def getVardirSize(self):
        self.server.exec_command("rm -rf %s/dump/*" % self.SP_SYS_VARDIR)
        self.server.exec_command("rm -rf %s/temp/*" % self.SP_SYS_VARDIR)
        self.server.exec_command("find %s/log -size +10M -exec cp /dev/null {} \;" % self.SP_SYS_VARDIR)

        cmd = "du -s %s | awk '{print $1}'" % (self.SP_SYS_VARDIR)
        res = self.server.exec_command(cmd)
        logger.info("getVardirSize with size=%s host_name=%s, splex_port=%s" % (res, self.host_name, self.splex_port))
        if not wbxutil.isNoneString(res):
            vardirsize = int(res)
            return vardirsize
        return -1

    def isNewBinaryInstalled(self, NEW_PRODDIR_NAME):
        self.NEW_SP_SYS_PRODDIR = self.SP_SYS_PRODDIR.replace(self.PRODDIR_NAME, NEW_PRODDIR_NAME)
        self.NEW_SPLEX_BIN_DIR=self.SPLEX_BIN_DIR.replace(self.PRODDIR_NAME, NEW_PRODDIR_NAME)
        cmd = "ls -al %s/data/param-defaults 2>/dev/null | awk '{print $9}'" % self.NEW_SP_SYS_PRODDIR
        res = self.server.exec_command(cmd)
        logger.info("isNewBinaryInstalled with fileexist=%s, NEW_SP_SYS_PRODDIR=%s, NEW_SPLEX_BIN_DIR=%s, host_name=%s, splex_port=%s" % (res, self.NEW_SP_SYS_PRODDIR, self.NEW_SPLEX_BIN_DIR, self.host_name, self.splex_port))
        return True if not wbxutil.isNoneString(res) else False

    def iscronjobexist(self):
        cmd = " crontab -l | grep %s | grep -v grep | grep restart_proc | wc -l" % self.splex_port
        res = self.server.exec_command(cmd)
        if res != "":
            jobcnt = int(res)
            if jobcnt > 0:
                return (True, 'Exist')
        return (False, 'Not Exist')

    def qstatus(self):
        cmd = ". %s; cd %s; echo qstatus | ./sp_ctrl" % (self.SPLEX_PROFILE, self.SPLEX_BIN_DIR)
        res = self.server.exec_command(cmd)
        logger.info("qstatus with res=%s host_name=%s, splex_port=%s" % (res, self.host_name, self.splex_port))
        return res

    def backupVardir(self):
        backupfile = "/tmp/vardir_%s.tar.gz" % self.splex_port
        if self.server.isFile(backupfile):
            cmd = "rm -rf %s" % backupfile
            self.server.exec_command(cmd)

        cmd = "tar -zcvf /tmp/vardir_%s.tar.gz --absolute-names %s" % (self.splex_port, self.SP_SYS_VARDIR)
        logger.info("backupVardir start cmd=%s host_name=%s, splex_port=%s" % (cmd, self.host_name, self.splex_port))
        self.server.exec_command(cmd)
        logger.info("backupVardir end host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))

    def stopShareplexService(self):
        logger.info("stopShareplexService servicename=%s host_name=%s, splex_port=%s" % (self.service_name, self.host_name, self.splex_port))
        cmd = ". /home/oracle/.11g_grid; crs_stop -f %s" % self.service_name
        res = self.server.exec_command(cmd)
        loops = 0
        while True:
            loops = loops + 1
            time.sleep(10)
            cmd = "ps aux | grep -v grep | grep sp_cop | grep %s | wc -l" % self.splex_port
            res = self.server.exec_command(cmd)
            if int(res) > 0 and loops > 12:
                raise wbxexception("stop shareplex port failed")
            elif int(res) == 0:
                break
        logger.info("stopShareplexService end host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))

    def startShareplexService(self):
        logger.info("startShareplexService service_name=%s host_name=%s, splex_port=%s" % (self.service_name, self.host_name, self.splex_port))
        cmd = ". /home/oracle/.11g_grid; crs_start %s" % self.service_name
        self.server.exec_command(cmd)
        loops = 0
        while True:
            hasIdle = False
            loops = loops + 1
            time.sleep(10)
            issucceed = True
            cmd = ". %s; cd %s; echo show | ./sp_ctrl" % (self.SPLEX_PROFILE, self.SPLEX_BIN_DIR)
            res = self.server.exec_command(cmd)
            for line in res.splitlines():
                if line.find("Capture") == 0 or line.find("Read") == 0:
                    state = line.split()[2]
                elif line.find("Post") == 0:
                    state = line.split()[3]
                else:
                    state = "Running"

                if state != "Running":
                    issucceed = False
                    if loops > 12:
                        raise wbxexception("This process %s state is not right" % line)
            if issucceed:
                break
        # if hasIdle:
        #     logger.warning("Warning: There are idle process. Please double on the server")
        logger.info("startShareplexService end host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))

    def preparecronjob(self):
        logger.info("preparecronjob start host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))
        self.backup_crontab="/tmp/crontab_%s.config" % self.splex_port
        if self.server.isFile(self.backup_crontab):
            self.server.removeFile(self.backup_crontab)

        cmd = """crontab -l > %s""" % self.backup_crontab
        self.server.exec_command(cmd)
        # Generate new crontab file
        cmd = """cat %s | grep splex8[63]*_restart_proc.*%s | wc -l""" % (self.backup_crontab, self.splex_port)
        self.crontab_exist = self.server.exec_command(cmd)
        if int(self.crontab_exist) > 0:
            cmd = """cat %s | grep splex8[63]*_restart_proc.*%s  | tail -n 1 | awk '{print $6}'""" % (self.backup_crontab, self.splex_port)
        else:
            cmd = """cat %s | grep splex8[63]_restart_proc | grep -v ^# | tail -n 1 | awk '{print $6}'""" % self.backup_crontab
        self.script_file = self.server.exec_command(cmd)
        if self.script_file == "":
            self.script_file="/u00/app/admin/dbarea/bin/splex921_restart_proc.sh"
        else:
            self.script_file = self.script_file.replace("splex8_restart_proc","splex921_restart_proc").replace("splex863_restart_proc","splex921_restart_proc")
        if not self.server.isFile(self.script_file):
            cmd = """cp /staging/Scripts/oracle/11g/crontab_monitor/dbarea/bin/splex921_restart_proc.sh %s""" % self.script_file
            self.server.exec_command(cmd)

        logger.info("preparecronjob end host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))

    def uncommentcronjob(self):
        logger.info("uncommentcronjob host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))
        if int(self.crontab_exist) > 0:
            cmd = "crontab -l | sed /splex8[63]*_restart_proc.*%s/s/splex8[63]*_restart_proc/splex921_restart_proc/ > %s; crontab %s" % (self.splex_port, self.backup_crontab, self.backup_crontab)
        else:
            cmd = """crontab -l > %s; echo "0,15,30,45 * * * * %s %s" >> %s; crontab %s """ % (self.backup_crontab, self.script_file, self.splex_port, self.backup_crontab, self.backup_crontab)
        self.server.exec_command(cmd)

    def changeparameter(self, paramdict):
        logger.info("changeparameter %s host_name=%s, splex_port=%s" % (paramdict, self.host_name, self.splex_port))
        for paramname, paramvalue in paramdict.items():
            if paramname != "SP_OCT_OLOG_USE_OCI":
                cmd = """ cd %s; . %s; sed -i "/%s/d" %s """ % (self.SPLEX_BIN_DIR, self.SPLEX_PROFILE, paramname, self.paramdb)
                self.server.exec_command(cmd)
                if not wbxutil.isNoneString(paramvalue):
                    cmd = """ cd %s; . %s; echo "%s  \"%s\"" >> %s """ % (self.SPLEX_BIN_DIR, self.SPLEX_PROFILE, paramname, paramvalue, self.paramdb)
                    self.server.exec_command(cmd)
            else:
                cmd = """ cd %s; . %s; sed -i "s/SP_OCT_ASM_USE_OCI/SP_OCT_OLOG_USE_OCI/g" %s """ % (self.SPLEX_BIN_DIR, self.SPLEX_PROFILE, self.paramdb)
                self.server.exec_command(cmd)
        #
        # for paramname, paramvalue in paramdict.items():
        #     if wbxutil.isNoneString(paramvalue):
        #         cmd = "reset param %s" % paramname
        #         cmd = """ cd %s; . %s; echo "reset param %s" | ./sp_ctrl """ % (self.SPLEX_BIN_DIR, self.SPLEX_PROFILE, paramname)
        #     else:
        #         cmd = """ cd %s; . %s; echo "set param %s %s" | ./sp_ctrl """  % (self.SPLEX_BIN_DIR, self.SPLEX_PROFILE, paramname, paramvalue)
        #     self.server.exec_command(cmd)

    def orasetup(self, splex_sid, splex_pwd):
        logger.info("orasetup start host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))
        cmd="cd %s; . %s; export ORACLE_SID=%s; ./ora_setup" % (self.NEW_SPLEX_BIN_DIR, self.profile_name, splex_sid)
        args=['n','n',self.DB_HOME,splex_sid, "system", "sysnotallow","n", self.splexuser, splex_pwd,"n","","","", "y", "y",""]
        res = self.server.exec_command(cmd, 300, False, *args)
        issucceed = False
        for line in res.splitlines():
            if line.find("completed successfully") > 0:
                issucceed = True
        logger.info(res)
        logger.info("orasetup %s host_name=%s, splex_port=%s" % (issucceed, self.host_name, self.splex_port))

    def changeProfile(self, NEW_PRODDIR_NAME):
        logger.info("changeProfile start PRODDIR_NAME=%s, NEW_PRODDIR_NAME=%s, host_name=%s, splex_port=%s" % (self.PRODDIR_NAME, NEW_PRODDIR_NAME, self.host_name, self.splex_port))
        cmd = """ cp %s %s""" % (self.SPLEX_PROFILE, self.NEW_SPLEX_BIN_DIR)
        self.server.exec_command(cmd)
        self.NEW_SPLEX_PROFILE="%s/%s" % (self.NEW_SPLEX_BIN_DIR, self.profile_name)
        # Because all below steps should based on original variable
        profile_link_name= ".profile_u%s" % self.splex_port
        cmd = "cd %s; ln -s %s %s " % (self.NEW_SPLEX_BIN_DIR, self.NEW_SPLEX_PROFILE, profile_link_name)
        logger.info(cmd)
        self.server.exec_command(cmd)
        cmd = """sed -i "s/%s/%s/g" %s """ % (self.PRODDIR_NAME, NEW_PRODDIR_NAME, self.NEW_SPLEX_PROFILE)
        self.server.exec_command(cmd)
        logger.info("changeProfile end NEW_SPLEX_PROFILE=%s, host_name=%s, splex_port=%s" % (self.NEW_SPLEX_PROFILE, self.host_name, self.splex_port))

    def upgradeServiceConfig(self, NEW_PRODDIR_NAME):
        logger.info("upgradeServiceConfig start PRODDIR_NAME=%s, NEW_PRODDIR_NAME=%s, host_name=%s, splex_port=%s" % (self.PRODDIR_NAME, NEW_PRODDIR_NAME, self.host_name, self.splex_port))
        try:
            self.SPLEX_BIN_DIR = self.NEW_SPLEX_BIN_DIR
            self.SP_SYS_PRODDIR = self.NEW_SP_SYS_PRODDIR
            self.SPLEX_PROFILE = self.NEW_SPLEX_PROFILE
            cmd = """ sudo sed -i "s/%s/%s/g" %s """ % (self.PRODDIR_NAME, NEW_PRODDIR_NAME, self.GRID_SCRIPT_FILE)
            self.server.exec_command(cmd)
            for servername in self.serverlist:
                cmd = "scp %s %s:%s/crs/script/" % (self.GRID_SCRIPT_FILE, servername, self.GRID_HOME)
                self.server.exec_command(cmd)
            self.server.exec_command(""" sudo sed -i "/^%s/d" /etc/oraport """ % (self.splex_port))
            self.server.exec_command(""" sudo echo "%s:%s" >> /etc/oraport """ % (self.splex_port, self.NEW_SPLEX_BIN_DIR))
            autostartfile = "%s/WbxSplexAutoStartStoppedProcess.config" % self.NEW_SPLEX_BIN_DIR
            if not self.server.isFile(autostartfile):
                self.server.exec_command("""echo "%s:Y" > %s """ % (self.splex_port, autostartfile))
            else:
                res = self.server.exec_command("""cat %s | grep ^%s: | wc -l""" % (autostartfile, self.splex_port))
                if int(res) == 0:
                    self.server.exec_command("""echo "%s:Y " >> %s """ % (self.splex_port, autostartfile))
        except Exception as e:
            logger.error(e)

        logger.info("upgradeServiceConfig end host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))

    def installShareplexBinary(self, new_splexversion):
        logger.info("upgradeServiceConfig start new_splexversion=%s,  host_name=%s, splex_port=%s" % (new_splexversion, self.host_name, self.splex_port))
        res = self.server.exec_command("sudo mkdir %s" % self.NEW_SP_SYS_PRODDIR)
        res = self.server.exec_command("sudo chown -R oracle:oinstall %s" % self.NEW_SP_SYS_PRODDIR)
        if new_splexversion in self.install_files:
            tpmfilename = self.install_files[new_splexversion]
        else:
            raise wbxexception("Can not find tpm file for shareplex versioin %s" % new_splexversion)

        tpmfile="/tmp/%s" % tpmfilename
        if not self.server.isFile(tpmfile):
            cmd="cp -f /staging/Software/Oracle/Software/Shareplex/%s /tmp/" % tpmfilename
            res = self.server.exec_command(cmd)
            cmd = "chmod 775 %s" % tpmfile
            res = self.server.exec_command(cmd)
            if not self.server.isFile(tpmfile):
                raise wbxexception("The file does not exist %s" % tpmfile)

            cmd="cksum %s | awk '{print $1}'" % tpmfile
            res = self.server.exec_command(cmd)
            if int(res) != 3241965581:
                raise wbxexception("The cksum value of %s is not 4138614057" % tpmfile)

        self.DEFAULT_VARDIR="/tmp/shareplex_vardir"
        if self.server.isDirectory(self.DEFAULT_VARDIR):
            # remove installation temp dir
            cmd = "rm -rf %s" % self.DEFAULT_VARDIR
            res = self.server.exec_command(cmd)

        cmd = "cd /tmp; ./%s" % tpmfilename
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
        res = self.server.exec_command(cmd, 300, *args)

        logger.info("upgradeServiceConfig end host_name=%s, splex_port=%s" % ( self.host_name, self.splex_port))

    def listParameters(self):
        self.paramdict = {}
        logger.info("getShareplexVersion start host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))
        cmd = r""". %s; cd %s; echo "list param all" | ./sp_ctrl | grep -E '(^SP|Default   Value:)' | sed 'N;s/\n/:/' | sed 's/:SP/\nSP/' """ % (self.SPLEX_PROFILE, self.SPLEX_BIN_DIR)
        res = self.server.exec_command(cmd)
        for line in res.splitlines():
            if line.find("SP_") == 0:
                name = line[0:32].strip()
                val = line[32:70].strip()
                units = line[70:79].strip()
                seta = line[79:94].strip()
                default_value = None
                if len(line) > 96:
                    default_value = line[119:].strip()

                param = ShareplexParam(name, val, units, seta, default_value)
                self.paramdict[name] = param

    def getParameterValue(self, paramName):
        if paramName in self.paramdict:
            return self.paramdict[paramName]
        else:
            return None

    def setParameterValue(self, param, newValue):
        cmd = """. %s; cd %s; echo "set param %s %s" | ./sp_ctrl """ % (self.SPLEX_PROFILE, self.SPLEX_BIN_DIR, param.name, newValue)
        res = self.server.exec_command(cmd)
        param.value = newValue

    def hasCRFile(self):
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        curserver = daomanagerfactory.getServer(self.host_name)
        spchannellist = curserver.getTgtChannelList()
        dbsidlist = set(spchannel.tgt_splex_sid for spchannel in spchannellist if spchannel.port == int(self.splex_port))
        # print(dbsidlist)
        for dbsid in dbsidlist:
            crfile = "%s/data/conflict_resolution.%s" % (self.SP_SYS_VARDIR, dbsid)
            if not self.server.isFile(crfile):
                self.server.exec_command(""" echo "!DEFAULT    IUD      %s.PKGWBXCR.PROCWBXCR" > %s """ % (self.splexuser, crfile))
            else:
                res = self.server.exec_command(""" grep -i "\!DEFAULT    IUD      %s.PKGWBXCR.PROCWBXCR" %s | wc -l """ % (self.splexuser, crfile))
                if wbxutil.isNoneString(res) or int(res) != 1:
                    self.server.exec_command(""" echo "!DEFAULT    IUD      %s.PKGWBXCR.PROCWBXCR" > %s """ % (self.splexuser, crfile))

    def checkCRDBpatch(self, dbid, db_name, splex_port, splex_sid):
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daomanagerfactory.getDaoManager(dbid)
        auditDao = daomanager.getDao(DaoKeys.DAO_DBAUDITDAO)
        issuccess = False
        try:
            daomanager.startTransaction()
            if not auditDao.isDBPatchInstalled(self.splexuser, "15845"):
                cmd = "/bin/bash /staging/gates/addcr_for_shareplex.sh %s %s EXECUTE %s" % (db_name, splex_port, splex_sid)
                res = self.server.exec_command(cmd)
                if not wbxutil.isNoneString(res):
                    for line in res.splitlines():
                        if line.find("RELEASE_NUMBER=15845") > 0:
                            issuccess = True
            else:
                logger.info("Rel#15845 is already deployed on db %s splex port %s" % (db_name, splex_port))
            daomanager.commit()
        except Exception as e:
            daomanager.rollback()
            logger.error("Error ocurred when execute checkCRDBpatch: %s" % e)
        return issuccess

    # shareplex port installation
    def setinstallstatus(self, isrunning):
        self.installStatus = isrunning

    def isInstalling(self):
        return self.installStatus

    def isPortNotExist(self):
        cmd = "ps aux | grep sp_cop | grep %s | grep -v grep | wc -l" % self.splex_port
        res = self.server.exec_command(cmd)
        if int(res) == 1:
            raise wbxexception("The shareplex port %s is running on server %s" % (self.splex_port, self.host_name))
        self.serverDict = {}
        for server in self.serverlist:
            ssh_user = "oracle"
            daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
            depotDaoManager = daoManagerFactory.getDefaultDaoManager()
            depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            try:
                depotDaoManager.startTransaction()
                ssh_pwd = depotdbDao.getOracleUserPwdByHostname(server)
                depotDaoManager.commit()
            except DatabaseError as e:
                logger.error("getUserPasswordByHostname met error %s" % e)
                raise wbxexception(
                    "Error ocurred when get oracle user password on the server %s in DepotDB with msg %s" % (
                        server, e))
            if wbxutil.isNoneString(ssh_pwd):
                raise wbxexception("Can not get oracle user password on the server %s in DepotDB" % server)

            self.serverDict[server] = ssh_pwd
            server_obj = wbxssh(server, 22, ssh_user, ssh_pwd)
            cmd = "ps -ef | grep sp_ | grep -c %s" % self.splex_port
            rst = server_obj.exec_command(cmd)
            if int(rst) > 1:
                raise wbxexception("the port %s is running on host %s" % (self.splex_port, server))

            cmd = "ps -ef | grep sp_ | egrep -vc 'grep|sp_ctrl|splex_action'"
            rst = server_obj.exec_command(cmd)
            if int(rst) > 1:
                raise wbxexception("sp_ process is running on host %s" % server)

        if self.server.isFile("/etc/oraport"):
            cmd = "cat /etc/oraport | grep -v ^# | grep ^%s | awk -F: '{print $2}'" % self.splex_port
            res = self.server.exec_command(cmd)
            if res:
                raise wbxexception("shareplex port %s exists in %s /etc/oraport file" % (self.splex_port, self.host_name))

        self.SP_SYS_VARDIR = "%s/vardir_%s" % (self.SP_SYS_PRODDIR, self.splex_port)
        if self.server.isDirectory(self.SP_SYS_VARDIR):
            raise wbxexception("The SP_SYS_VARDIR: %s has existed." % self.SP_SYS_VARDIR)

        self.SPLEX_PROFILE = "%s/%s" % (self.SPLEX_BIN_DIR, self.profile_name)
        if self.server.isFile(self.SPLEX_PROFILE):
            raise wbxexception('The profile %s has existed' % self.SPLEX_PROFILE)
        if self.server.isFile(".profile_u%s" % self.splex_port):
            raise wbxexception('The link_profile %s has existed' % ".profile_u%s" % self.splex_port)

        if self.server.isFile(self.SPLEX_PROFILE):
            raise wbxexception("The shareplex profile for the %s port is already configured, so cannot continue with the port setup." % self.splex_port)

        logger.info("SP_SYS_VARDIR=%s, SPLEX_PROFILE= %s" % (self.SP_SYS_VARDIR, self.SPLEX_PROFILE))


    @staticmethod
    def newInstallInstance(host_name, splex_port):

        if wbxutil.isNoneString(host_name):
            raise wbxexception("host_name can not be null")
        if isinstance(splex_port, String) and wbxutil.isNoneString(splex_port):
            raise wbxexception("splex_port can not be null")

        host_name = host_name.split(".")[0]
        key = "%s_%s" % (host_name, splex_port)
        sp = curcache.get(key)
        if sp is None or sp.reload:
            ssh_user = "oracle"
            daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
            depotDaoManager = daoManagerFactory.getDefaultDaoManager()
            depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            try:
                depotDaoManager.startTransaction()
                ssh_pwd = depotdbDao.getOracleUserPwdByHostname(host_name)
                depotDaoManager.commit()
            except DatabaseError as e:
                logger.error("getUserPasswordByHostname met error %s" % e)
                raise wbxexception(
                    "Error ocurred when get oracle user password on the server %s in DepotDB with msg %s" % (
                        host_name, e))
            if wbxutil.isNoneString(ssh_pwd):
                raise wbxexception("Can not get oracle user password on the server %s in DepotDB" % host_name)

            server = wbxssh(host_name, 22, ssh_user, ssh_pwd)
            sp = wbxshareplexport(server, splex_port)
            curcache.set(key, sp)
        return sp

    def checkUserGroup(self):
        # cmd = """
        # sudo /usr/bin/id | awk '{split($0, val, " "); split(val[3], gr, "="); print gr[2];}
        # """
        # root_cnt = self.server.exec_command(cmd)
        # if "oinstall" not in root_cnt or "spadmin" not in root_cnt:
        #     raise wbxexception("User root is not part of oinstall OR spadmin group. Verify if root are part of the spadmin in /etc/group")
        cmd = """
        /usr/bin/id | awk '{split($0, val, " "); split(val[3], gr, "="); print gr[2];}
        """
        oracle_cnt = self.server.exec_command(cmd)
        if "oinstall" not in oracle_cnt or "spadmin" not in oracle_cnt:
            raise wbxexception("User oracle is not part of oinstall OR spadmin group. Verify if oracle are part of the oinstall in /etc/group")

    def checkInstallEnviromentConfig(self, splex_prod_dir, splex_sid, shareplex_version, db_name):
        self.SP_SYS_PRODDIR = splex_prod_dir # the dir not exists if binary not installed
        self.SPLEX_BIN_DIR = "%s/bin" % self.SP_SYS_PRODDIR # the dir not exists if binary not installed

        if self.server.isDirectory(self.script_dir):
            raise Exception("script_dir: %s is not a directory" % self.script_dir)

        if "_SPLEX" not in splex_sid:
            raise wbxexception("ORACLE_SID: %s is invalid" % splex_sid)
        self.ORACLE_SID = splex_sid

        if shareplex_version not in self.install_files:
            raise wbxexception("the install file of shareplex version: %s not exists" % shareplex_version)

        db_file = "/home/oracle/.11g_db"
        grid_file = "/home/oracle/.11g_grid"
        if not self.server.isFile(db_file):
            raise wbxexception("%s does not exist" % db_file)
        if not self.server.isFile(grid_file):
            raise wbxexception("%s does not exist" % grid_file)

        cmd = ". /home/oracle/.11g_grid; echo $ORACLE_HOME"
        self.GRID_HOME = self.server.exec_command(cmd)
        if not self.server.isDirectory(self.GRID_HOME):
            raise wbxexception("GRID_HOME: %s is not a directory" % self.GRID_HOME)

        cmd = ". /home/oracle/.11g_db; echo $ORACLE_HOME"
        self.DB_HOME = self.server.exec_command(cmd)
        if not self.server.isDirectory(self.DB_HOME):
            raise wbxexception("DB_HOME: %s is not a directory" % self.DB_HOME)

        cmd = ". /home/oracle/.11g_db; echo $ORACLE_BASE"
        self.ORACLE_BASE = self.server.exec_command(cmd)
        if not self.server.isDirectory(self.ORACLE_BASE):
            raise wbxexception("ORACLE_BASE: %s is not a directory" % self.ORACLE_BASE)

        logger.info("SP_SYS_PRODDIR=%s, SPLEX_BIN_DIR=%s, ORACLE_SID=%s, GRID_HOME=%s, DB_HOME=%s, ORACLE_BASE=%s" % (self.SP_SYS_PRODDIR, self.SPLEX_BIN_DIR, self.ORACLE_SID, self.GRID_HOME, self.DB_HOME, self.ORACLE_BASE))

        cmd = ". /home/oracle/.11g_grid; olsnodes"
        res = self.server.exec_command(cmd)
        self.serverlist = res.splitlines()
        for host_name in self.serverlist:
            cmd = """ ssh %s echo "TRUE" """ % host_name
            res = self.server.exec_command(cmd)
            if res.find("TRUE") == -1:
                raise wbxexception(
                    "Can not login to %s from current server %s with non-password login" % (host_name, self.host_name))
        logger.info("serverlist=%s" % self.serverlist)


        if not self.server.isFile("%s/conf/shareplexport_template_new.cap" % self.script_dir):
            raise wbxexception(
                "the crs template cap file: %s/conf/shareplexport_template_new.cap not exists" % self.script_dir)
        if not self.server.isFile("%s/conf/splex_action_template.sh" % self.script_dir):
            raise wbxexception(
                "the crs template action file: %s/conf/splex_action_template.sh not exists" % self.script_dir)

    def getInstallSplexuserPassword(self, db_name):
        logger.info("getSplexuserPassword start host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        userlist = []
        try:
            depotDaoManager.startTransaction()
            userlist = depotdbDao.getOldSplexuserPasswordByPort(db_name, self.splex_port)
            depotDaoManager.commit()
        except DatabaseError as e:
            logger.error("getUserPasswordByHostname met error %s" % e)
        if not userlist:
            raise Exception("cannot get password of shareplex user %s of oracle db %s from depot" % (self.splexuser, db_name))
        for userinfo in userlist:
            trim_host = userinfo[0]
            db_name = userinfo[1]
            pwd = userinfo[3]
            splex_sid = userinfo[4]
            logger.info("verify user %s password in db %s" % (self.splexuser, splex_sid))
            #####################################################
            # check splex user not in db
            #####################################################

        logger.info("getSplexuserPassword end host_name=%s, splex_port=%s" % (self.host_name, self.splex_port))

    def checkTablespace(self):
        tablespace_source_dict = {
            "splex_data_tblsps": "splex_port_setup_tblsp_data.txt",
            "splex_indx_tblsps": "splex_port_setup_tblsp_indx.txt",
            "splex_temp_tblsps": "splex_port_setup_tblsp_temp.txt"
        }
        tablespace_rst_dict = {
            "splex_data_tblsps": "",
            "splex_indx_tblsps": "",
            "splex_temp_tblsps": ""
        }
        for k, v in tablespace_source_dict.items():
            cmd = "cat %s/logs/%s | egrep -v '^$|^ ' |tr '%s' ',' | tr -d [:space:] | sed 's/.$//'" % (
            self.script_dir, v, "\\n")
            rst = self.server.exec_command(cmd)
            tablespace_rst_dict[k] = rst
            if "," in rst:
                raise wbxexception("get invalid %s tablespace from %s : %s" % (k, v, rst))
        self.splex_data_tblsp = tablespace_rst_dict["splex_data_tblsps"]
        self.splex_indx_tblsp = tablespace_rst_dict["splex_indx_tblsps"]
        self.splex_temp_tblsp = tablespace_rst_dict["splex_temp_tblsps"]
        logger.info("SPLEX DATA Tablespace(s): %s" % self.splex_data_tblsp)
        logger.info("SPLEX INDX Tablespace(s): %s" % self.splex_indx_tblsp)
        logger.info("SPLEX TEMP Tablespace(s): %s" % self.splex_temp_tblsp)

    def CreateProfileAndVardir(self):
        if not self.server.isDirectory(self.SPLEX_BIN_DIR):
            raise wbxexception("SPLEX_BIN_DIR: %s not exists" % self.SPLEX_BIN_DIR)
        # creating the profile
        logger.info("Creating the SPLEX profile (%s/.profile_%s) ..." % (self.SPLEX_BIN_DIR, self.splex_port))
        cmd = """
                cat /dev/null > .profile_%s
                echo "ORACLE_SID=%s; export ORACLE_SID
                SP_COP_TPORT=%s; export SP_COP_TPORT
                SP_COP_UPORT=%s; export SP_COP_UPORT
                SP_SYS_VARDIR=%s; export SP_SYS_VARDIR
                SP_SYS_HOST_NAME=%s; export SP_SYS_HOST_NAME
                SP_SYS_PRODDIR=%s; export SP_SYS_PRODDIR
                ORACLE_BASE=%s; export ORACLE_BASE
                ORACLE_HOME=%s; export ORACLE_HOME
                NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1; export NLS_LANG
                EDITOR=vi; export EDITOR
                ulimit -n 1024"      >> .profile_%s
                """ % (
        self.SPLEX_BIN_DIR, self.splex_port, self.ORACLE_SID, self.splex_port, self.splex_port, self.SP_SYS_VARDIR,
        self.host_vip, self.SP_SYS_PRODDIR, self.ORACLE_BASE, self.DB_HOME, self.SPLEX_BIN_DIR, self.splex_port)
        self.server.exec_command(cmd)
        if not self.server.isFile("%s/.profile_%s" % (self.SPLEX_BIN_DIR, self.splex_port)):
            raise wbxexception("%s/.profile_%s has not been created properly" % self.SPLEX_BIN_DIR, self.splex_port)

        cmd = "ln -s %s/.profile_%s %s/.profile_u%s" % (self.SPLEX_BIN_DIR, self.splex_port, self.SPLEX_BIN_DIR, self.splex_port)
        self.server.exec_command(cmd)

        # make the vardir and structure
        logger.info("Creating the vardir and all the other directories")
        cmd = """
                sudo mkdir -p %s
                sudo chown -R oracle:oinstall %s
                cd %s
                mkdir config data db dump idx log rim save state temp
                """ % (self.SP_SYS_VARDIR, self.SP_SYS_VARDIR, self.SP_SYS_VARDIR)
        self.server.exec_command(cmd)

        # get host_ids for next step
        splex_uname = ""
        if self.server.isFile("%s/util/splex_uname" % self.SP_SYS_PRODDIR):
            splex_uname = "%s/util/splex_uname" % self.SP_SYS_PRODDIR
        elif self.server.isFile("%s/install/splex_uname" % self.SP_SYS_PRODDIR):
            splex_uname = "%s/install/splex_uname" % self.SP_SYS_PRODDIR
        if not splex_uname:
            raise Exception("%s/util/splex_uname AND %s/install/splex_uname" % (self.SP_SYS_PRODDIR, self.SP_SYS_PRODDIR))
        logger.info("SPLEX Uname binary file: %s" % splex_uname)

        ssh_user = "oracle"
        host_id_list = []
        for k, v in self.serverDict.items():
            server_obj = wbxssh(k, 22, ssh_user, v)
            cmd = "%s | grep 'Host ID' | awk -F'=' '{print $2}' | tr -d [:space:]" % splex_uname
            host_id = server_obj.exec_command(cmd)
            host_id_list.append(host_id)
        logger.info("SPLEX Host IDs: %s" % host_id_list)

        # create the param db
        logger.info("Creating the paramdb (%s/data/paramdb) ..." % self.SP_SYS_VARDIR)
        cmd = "cat /dev/null > %s/data/paramdb" % self.SP_SYS_VARDIR
        self.server.exec_command(cmd)
        for h_id in host_id_list:
            cmd = """
                    echo "SP_SYS_LIC_%s \"${_license_value}:${_license_customer}\""  >> %s/data/paramdb
                    """ % (h_id, self.SP_SYS_VARDIR)
            self.server.exec_command(cmd)

        # verify the parameter is in the param-defaults before adding
        param_value_dict = {
            "SP_OPO_READRELEASE_INTERVAL": "10000",
            "SP_OPO_SQL_CACHE_DISABLE": "1",
            "SP_XPT_KEEPALIVE": "1",
            "SP_OCT_REPLICATE_DDL": "0",
            "SP_OCT_REPLICATE_ALL_DDL": "0",
            "SP_OCT_AUTOADD_ENABLE": "0",
            "SP_OCT_DDL_UPDATE_CONFIG": "0",
            "SP_OPO_GENERIC_CR": "1",
            "SP_OCT_REDOLOG_ENSURE": "2",
            "SP_OCT_ASM_SUPPORT": "1",
            "SP_OCT_USE_DST": "0",
            "SP_OCT_AUTOADD_ENABLE": "0",
            "SP_OCT_OLP_TRACE": "0",
            "SP_ORD_BATCH_ENABLE": "0",
            "SP_OPO_SUPPRESSED_OOS": "0",
            "SP_SYS_TARGET_COMPATIBILITY": "7",
            "SP_OCT_OLOG_USE_OCI": "1",
            "SP_OCT_OLOG_NO_DATA_DELAY": "5000000"
        }
        cmd = "cat %s/data/param-defaults" % self.SP_SYS_PRODDIR
        param_defaults_rst = self.server.exec_command(cmd)
        cmd = "cat %s/data/paramdb" % self.SP_SYS_VARDIR
        paramdb_rst = self.server.exec_command(cmd)
        for key, value in param_value_dict.items():
            if key in param_defaults_rst and key not in param_value_dict:
                cmd = """
                    echo "%s \"%s\""           >> %s/data/paramdb
                    """ % (key, value, self.vardir)
                rst, rst_bool = self.server.exec_command(cmd)
                if not rst_bool:
                    self.server.disconn_server()
                    raise Exception("Failed to execute %s" % cmd)
                self.Log("%s\" %s\"" % (key, value))

    # def registerCRSService(self):



class ShareplexParam:
    def __init__(self, name, value, units, seta, default_value):
        self.name = name
        self.value = value
        self.units = units
        self.seta = seta
        self.default_value = default_value

