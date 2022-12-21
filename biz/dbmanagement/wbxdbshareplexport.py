import logging
import os
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from sqlalchemy import Table, Column, MetaData, String, DateTime, Integer, and_, create_engine
from sqlalchemy.exc import  DBAPIError, DatabaseError
from sqlalchemy.pool import NullPool


logger = logging.getLogger("DBAMONITOR")

class wbxdbshareplexport:
    def __init__(self, port, server, splexpwd = None):
        self._port = port
        self._profile = None
        self._splexuser = "splex%s" % port
        self._service_name = "shareplex%s" % port
        self._SPLEX_BIN_DIR = None
        self._SP_SYS_VARDIR = None
        self._SP_SYS_PRODDIR = None
        self._profile = None
        self._uprofile = None
        self._paramdb = None
        self._dbList = {}
        # A db has different splex sid for different splex port in tahoe, db object also has a default splex_sid
        self._splexsiddict = {}
        self._current_splex_sid = None
        self._server = server
        self._pwd = splexpwd
        self._srcdbDict = {}
        self._tgtdbDict = {}
        self._tbs_data = None
        self._tbs_temp = None
        self._tbs_idx = None
        self.install_files = {"9.2.1": "/staging/Software/Oracle/Software/Shareplex/SharePlex-9.2.1-b39-ONEOFF-SPO3828-SPO17377-rhel-amd64-m64.tpm",
                              "8.6.3": "/staging/Software/Oracle/Software/Shareplex8.6.2/SharePlex-8.6.3-b171-ONEOFF-CR112138-CR112068-CR112118-CR112472-CR110659-CR112053b-CR112069-oracle110-rh-40-amd64-m64.tpm"}
        self._oraport_file = "/etc/oraport"

    def addDatabase(self, db, splex_sid):
        db_name = db.getDBName()
        if db_name not in self._dbList:
            self._dbList[db_name] = db
            self._splexsiddict[db_name] = splex_sid

    def getServer(self):
        return self._server

    def getDBList(self):
        return self._dbList

    def addSrceDB(self, srcdb):
        self._srcdbDict[srcdb.getDBName()] = srcdb

    def addTgtDB(self,tgtdb):
        self._tgtdbDict[tgtdb.getDBName()] = tgtdb

    def getSrcDBList(self):
        return list(self._srcdbDict.values())

    def getTgtDBList(self):
        return list(self._tgtdbDict.values())

    def setDataTablespace(self, tbsname):
        self._tbs_data = tbsname

    def getDataTablespace(self):
        return self._tbs_data

    def setIndexTablespace(self, tbsname):
        self._tbs_idx = tbsname

    def getIndexTablespace(self):
        return self._tbs_idx

    def setTempTablespace(self, tbsname):
        self._tbs_temp = tbsname

    def getTempTablespace(self):
        return self._tbs_temp

    def removeChannel(self, db):
        db_name = db.getDBName()
        if db_name in self._srcdbDict:
            self._srcdbDict.pop(db_name)
        if db_name in self._tgtdbDict:
            self._tgtdbDict.pop(db_name)

    def getPort(self):
        return self._port

    def getPwd(self):
        return self._pwd

    def setPwd(self, pwd):
        self._pwd = pwd

    def getProddir(self):
        return self._SP_SYS_PRODDIR

    def getVardir(self):
        return self._SP_SYS_VARDIR

    def getProfile(self):
        return self._profile

    # For new port, it need to get enviroment config by setEnviromentConfig()
    # for but existed port, it should call checkPortExists() to initialize enviroment items
    def setEnviromentConfig(self, prod_dir, splex_sid):
        self._SP_SYS_PRODDIR = prod_dir
        self._PRODDIR_NAME = os.path.basename(self._SP_SYS_PRODDIR)
        self._SPLEX_BIN_DIR = self._SP_SYS_PRODDIR +"/bin"
        self._SP_SYS_VARDIR = os.path.dirname(self._SP_SYS_PRODDIR) + "/vardir_{}".format(self._port)
        self._profile = self._SPLEX_BIN_DIR + "/.profile_{}".format(self._port)
        self._uprofile = self._SPLEX_BIN_DIR + "/.profile_u{}".format(self._port)
        self._paramdb = self._SP_SYS_VARDIR + "/data/paramdb"
        self._current_splex_sid = splex_sid

    def preverifyForExistPort(self):
        self._server.checkEnviromentConfig()
        logger.info("check shareplex port %s on server %s" % (self._port, self._server.getHostname()))
        cmd = "cat /etc/oraport | grep ^%s | awk -F: '{print $2}'" % self._port
        self._SPLEX_BIN_DIR = self._server.exec_command(cmd)
        if wbxutil.isNoneString(self._SPLEX_BIN_DIR):
            raise wbxexception("Does not find the port %s in /etc/oraport on host %s" % (self._port, self._server.getHostname()))
        if not self._server.isDirectory(self._SPLEX_BIN_DIR):
            raise wbxexception("%s is not a directory on server %s" % (self._SPLEX_BIN_DIR, self._server.getHostname()))

        self._profile = self._SPLEX_BIN_DIR + "/.profile_{}".format(self._port)
        self._uprofile = self._SPLEX_BIN_DIR + "/.profile_u{}".format(self._port)
        if not self._server.isFile(self._profile):
            raise wbxexception(
                'The profile %s does not exist on server %s' % (self._profile, self._server.getHostname()))
        logger.info("profile:%s" % self._profile)

        cmd = "ps -ef | grep sp_cop | grep -v grep | grep %s | wc -l" % self._port
        res = self._server.exec_command(cmd)
        if int(res) == 0:
            raise wbxexception("The shareplex port %s is not running or not exist on this server %s" % (
            self._port, self._server.getHostname()))

        cmd = ". %s; echo ORACLE_SID:$ORACLE_SID\;SP_SYS_VARDIR:$SP_SYS_VARDIR\;SP_SYS_PRODDIR:$SP_SYS_PRODDIR" % (
            self._profile)
        res = self._server.exec_command(cmd)
        itemdict = {proitem.split(":")[0]: proitem.split(":")[1] for proitem in res.split(";")}
        self.ORACLE_SID = itemdict["ORACLE_SID"]
        self._SP_SYS_VARDIR = itemdict["SP_SYS_VARDIR"]
        self._SP_SYS_PRODDIR = itemdict["SP_SYS_PRODDIR"]
        self._PRODDIR_NAME = os.path.basename(self._SP_SYS_PRODDIR)

        self._paramdb = "%s/data/paramdb" % self._SP_SYS_VARDIR
        connectionfile = "%s/data/connections.yaml" % self._SP_SYS_VARDIR
        res = self._server.exec_command(
            r"""cat %s | tr '\n' ' ' | sed "s/o\./\n/g" | grep -i splex%s | awk '{print $1}' | awk -F: '{print $1}' """ % (
                connectionfile, self._port))
        self.dbsiddict = {}
        if not wbxutil.isNoneString(res):
            splex_sids = res.splitlines()
            for splex_sid in splex_sids:
                self.dbsiddict[splex_sid] = ""
        logger.info("SP_SYS_PRODDIR=%s, SP_SYS_VARDIR=%s, dbsids=%s on host %s splex_port %s" % (
            self._SP_SYS_PRODDIR, self._SP_SYS_VARDIR, res, self._server.getHostname(), self._port))

        GRID_HOME = self._server.getGridHome()
        self.GRID_PROFILE_FILE = "%s/crs/profile/shareplex%s.cap" % (GRID_HOME, self._port)
        self.GRID_SCRIPT_FILE = "%s/crs/script/splex_action_%s.sh" % (GRID_HOME, self._port)
        if not self._server.isFile(self.GRID_PROFILE_FILE):
            raise wbxexception("%s does not exist" % self.GRID_PROFILE_FILE)
        if not self._server.isFile(self.GRID_SCRIPT_FILE):
            raise wbxexception("%s does not exist" % self.GRID_SCRIPT_FILE)

        status = self._server.getCRSServiceStatus(self._service_name)
        if status == "OFFLINE":
            raise wbxexception("The shareplex service %s is offline." % self._service_name)

    def preverifyForNewPort(self, splex_version):
        try:
            logger.info("wbxdbshareplexport.preverifyForNewPort(%s)" % self._port)
            self._server.connect()
            self._server.checkEnviromentConfig()
            if self._server.isDirectory(self._SP_SYS_VARDIR):
                if not self._server.isNullDirectory(self._SP_SYS_VARDIR):
                    raise wbxexception("The vardir dir %s is not Null")
            self._server.exec_command("sudo mkdir %s" % self._SP_SYS_VARDIR)
            self._server.exec_command("sudo chown -R oracle:oinstall %s" % self._SP_SYS_VARDIR)
            if not self._server.isNullDirectory(self._SP_SYS_VARDIR):
                raise wbxexception("The vardir %s is not NULL" % self._SP_SYS_VARDIR)
            # self._server.exec_command("cd %s; mkdir config data db dump idx log rim save state temp " % self._SP_SYS_VARDIR)

            if not self._server.isDirectory(self._SP_SYS_PRODDIR):
                self._server.exec_command("sudo mkdir %s" % self._SP_SYS_PRODDIR)
                self._server.exec_command("sudo chown -R oracle:oinstall %s" % self._SP_SYS_PRODDIR)
            if not self._server.isNullDirectory(self._SP_SYS_PRODDIR):
                raise wbxexception("The prod dir %s is not NULL" % self._SP_SYS_PRODDIR)

            if splex_version not in self.install_files:
                raise wbxexception("Do not find the binary installation file for the splex version %s" % splex_version)
            binary_file = self.install_files[splex_version]
            if not self._server.isFile(binary_file):
                raise wbxexception("The binary file for the splex version %s does not exist" % splex_version)

            if wbxutil.isNoneString(self._tbs_data):
                raise wbxexception("Data tablespace for splex user does not exist")
            if wbxutil.isNoneString(self._tbs_idx):
                raise wbxexception("Index tablespace for splex user does not exist")
            if wbxutil.isNoneString(self._tbs_temp):
                raise wbxexception("Temp tablespace for splex user does not exist")
        finally:
            self._server.close()


    def isBinaryInstalled(self, PROD_DIR):
        param_file=PROD_DIR + "/data/param-defaults"
        return self._server.isFile(param_file)

    def getBinaryFile(self, splexverion):
        if splexverion in self.install_files:
            return self.install_files[splexverion]
        return None

    def installShareplexBinary(self, splex_version):
        logger.info("installShareplexBinary with version=%s on server %s" % (splex_version, self._server.getHostname()))
        if not self._server.isDirectory(self._SP_SYS_PRODDIR):
            res = self._server.exec_command("sudo mkdir %s" % self._SP_SYS_PRODDIR)
            res = self._server.exec_command("sudo chown -R oracle:oinstall %s" % self._SP_SYS_PRODDIR)
        if splex_version in self.install_files:
            sp_install_file = self.install_files[splex_version]
        else:
            raise wbxexception("Can not find tpm file for shareplex versioin %s" % splex_version)

        tpmfilename=os.path.basename(sp_install_file)
        tpmfile="/home/oracle/%s" % tpmfilename
        if not self._server.isFile(tpmfile):
            cmd="cp -f %s /home/oracle/" % sp_install_file
            res = self._server.exec_command(cmd)
            cmd = "chmod 775 %s" % tpmfile
            res = self._server.exec_command(cmd)
            if not self._server.isFile(tpmfile):
                raise wbxexception("The file does not exist %s" % tpmfile)

        DEFAULT_VARDIR="/tmp/shareplex_vardir"
        if self._server.isDirectory(DEFAULT_VARDIR):
            # remove installation temp dir
            cmd = "rm -rf %s" % DEFAULT_VARDIR
            res = self._server.exec_command(cmd)
        cmd = "mkdir %s" % DEFAULT_VARDIR
        res = self._server.exec_command(cmd)
        if not wbxutil.isNoneString(res):
            raise wbxexception("Create %s failed with msg:%s" %(DEFAULT_VARDIR, res))
        if not self._server.isDirectory(DEFAULT_VARDIR):
            raise wbxexception("The default shareplex vardir does not exist with exception %s" % res)

        cmd = "cd /home/oracle; ./%s" % tpmfilename
        args = []
        if self.version_count > 0:
            args.append(str(self.version_count + 1))
        args.append(self._SP_SYS_PRODDIR)
        args.append(DEFAULT_VARDIR)
        args.append("1")
        if splex_version == "8.6.3":
            args.append(self._current_splex_sid)
            args.append(self._server.getOracleHome())
        args.append("2100")
        args.append("yes")
        args.append("yes")
        args.append("DZHCEZ8VJ8V54WPGAJ2NL73N8SQVZR6Z7B")
        args.append("CISCO SYSTEMS INC")
        # self._server.connect()
        res = self._server.exec_command(cmd, 300, True, *args)
        # self._server.close()
        logger.info("installShareplexBinary end host_name=%s, splex_port=%s" % ( self._server.getHostname(), self._port))


    def getShareplexInstalledCount(self):
        self.version_count = 0
        installfile = "/home/oracle/.shareplex/install.conf"
        if self._server.isFile(installfile):
            res = self._server.exec_command("""cat %s | grep "\/" | awk -F: '{print $1}' | sed "s/[[:space:]]//g" """ % installfile)
            if not wbxutil.isNoneString(res):
                for row in res.splitlines():
                    param_file = row + "/data/param-defaults"
                    if self._server.isFile(param_file):
                        self.version_count += 1
        logger.info("version_count=%s on host %s splex_port %s" % (self.version_count, self._server.getHostname(), self._port))

    def getShareplexVersion(self):
        res = self.exec_command("version")
        lines = res.splitlines()
        for line in lines:
            if line.find("SharePlex Version") >= 0:
                releaseversion = line.split('=')[1].strip().replace("\.","")
                return releaseversion
        return None

    def addProfile(self, splex_sid):
        logger.info("add profile for shareplex port %s" % self._port)
        if self._server.isFile(self._profile):
            self._server.removeFile(self._profile)

        cmd = """echo "ORACLE_SID=%s; export ORACLE_SID
SP_COP_TPORT=%s; export SP_COP_TPORT
SP_COP_UPORT=%s; export SP_COP_UPORT
SP_SYS_VARDIR=%s; export SP_SYS_VARDIR
SP_SYS_HOST_NAME=%s; export SP_SYS_HOST_NAME
SP_SYS_PRODDIR=%s; export SP_SYS_PRODDIR
ORACLE_BASE=%s; export ORACLE_BASE
ORACLE_HOME=%s; export ORACLE_HOME
NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1; export NLS_LANG
EDITOR=vi; export EDITOR
ulimit -n 1024"      >> %s """ % (
            splex_sid, self._port, self._port, self._SP_SYS_VARDIR,
            self._server.getVIPName(), self._SP_SYS_PRODDIR, self._server.getOracleBase(), self._server.getOracleHome(),
            self._profile)
        logger.info("items in profile %s" % cmd)
        self._server.exec_command(cmd)
        profile_uname = os.path.basename(self._uprofile)

        if not self._server.isFile(self._uprofile):
            logger.info("profile_u<port> does not exist, create it now")
            cmd = "cd %s; ln -s %s %s" %(self._SPLEX_BIN_DIR, self._profile, profile_uname)
            self._server.exec_command(cmd)
        tport = self._server.exec_command("source %s; echo $SP_COP_TPORT" % self._uprofile)
        if tport != str(self._port):
            raise wbxexception("the profile %s is generated failed" % self._profile)
        logger.info("Profile for the port %s succeed" % self._port)

    def changeProfile(self, PRODDIR_NAME, NEW_PRODDIR_NAME, NEW_SPLEX_BIN_DIR):
        logger.info("changeProfile start PRODDIR_NAME=%s, NEW_PRODDIR_NAME=%s, host_name=%s, splex_port=%s" % (PRODDIR_NAME, NEW_PRODDIR_NAME,  self._server.getHostname(), self._port))
        cmd = """ cp %s %s""" % (self._profile, NEW_SPLEX_BIN_DIR)
        self._server.exec_command(cmd)
        NEW_SPLEX_PROFILE = "%s/.profile_%s" %(NEW_SPLEX_BIN_DIR,self._port)

        # NEW_SPLEX_PROFILE="%s/%s" % (NEW_SPLEX_BIN_DIR, self._profile)
        # Because all below steps should based on original variable
        profile_link_name= ".profile_u%s" % self._port
        cmd = "cd %s; ln -s %s %s " % (NEW_SPLEX_BIN_DIR, NEW_SPLEX_PROFILE, profile_link_name)
        logger.info(cmd)
        self._server.exec_command(cmd)
        cmd = """sed -i "s/%s/%s/g" %s """ % (PRODDIR_NAME, NEW_PRODDIR_NAME, NEW_SPLEX_PROFILE)
        self._server.exec_command(cmd)
        self._old_profile = self._profile
        self._OLD_PRODDIR_NAME = PRODDIR_NAME
        self._profile = NEW_SPLEX_PROFILE
        self._uprofile ="%s/.profile_u%s" %(NEW_SPLEX_BIN_DIR,self._port)
        self._SPLEX_BIN_DIR = NEW_SPLEX_BIN_DIR
        self._SP_SYS_PRODDIR = self._SP_SYS_PRODDIR.replace(PRODDIR_NAME, NEW_PRODDIR_NAME)
        logger.info("changeProfile end NEW_SPLEX_PROFILE=%s, host_name=%s, splex_port=%s" % (NEW_SPLEX_PROFILE, self._server.getHostname(), self._port))

    def addDefaultParameter(self):
        logger.info("add parameter for the port %s" % self._port)
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
            "SP_OCT_ASM_SID": self._server.getASMSid(),
            "SP_OCT_USE_DST": "0",
            "SP_OCT_OLP_TRACE": "0",
            "SP_ORD_BATCH_ENABLE": "0",
            "SP_OPO_SUPPRESSED_OOS": "0",
            "SP_SYS_TARGET_COMPATIBILITY": "7",
            "SP_OCT_OLOG_USE_OCI": "1",
            "SP_OCT_OLOG_NO_DATA_DELAY": "5000000"
        }
        cmd = "cat %s/data/param-defaults | grep -v ^# | awk '{print $1}'" % self._SP_SYS_PRODDIR
        str_param_defaults= self._server.exec_command(cmd)
        cmd = "cat %s/data/paramdb" % self._SP_SYS_VARDIR
        str_paramdb = self._server.exec_command(cmd)
        param_default_dict = {line:1 for line in str_param_defaults.splitlines()}
        paramdbdict = {line.split()[0]: line.split()[1] for line in str_paramdb.splitlines()}
        for key, value in param_value_dict.items():
            if key in param_default_dict and key not in paramdbdict:
                cmd = """echo "%s \"%s\""           >> %s""" % (key, value, self._paramdb)
                logger.info("add parameter %s" % cmd)
                self._server.exec_command(cmd)

    def backupVardir(self):
        backupfile = "/tmp/vardir_%s.tar.gz" % self._port
        if self._server.isFile(backupfile):
            cmd = "rm -rf %s" % backupfile
            self._server.exec_command(cmd)

        cmd = "tar -zcvf /tmp/vardir_%s.tar.gz --absolute-names %s" % (self._port, self._SP_SYS_VARDIR)
        logger.info("backupVardir start cmd=%s host_name=%s, splex_port=%s" % (cmd, self._server.getHostname(), self._port))
        self._server.exec_command(cmd)
        logger.info("backupVardir end host_name=%s, splex_port=%s" % (self._server.getHostname(), self._port))

    def exec_command(self,cmd):
        logger.info('''execute command "%s" for splex_port=%s at host_name=%s''' % (cmd, self._port, self._server.getHostname()))
        cmd = '''cd %s; . %s; echo "%s" | ./sp_ctrl | grep -v ^* ''' % (self._SPLEX_BIN_DIR, self._profile, cmd)
        resList = self._server.exec_command(cmd)
        logger.info(resList)
        return resList

    def changeparameter(self, paramdict):
        logger.info("changeparameter %s host_name=%s, splex_port=%s" % (paramdict, self._server.getHostname(), self._port))

        for paramname, paramvalue in paramdict.items():
            if paramname != "SP_OCT_OLOG_USE_OCI":
                cmd = """ cd %s; . %s; sed -i "/%s/d" %s """ % (self._SPLEX_BIN_DIR, self._profile, paramname, self._paramdb)
                self._server.exec_command(cmd)
                if not wbxutil.isNoneString(paramvalue):
                    cmd = """ cd %s; . %s; echo "%s  \"%s\"" >> %s """ % (self._SPLEX_BIN_DIR, self._profile, paramname, paramvalue, self._paramdb)
                    self._server.exec_command(cmd)
            else:
                cmd = """ cd %s; . %s; sed -i "s/SP_OCT_ASM_USE_OCI/SP_OCT_OLOG_USE_OCI/g" %s """ % (self._SPLEX_BIN_DIR, self._profile, self._paramdb)
                self._server.exec_command(cmd)

    def preparecronjob(self):
        logger.info("preparecronjob start host_name=%s, splex_port=%s" % (self._server.getHostname(), self._port))
        self.backup_crontab="/tmp/crontab_%s.config" % self._port
        if self._server.isFile(self.backup_crontab):
            self._server.removeFile(self.backup_crontab)

        cmd = """crontab -l > %s""" % self.backup_crontab
        self._server.exec_command(cmd)
        # Generate new crontab file
        cmd = """cat %s | grep splex8[63]*_restart_proc.*%s | wc -l""" % (self.backup_crontab, self._port)
        self.crontab_exist = self._server.exec_command(cmd)
        if int(self.crontab_exist) > 0:
            cmd = """cat %s | grep splex8[63]*_restart_proc.*%s  | tail -n 1 | awk '{print $6}'""" % (self.backup_crontab, self._port)
        else:
            cmd = """cat %s | grep splex8[63]_restart_proc | grep -v ^# | tail -n 1 | awk '{print $6}'""" % self.backup_crontab
        self.script_file = self._server.exec_command(cmd)
        if self.script_file == "":
            self.script_file="/u00/app/admin/dbarea/bin/splex921_restart_proc.sh"
        else:
            self.script_file = self.script_file.replace("splex8_restart_proc","splex_restart_proc").replace("splex863_restart_proc","splex_restart_proc")
        if not self._server.isFile(self.script_file):
            cmd = """cp /staging/Scripts/oracle/11g/crontab_monitor/dbarea/bin/splex921_restart_proc.sh %s""" % self.script_file
            self._server.exec_command(cmd)
        logger.info("preparecronjob end host_name=%s, splex_port=%s" % (self._server.getHostname(), self._port))

    def uncommentcronjob(self):
        logger.info("uncommentcronjob host_name=%s, splex_port=%s" % (self._server.getHostname(), self._port))
        if int(self.crontab_exist) > 0:
            cmd = "crontab -l | sed /splex8[63]*_restart_proc.*%s/s/splex8[63]*_restart_proc/splex921_restart_proc/ > %s; crontab %s" % (self._port, self.backup_crontab, self.backup_crontab)
        else:
            cmd = """crontab -l > %s; echo "0,15,30,45 * * * * %s %s" >> %s; crontab %s """ % (self.backup_crontab, self.script_file, self._port, self.backup_crontab, self.backup_crontab)
        self._server.exec_command(cmd)

    def addcronjob(self):
        logger.info("add cronjob splex8_restart_proc for this port")

    def registerIntoCRS(self):
        logger.info("Register this port into CRS as service")

    def unregisterFromCRS(self):
        logger.info("Unregister this port from CRS")

    def addshsetport(self):
        logger.info("Start to check shsetport file")
        if not self._server.isFile("/usr/bin/shsetport"):
            cmd = "sudo cp /staging/Scripts/oracle/port_setup/conf/shsetport_template.sh /usr/bin/shsetport"
            self._server.exec_command(cmd)
            cmd = "sudo chown oracle:oinstall /usr/bin/shsetport"
            self._server.exec_command(cmd)
            cmd = "chmod 755 /usr/bin/shsetport"
            self._server.exec_command(cmd)
        else:
            logger.info("shsetport file already exists")

    def setSplexHomeDir(self, splex_home):
        self._SP_SYS_PRODDIR = splex_home
        vardir_name = "vardir_%s" % self._port
        self._SP_SYS_VARDIR = os.path.join(os.path.dirname(splex_home), vardir_name)
        self._SPLEX_BIN_DIR = os.path.join(self._SP_SYS_PRODDIR, "bin")
        self._profile = os.path.join(self._SPLEX_BIN_DIR, ".profile_%s" % self._port)
        self._uprofile = os.path.join(self._SPLEX_BIN_DIR, ".profile_u%s" % self._port)

    def getPartitionFile(self):
        partitionfile="%s/data/horizontal_partitioning.yaml" % self._SP_SYS_VARDIR
        res = self._server.exec_command("if [ -f %s ]; then cat %s; fi" % (partitionfile, partitionfile))
        print("check on server %s with %s" % (self._server.getHostname(), res))

    def getVardirSize(self):
        self._server.exec_command("rm -rf %s/dump/*" % self._SP_SYS_VARDIR)
        self._server.exec_command("rm -rf %s/temp/*" % self._SP_SYS_VARDIR)
        self._server.exec_command("find %s/log -size +10M -exec cp /dev/null {} \;" % self._SP_SYS_VARDIR)

        cmd = "du -s %s | awk '{print $1}'" % (self._SP_SYS_VARDIR)
        res = self._server.exec_command(cmd)
        logger.info("getVardirSize with size=%s host_name=%s, splex_port=%s" % (res, self._server.getHostname(), self._port))
        if not wbxutil.isNoneString(res):
            vardirsize = int(res)
            return vardirsize
        return -1

    def qstatus(self):
        res = self.exec_command("qstatus")
        return res

    def getShareplexUserName(self):
        return self._splexuser

    def getShareplexServiceName(self):
        return self._service_name

    def getSplexsidByDBName(self, db_name):
        if db_name in self._splexsiddict:
            return self._splexsiddict[db_name]
        return None

    def verifySplexuserPassword(self):
        logger.info("getSplexuserPassword start host_name=%s, splex_port=%s" % (self._server.getHostname(), self._port))
        for db_name, db in self._dbList.items():
            pwd = db.getSchemaPassword(self._splexuser)
            try:
                connectionurl = db.getConnectionURL()
                engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (self._splexuser, pwd, connectionurl),
                                       poolclass=NullPool, echo=False)
                connect = engine.connect()
                res = connect.execute("select sysdate as curtime from dual")
                res.close()
                connect.connection.commit()
            except DatabaseError as e:
                raise wbxexception("Can not login to db %s with splex user %s password in DepotDB" % (db.getDBName(), self._splexuser))

    def getSharelexConfigFromProfile(self):
        if self._server.isFile(self._profile):
            cmd = ". %s; echo ORACLE_SID:$ORACLE_SID\;SP_SYS_VARDIR:$SP_SYS_VARDIR\;SP_SYS_PRODDIR:$SP_SYS_PRODDIR" % (self._profile)
            res = self._server.exec_command(cmd)
            itemdict = {proitem.split(":")[0]: proitem.split(":")[1] for proitem in res.split(";")}
            self.ORACLE_SID = itemdict["ORACLE_SID"]
            self._SP_SYS_VARDIR = itemdict["SP_SYS_VARDIR"]
            self._SP_SYS_PRODDIR = itemdict["SP_SYS_PRODDIR"]
            self._PRODDIR_NAME = os.path.basename(self._SP_SYS_PRODDIR)

    def stopShareplexService(self):
        logging.info("stop shareplex port %s on server %s" % (self._port, self._server.getHostname()))
        checkcmd = "ps aux | grep sp_cop | grep -v grep | grep %s | wc -l" % self._port
        processcnt = self._server.exec_command(checkcmd)
        if int(processcnt) == 0:
            return True
        stopcmd = ''' source /home/oracle/.11g_grid; crs_stop %s ''' % self._service_name
        res = self._server.exec_command(stopcmd)
        processcnt = self._server.exec_command(checkcmd)
        if int(processcnt) == 0:
            return True
        stopcmd = ''' source /home/oracle/.11g_grid; crs_stop -f %s ''' % self._service_name
        res = self._server.exec_command(stopcmd)
        processcnt = self._server.exec_command(checkcmd)
        if int(processcnt) > 0:
            raise wbxexception(
                "Failed to stop shareplex port %s on host %s with msg: %s" % (self._port, self._server.host_name, res))
        logging.warning("stop shareplex port %s on server %s end" % (self._port, self._server.getHostname()))


    def startShareplexService(self):
        logging.warning("start shareplex port %s on server %s" % (self._port, self._server.getHostname()))
        startcmd = ''' source /home/oracle/.11g_grid; crs_start %s ''' % self._service_name
        res = self._server.exec_command(startcmd)
        checkcmd = ''' source /home/oracle/.11g_grid; crsstat | grep %s ''' % self._service_name
        ckres = self._server.exec_command(checkcmd)
        if ckres.find("OFFLINE") > 0:
            raise wbxexception("Failed to start shareplex port %s on host %s with msg: %s" % (
            self._port, self._server.host_name, ckres))
        logging.warning("start shareplex port %s on server %s end" % (self._port, self._server.getHostname()))

    def orasetup(self, splex_sid, *args):
        logger.info("ora_setup shareplex port %s on server %s with args=%s" % (self._port, self._server.getHostname(), args))
        if not self._server.isFile(self._paramdb):
            self._server.exec_command("cd %s; mkdir config data db dump idx log rim save state temp " % self._SP_SYS_VARDIR)
            cmd = "cat /dev/null > %s" % self._paramdb
            self._server.exec_command(cmd)

        setup_log_file="/tmp/ora_setup_%s.log" % self._port
        setup_err_file="/tmp/ora_setup_%s.err" % self._port
        if not self._server.isFile(setup_err_file):
            cmd="touch %s" % setup_err_file
            self._server.exec_command(cmd)
        if not self._server.isFile(setup_log_file):
            cmd="touch %s" % setup_log_file
            self._server.exec_command(cmd)

        cmd = "cd %s; source %s; export ORACLE_SID=%s; ./ora_setup 1>%s 2>%s" % (self._SPLEX_BIN_DIR, self._profile, splex_sid, setup_log_file, setup_err_file)
        # args = ['n', 'n', db.getDBName(), splex_sid, "system", "sysnotallow", "n", self._splexuser, self._pwd, "n", "","", "", "y", "y", ""]
        res = self._server.exec_command(cmd, 300, False, *args)
        if wbxutil.isNoneString(res):
            if self._server.isFile(setup_err_file):
                cmd="cat %s" % setup_err_file
                res = self._server.exec_command(cmd)

        issucceed = False
        for line in res.splitlines():
            if line.find("completed successfully") > 0:
                issucceed = True
        if not issucceed:
            raise wbxexception("ora_setup failed, please check log")
        logger.info(res)
        logger.info("setup shareplex port %s on server %s end" % (self._port, self._server.getHostname()))


    def upgradeServiceConfig(self, PRODDIR_NAME, NEW_PRODDIR_NAME, NEW_SPLEX_BIN_DIR, serverlist):
        logger.info("upgradeServiceConfig start PRODDIR_NAME=%s, NEW_PRODDIR_NAME=%s, host_name=%s, splex_port=%s" % (PRODDIR_NAME, NEW_PRODDIR_NAME, self._server.getHostname(), self._port))
        try:
            cmd = """ sudo sed -i "s/%s/%s/g" %s """ % (PRODDIR_NAME, NEW_PRODDIR_NAME, self.GRID_SCRIPT_FILE)
            self._server.exec_command(cmd)
            GRID_HOME = self._server.getGridHome()
            for servername in serverlist:
                cmd = "scp %s %s:%s/crs/script/" % (self.GRID_SCRIPT_FILE, servername, GRID_HOME)
                self._server.exec_command(cmd)
            self._server.exec_command(""" sudo sed -i "/^%s/d" /etc/oraport """ % (self._port))
            self._server.exec_command(""" sudo echo "%s:%s" >> /etc/oraport """ % (self._port, self._SPLEX_BIN_DIR))
            autostartfile = "%s/WbxSplexAutoStartStoppedProcess.config" % NEW_SPLEX_BIN_DIR
            if not self._server.isFile(autostartfile):
                self._server.exec_command("""echo "%s:Y" > %s """ % (self._port, autostartfile))
            else:
                res = self._server.exec_command("""cat %s | grep ^%s: | wc -l""" % (autostartfile, self._port))
                if int(res) == 0:
                    self._server.exec_command("""echo "%s:Y " >> %s """ % (self._port, autostartfile))
        except Exception as e:
            logger.error(e)

        logger.info("upgradeServiceConfig end host_name=%s, splex_port=%s" % (self._server.getHostname(), self._port))

