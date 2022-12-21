import os
import logging
from common.wbxssh import wbxssh
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys
from common.wbxexception import WbxDaoException, wbxexception
from common.wbxutil import wbxutil

logger = logging.getLogger("DBAMONITOR")

class wbxdbserver(wbxssh):
    def __init__(self, hostname, login_pwd = None, login_user="oracle"):
        super(wbxdbserver,self).__init__(hostname,22, login_user, login_pwd)
        self._11g_db_file = "/home/oracle/.11g_db"
        self._11g_grid_file = "/home/oracle/.11g_grid"
        self._cronjob_script_dir = "/u00/app/admin/dbarea/"
        self._cname = None
        self._domain = None
        self._region_name = None
        self._login_user = "oracle"
        self._site_code = None
        self._host_ip = None
        self._vip_name = None
        self._vip_ip = None
        self._priv_name = None
        self._priv_ip = None
        self._scan_name = None
        self._scan_ip1 = None
        self._scan_ip2 = None
        self._scan_ip3 = None
        self._os_type_code = None
        self._processor = None
        self._kernel_release = None
        self._hardware_platform = None
        self._physical_cpu = None
        self._cores = None
        self._cpu_model = None
        self._flag_node_virtual = None
        self._install_date = None
        self._comments = None
        self._ssh_port = None
        self._spportDict = {}
        self._dbDict = {}
        self._splexHome = {}
        self._asm_sid = None

    def addServerInfo(self, servervo):
        self._site_code = servervo.site_code
        self._host_ip = servervo.host_ip
        self._vip_name = servervo.vip_name
        self._vip_ip = servervo.vip_ip
        self._priv_name = servervo.priv_name
        self._priv_ip = servervo.priv_ip
        self._scan_name = servervo.scan_name
        self._scan_ip1 = servervo.scan_ip1
        self._scan_ip2 = servervo.scan_ip2
        self._scan_ip3 = servervo.scan_ip3

    def setServerInfo(self, cname,domain,site_code,region_name,host_ip,priv_ip,os_type_code,processor,
                        kernel_release,hardware_platform,physical_cpu,cores,cpu_model,flag_node_virtual,install_date,
                       comments,ssh_port):
        self._cname = cname
        self._domain = domain
        self._site_code = site_code
        self._region_name = region_name
        self._host_ip = host_ip
        self._priv_ip = priv_ip
        self._os_type_code = os_type_code
        self._processor = processor
        self._kernel_release = kernel_release
        self._hardware_platform = hardware_platform
        self._physical_cpu = physical_cpu
        self._cores = cores
        self._cpu_model = cpu_model
        self._flag_node_virtual = flag_node_virtual
        self._install_date = install_date
        self._comments = comments
        self.ssh_port = ssh_port

    def getVIPIP(self):
        return self._vip_ip

    def getVIPName(self):
        if self._vip_name is None:
            self._vip_name = "%s-vip" % self.host_name
        return self._vip_name

    def getPrivIP(self):
        return self._priv_ip

    def setScanname(self, scanname):
        self._scan_name = scanname

    def getScanname(self):
        return self._scan_name

    def setScanip1(self, scanip):
        self._scan_ip1 = scanip

    def getScanip1(self):
        return self._scan_ip1

    def setScanip2(self, scanip):
        self._scan_ip2 = scanip

    def getScanip2(self):
        return self._scan_ip2

    def setScanip3(self, scanip):
        self._scan_ip3 = scanip

    def getScanip3(self):
        return self._scan_ip3

    def preverify(self):
        if self.login_pwd is None:
            self.login_pwd = self.getOracleUserPwdByHostname()
        try:
            self.connect()
            if not self.isFile(self._11g_db_file):
                raise wbxexception("The file %s does not exist" % self._11g_db_file)
            if not self.isFile(self._11g_grid_file):
                raise wbxexception("The file %s does not exist" % self._11g_db_file)
        finally:
            self.close()

    def getDBDict(self):
        return self._dbDict

    def getHostname(self):
        return self.host_name

    def getASMSid(self):
        return self._asm_sid

    def addDatabase(self, db):
        db_name = db.getDBName()
        if db_name not in self._dbDict:
            self._dbDict[db_name] = db

    def checkEnviromentConfig(self):
        logger.info("checkEnviromentConfiguration on host_name=%s" %(self.host_name))
        db_file = "/home/oracle/.11g_db"
        grid_file = "/home/oracle/.11g_grid"
        if not self.isFile(db_file):
            raise wbxexception("%s does not exist" % db_file)
        if not self.isFile(grid_file):
            raise wbxexception("%s does not exist" % grid_file)
        cmd = ". /home/oracle/.11g_grid; echo ${ORA_GRID_HOME}"
        self.GRID_HOME = self.exec_command(cmd)
        if wbxutil.isNoneString(self.GRID_HOME):
            raise wbxexception("Can not get ORA_GRID_HOME from /home/oracle/.11g_grid file on the server %s" % self.host_name)

        cmd = ". /home/oracle/.11g_grid; echo ${ORA_DB_HOME}"
        self.ORACLE_HOME = self.exec_command(cmd)
        if wbxutil.isNoneString(self.ORACLE_HOME):
            raise wbxexception("Can not get ORA_DB_HOME from /home/oracle/.11g_grid file on the server %s" % self.host_name)

        cmd = ". /home/oracle/.11g_grid; echo ${ORACLE_BASE}"
        self.ORACLE_BASE = self.exec_command(cmd)
        if wbxutil.isNoneString(self.ORACLE_BASE):
            raise wbxexception("Can not get ORACLE_BASE from /home/oracle/.11g_grid file on the server %s" % self.host_name)
        cmd = "ps aux | grep lgwr | grep -v grep | grep -i ASM | awk '{print $NF}' | awk -F_ '{print $NF}'"
        self._asm_sid=self.exec_command(cmd)
        logger.info("wbxdbserver.checkEnviromentConfig() succeed")

    def getGridHome(self):
        return self.GRID_HOME

    def getOracleHome(self):
        return self.ORACLE_HOME

    def getOracleBase(self):
        return self.ORACLE_BASE

    def registerService(self, servicename):
        pass

    def unregisterService(self, servicename):
        pass

    def getCRSServiceStatus(self, servicename):
        cmd = ". /home/oracle/.11g_grid; crsstat | grep %s" % servicename
        res = self.exec_command(cmd)
        if wbxutil.isNoneString(res):
            raise wbxexception("The service %s does not exist in CRS" % servicename)
        else:
            svcstatus = res.split()
            if svcstatus[2] != "ONLINE" or svcstatus[3] != "ONLINE":
                return "OFFLINE"
            else:
                return "ONLINE"

    def getRacServerList(self, db_name):
        try:
            self.connect()
            cmd = '''source /home/oracle/.bash_profile; srvctl status database -d %s | grep -vi not | awk '{print $2":"$7}' ''' % db_name
            rows = self.exec_command(cmd)
            if wbxutil.isNoneString(rows):
                raise wbxexception("Do not find the database %s on server %s" % (db_name, self.host_name))
            instancedict = {}
            for row in rows.splitlines():
                instance_name, server_name = row.split(":")
                instancedict[server_name] = instance_name

            cmd=''' source /home/oracle/.bash_profile; srvctl config scan |grep -E "SCAN [0-9]|scan[0-9]|SCAN name"|awk -F ":" '{split($0,b,",");print b[1] }' '''
            rows = self.exec_command(cmd)
            for row in rows.splitlines():
                scan_id=row.split(":")[0]
                scan_val = row.split(":")[1].strip()
                if scan_id.find("SCAN name")>=0:
                    self._scan_name = scan_val.split("-")[0].strip()
                if scan_id.find("scan1")>=0 or scan_id.find("SCAN 1")>=0:
                    self._scan_ip1=scan_val
                elif scan_id.find("scan2")>=0 or scan_id.find("SCAN 2")>=0:
                    self._scan_ip2 = scan_val
                elif scan_id.find("scan3")>=0 or scan_id.find("SCAN 3")>=0:
                    self._scan_ip3 = scan_val

            cmd=''' source /home/oracle/.bash_profile; srvctl config database -d %s | grep Services: | awk '{print $NF}' ''' % db_name
            service_name = self.exec_command(cmd)
            if wbxutil.isNoneString(service_name):
                raise wbxexception("Do not find HA service name for the db %s" % db_name)

            return instancedict
        finally:
            self.close()

    def getOracleUserPwdByHostname(self):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            depotDaoManager.startTransaction()
            login_pwd = depotdbDao.getOracleUserPwdByHostname(self.host_name)
            if wbxutil.isNoneString(login_pwd):
                raise wbxexception("Can not get oracle user password on the server %s in DepotDB" % self.host_name)
            depotDaoManager.commit()
            return login_pwd
        except Exception as e:
            depotDaoManager.rollback()
            errormsg="wbxdbcutoverserver.getOracleUserPwdByHostname(%s) with errormsg %s" % (self.host_name, e)
            logger.error(errormsg)
            raise wbxexception(errormsg)
        finally:
            depotDaoManager.close()

    def getShareplexPort(self, port):
        if port in self._spportDict:
            return self._spportDict[port]
        return None

    def addShareplexPort(self, spport):
        splex_port = spport.getPort()
        self._spportDict[splex_port] = spport

    def removeShareplexPort(self, port):
        if port in self._spportDict:
            self._spportDict.pop(port)

    def removeShareplexChannel(self, port, db):
        spport = self.getShareplexPort(port)
        if spport is not None:
            spport.removeChannel(db)

    def getShareplexPortList(self):
        return list(self._spportDict.values())

    def getShareplexPortListFromCRS(self):
        try:
            cmd = ''' source /home/oracle/.bash_profile; crsstat | grep ^shareplex | awk '{print $1":"$NF":"$(NF-1)}' | sed "s/shareplex//g"  | sed "s/(//g" | sed "s/)//g" '''
            self.connect()
            res = self.exec_command(cmd)
            if not wbxutil.isNoneString(res):
                return {int(line.split(":")[0]): line.split(":")[1] for line in res.splitlines()},\
                       {int(line.split(":")[0]): line.split(":")[2] for line in res.splitlines()}
            return {}
        finally:
            self.close()

    def getShareplexHomeFromServer(self):
        self._splexHome = {"8.6.3":"","9.2.1":""}

    def getShareplexHomeByVersion(self, version):
        if version in self._splexHome:
            return self._splexHome[version]
        return None

    def getDBConnectionString(self, db_name):
        if db_name == 'RACPSYT':
            return "(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.6.153)(PORT = 1701)) (ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.6.154)(PORT = 1701)) (ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.6.157)(PORT = 1701)) (LOAD_BALANCE = yes) (FAILOVER = on) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = systoolha.webex.com) (FAILOVER_MODE = (TYPE = SELECT) (METHOD = BASIC) (RETRIES = 3) (DELAY = 5))))"

    def getRacNodeDict(self):
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        res={}
        try:
            cmd="CSSD_DIR=`dirname $( ps -eo args | grep ocssd.bin | grep -v grep | awk '{print $1}')`;${CSSD_DIR}/olsnodes"
            self.connect()
            rows = self.exec_command(cmd)
            if wbxutil.isNoneString(rows):
                raise wbxexception("Do not RAC nodes on host: %s" % (self.host_name))
            step=0
            for item in rows.splitlines():
                step += 1
                key="node%s" %str(step)
                res[key]=daomanagerfactory.getServer(item)
            return res
        finally:
            self.close()