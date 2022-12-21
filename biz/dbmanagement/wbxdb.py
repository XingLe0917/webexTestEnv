import logging
from biz.dbmanagement.wbxdbserver import wbxdbserver
from biz.dbmanagement.wbxdbinstance import wbxdbinstance
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys
from common.wbxexception import WbxDaoException, wbxexception
from common.wbxutil import wbxutil
from sqlalchemy import Table, Column, MetaData, String, DateTime, Integer, and_, create_engine
from sqlalchemy.exc import DBAPIError, DatabaseError
from sqlalchemy.pool import NullPool
from common.wbxssh import wbxssh
import time
from enum import Enum

logger = logging.getLogger("DBAMONITOR")

# what is the primary key of a db. (dbname + trim_host)
# I tried to set (dbname + db_type) as primary key. But it failed, because RACPSYT is 2 db's dbname on production
class db_vendor(Enum):
    DB_VENDOR_ORACLE = "Oracle"
    DB_VENDOR_POSTGRES = "POSTGRESQL"


class wbxdb:
    def __init__(self, db_name, splex_sid=None, db_id=None):
        self._db_name = db_name
        self._trim_host = None
        self._host_name = None
        # This is db default splex_sid, a db may has special splex_sid for a shareplex port in tahoe
        self._splex_sid = splex_sid
        self._db_vendor = db_vendor.DB_VENDOR_ORACLE.value
        self._db_version = None
        self._db_type = None
        self._appln_support_code = None
        self._application_type = None
        self._db_home = None
        self._service_name = "{}ha".format(db_name)
        self._listener_port = 1701
        self._wbx_cluster = None
        self._web_domain = None
        self._monitored = None
        self._ORACLE_HOME = ""
        self._instanceDict = {}  # key is server host_name, because host_name is more frequent referenced
        self._tsList = []  # tablespace list
        self._userDict = {}
        self._connectioninfo = None
        self._dbserver = None

    def setTrimHost(self, trim_host):
        self._trim_host = trim_host

    def getTrimHost(self):
        return self._trim_host

    def getDBName(self):
        return self._db_name

    def getSplexSid(self):
        return self._splex_sid

    def setDBCutoverRole(self, role):
        self._dbRole = role

    def setdbServer(self, dbserver):
        self._dbserver = dbserver

    def getdbServer(self):
        return self._dbserver

    def setApplicationType(self, application_type):
        self._application_type = application_type

    def getApplicationType(self):
        return self._application_type

    def getServiceName(self):
        return self._service_name

    def setServicename(self, servicename):
        self._service_name = servicename

    def getDBVendor(self):
        return self._db_vendor

    def setDBVendor(self, dbvendor):
        self._db_vendor = dbvendor

    def getDBVersion(self):
        return self._db_version

    def setDBVersion(self, dbversion):
        self._db_version = dbversion

    def setApplnSupportCode(self, appln_support_code):
        self._appln_support_code = appln_support_code

    def getApplnSupportCode(self):
        return self._appln_support_code

    def setListenerPort(self, listener_port):
        self._listener_port = listener_port

    def setClusterName(self, cluster_name):
        self._wbx_cluster = cluster_name

    def setWebDomain(self, webdomain):
        self._web_domain = webdomain

    def setConnectionURL(self, connectioninfo):
        self._connectioninfo = connectioninfo

    def getConnectionURL(self):
        if self._connectioninfo is not None:
            return self._connectioninfo
        elif self._db_vendor == db_vendor.DB_VENDOR_POSTGRES.value:
            return "%s:%s/%s" % (self._dbserver.getPrivIP(), self._listener_port, self._db_name.lower())
        elif self._db_vendor == db_vendor.DB_VENDOR_ORACLE.value:
            host1 = self.getServer(None)
            if wbxutil.isNoneString(host1.getScanip1()):
                service_name = self._service_name
                if service_name.find(".") < 0:
                    service_name = "%s.webex.com" % service_name
                url = '(DESCRIPTION ='
                for name, dbinstance in self._instanceDict.items():
                    dbserver = dbinstance.getServer()
                    url = "%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))" % (
                    url, dbserver.getVIPIP(), self._listener_port)

                url = "%s (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 180)(DELAY = 5))))" \
                      % (url, service_name)
                return url

            else:
                return "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%s))" \
                       "(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%s))" \
                       "(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%s))" \
                       "(LOAD_BALANCE=yes)(FAILOVER=on)" \
                       "(CONNECT_DATA=(SERVER=DEDICATED)" \
                       "(SERVICE_NAME=%s.webex.com)(METHOD=BASIC)(RETRIES=180)(DELAY=5)))" % \
                       (host1.getScanip1(), self._listener_port, host1.getScanip2(), self._listener_port,
                        host1.getScanip3(),
                        self._listener_port, self._service_name)

    # we use host_name as key, not instance_name
    def addInstance(self, dbinstance):
        host_name = dbinstance.getServer().getHostname()
        if host_name not in self._instanceDict:
            self._instanceDict[host_name] = dbinstance

    def getInstanceByHostname(self, host_name):
        if host_name in self._instanceDict:
            return self._instanceDict[host_name]
        return None

    def removeInstance(self, instancenameList):
        for instancename, dbinstance in self._instanceDict.items():
            if instancename not in instancenameList:
                self._instanceDict.pop(instancename, None)

    def getServer(self, host_name):
        if host_name is None:
            return list(self._instanceDict.values())[0].getServer()
        for iname, dbinstance in self._instanceDict.items():
            dbserver = dbinstance.getServer()
            if dbserver.getHostname() == host_name:
                return dbserver
        else:
            return None

    def getServerNameList(self):
        return list(self._instanceDict.keys())

    def getServerDict(self):
        serverDict = {}
        if self._db_vendor == "Oracle":
            for instance_name, dbinstance in self._instanceDict.items():
                dbserver = dbinstance.getServer()
                serverDict[dbserver.getHostname()] = dbserver
        elif self._db_vendor == "POSTGRESQL":
            serverDict[self._dbserver.host_name] = self._dbserver
        return serverDict

    def removeSchemaList(self):
        self._userDict.clear()

    def getSchemaBySchemaType(self, appln_support_code, schemaType):
        userList = []
        for username, dbuser in self._userDict.items():
            if dbuser.getSchemaType() == schemaType and dbuser.getApplnSupportCode().lower() == appln_support_code.lower():
                userList.append(dbuser)
        return userList

    def getSchemaPassword(self, username):
        if username in self._userDict:
            return self._userDict[username].getPassword()
        return None

    def getUserDict(self):
        return self._userDict

    def getUserByUserName(self, username):
        try:
            connectionurl = self.getConnectionURL()
            engine = create_engine('oracle+cx_oracle://%s:%s@%s' % ("system", "sysnotallow", connectionurl),
                                   poolclass=NullPool, echo=False)
            connect = engine.connect()
            res = connect.execute(
                "select username,user_id, account_status,default_tablespace, temporary_tablespace, profile from dba_users where username=upper('%s')" % username)
            row = res.fetchone()
            res.close()
            connect.connection.commit()
            return row
        except DatabaseError as e:
            raise wbxexception("Login to db %s failed with errormsg: %s" % (self._db_name, str(e)))

    def getTablespace(self):
        try:
            connectionurl = self.getConnectionURL()
            engine = create_engine('oracle+cx_oracle://%s:%s@%s' % ("system", "sysnotallow", connectionurl),
                                   poolclass=NullPool, echo=False)
            connect = engine.connect()
            csr = connect.execute("select tablespace_name, status, contents from dba_tablespaces")
            rows = csr.fetchall()
            csr.close()
            connect.connection.commit()
            return rows
        except DatabaseError as e:
            raise wbxexception("Can not login to db %s" % (self._db_name))

    def addUser(self, dbuser):
        username = dbuser.getUserName()
        self._userDict[username] = dbuser

    def addServer(self, server):
        self._instanceDict[server.getHostname()] = server

    def initFromServer(self, hostname):
        logging.warning("init database %s from server %s" % (self._db_name, hostname))
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daomanagerfactory.getDefaultDaoManager()

        loginpwd = None
        try:
            daomanager.startTransaction()
            dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            userList = dao.getLoginUser([hostname])
            if len(userList) == 0:
                raise wbxexception("Can not get login user password for host %s in depotdb" % hostname)
            for userinfo in userList:
                if userinfo[0] == hostname:
                    loginpwd = userinfo[1]
            if loginpwd is None:
                raise wbxexception("Can not get login user password for host %s in depotdb" % hostname)
            dbserver = wbxdbserver(hostname, loginpwd)
            dbserver.verifyConnection()
            instanceDict = dbserver.getRacServerList(self._db_name)
            hostnamelist = list(instanceDict.keys())
            userList = dao.getLoginUser(hostnamelist)
            for server_name, instance_name in instanceDict.items():
                haspassword = False
                for uhostname, pwd in userList:
                    if server_name == uhostname:
                        haspassword = True
                        idbserver = wbxdbserver(server_name, pwd)
                        idbserver.verifyConnection()
                        idbserver.setScanname(dbserver.getScanname())
                        idbserver.setScanip1(dbserver.getScanip1())
                        idbserver.setScanip2(dbserver.getScanip2())
                        idbserver.setScanip3(dbserver.getScanip3())
                        dbinstance = wbxdbinstance(self, instance_name, idbserver)
                        self.addInstance(dbinstance)
                if not haspassword:
                    raise wbxexception("Can not get login user password for host %s in depotdb" % server_name)

            daomanager.commit()
        except Exception as e:
            daomanager.rollback()
            raise e
        finally:
            daomanager.close()

    def getShareplexUserDataTablespace(self):
        return "SPLEX_DATA"

    def getShareplexUserIndexTablespace(self):
        for tbsname in self._tsList:
            if tbsname.find("SPLEX_I") == 0:
                return tbsname

    def getTempTablespace(self):
        return "temp"

    def getConnectionInfoFromDepotDB(self):
        for tbsname in self._tsList:
            if tbsname.find("SPLEX_I") == 0:
                return tbsname

    def getConnectionInfoFromServer(self):
        dbinstance = list(self._instanceDict.values())[0]
        dbserver = dbinstance.getServer()
        self._db_user = "system"
        self._db_pwd = "sysnotallow"
        self._connectString = dbserver.getDBConnectionString()

    def getShareplexPortList(self):
        spportList = []
        for dbinstance in self._instanceDict.values():
            subspportList = dbinstance.getServer().getShareplexPortList()
            spportList.extend(subspportList)
        return spportList

    def getAppSchemas(self):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        reslist = []
        try:
            depotDaoManager.startTransaction()
            reslist = depotdbDao.getDBAppschema(self._db_name)
            return reslist
        except Exception as e:
            depotDaoManager.rollback()
            logger.error(e)
        finally:
            depotDaoManager.close()

    def getSplxPortbydb(self):
        spportdict = {"src": [], "tgt": []}
        dbspportdict = {}
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            depotDaoManager.startTransaction()
            hostlist = self._instanceDict.keys()
            splexlist = depotdbDao.getsplexportbydb(self._db_name, *hostlist)
            for item in splexlist:
                src_host = item[1].split('?')[0]
                src_db = item[1].split('?')[1]
                tgt_host = item[1].split('?')[2]
                tgt_db = item[1].split('?')[3]
                port = int(item[1].split('?')[4])
                splexinfo = {"src_host": src_host, "src_db": src_db, "tgt_host": tgt_host, "tgt_db": tgt_db,
                             "port": port}

                if item[0] == 'src':
                    spportdict["src"].append(splexinfo)
                    dbspportdict[port] = src_host
                else:
                    spportdict["tgt"].append(splexinfo)
                    dbspportdict[port] = tgt_host
            depotDaoManager.commit()
            return spportdict, dbspportdict
        except Exception as e:
            depotDaoManager.rollback()
            logger.error(e)
        finally:
            depotDaoManager.close()

    def getCISplxPortbydb(self):
        spportdict = {"src": [], "tgt": []}
        dbspportdict = {}
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            depotDaoManager.startTransaction()
            hostlist = self._instanceDict.keys()
            splexlist = depotdbDao.getsplexportbydb(self._db_name, *hostlist)
            for item in splexlist:
                tgt_db = item[1].split('?')[3]
                if "ID" in tgt_db:
                    src_host = item[1].split('?')[0]
                    src_db = item[1].split('?')[1]
                    tgt_host = item[1].split('?')[2]
                    port = int(item[1].split('?')[4])
                    splexinfo = {"src_host": src_host, "src_db": src_db, "tgt_host": tgt_host, "tgt_db": tgt_db,
                                 "port": port}
                    if item[0] == 'src':
                        spportdict["src"].append(splexinfo)
                        dbspportdict[port] = src_host
                    else:
                        spportdict["tgt"].append(splexinfo)
                        dbspportdict[port] = tgt_host
            depotDaoManager.commit()
            return spportdict, dbspportdict
        except Exception as e:
            depotDaoManager.rollback()
            logger.error(e)
        finally:
            depotDaoManager.close()

    def getShareplexPort(self, port):
        for dbinstance in list(self._instanceDict.values()):
            for spport in dbinstance.getServer().getShareplexPortList():
                if spport.getPort() == port:
                    return spport

    def removeShareplexPort(self, port):
        for dbinstance in list(self._instanceDict.values()):
            dbinstance.getServer().removeShareplexPort(port)

    def removeShareplexChannel(self, port, db):
        for dbinstance in list(self._instanceDict.values()):
            dbserver = dbinstance.getServer()
            dbserver.removeShareplexChannel(port, db)

    def getShareplexPortListFromDepotDB(self):
        host_name = list(self._instanceDict.keys())[0]
        # if len(hostList) > 0:
        #     host_name = hostList[0]
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            depotDaoManager.startTransaction()
            splex_port_list = depotdbDao.getSplexuserPasswordByDBName(host_name, self._db_name)
            depotDaoManager.commit()
        except Exception as e:
            depotDaoManager.rollback()
            errormsg = "wbxdatabase.initFromServer(%s) with errormsg %s" % (host_name, e)
            logging.error(errormsg)
            raise wbxexception(errormsg)
        finally:
            depotDaoManager.close()
        return splex_port_list

    def getDBHome(self):
        server = self.getServer(None)
        self._ORACLE_HOME = server.getOracleHome(self._db_name)

    def getDBTabspacesSize(self):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDaoManager(self._db_name, "SYSTEM")
        dao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
        try:
            daoManager.startTransaction()
            res = dao.getTablespaceSize()
            return res
        except Exception as e:
            daoManager.rollback()
            raise e
        finally:
            daoManager.close()

    def getDBMemoSize(self):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDaoManager(self._db_name, "SYSTEM")
        dao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
        try:
            daoManager.startTransaction()
            res = dao.getDBMemoSize()
            return res
        except Exception as e:
            daoManager.rollback()
            raise e
        finally:
            daoManager.close()

    def executeCutover(self, processvo):
        server = None
        cmd = None
        retcode = "SUCCEED"
        server_type = processvo.parameter["server_type"].lower()
        module = processvo.parameter["module"]
        stage = processvo.parameter["stage"]
        db_name = processvo.db_name
        splex_port = processvo.splex_port
        host_name = processvo.host_name
        commond = processvo.parameter["command"]
        if module == "DB":
            if stage == "REGISTER":
                params = "%s %s %s %s" % (
                self._db_name, processvo.parameter["application_type"], processvo.parameter["appln_support_code"],
                processvo.parameter["db_type"])
            elif stage == "SETENVIROMENT":
                params = "%s" % processvo.parameter["opaction"]
            else:
                params = "%s %s %s" % (module, db_name, stage)
        elif module == "SHAREPLEX":
            if server_type == "src":
                param = "%s:%s:%s:%s:%s" % (
                processvo.parameter["src_db"], processvo.db_name, splex_port, processvo.parameter["old_host"],
                processvo.parameter["new_host"])
                params = '''%s "%s" %s ''' % (module, param, stage)
            else:
                if stage == "ADDCR":
                    params = "%s %s EXECUTE %s" % (db_name, splex_port, processvo.parameter["splex_sid"])
                else:
                    param = "%s:%s:%s:%s" % (
                    db_name, splex_port, processvo.parameter["old_host"], processvo.parameter["new_host"])
                    params = '''%s "%s" %s ''' % (module, param, stage)
        elif module == "SERVER":
            params = ""
        cmd = "sh %s %s" % (commond, params)
        res = "%s on server %s" % (cmd, host_name)
        logger.info(res)
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)
            wbxvo = wbxssh(server.host_name, server.ssh_port, server.login_user, server.login_pwd)
            wbxvo.connect()
            wbxvo.send(cmd)
            kargs = {}
            time.sleep(1)
            rows = ""
            while True:
                buff = wbxvo.recvs(**kargs)
                logger.info(buff.replace("‘", "'").replace("’", "'"))
                if buff:
                    rows += buff
                    if buff.strip().endswith(('$')):
                        if rows.find("WBXERROR") >= 0:
                            raise wbxexception("Error occurred with command %s" % (cmd))
                        break
        except Exception as e:
            retcode = "FAILED"
            logger.error(e)
        finally:
            wbxvo.close()
        return retcode

    def executeCutover_cisep(self, processvo):
        server = None
        cmd = None
        wbxvo = None
        retcode = "SUCCEED"
        params = ""
        server_type = processvo.parameter["server_type"].lower()
        module = processvo.parameter["module"]
        stage = processvo.parameter["stage"]
        db_name = processvo.db_name
        splex_port = processvo.splex_port
        host_name = processvo.host_name
        commond = processvo.parameter["command"]
        if module == "DB":
            if stage == "REGISTER":
                params = "%s %s %s %s" % (
                self._db_name, processvo.parameter["application_type"], processvo.parameter["appln_support_code"],
                processvo.parameter["db_type"])
            elif stage == "SETENVIROMENT":
                params = "%s" % processvo.parameter["opaction"]
            else:
                params = "%s %s:%s %s" % (
                module, processvo.parameter["old_db_name"], processvo.parameter["new_db_name"], stage)
        elif module == "SHAREPLEX":
            if server_type == "src":
                param = "%s:%s:%s:%s:%s:%s:%s" % (
                processvo.parameter["src_db"], processvo.parameter["old_db_name"], splex_port,
                processvo.parameter["old_host"], processvo.parameter["new_db_name"],
                processvo.parameter["new_splex_port"], processvo.parameter["new_host"])
                params = '''%s "%s" %s ''' % (module, param, stage)
            else:
                if stage == "ADDCR":
                    params = "%s %s EXECUTE %s" % (
                    processvo.parameter["new_db_name"], processvo.parameter["new_splex_port"],
                    processvo.parameter["splex_sid"])
                else:
                    param = "%s:%s:%s:%s:%s:%s" % (
                    processvo.parameter["old_db_name"], splex_port, processvo.parameter["old_host"],
                    processvo.parameter["new_db_name"], processvo.parameter["new_splex_port"],
                    processvo.parameter["new_host"])
                    params = '''%s "%s" %s ''' % (module, param, stage)
        elif module == "SERVER":
            params = ""
        cmd = "sh %s %s" % (commond, params)
        print(cmd)
        # cmd = "hostname"
        res = "%s on server %s" % (cmd, host_name)
        logger.info(res)
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)
            wbxvo = wbxssh(server.host_name, server.ssh_port, server.login_user, server.login_pwd)
            wbxvo.connect()
            wbxvo.send(cmd)
            kargs = {}
            time.sleep(1)
            rows = ""
            while True:
                buff = wbxvo.recvs(**kargs)
                logger.info(buff.replace("‘", "'").replace("’", "'"))
                if buff:
                    rows += buff
                    if buff.strip().endswith(('$')):
                        if rows.find("WBXERROR") >= 0:
                            raise wbxexception("Error occurred with command %s" % (cmd))
                        break
        except Exception as e:
            retcode = "FAILED"
            logger.error(e)
        finally:
            wbxvo.close()
        return retcode

    def listTableWithSchema(self, schema_name):
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = None
        try:
            daomanager = daomanagerfactory.getDaoManager(self._db_name)
            dao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            daomanager.startTransaction()
            tableList = dao.listTableForMigration(schema_name, self._db_vendor)
            daomanager.commit()
            return tableList
        except Exception as e:
            if daomanager is not None:
                daomanager.rollback()
            raise e
        finally:
            if daomanager is not None:
                daomanager.close()

    def listTableStructureWithSchema(self, schema_name,*tablelist):
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = None
        try:
            daomanager = daomanagerfactory.getDaoManager(self._db_name)
            dao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            daomanager.startTransaction()
            tableList = dao.listTableColumns(schema_name, self._db_vendor,*tablelist)
            daomanager.commit()
            return tableList
        except Exception as e:
            if daomanager is not None:
                daomanager.rollback()
            raise e
        finally:
            if daomanager is not None:
                daomanager.close()
