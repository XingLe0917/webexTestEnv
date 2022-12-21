from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool
from common.wbxexception import wbxexception
from dao.vo.depotdbvo import wbxdatabasemanager, wbxrac
from common.Config import Config
import threading
import json
import sys
import os
import logging
import traceback

threadlocal = threading.local()
threadlocal.current_session = {}

logger = logging.getLogger("DBAMONITOR")
# logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
# logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)
# logger.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

class wbxdaomanagerfactory(object):

    daomanagerfactory = None
    dbcache = {}
    appcodedbcache = {}
    daoManagerCache = {}
    servercache = {}
    spchannelcache = {}
    kafkachannelcach= {}
    webdomaincache = {}
    tahoepoolcache = {}
    clustercache = []
    raccache = {}
    DEFAULT_DBID="DEFAULT"
    PG_DEFAULT_DBID="PGDEFAULT"
    DEFAULT_LOGINSCHEMA="depot"

    _lock = threading.Lock()

    def __init__(self):
        raise wbxexception("Can not instance a wbxdaomanagerfactory, please call getDaoManagerFactory()")

    # 2 cases; case1: add db when loop dblist; case 2: add db when loop schemalist
    def addDatabase(self, db, supportcode = None):
        db_name = db.getDBName()
        if db_name in self.dbcache:
            del self.dbcache[db_name]
        self.dbcache[db_name] = db
        # For case 1, no supportcode
        if supportcode is None:
            supportcode=db.getApplnSupportCode()

        # Organize the dblist by database_info.appln_support_code
        supportcode = supportcode.upper()
        if supportcode not in self.appcodedbcache:
            self.appcodedbcache[supportcode] = []
        dbList = self.appcodedbcache[supportcode]
        isexist = False
        for db1 in dbList:
            if db1.getDBName() == db.getDBName():
                isexist = True
        if not isexist:
            dbList.append(db)


    def addWebDomain(self, domainname, webdomain):
        if domainname not in self.webdomaincache:
            self.webdomaincache[domainname] = webdomain

    def getWebDomain(self, domainname):
        if domainname is not None and domainname in self.webdomaincache:
            return self.webdomaincache[domainname]
        return None

    def addTahoePool(self, poolname, pool):
        if poolname  not in self.tahoepoolcache:
            self.tahoepoolcache[poolname] = pool

    def getTahoePool(self, poolname):
        if poolname is not None and poolname in self.tahoepoolcache:
            return self.tahoepoolcache[poolname]
        return None

    def addCluster(self, cluster):
        self.clustercache.append(cluster)

    def getAllCluster(self):
        return self.clustercache

    def getAllDatabase(self):
        return self.dbcache

    def getDatabaseByDBID(self, dbid):
        if dbid in self.dbcache:
            return self.dbcache[dbid]
        raise wbxexception("The dbid=%s does not exists" % dbid)

    def getDatabaseByDBName(self, db_name):
        if db_name in self.dbcache:
            return self.dbcache[db_name]
        return None

    def isDBExist(self, dbid):
        return dbid in self.dbcache

    def getDBListByAppCode(self, appcode):
        if appcode in self.appcodedbcache:
            return self.appcodedbcache[appcode]
        else:
            return None

    def addShareplexChannel(self, spchannel):
        channelid = spchannel.channelid
        if spchannel.isKafkaChannel():
            self.kafkachannelcach[channelid] = spchannel

        self.spchannelcache[channelid] = spchannel

    def getShareplexChannels(self):
        return self.spchannelcache

    def getKafkaShareplexChannel(self):
        return self.kafkachannelcach

    def clearCache(self):
        self.spchannelcache.clear()
        self.appcodedbcache.clear()
        self.servercache.clear()
        for name, db in self.dbcache.items():
            if name != "AUDITDB":
                del self.dbcache[name]
        self.raccache.clear()


    def addServer(self, server):
        host_name = server.getHostname()
        if host_name in self.servercache:
            del self.servercache[host_name]

        self.servercache[host_name] = server


        # We can get RAC info by go through all servers, but no need to have a RAC vo based on current requirement
        # scan_name = server.getScanname()
        # if scan_name is None:
        #     return
        # else:
        #     if scan_name not in self.raccache:
        #         rac = wbxrac()
        #         rac.addServer(server)
        #         self.raccache[scan_name] = rac
        #     else:
        #         rac = self.raccache[scan_name]
        #         rac.addServer(server)

    def getServer(self, hostname):
        if hostname in self.servercache:
            return self.servercache[hostname]
        return None

    def getServerListByTrimHost(self, trimhost):
        if trimhost in self.servercache:
            return self.servercache[trimhost]["servers"]
        return None

    def getRacByScanName(self, scan_name):
        if scan_name in self.raccache:
            return self.raccache[scan_name]
        return None

    def removeRacByScanName(self, scan_name):
        if scan_name in self.raccache:
            self.raccache.pop(scan_name)

    def getAllRAC(self):
        return self.raccache

    @staticmethod
    def getDaoManagerFactory():
        if wbxdaomanagerfactory.daomanagerfactory is None:
            wbxdaomanagerfactory.daomanagerfactory = object.__new__(wbxdaomanagerfactory)
        return wbxdaomanagerfactory.daomanagerfactory

    def getDaoManagerForJobManager(self):
        dbid = "sjdborbt_STAPDB"
        schemaname = "seuser"
        daoManager = self.getDaoManager(dbid, schemaname)
        return daoManager

    def getOPDBDaoManager(self):
        dbid = "RACOPDB"
        schemaname = "test"
        daoManager = self.getDaoManager(dbid, schemaname)
        return daoManager

    def getDefaultDaoManager(self):
        config = Config.getConfig()
        daoManager = self.getDaoManager(config.getDepotdbname(), self.DEFAULT_LOGINSCHEMA, poolsize=30)
        return daoManager

    def getPGDefaultDaoManager(self):
        config = Config.getConfig()
        daoManager = self.getDaoManager(config.getPGDepotdbname(), config.getPGDepotLoginUser(), poolsize=30)
        return daoManager

    # This method is used to get BTS/Prod ConfigDB DaoManager
    def getDaomanagerForConfigDB(self, db_type):
        dblist = self.getDBListByAppCode(wbxdatabasemanager.APPLN_SUPPORT_CODE_CONFIGDB)
        for db in dblist:
            if db.db_type == db_type:
                schemalist = db.getSchemaBySchemaType(wbxdatabasemanager.SCHEMATYPE_APP)
                if schemalist is not None:
                    schema = schemalist[0]
                    schemaname = schema.schema
                    dbkey = "%s_%s" % (db.getdbid(), schemaname)
                    if dbkey in self.daoManagerCache:
                        daomanager = self.daoManagerCache[dbkey]
                        daomanager.setLocalSession()
                        return daomanager
                    else:
                        daoManager = self.getDaoManager(db.getdbid(), schemaname)
                    return daoManager
        return None

    # There is potential risk at here, because there is dbmanagercache which initialize enginer at first time, so the poolengine parameter is fixed at first initialization;
    def getDaoManager(self, dbid, schemaname = None, expire_on_commit= False, poolengine=True, poolsize=2):
        if dbid in self.dbcache:
            wbxdatabase = self.dbcache[dbid]
            connectionurl = wbxdatabase.getConnectionURL()
            if schemaname is None:
                schemaList = wbxdatabase.getSchemaBySchemaType(wbxdatabase.getApplnSupportCode(), wbxdatabasemanager.SCHEMATYPE_DBA)
                schema = schemaList[0]
                schemaname = schema.getUserName()

            dbkey = "%s_%s" % (dbid, schemaname)

            # Each daomanager is shared, but the connection is not shared, each Daomanager has a separate connection
            if dbkey in self.daoManagerCache:
                daomanager = self.daoManagerCache[dbkey]
                daomanager.setLocalSession()
                return daomanager

            schemapwd = wbxdatabase.getSchemaPassword(schemaname)
            if schemapwd is None:
                raise wbxexception("Not find schema %s or not find password for this user in %s" % (schemaname, dbid))
            with self._lock:
                # logger.info("%s:%s@%s" % (schemaname, schemapwd, connectionurl))
                db_vendor = wbxdatabase.getDBVendor()
                if poolengine:
                    if db_vendor == "Oracle":
                        engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (schemaname, schemapwd, connectionurl),
                                           pool_recycle=600, pool_size = poolsize, max_overflow = 10,pool_timeout=10,
                                           echo_pool=False, echo=False)
                    elif db_vendor == "POSTGRESQL":
                        cfg = Config()
                        sslcert_file = cfg.getSSLCert()
                        sslkey_file = cfg.getSSLKey()
                        sslrootcert_file = cfg.getSSLRootCert()

                        # return "%s:%s@%s:%s/auditdb" % (
                        # self.configDict[self.DEPOT_USERNAME], self.configDict[self.DEPORT_PASSWORD],
                        # self.configDict[self.DEPOT_IP], self.configDict[self.DEPOTDB_PORT])

                        engine = create_engine('postgresql+psycopg2://%s:%s@%s' % (schemaname,schemapwd,connectionurl),
                                               connect_args={"application_name": "pg_pccp",
                                                             "connect_timeout": 10,
                                                             "sslmode": "verify-ca",
                                                             "sslkey": sslkey_file,
                                                             "sslrootcert": sslrootcert_file,
                                                             "sslcert": sslcert_file},
                                               pool_recycle=600, pool_size=10, max_overflow=10, pool_timeout=10,
                                               echo_pool=False,
                                               echo=False)

                else:
                    if db_vendor == "Oracle":
                        engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (schemaname, schemapwd, connectionurl), poolclass=NullPool, echo=True)
                    elif db_vendor == "POSTGRESQL":
                        cfg = Config()
                        sslcert_file = cfg.getSSLCert()
                        sslkey_file = cfg.getSSLKey()
                        sslrootcert_file = cfg.getSSLRootCert()
                        ####TO-DO

                daomanager = wbxdaomanager(engine, expire_on_commit)

                self.daoManagerCache[dbkey] = daomanager
                return daomanager
        else:
            raise wbxexception("Not find the database with dbid=%s" % dbid)


'''
each session is a connection, and the engine have a pool attributes. so wbxdaomanager have a engine property, and each time it will return a new session for each DaoManager
'''
class wbxdaomanager():
    def __init__(self, engine, expire_on_commit=False):
        self.daoMap={}
        self.loadDaoMap()
        self.engine = engine
        self.expire_on_commit = expire_on_commit
        self.setLocalSession()

    def loadDaoMap(self):
        currentfile = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
        daoconfigfile = os.path.join(os.path.join(currentfile, "conf"),"dao_mapper.json")

        # daoconfigfile = "conf/dao_mapper.json"
        f = open(daoconfigfile)
        jsonstr = f.read()
        f.close()
        daoDict = json.loads(jsonstr)
        for daoName, daoClz in daoDict.items():
            idxsize = daoClz.rfind(".")
            modulepath=daoClz[0:idxsize]
            clzname=daoClz[idxsize + 1:]
            amodule = sys.modules[modulepath]
            daocls = getattr(amodule, clzname)
            wbxdao = daocls()
            self.daoMap[daoName] = wbxdao

        # logger.info(self.daoMap.keys())

    def getLocalSession(self):
        session = threadlocal.current_session[self.engine]
        return session

    # Session is not thread-safe. so each thread we create a new Session
    # http://docs.sqlalchemy.org/en/latest/orm/session_basics.html
    # Session is not appropriate to cache object; All cached data in Session should be removed after commit;
    # But because we do not introduce Cache Module in this project, so Session is also used as a cache, it is workaround method
    def setLocalSession(self):
        if not hasattr(threadlocal, "current_session"):
            threadlocal.current_session={}
        if self.engine not in threadlocal.current_session:
            sessionclz = sessionmaker(bind=self.engine, expire_on_commit=self.expire_on_commit)
            session = sessionclz()
            threadlocal.current_session[self.engine] = session

    def startTransaction(self):
        session = self.getLocalSession()
        return session

    def commit(self):
        try:
            session = self.getLocalSession()
            if session is not None:
                session.commit()
        except Exception as e:
            logger.error("Error occurred at session.commit with %s" % e)
            logger.error(traceback.format_exc())


    def rollback(self):
        try:
            session = self.getLocalSession()
            if session is not None:
                session.rollback()
        except Exception as e:
            pass

    def getDao(self, daoKey):
        wbxdao = self.daoMap[daoKey]
        wbxdao.setDaoManager(self)
        return wbxdao

    def flush(self):
        session = self.getLocalSession()
        session.dirty()

    def close(self):
        session = self.getLocalSession()
        session.close()

class wbxdao(object):
    wbxdaomanager = None

    def setDaoManager(self, daoManager):
        self.wbxdaomanager = daoManager

    def getLocalSession(self):
        return self.wbxdaomanager.getLocalSession()

class DaoKeys():
    DAO_DEPOTDBDAO = "DAO_DEPOTDBDAO"
    DAO_PGDEPOTDBDAO = "DAO_PGDEPOTDBDAO"
    DAO_AUTOTASKDAO = "DAO_AUTOTASKDAO"
    DAO_JOBMANAGERDAO = "DAO_JOBMANAGERDAO"
    DAO_DBAUDITDAO = "DAO_DBAUDITDAO"
    DAO_SHAREPLEXMONITORDAO = "DAO_SHAREPLEXMONITORDAO"
    DAO_CONFIGDBDAO = "DAO_CONFIGDBDAO"
    DAO_DBCUTOVERDAO = "DAO_DBCUTOVERDAO"
    DAO_CRONJOBMANAGEMENTDAO = "DAO_CRONJOBMANAGEMENTDAO"
    DAO_ORA2PGDAO = "DAO_ORA2PGDAO"







    


