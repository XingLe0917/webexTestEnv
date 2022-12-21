from datetime import datetime

from sqlalchemy import Column,Integer,String, DateTime, func, text, select
from sqlalchemy.orm import column_property
from dao.vo.wbxvo import Base
from common.wbxutil import wbxutil


class wbxoraexception(object):
    ORA_INVALID_USERNAME_PASSWORD="ORA-01017"
    ORA_CONNECTION_TIMEOUT = "ORA-12170"


class wbxdatabasemanager(object):
    APPLN_SUPPORT_CODE_WEBDB= "WEB"
    APPLN_SUPPORT_CODE_TAHOEDB= "TEL"
    APPLN_SUPPORT_CODE_CONFIGDB= "CONFIG"
    APPLN_SUPPORT_CODE_BILLINGDB= "OPDB"
    APPLN_SUPPORT_CODE_TEODB = "TEO"
    APPLN_SUPPORT_CODE_GLOOKUPDB = "LOOKUP"
    APPLN_SUPPORT_CODE_DIAGNSDB = "DIAGNS"
    APPLN_SUPPORT_CODE_MMP = "MMP"
    APPLN_SUPPORT_CODE_MEDIATE = "MEDIATE"

    DC_SJC="SJC02"
    DC_AMS="AMS01"
    DC_NRT="NRT02"

    SCHEMATYPE_APP="app"
    SCHEMATYPE_SPLEX="splex"
    SCHEMATYPE_SPLEXDENY = "splex_deny"
    SCHEMATYPE_WBXMAINT = "wbxmaint"
    SCHEMATYPE_GLOOKUP = "glookup"
    SCHEMATYPE_BACKUP = "backup"
    SCHEMATYPE_STAPRO = "stap_ro"
    SCHEMATYPE_DBA="dba"

    DB_TYPE_PROD="PROD"
    DB_TYPE_BTS="BTS_PROD"

    APPLICATION_TYPE_PRI="PRI"
    APPLICATION_TYPE_GSB="GSB"

    FEDERAMP_DBS=["RACFWEB","RACAFWEB","TSJ35", "TTA35","RACFMMP","RACFTMMP","RACINTH","SJFTOOLS","FSTAPDB","TTA136","TSJ136","RACFCSP","RACAFCSP"]
    # UNUSED_TAHOE_POOL = ["TSJ48", "TTA48","TBT2","TTA35","TSJ35"]

    def __init__(self):
        self.dbtypedict={}
        self.dbdict={}

class wbxloginuser(Base):
    __tablename__ = "host_user_info"
    host_name = Column(String(30), primary_key=True)
    trim_host = Column(String(30))
    username = Column(String(30))
    pwd = Column(String(64))
    createtime = Column(DateTime, default=func.now())
    lastmodifieddate = Column(DateTime, default=func.now(), onupdate=func.now())


class wbxserver(Base):
    __tablename__ = "host_info"
    trim_host=Column(String(30))
    host_name=Column(String(30), primary_key=True)
    domain = Column(String(50))
    site_code=Column(String(5))
    host_ip=Column(String(30))
    vip_name=Column(String(30))
    vip_ip=Column(String(30))
    priv_name=Column(String(30))
    priv_ip=Column(String(30))
    scan_name=Column(String(30))
    scan_ip1 = Column(String(30))
    scan_ip2 = Column(String(30))
    scan_ip3 = Column(String(30))
    os_type_code = Column(String(30))
    processor = Column(String(15))
    kernel_release = Column(String(30))
    hardware_platform = Column(String(30))
    physical_cpu = Column(String(22))
    cores = Column(String(22))
    cpu_model = Column(String(50))
    flag_node_virtual = Column(String(1))
    install_date = Column(DateTime, default=func.now())
    date_added = Column(DateTime, default=func.now())
    lastmodifieddate = Column(DateTime, default=func.now(), onupdate=func.now())
    comments = Column(String(100))
    lc_code = Column(String(5))
    ssh_port = Column(String(15))
    created_by = Column(String(100))
    modified_by = Column(String(100))


    def __init__(self):
        self.srcchannellist = []
        self.tgtchannellist = []
        self.loginuser = None

    def addShareplexChannel(self, spchannel):
        if spchannel.src_host == self.host_name:
            self.srcchannellist.append(spchannel)
        if spchannel.tgt_host == self.host_name:
            self.tgtchannellist.append(spchannel)

    def getSrcChannelList(self):
        return self.srcchannellist

    def getTgtChannelList(self):
        return self.tgtchannellist

    def __str__(self):
        return "hostname=%s, scanip1=%s, scanip2=%s, scanip3=%s" % (self.host_name, self.scan_ip1, self.scan_ip2, self.scan_ip3)

class wbxdatabase(Base):
    __tablename__ = "database_info"
    trim_host = Column(String(30), primary_key=True)
    db_vendor = Column(String(20))
    db_version = Column(String(10))
    db_type = Column(String(10))
    application_type=Column(String(10))
    appln_support_code = Column(String(15))
    db_home = Column(String(100))
    db_name = Column(String(25), primary_key=True)
    failover_trim_host = Column(String(30))
    failover_db = Column(String(30))
    service_name = Column(String(50))
    listener_port = Column(String(22))
    backup_method = Column(String(10))
    backup_server = Column(String(30))
    catalog_trim_host = Column(String(30))
    catalog_db = Column(String(30))
    monitor = Column(String(1))
    appln_contact = Column(String(70))
    contents = Column(String(70))
    createddate = Column(DateTime, default=func.now())
    wbx_cluster = Column(String(25))
    date_added = Column(DateTime, default=func.now())
    lastmodifieddate = Column(DateTime, default=func.now(), onupdate=func.now())
    web_domain = Column(String(13))
    created_by = Column(String(100), default="DEPOT")
    modified_by = Column(String(100), default="DEPOT")


    def __init__(self):
        self.connectioninfo = None
        self.hosts = []
        self.instances = {}
        self.schemadict = {}
        self.shareplexchanneldict = {}
        self.domaindict = {}

    def getConnectionURL(self):
        if self.connectioninfo is not None:
            return self.connectioninfo
        else:
            host1 = self.hosts[0]
            if wbxutil.isNoneString(host1.scan_ip1):
                service_name= self.service_name
                if service_name.find(".") < 0:
                    service_name="%s.webex.com" % service_name

                url = '(DESCRIPTION ='
                for host in self.hosts:
                    url = "%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))" % (url, host.vip_ip, self.listener_port)

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
                       (host1.scan_ip1, self.listener_port, host1.scan_ip2, self.listener_port, host1.scan_ip3,
                        self.listener_port, self.service_name)

    def getConnectionInfoByVIP(self, hostname):
        return ""

    def addSchema(self, schema):
        schemname=schema.getSchema()
        self.schemadict[schemname] = schema

    def getSchema(self, schemaname):
        if schemaname in self.schemadict:
            return self.schemadict[schemaname]
        return None

    def getSchemaBySchemaType(self, appln_support_code, schemaType):
        schemaList = []
        for schema in self.schemadict.values():
            if schema.schematype == schemaType and schema.appln_support_code.lower() == appln_support_code.lower():
                schemaList.append(schema)
        return schemaList

    def getSchemaPassword(self, schemaname):
        if schemaname in self.schemadict:
            return self.schemadict[schemaname].getPassword()
        return None

    def getExpectedCharacterSet(self):
        if self.appln_support_code.find('WAPI') >= 0 or self.appln_support_code=="JBR":
            dbcharacterset = "AL32UTF8"
        else:
            dbcharacterset = "WE8ISO8859P1"
        return dbcharacterset

    def getdbid(self):
        return "%s_%s" % (self.trim_host, self.db_name)

    def getFailoverdb(self):
        return self.failoverdb

    def setFailoverdb(self,failoverdb):
        self.failoverdb = failoverdb

    def addInstance(self, instance):
        host_name = instance.host_name
        if host_name not in self.instances:
            self.instances[host_name] = instance

    def addServer(self, server):
        host_name = server.host_name
        if host_name in self.instances:
            self.hosts.append(server)

    def getServer(self, host_name):
        for host in self.hosts:
            if host.host_name == host_name:
                return host

    # shareplexchanneldict includes shareplex channel from current db or to current db
    def addShareplexChannel(self, spchannel):
        channelid = spchannel.channelid
        self.shareplexchanneldict[channelid] = spchannel

    def getShareplexChannel(self, srchost, srcdb, port, tgthost, tgtdb, direction):
        for channelid, spchannel in self.shareplexchanneldict.items():
            if spchannel.src_host == srchost and spchannel.src_db == srcdb and spchannel.tgt_host == tgthost and \
                spchannel.tgt_db == tgtdb and int(spchannel.port) == int(port) and spchannel.getDirection() == direction:
                    return spchannel
        return None

    def getShareplexChannelCountBySrcDB(self, src_host, src_db):
        channelcount = 0
        for channelid, spchannel in self.shareplexchanneldict.items():
            if spchannel.src_host == src_host and spchannel.src_db == src_db:
                channelcount += 1
        return channelcount

    def getShareplexChannelByChannelid(self, channelid):
        if channelid in self.shareplexchanneldict:
            return self.shareplexchanneldict[channelid]
        else:
            return None

    def getShareplexPortList(self, hostname):
       return [spchannel.port for spchannel in self.shareplexchanneldict.values() if hostname == spchannel.src_host or hostname == spchannel.tgt_host]

    def addDomain(self, poolname, domain):
        if poolname not in self.domaindict:
            self.domaindict[poolname] = domain

    def getDomainBySchemaname(self, schemname):
        if self.appln_support_code == wbxdatabasemanager.APPLN_SUPPORT_CODE_TAHOEDB:
            if schemname in self.domaindict:
                return self.domaindict[schemname]
        return None

    def getDomainDict(self):
        return self.domaindict


class wbxinstance(Base):
    __tablename__ = "instance_info"
    trim_host = Column(String(30))
    host_name = Column(String(30), primary_key=True)
    db_name = Column(String(25), primary_key=True)
    instance_name  = Column(String(35))
    date_added = Column(DateTime)
    lastmodifieddate = Column(DateTime)
    created_by = Column(String(100))
    modified_by = Column(String(100))

    def getDBID(self):
        return "%s_%s" % (self.trim_host, self.db_name)

class wbxshareplexchannel(Base):
    __tablename__ = "shareplex_info"
    src_host = Column(String(50))
    src_db = Column(String(30), primary_key=True)
    port = Column(String(22), primary_key=True)
    qname = Column(String(64), primary_key=True)
    tgt_host = Column(String(50))
    tgt_db = Column(String(30), primary_key=True)
    replication_to = Column(String(30))
    src_splex_sid = Column(String(30))
    tgt_splex_sid = Column(String(50))
    src_schema = Column(String(22))
    tgt_schema = Column(String(30))
    created_by = Column(String(100), default="DEPOT")
    modified_by = Column(String(100), default="DEPORT")
    date_added = Column(DateTime, default=func.now())
    lastmodifieddate = Column(DateTime, default=func.now(), onupdate=func.now())


    def getSourceDBID(self):
        return "%s_%s" % (self.src_host[0:-1], self.src_db)

    def getTargetDBID(self):
        return "%s_%s" % (self.tgt_host[0:-1], self.tgt_db)

    def getMonitorTableName(self):
        if wbxutil.isNoneString(self.qname):
            table_name = "SPLEX_MONITOR_ADB"
        else:
            table_name = "SPLEX_MONITOR_ADB_%s" % self.qname.upper()
        return table_name

    def getSchemaname(self):
        return "SPLEX%s" % self.port

    def getDirection(self):
        if self.isKafkaChannel():
            repto = self.replication_to.split("_")[:2]
            direction = "%s_%s" % ("_".join(repto), self.qname) if not wbxutil.isNoneString(self.qname) else "_".join(repto)
        else:
            direction = "%s_%s" % (self.replication_to, self.qname) if not wbxutil.isNoneString(self.qname) else self.replication_to
        return direction

    def isKafkaChannel(self):
        if self.tgt_splex_sid == "KAFKA":
            return True
        else:
            return False

    def __str__(self):
        return "str: from %s.%s to %s.%s under port %s with monitor_table=%s" % (self.src_host, self.src_db,  self.tgt_host, self.tgt_db, self.port, self.getMonitorTableName())

    def __repr__(self):
        return "repr: %s.%s on port %s to %s.%s" % (self.src_host, self.src_db, self.port, self.tgt_host, self.tgt_db)

class wbxschema(Base):
    __tablename__ = "appln_pool_info"
    trim_host = Column(String(30), primary_key=True)
    db_name = Column(String(25), primary_key=True)
    appln_support_code = Column(String(25))
    schema = Column(String(35), primary_key=True)
    password = Column(String(512))
    date_added = Column(DateTime, default=func.now())
    lastmodifieddate = Column(DateTime, default=func.now(), onupdate=func.now())
    created_by = Column(String(100), default="DEPOT")
    modified_by = Column(String(100), default="DEPOT")
    km_version = Column(String(10), default=-1)
    schematype = Column(String(16))
    new_password = Column(String(512))
    track_id = Column(String(512))
    change_status = Column(Integer, default=0)

    def getSchema(self):
        return self.schema

    def getPassword(self):
        return self.password

    def getdbid(self):
        return "%s_%s" % (self.trim_host, self.db_name)

    def __str__(self):
        return "trim_host=%s, db_name=%s, schemaname=%s, password=%s, schematype=%s" % (self.trim_host, self.db_name, self.schema, self.password, self.schematype)

class wbxmappinginfo(Base):
    __tablename__ = "appln_mapping_info"
    trim_host = Column(String(30), primary_key=True)
    db_name = Column(String(25), primary_key=True)
    appln_support_code = Column(String(25))
    mapping_name = Column(String(30), primary_key=True)
    schema = Column(String(30))

    def getdbid(self):
        return "%s_%s" % (self.trim_host, self.db_name)

    def getdb(self):
        return  self.db_name


class wbxuser(Base):
    __tablename__ = "wbxuser"
    siteid = Column(Integer, primary_key=True)
    userid = Column(Integer, primary_key=True)
    username = Column(String(100))


class wbxadbmon(Base):
    __tablename__ = "wbxadbmon"
    src_host = Column(String(50), primary_key=True)
    src_db=Column(String(30), primary_key=True)
    port= Column(String(10), primary_key=True)
    replication_to=Column(String(25), primary_key=True)
    tgt_host = Column(String(50), primary_key=True)
    tgt_db = Column(String(30), primary_key=True)
    lastreptime=Column(DateTime)
    montime = Column(DateTime)

    def getSourceDBID(self):
        return "%s_%s" % (self.src_host[0:-1], self.src_db)

    def getTargetDBID(self):
        return "%s_%s" % (self.tgt_host[0:-1], self.tgt_db)

    def isKafkaChannel(self):
        return self.tgt_db.find('kafka') >= 0

    def __str__(self):
        return "wbxadbmon: src_host=%s, src_db=%s, port=%s, replication_to=%s, tgt_host=%s,tgt_db=%s, lastreptime=%s" % (
        self.src_host, self.src_db, self.port, self.replication_to, self.tgt_host, self.tgt_db, wbxutil.convertDatetimeToString(self.lastreptime))

class wbxdomain:
    def __init__(self, domainid, domainname, dbid, meetingkeysegment, isprimary):
        self.domainid = domainid
        self.domainname = domainname
        self.dbid = dbid
        self.meetingkeysegment = meetingkeysegment
        self.isprimary = isprimary

class wbxcluster:
    def __init__(self, appln_support_code, db_type, primarydomain, primarydbid, gsbdomain, gsbdbid):
        self.appln_support_code = appln_support_code
        self.db_type = db_type
        self.primarydomain = primarydomain
        self.primarydbid = primarydbid
        self.gsbdomain = gsbdomain
        self.gsbdbid = gsbdbid
        self.appstatus = "PRI" #app is on pri or on gsb

class wbxrac:
    def __init__(self):
        self.dbcache = {}
        self.hostcache = {}

    def addServer(self, server):
        host_name = server.host_name
        if host_name not in self.hostcache:
            self.hostcache[host_name] = server

    def getAllHosts(self):
        return self.hostcache

    def addDatabase(self, db):
        dbid = db.getdbid()
        if dbid not in self.dbcache:
            self.dbcache[dbid] = db

    def getAllDatabases(self):
        return self.dbcache


class DBPatchReleaseVO(Base):
    __tablename__ = "wbxdbpatchrelease"
    releasenumber = Column(Integer, primary_key=True)
    appln_support_code = Column(String(15), primary_key=True)
    schematype = Column(String(30), primary_key=True)
    major_number = Column(Integer)
    minor_number = Column(Integer)
    description = Column(String(512))
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())
    def __str__(self):
        return "releasenumber=%s, appln_support_code=%s, schematype=%s" % (self.releasenumber, self.appln_support_code, self.schematype)

class DBPatchDeploymentVO(Base):
    __tablename__ = "wbxdbpatchdeployment"
    deploymentid = Column(String(64), primary_key=True, default=func.sys_guid(), server_default=text("SYS_GUID()"))
    releasenumber = Column(Integer)
    appln_support_code = Column(String(15))
    db_type  = Column(String(15))
    schematype = Column(String(30))
    trim_host = Column(String(30))
    db_name = Column(String(30))
    schemaname = Column(String(30))
    cluster_name = Column(String(30))
    major_number = Column(Integer)
    minor_number = Column(Integer)
    deploytime =  Column(DateTime)
    deploystatus = Column(String(16))
    spdeploytime = Column(DateTime)
    spdeploystatus = Column(String(16))
    change_id = Column(String(30))
    change_sch_start_date = Column(String(30))
    change_completed_date = Column(String(30))
    change_imp = Column(String(50))
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())

    def __str__(self):
        return "release_number=%s, db_name=%s, appln_support_code=%s, schematype=%s, deploystatus=%s" % (self.releasenumber, self.db_name, self.appln_support_code, self.schematype, self.deploystatus)

class ShareplexBaselineVO(Base):
    __tablename__ = "wbxshareplexbaseline"
    releasenumber = Column(Integer, primary_key=True)
    src_appln_support_code = Column(String(25), primary_key=True)
    src_schematype = Column(String(16), primary_key=True)
    src_tablename  = Column(String(30), primary_key=True)
    tgt_appln_support_code = Column(String(25), primary_key=True)
    tgt_schematype = Column(String(16), primary_key=True)
    tgt_tablename = Column(String(30), primary_key=True)
    tgt_application_type = Column(String(15))
    tablestatus = Column(String(16))
    specifiedkey = Column(String(200))
    columnfilter = Column(String(4000))
    specifiedcolumn = Column(String(4000))
    changerelease = Column(Integer)


    def __str__(self):
        return "release_number=%s, src_appln_support_code=%s, src_tablename=%s, tgt_appln_support_code=%s" % (self.releasenumber, self.src_appln_support_code, self.src_tablename, self.tgt_appln_support_code)

class ShareplexCRDeploymentVO(Base):
    __tablename__ = "wbxshareplexcrdeployment"
    trim_host = Column(String(30), primary_key=True)
    db_name = Column(String(30), primary_key=True)
    port = Column(String(22), primary_key=True)
    release_number = Column(Integer)
    major_number = Column(Integer)
    monitor_time = Column(DateTime)
    recentcrdate = Column(DateTime)

class MeetingDataMonitorVO(Base):
    __tablename__ = "wbxmeetingdatamonitor"
    trim_host = Column(String(30), primary_key=True)
    db_name = Column(String(30), primary_key=True)
    cluster_name = Column(String(30))
    case1 = Column(Integer)
    case2 = Column(Integer)
    case3 = Column(Integer)
    case4 = Column(Integer)
    case5 = Column(Integer)
    monitor_time = Column(DateTime)

class DBLinkBaselineVO(Base):
    __tablename__ = "wbxdblinkbaseline"
    dblinkid = Column(String(64), primary_key=True)
    db_type  = Column(String(64))
    appln_support_code = Column(String(64))
    application_type = Column(String(64))
    schematype = Column(String(64))
    tgt_db_type  = Column(String(64))
    tgt_appln_support_code = Column(String(64))
    tgt_application_type  = Column(String(64))
    tgt_schematype = Column(String(64))
    dblink_name = Column(String(64))
    status =  Column(Integer)
    dblinktarget = Column(String(16))
    description = Column(String(64))

    def __str__(self):
        return "db_type=%s, appln_support_code=%s, schematype=%s, tgt_db_type=%s, tgt_appln_support_code=%s, tgt_schematype=%s" % (self.db_type, self.appln_support_code, self.schematype, self.tgt_db_type, self.tgt_appln_support_code, self.tgt_schematype)


class DBLinkMonitorResultVO(Base):
    __tablename__ = "wbxdblinkmonitordetail"
    trim_host = Column(String(30), primary_key=True)
    db_name  = Column(String(25), primary_key=True)
    schema_name = Column(String(35), primary_key=True)
    dblink_name = Column(String(30), primary_key=True)
    status = Column(String(16))
    errormsg  = Column(String(4000))
    monitor_time = Column(DateTime)

class WebDomainDataMonitorVO(Base):
    __tablename__ = "wbxwebdomaindatammonitor"
    clustername = Column(String(16), primary_key=True)
    itemname  = Column(String(64), primary_key=True)
    itemvalue = Column(String(4000), primary_key=True)
    monitortime = Column(DateTime)

class WbxCronjobLogVO(Base):
    __tablename__ = "wbxcronjoblog"
    logid = Column(String(64), primary_key=True, server_default=text("SYS_GUID()"))
    hostname = Column(String(100))
    name = Column(String(300))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    result = Column(String(500))
    status  = Column(String(20))
    custom_id = Column(String(20))

'''
create table wbxdbwaiteventmonitor(
    db_name            VARCHAR2(30) NOT NULL,
    instance_number    NUMBER(1)    NOT NULL,
    event              VARCHAR2(64) NOT NULL,
    osuser             VARCHAR2(30) NOT NULL,
    machine            VARCHAR2(64) NOT NULL,
    sid                NUMBER(10)   NOT NULL,
    program            VARCHAR2(48) NOT NULL,
    username           VARCHAR2(30) NOT NULL,
    sql_id             VARCHAR2(13) NOT NULL,
    SQL_EXEC_START     DATE         NOT NULL,
    monitor_time       DATE         NOT NULL,
    constraint pk_wbxdbwaiteventmonitor primary key(db_name, sid,monitor_time) using index tablespace wbxobj_small_idx
) TABLESPACE wbxobj_small
'''
class wbxdbwaiteventvo(Base):
    __tablename__ = "wbxdbwaiteventmonitor"
    db_name = Column(String, primary_key=True)
    sid  = Column(Integer, primary_key=True)
    sql_exec_start = Column(DateTime, primary_key=True)
    monitor_time = Column(DateTime)
    instance_number = Column(Integer)
    event = Column(String)
    osuser = Column(String)
    machine = Column(String)
    program = Column(String)
    username = Column(String)
    sql_id = Column(String)
    duration = Column(Integer)


