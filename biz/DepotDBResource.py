import re
import logging
from common.Config import Config
from dao.wbxdaomanager import wbxdaomanagerfactory, wbxdaomanager, wbxdao, DaoKeys
from dao.vo.depotdbvo import wbxdatabasemanager, wbxschema, wbxdatabase, wbxshareplexchannel, wbxserver, wbxdomain, wbxcluster, WbxCronjobLogVO
from biz.dbmanagement.wbxdb import wbxdb
from biz.dbmanagement.wbxdbserver import wbxdbserver
from biz.dbmanagement.wbxdbinstance import wbxdbinstance
from biz.dbmanagement.wbxdbuser import wbxdbuser
from biz.dbmanagement.wbxdbshareplexport import wbxdbshareplexport

logger = logging.getLogger("DBAMONITOR")

DEFAULT_DBID="DEFAULT"
LOGINSCHEMA="depot"

def loadDepotDBInfo():
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    daoManager.startTransaction()
    depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    dbList = depotdbDao.getDatabaseInfo()
    instanceList = depotdbDao.getInstanceInfo()
    spchannelList = depotdbDao.getShareplexChannel()
    serverList = depotdbDao.getServerList()
    loginuserList = depotdbDao.getLoginUserList()
    schemaList = depotdbDao.getSchemaList()
    mappingInfoList = depotdbDao.getMappingInfoList()
    haswarning = False

    logger.info("server size =%d" % len(serverList))
    daoManagerFactory.clearCache()

    for server in serverList:
        dbserver = wbxdbserver(server.host_name)
        dbserver.addServerInfo(server)
        daoManagerFactory.addServer(dbserver)

    for uservo in loginuserList:
        dbserver = daoManagerFactory.getServer(uservo.host_name)
        if dbserver is not None:
            dbserver.setLoginpwd(uservo.pwd)

    logger.info("db size=%d" % len(dbList))
    dbdict = {}
    for dbvo in dbList:
        db_name = dbvo.db_name
        if db_name in wbxdatabasemanager.FEDERAMP_DBS:
            continue
        if db_name == "PATCHDB" or dbvo.db_type == "PRE_PROD":
            continue

        db = wbxdb(db_name)
        db.setTrimHost(dbvo.trim_host)
        db.setApplnSupportCode(dbvo.appln_support_code)
        db.setApplicationType(dbvo.application_type)
        db.setServicename(dbvo.service_name)
        db.setListenerPort(dbvo.listener_port)
        db.setWebDomain(dbvo.wbx_cluster)
        db.setWebDomain(dbvo.web_domain)
        dbdict[db_name] = db

        dbuser = wbxdbuser(db_name=dbvo.db_name, username="SYSTEM")
        dbuser.setApplnSupportCode(dbvo.appln_support_code)
        dbuser.setPassword("sysnotallow")
        dbuser.setSchemaType("dba")
        db.addUser(dbuser)

        daoManagerFactory.addDatabase(db, db.getApplnSupportCode())
        #  Will implement webdomain feature in future
        # if db.appln_support_code.upper() == "WEB":
        #     isprimary = True if db.application_type == wbxdatabasemanager.APPLICATION_TYPE_PRI else False
        #     webdomain = wbxdomain(domainid=0, domainname=db.web_domain, dbid=dbid, meetingkeysegment=0, isprimary=isprimary)
        #     daoManagerFactory.addWebDomain(webdomain.domainname, webdomain)
        #     db.addDomain(webdomain.domainname, webdomain)

    # Because we get prod or bts instance, so it does not have full db name list
    logger.info("instance size =%d" % len(instanceList))
    for instance in instanceList:
        i_db_name = instance.db_name
        idb = daoManagerFactory.getDatabaseByDBName(i_db_name)
        if i_db_name in wbxdatabasemanager.FEDERAMP_DBS:
            continue
        if idb is None:
            # logger.info("The instance_info.db_name %s does not exist in database_info table" % i_db_name)
            continue
        if idb.getTrimHost() != instance.trim_host:
            # logger.error("The instance_info.db_name %s exist in database_info table but with different trim_host, Remove it ASAP" % i_db_name)
            continue
        i_hostname = instance.host_name
        iserver = daoManagerFactory.getServer(i_hostname)
        if iserver is None:
            # logger.info("The instance_info.host_name %s does not exist in host_info table" % i_db_name)
            continue

        dbinstance = wbxdbinstance(idb, instance.instance_name,iserver)
        idb.addInstance(dbinstance)
        iserver.addDatabase(idb)

    logger.info("schemaList size=%d" % len(schemaList))
    splex_user_format=re.compile("splex\d{4}")
    for schema in schemaList:
        s_db_name = schema.db_name
        sdb = daoManagerFactory.getDatabaseByDBName(s_db_name)
        if len(schema.password) == 32:
            # logger.error("Schema Error: double check why the password is not decrypted. %s" % schema)
            haswarning = True
        if sdb is None:
            # logger.info("The appln_pool_info.db name %s does not exist in database_info table" % s_db_name)
            continue
        if sdb.getTrimHost() != schema.trim_host:
            # logger.error("The appln_pool_info.db_name %s exist in database_info table but with different trim_host, Remove it ASAP" % s_db_name)
            continue
        dbuser = wbxdbuser(s_db_name, schema.schema)
        dbuser.setApplnSupportCode(schema.appln_support_code)
        dbuser.setSchemaType(schema.schematype)
        dbuser.setPassword(schema.password)
        sdb.addUser(dbuser)

        # one database store different schema data. For example, MMP db store CSP schema data
        # dbdict is very important for future usage
        if schema.appln_support_code.upper() != sdb.getApplnSupportCode():
            daoManagerFactory.addDatabase(sdb, schema.appln_support_code)
        if schema.schema == "splex_deny" and schema.schematype != "splex_deny":
            logger.error("%s is splex_deny user, but schematype is not splex_deny" % schema)
            haswarning = True
        elif splex_user_format.match(schema.schema) is not None and schema.schematype != "splex":
            logger.error("%s is splex user, but schematype is not SPLEX" % schema)
            haswarning = True

    logger.info("mappingInfoList size=%d" % len(mappingInfoList))
    for mappingInfo in mappingInfoList:
        m_db_name = mappingInfo.db_name
        mdb = daoManagerFactory.getDatabaseByDBName(m_db_name)
        if mdb is None:
            # logger.info("The appln_mapping_info.db name %s does not exist in database_info table" % m_db_name)
            continue
        if mdb.getTrimHost() != mappingInfo.trim_host:
            # logger.error("The appln_mapping_info.db_name %s exist in database_info table but with different trim_host, Remove it ASAP" % m_db_name)
            continue
        schemaname = mappingInfo.schema
        #
        # if mdbid in dbdict:
        #     db = dbdict[mdbid]
        #     # db.setMappingName(mappingInfo.mapping_name, mappingInfo.schema)
        #     isprimary = True if db.application_type == wbxdatabasemanager.APPLICATION_TYPE_PRI else False
        #     if db.appln_support_code.upper() == "TEL":
        #         tahoepool = wbxdomain(domainid=0, domainname=mappingInfo.mapping_name, dbid=mdbid, meetingkeysegment=0, isprimary=isprimary)
        #         daoManagerFactory.addTahoePool(tahoepool.domainname, tahoepool)
        #         db.addDomain(schemaname, tahoepool)

    logger.info("spchannelList size=%d" % len(spchannelList))
    for spchannel in spchannelList:
        srcdbname = spchannel.src_db
        srchost = spchannel.src_host
        tgtdbname = spchannel.tgt_db
        tgthost = spchannel.tgt_host
        src_splex_sid = spchannel.src_splex_sid
        tgt_splex_sid = spchannel.tgt_splex_sid
        srcserver = daoManagerFactory.getServer(srchost)
        tgtserver = daoManagerFactory.getServer(tgthost)
        if srcserver is None:
            # logger.info("Does not get server by shareplex_info.src_host=%s" % srchost)
            continue
        if tgtserver is None:
            # logger.info("Does not get server by shareplex_info.tgt_host=%s" % tgthost)
            continue
        srcdb = daoManagerFactory.getDatabaseByDBName(srcdbname)
        tgtdb = daoManagerFactory.getDatabaseByDBName(tgtdbname)
        if srcdb is None:
            # logger.info("Does not get server by shareplex_info.src_db=%s" % srcdbname)
            continue
        if tgtdb is None:
            # logger.info("Does not get server by shareplex_info.tgt_host=%s" % tgtdbname)
            continue
        srcport = srcserver.getShareplexPort(spchannel.port)
        if srcport is None:
            srcport = wbxdbshareplexport(spchannel.port, srcserver)
            srcserver.addShareplexPort(srcport)
        srcport.addDatabase(srcdb, src_splex_sid)

        tgtport = tgtserver.getShareplexPort(spchannel.port)
        if tgtport is None:
            tgtport = wbxdbshareplexport(spchannel.port, tgtserver)
            tgtserver.addShareplexPort(tgtport)
        tgtport.addDatabase(tgtdb, tgt_splex_sid)
        srcport.addTgtDB(tgtdb)
        tgtport.addSrceDB(srcdb)

    daoManager.commit()
    return haswarning

'''Load pg metadata, including server, db, dbuser, osuser
   all these data are cached in memory. and need to refresh or updated
'''
def loadPGDepotDBInfo():
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getPGDefaultDaoManager()
    daoManager.startTransaction()
    pgdepotdbDao = daoManager.getDao(DaoKeys.DAO_PGDEPOTDBDAO)
    dbList = pgdepotdbDao.listDatabases()
    serverList = pgdepotdbDao.listServers()
    dbuserList = pgdepotdbDao.listdbUsers()
    osuserList = pgdepotdbDao.listosUsers()

    # logger.info("pgserver size =%d" % len(serverList))
    for server in serverList:
        dbserver = wbxdbserver(server.host_name,login_user="postgres")
        dbserver.setServerInfo(server[0],server[2],server[3],server[4],server[5],server[6],server[7],
                               server[8],server[9],server[10],server[11],server[12],server[13],
                               server[14],server[15],server[16],server[17])
        daoManagerFactory.addServer(dbserver)

    for uservo in osuserList:
        dbserver = daoManagerFactory.getServer(uservo.host_name)
        if dbserver is not None:
            # dbserver.setLoginuser("postgres")
            dbserver.setLoginpwd(uservo.pwd)

    logger.info("db size=%d" % len(dbList))
    for dbvo in dbList:
        db_name = dbvo.db_name.upper()
        host_name = dbvo.host_name
        if db_name in wbxdatabasemanager.FEDERAMP_DBS:
            continue
        if db_name == "PATCHDB" or dbvo.db_type == "PRE_PROD":
            continue
        dbserver = daoManagerFactory.getServer(host_name)
        if dbserver is None:
            continue

        db = wbxdb(db_name)
        db.setDBVendor(dbvo.db_vendor)
        db.setDBVersion(dbvo.db_version)
        db.setApplicationType(dbvo.application_type)
        db.setApplnSupportCode(dbvo.appln_support_code)
        db.setListenerPort(dbvo.listener_port)
        db.setWebDomain(dbvo.web_domain)
        db.setdbServer(dbserver)
        dbserver.addDatabase(db)
        daoManagerFactory.addDatabase(db, db.getApplnSupportCode())

    for schema in dbuserList:
        db_name = schema['db_name']
        db = daoManagerFactory.getDatabaseByDBName(db_name)
        if db is not None:
            dbuser = wbxdbuser(db_name, schema['schemaname'])
            dbuser.setApplnSupportCode(schema['appln_support_code'])
            dbuser.setSchemaType(schema['schematype'])
            dbuser.setPassword(schema['password'])
            db.addUser(dbuser)

def getDBNameList(appSupportCode = None):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    if appSupportCode is not None:
        dbDict = daoManagerFactory.getDBListByAppCode(appSupportCode)
    else:
        dbDict = daoManagerFactory.dbcache
    return list(dbDict.keys())

def getDatabaseByDBID(dbid):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    return daoManagerFactory.getDatabaseByDBID(dbid)

def loadDatabaseByHostName(hostname):
    pass

# If add/remove server/user/shareplex port cases
# another case: the server is used for db1, but then used for db2. loadDatabaseByHostname function covered this case
def loadDatabaseByDBName(db_name):
    db_type = Config.getConfig().getDBType()
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        schemaList = depotdbDao.getSchemaListByDBName(db_name)
        rows = depotdbDao.getInstanceInfoByDBName(db_name, db_type)
        hostlist = []
        instancenameList = []
        db = daoManagerFactory.getDatabaseByDBName(db_name)
        # Remove connection URL, so it will re-generate the URL at next get it
        db.setConnectionURL(None)
        # Remove shareplex port list at first
        if db is not None:
            oldspportlist = db.getShareplexPortList()
            for oldspport in oldspportlist:
                port = oldspport.getPort()
                tgtdbList = oldspport.getTgtDBList()
                srcdbList = oldspport.getSrcDBList()
                db.removeShareplexPort(port)
                for srcdb in srcdbList:
                    srcdb.removeShareplexChannel(port, db)
                for tgtdb in tgtdbList:
                    tgtdb.removeShareplexChannel(port, db)

        for dbvo, ivo, server in rows:
            db_name = dbvo.db_name
            if db_name in wbxdatabasemanager.FEDERAMP_DBS:
                continue
            if db_name == "PATCHDB" or dbvo.db_type == "PRE_PROD":
                continue

            db = daoManagerFactory.getDatabaseByDBName(db_name)
            if db is None:
                db = wbxdb(db_name)
                daoManagerFactory.addDatabase(db,dbvo.appln_support_code)

            db.setTrimHost(dbvo.trim_host)
            db.setApplnSupportCode(dbvo.appln_support_code)
            db.setApplicationType(dbvo.application_type)
            db.setServicename(dbvo.service_name)
            db.setListenerPort(dbvo.listener_port)
            db.setWebDomain(dbvo.wbx_cluster)
            db.setWebDomain(dbvo.web_domain)

            dbserver = daoManagerFactory.getServer(server.host_name)
            hostlist.append(server.host_name)
            if dbserver is None:
                dbserver = wbxdbserver(server.host_name)
                daoManagerFactory.addServer(dbserver)
            dbserver.addServerInfo(server)

            dbinstance = db.getInstanceByInstanceName(ivo.instance_name)
            instancenameList.append(ivo.instance_name)
            if dbinstance is None:
                dbinstance = wbxdbinstance(db, ivo.instance_name, dbserver)
                db.addInstance(dbinstance)
                dbserver.addDatabase(db)
        # Cover the remove node case
        db.removeInstance(instancenameList)

        sshUserList = depotdbDao.getLoginUser(hostlist)
        for row in sshUserList:
            dbserver = daoManagerFactory.getServer(row[0])
            if dbserver is not None:
                dbserver.setLoginpwd(row[1])

        db.removeSchemaList()
        for schema in schemaList:
            dbuser = wbxdbuser(db_name, schema.schema)
            dbuser.setApplnSupportCode(schema.appln_support_code)
            dbuser.setSchemaType(schema.schematype)
            dbuser.setPassword(schema.password)
            db.addUser(dbuser)

        spchannelList = depotdbDao.getShareplexChannelByDBName(db_name, hostlist)
        for spchannel in spchannelList:
            srcdbname = spchannel.src_db
            srchost = spchannel.src_host
            tgtdbname = spchannel.tgt_db
            tgthost = spchannel.tgt_host
            srcserver = daoManagerFactory.getServer(srchost)
            tgtserver = daoManagerFactory.getServer(tgthost)
            if srcserver is None:
                # logger.info("Does not get server by shareplex_info.src_host=%s" % srchost)
                continue
            if tgtserver is None:
                # logger.info("Does not get server by shareplex_info.tgt_host=%s" % tgthost)
                continue
            srcdb = daoManagerFactory.getDatabaseByDBName(srcdbname)
            tgtdb = daoManagerFactory.getDatabaseByDBName(tgtdbname)
            if srcdb is None:
                # logger.info("Does not get server by shareplex_info.src_db=%s" % srcdbname)
                continue
            if tgtdb is None:
                # logger.info("Does not get server by shareplex_info.tgt_host=%s" % tgtdbname)
                continue
            srcport = srcserver.getShareplexPort(spchannel.port)
            if srcport is None:
                srcport = wbxdbshareplexport(spchannel.port, srcserver)
                srcserver.addShareplexPort(srcport)
            srcport.addDatabase(srcdb, srcdb.getSplexSid())

            tgtport = tgtserver.getShareplexPort(spchannel.port)
            if tgtport is None:
                tgtport = wbxdbshareplexport(spchannel.port, tgtserver)
                tgtserver.addShareplexPort(tgtport)
            tgtport.addDatabase(tgtdb, tgtdb.getSplexSid())
            srcport.addTgtDB(tgtdb)
            tgtport.addSrceDB(srcdb)

        # for dbvo in dbList:
        dbuser = wbxdbuser(db_name=db.db_name, username="SYSTEM")
        dbuser.setApplnSupportCode(db.appln_support_code)
        dbuser.setPassword("sysnotallow")
        dbuser.setSchemaType("dba")
        db.addUser(dbuser)
        # hostlist.append(s.host_name)


        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        raise e
    finally:
        depotDaoManager.close()

def saveDatabase(params):
    print(params)
    trimhost = params["trimhost"]
    dbname = params["dbname"]
    db_vendor = params["dbvendor"]
    db_version = params["dbversion"]
    db_type = params["dbtype"]
    application_type = params["applntype"]
    db_home = params["dbhome"]
    appln_support_code = params["applnsupportcode"]
    failover_trim_host = params["ftrimhost"]
    failover_db = params["fdbname"]
    service_name = params["servicename"]
    listener_port = params["listenerport"]
    backup_method = params["backupmethod"]
    backup_server = params["backupserver"]
    catalog_trim_host = params["ctrimhost"]
    catalog_db = params["cdbname"]
    monitor = params["monitor"]
    appln_contact = params["applncontact"]
    contents = params["content"]
    wbx_cluster = params["wbxcluster"]
    web_domain = params["webdomain"]
    created_by = params["createuser"]
    modified_by = params["modifyuser"]
    dbid = "%s_%s" % (trimhost, dbname)
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    db = daoManagerFactory.getDatabaseByDBID(dbid)
    daoManager = daoManagerFactory.getDaoManager(DEFAULT_DBID, LOGINSCHEMA)
    depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    daoManager.startTransaction()
    if db is None:
        db = wbxdatabase(
            trim_host = trimhost,
            db_name = dbname,
            db_vendor = db_vendor,
            db_version = db_version,
            db_type = db_type,
            application_type = application_type,
            db_home = db_home,
            appln_support_code=appln_support_code,
            failover_trim_host= failover_trim_host,
            failover_db = failover_db,
            service_name = service_name,
            listener_port = listener_port,
            backup_method = backup_method,
            backup_server = backup_server,
            catalog_trim_host = catalog_trim_host,
            catalog_db = catalog_db,
            monitor = monitor,
            appln_contact = appln_contact,
            contents = contents,
            wbx_cluster = wbx_cluster,
            web_domain = web_domain,
            created_by = created_by,
            modified_by = modified_by
        )
        depotdbDao.addDatabaseInfo(db)
        daoManagerFactory.addDatabase(dbid, db)
    else:
        db.db_vendor = db_vendor
        db.db_version = db_version
        db.db_type = db_type
        db.application_type = application_type
        db.db_home = db_home
        db.appln_support_code = appln_support_code
        db.failover_trim_host = failover_trim_host
        db.failover_db = failover_db
        db.service_name = service_name
        db.listener_port = listener_port
        db.backup_method = backup_method
        db.backup_server = backup_server
        db.catalog_trim_host = catalog_trim_host
        db.catalog_db = catalog_db
        db.monitor = monitor
        db.appln_contact = appln_contact
        db.contents = contents
        db.wbx_cluster = wbx_cluster
        db.web_domain = web_domain
        db.created_by = created_by
        db.modified_by = modified_by


    daoManager.commit()
    return dbid


def deleteSchema(dbid, schemaname):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    db = daoManagerFactory.getDatabaseByDBID(dbid)
    schema = db.getSchema(schemaname)
    if schema is not None:
        daoManager = daoManagerFactory.getDaoManager(DEFAULT_DBID, LOGINSCHEMA)
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        depotdbDao.deleteSchema(schema)
        db.schemadict.pop(schemaname)
        daoManager.commit()

def saveSchema(dbid, schemaname, pwd, schematype):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDaoManager(DEFAULT_DBID, LOGINSCHEMA)
    try:
        db = daoManagerFactory.getDatabaseByDBID(dbid)
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        daoManager.startTransaction()

        schema = db.getSchema(schemaname)
        if schema is not None:
            schema.password = pwd
            schema.schematype = schematype
            schema.new_password = pwd
        else:
            schema = wbxschema(trim_host=db.trim_host, db_name=db.db_name, schema=schemaname,
                       appln_support_code=db.appln_support_code, password=pwd, schematype=schematype,
                       new_password=pwd)
            depotdbDao.addSchema(schema)
        db.schemadict[schemaname] = schema
        daoManager.commit()
    except:
        daoManager.rollback()


def getServerListByTrimhost(trimhost):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    return daoManagerFactory.getServerListByTrimHost(trimhost)


def saveServerInfo(params):
    trimhost=params["trimhost"]
    hostname = params["hostname"]
    domain = params["domain"]
    sitecode = params["sitecode"]
    hostip = params["hostip"]
    vipname = params["vipname"]
    vipip = params["vipip"]
    privname = params["privname"]
    privip = params["privip"]
    scanname = params["scanname"]
    scanip1 = params["scanip1"]
    scanip2 = params["scanip2"]
    scanip3 = params["scanip3"]
    ostypecode = params["ostypecode"]
    processor = params["processor"]
    kernelrelease = params["kernelrelease"]
    platform = params["platform"]
    cpucount = params["cpucount"]
    cpucorecount = params["cpucorecount"]
    cpumodel = params["cpumodel"]
    flagnodevertual = params["flagnodevertual"]
    comments = params["comments"]
    lccode = params["lccode"]
    sshport = params["sshport"]
    createuser = params["createuser"]
    modifyuser = params["modifyuser"]

    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    serverList = daoManagerFactory.getServerListByTrimHost(trimhost)

    daoManager = daoManagerFactory.getDaoManager(DEFAULT_DBID, LOGINSCHEMA)
    depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    daoManager.startTransaction()

    hasserver = False
    if serverList is not None:
        for server in serverList:
            if hostname == server.host_name:
                server.domain = domain
                server.site_code = sitecode
                server.host_ip = hostip
                server.vip_name = vipname
                server.vip_ip = vipip
                server.priv_name = privname
                server.priv_ip = privip
                server.scan_name = scanname
                server.scan_ip1 = scanip1
                server.scan_ip2 = scanip2
                server.scan_ip3 = scanip3
                server.os_type_code = ostypecode
                server.processor = processor
                server.kernel_release = kernelrelease
                server.hardware_platform = platform
                server.physical_cpu = cpucount
                server.cores = cpucorecount
                server.cpu_model = cpumodel
                server.flag_node_virtual = flagnodevertual
                server.comments = comments
                server.lc_code = lccode
                server.ssh_port = sshport
                server.created_by = createuser
                server.modified_by = modifyuser
                hasserver = True
                daoManager.commit()
    if not hasserver:
        server = wbxserver(
            trim_host = trimhost,
            host_name = hostname,
            domain = domain,
            site_code = sitecode,
            host_ip = hostip,
            vip_name = vipname,
            vip_ip = vipip,
            priv_name = privname,
            priv_ip = privip,
            scan_name = scanname,
            scan_ip1 = scanip1,
            scan_ip2 = scanip2,
            scan_ip3 = scanip3,
            os_type_code = ostypecode,
            processor = processor,
            kernel_release = kernelrelease,
            hardware_platform = platform,
            physical_cpu = cpucount,
            cores = cpucorecount,
            cpu_model = cpumodel,
            flag_node_virtual = flagnodevertual,
            comments = comments,
            lc_code = lccode,
            ssh_port = sshport,
            created_by = createuser,
            modified_by = modifyuser)
        depotdbDao.addServer(server)
        daoManagerFactory.addServer(server)
    daoManager.commit()

def deletServerInfo(params):
    trimhost = params["trimhost"]
    hostname = params["hostname"]
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    serverList = daoManagerFactory.getServerListByTrimHost(trimhost)

    daoManager = daoManagerFactory.getDaoManager(DEFAULT_DBID, LOGINSCHEMA)
    depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    daoManager.startTransaction()

    for server in serverList:
        if hostname == server.host_name:
            depotdbDao.deleteServer(server)
            break
    daoManager.commit()


def saveShareplexChannel(dbid, params):
    channelid=params["channelid"]
    srchost=params["srchost"]
    srcdb=params["srcdb"]
    port=params["port"]
    repto=params["repto"]
    qname=params["qname"]
    tgthost = params["tgthost"]
    tgtdb = params["tgtdb"]
    srcsplexid = params["srcsplexid"]
    srcschema = params["srcschema"]
    tgtsplexid = params["tgtsplexid"]
    tgtschema = params["tgtschema"]
    createuser = params["createuser"]
    modifyuser = params["modifyuser"]

    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    db = daoManagerFactory.getDatabaseByDBID(dbid)
    daoManager = daoManagerFactory.getDaoManager(DEFAULT_DBID, LOGINSCHEMA)
    depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    daoManager.startTransaction()

    # spchannel = db.getShareplexChannel(srchost,srcdb, port, tgthost,tgtdb, srcschema, tgtschema, qname, repto)
    spchannel = db.getShareplexChannelByChannelid(channelid)
    if spchannel is not None:
        spchannel.src_host = srchost
        spchannel.src_db = srcdb
        spchannel.replication_to = repto
        spchannel.port = port
        spchannel.qname = qname
        spchannel.tgt_host = tgthost
        spchannel.tgt_db = tgtdb
        spchannel.src_schema = srcschema
        spchannel.src_splex_id = srcsplexid
        spchannel.tgt_splex_id = tgtsplexid
        spchannel.tgt_schema = tgtschema
        spchannel.created_by = createuser
        spchannel.modified_by = modifyuser
    else:
        spchannel = wbxshareplexchannel(src_host = srchost, src_db = srcdb, replication_to = repto, port = port, qname = qname,
                                        tgt_host=tgthost, tgt_db = tgtdb, src_schema = srcschema, src_splex_id = srcsplexid,
                                        tgt_splex_id=tgtsplexid, tgt_schema = tgtschema, created_by = createuser, modified_by = modifyuser)
        depotdbDao.addShareplexChannel(spchannel)
        db.addShareplexChannel(spchannel)
    daoManager.commit()


def deleteShareplexChannel(dbid, params):
    channelid = params["channelid"]
    # srchost = params["srchost"]
    # srcdb = params["srcdb"]
    # port = params["port"]
    # repto = params["repto"]
    # qname = params["qname"]
    # tgthost = params["tgthost"]
    # tgtdb = params["tgtdb"]
    # srcschema = params["srcschema"]
    # tgtschema = params["tgtschema"]

    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    db = daoManagerFactory.getDatabaseByDBID(dbid)
    daoManager = daoManagerFactory.getDaoManager(DEFAULT_DBID, LOGINSCHEMA)
    depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    daoManager.startTransaction()

    spchannel = db.getShareplexChannelByChannelid(channelid)
    if spchannel is not None:
            depotdbDao.deleteShareplexChannel(spchannel)
    daoManager.commit()


def checkSplitMeeting(db):
    if db.appln_support_code == "WEB" and db.application_type == wbxdatabasemanager.APPLICATION_TYPE_PRI:
        domainname = db.web_domain
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        cluster = daomanagerfactory.getClusterByClusterName(domainname)

        priwebdomain = daomanagerfactory.getWebDomain(domainname)
        clusterurl = priwebdomain.clusterurl

def addcronjoblog(jsondata):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        daoManager.startTransaction()
        # jsondata = json.loads(strjsondata)
        logvo = WbxCronjobLogVO.loadFromJson(jsondata)
        depotdbDao.addCronjobLog(logvo)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        raise e






