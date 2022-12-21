import logging
import json
import datetime
import re
import os


from common.wbxutil import wbxutil
import threading
import traceback

from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from dao.vo.depotdbvo import DBPatchReleaseVO, DBPatchDeploymentVO, wbxdatabasemanager, ShareplexCRDeploymentVO, MeetingDataMonitorVO, WebDomainDataMonitorVO
from common.wbxmail import wbxemailmessage, wbxemailtype, sendalert
from biz.DBPatchJob import loaddbpatchreleasexml
from biz.DBLinkMonitor import monitordblink
from biz.ShareplexMonitor import monitorCREnabled

from sqlalchemy import Table, Column, MetaData, String, DateTime, Integer, and_, create_engine
from sqlalchemy.orm import mapper
from wbxredis.wbxredis import wbxredis

logger = logging.getLogger("DBAMONITOR")

metadata = MetaData()
dbversiontable = {}
dbtable = {}

_lock = threading.Lock()

# schematype can only be 'test','app','glookup','xxrpth'
# and we only check 'CONFIG','WEB','OPDB','LOOKUP','MMP','TEO','TEL'
# This function need to process 4 tables: wbxdatabase, wbxdatabaseversion, wbxdbpatchrelease, wbxdbpatchdeployment
def getdbpatchDeployment(dbid):
    logger.info("getdbpatchDeployment(dbid=%s) start" % dbid)
    global dbversiontable
    global dbtable
    daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
    db = daomanagerfactory.getDatabaseByDBID(dbid)

    # no wbxdatabaseversion table for JBR database
    if db.appln_support_code not in  [wbxdatabasemanager.APPLN_SUPPORT_CODE_CONFIGDB, wbxdatabasemanager.APPLN_SUPPORT_CODE_WEBDB, wbxdatabasemanager.APPLN_SUPPORT_CODE_TAHOEDB,
                                      wbxdatabasemanager.APPLN_SUPPORT_CODE_BILLINGDB, wbxdatabasemanager.APPLN_SUPPORT_CODE_GLOOKUPDB, wbxdatabasemanager.APPLN_SUPPORT_CODE_TEODB,
                                      wbxdatabasemanager.APPLN_SUPPORT_CODE_MMP, wbxdatabasemanager.APPLN_SUPPORT_CODE_MEDIATE]:
        return

    dbver_table_name = "wbxdatabaseversion"
    db_table_name = "wbxdatabase"
    # if db.appln_support_code in ['CONFIG','WEB','OPDB','LOOKUP','MMP','TEO','TEL']:
    # if dbid == "sjdbth38_TSJ22":
    #     print(db.schemadict)

    for schemaname, schema in db.schemadict.items():
        if schema.schematype in ['test', 'app', 'glookup', 'xxrpth']:
            schematype = schema.schematype
            daomanager = daomanagerfactory.getDaoManager(db.getdbid())

            auditDao = daomanager.getDao(DaoKeys.DAO_DBAUDITDAO)
            tablevo = auditDao.getTableByTableName(schemaname.upper(), dbver_table_name.upper())
            if tablevo is not None:
                if schemaname in dbversiontable:
                    tdbver = dbversiontable[schemaname]
                    tdb = dbtable[schemaname]
                else:
                    tdbver = type(dbver_table_name, (object,), dict())
                    dbvertbl = Table(
                        dbver_table_name,
                        metadata,
                        Column('release_number', Integer, primary_key=True),
                        Column('major_number', Integer),
                        Column('minor_number', Integer),
                        Column('dbtype', String(56)),
                        Column('description', String(512)),
                        schema=schemaname
                    )
                    mapper(tdbver, dbvertbl)
                    dbversiontable[schemaname] = tdbver

                    tdb = type(db_table_name, (object,), dict())
                    dbtbl = Table(
                        db_table_name,
                        metadata,
                        Column('createtime', DateTime, primary_key=True),
                        Column('version', String, primary_key=True),
                        schema=schemaname
                    )
                    mapper(tdb, dbtbl)
                    dbtable[schemaname] = tdb

                try:
                    dbvervo = auditDao.getDatabaseVersion(tdbver)
                    dbvoList = auditDao.getDatabaseBydbname(tdb, 30)
                    daomanager.commit()
                    if dbvervo is None:
                        continue

                    release_number = dbvervo.release_number

                    if schema.appln_support_code != db.appln_support_code:
                        appln_support_code = schema.appln_support_code.upper()
                    else:
                        appln_support_code = db.appln_support_code.upper()
                    db_type = db.db_type
                except Exception as e:
                    logger.error("Error occured on dbid=%s with error msg %s" % (dbid, e))
                    daomanager.rollback()

                defaultDaoManager = daomanagerfactory.getDefaultDaoManager()
                try:
                    defaultDaoManager.startTransaction()
                    depotdbDao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                    for dbvo in dbvoList:
                        try:
                            ireleasenumber = int(dbvo.release_number)
                        except Exception as e:
                            logger.error("Error occurred when convert releasenumber {0}".format(e))
                            continue

                        release_number = int(dbvo.release_number)
                        # if release_number != 15884:
                        #     continue

                        logger.info("start to update Release %s deployment status on db %s" % (release_number, dbid))
                        deploytime = dbvo.deploytime
                        spdeploytime = dbvo.spdeploytime
                        description = None
                        major_number = None
                        minor_number = None
                        if dbvervo.release_number == release_number:
                            description = dbvervo.description
                            major_number = dbvervo.major_number
                            minor_number = dbvervo.minor_number

                        dbpatchreleasevo = depotdbDao.getDBPatchRelease(release_number, appln_support_code, schematype)
                        defaultDaoManager.commit()

                        # This is first deployment db
                        if dbpatchreleasevo is None:
                            if _lock.acquire(True):
                                try:
                                    dbpatchreleasevo = depotdbDao.getDBPatchReleaseByReleaseNumber(release_number)
                                    deploymentvo = None

                                    # It is necessary to get the vo again
                                    dbpatchreleasevo = depotdbDao.getDBPatchRelease(release_number, appln_support_code,
                                                                                    schematype)
                                    deploymentvo = None
                                    if dbpatchreleasevo is None:
                                        logger.info("insert new release with release_number=%d on db %s" % (release_number, dbid))
                                        loaddbpatchreleasexml(release_number, dbid)
                                        # dbpatchreleasevo = DBPatchReleaseVO(releasenumber=release_number,
                                        #                                     appln_support_code=appln_support_code,
                                        #                                     schematype=schematype,
                                        #                                     major_number=major_number,
                                        #                                     minor_number=minor_number,
                                        #                                     description=description)
                                        # depotdbDao.addDBPatchRelease(dbpatchreleasevo)
                                        # # Do not commit at here, otherwise it may has issue when parallel
                                        # defaultDaoManager.commit()
                                        #
                                        # # This thread will download and install dbpatch and parse release.xml file
                                        # # It also update wbxdbpatchrelease table but not update wbxdbpatchdeployment data
                                        # releasevo = depotdbDao.getDBPatchReleaseByReleaseNumber(release_number)
                                        # if releasevo is None:
                                        #     threadpool = wbxthreadpool.getThreadPool()
                                        #     threadpool.submit(loaddbpatchreleasexml, release_number)
                                        #
                                        # dbList = daomanagerfactory.getDBListByAppCode(appln_support_code)
                                        # for ldb in dbList:
                                        #     if ldb.db_type == "PRE_PROD":
                                        #         continue
                                        #
                                        #     # Because a tahoeDB has multiple app schema, so at here it need to scan all app schemas
                                        #     schemalist = ldb.getSchemaBySchemaType(appln_support_code, schematype)
                                        #     db_type = ldb.db_type
                                        #     #
                                        #     # if ldb.db_name in ('RACAUWEB'):
                                        #     #     print(ldb.db_name)
                                        #
                                        #     for lschema in schemalist:
                                        #         lschemname = lschema.schema
                                        #         if ldb.getdbid() == db.getdbid() and lschemname == schemaname:
                                        #             deploystatus = "NOTDEPLOYED" if dbvo.deploytime is None else "DEPLOYED"
                                        #             spdeploystatus = "NOTDEPLOYED" if dbvo.spdeploytime is None else "DEPLOYED"
                                        #             if dbvervo.release_number == release_number:
                                        #                 major_number = dbvervo.major_number
                                        #                 minor_number = dbvervo.minor_number
                                        #         else:
                                        #             major_number = None
                                        #             minor_number = None
                                        #             deploystatus = "NOTDEPLOYED"
                                        #             spdeploystatus = "NOTDEPLOYED"
                                        #             deploytime = None
                                        #
                                        #         clustername = None
                                        #         if appln_support_code == wbxdatabasemanager.APPLN_SUPPORT_CODE_WEBDB:
                                        #             clustername = ldb.web_domain
                                        #         elif appln_support_code == wbxdatabasemanager.APPLN_SUPPORT_CODE_TAHOEDB:
                                        #             if schematype == wbxdatabasemanager.SCHEMATYPE_APP:
                                        #                 domain = ldb.getDomainBySchemaname(lschemname)
                                        #                 if domain is not None:
                                        #                     clustername = domain.domainname
                                        #             else:
                                        #                 clustername = ldb.db_name
                                        #         else:
                                        #             clustername = ldb.db_name
                                        #         (scheduled_start_date, completed_date, summary, change_imp,infrastructure_change_id) = getDBpatchDeploymentChange(release_number, ldb, lschemname)
                                        #
                                        #         dbpatchdeployvo = DBPatchDeploymentVO(releasenumber=release_number,
                                        #                                               appln_support_code=appln_support_code,
                                        #                                               db_type=db_type,
                                        #                                               trim_host=ldb.trim_host,
                                        #                                               db_name=ldb.db_name,
                                        #                                               schemaname=lschemname,
                                        #                                               schematype=schematype,
                                        #                                               cluster_name=clustername,
                                        #                                               deploytime=deploytime,
                                        #                                               deploystatus=deploystatus,
                                        #                                               spdeploystatus=spdeploystatus,
                                        #                                               major_number=major_number,
                                        #                                               minor_number=minor_number,
                                        #                                               change_id=infrastructure_change_id,
                                        #                                               change_sch_start_date=scheduled_start_date,
                                        #                                               change_completed_date=completed_date,
                                        #                                               change_imp=change_imp
                                        #                                               )
                                        #         try:
                                        #             depotdbDao.addDBPatchDeployment(dbpatchdeployvo)
                                        #         except Exception as e:
                                        #             logger.error(e)
                                        #         logger.info("insert process, insert vo  %s " % dbpatchdeployvo)
                                        # defaultDaoManager.commit()
                                    else:
                                        deploymentvo = depotdbDao.getDBPatchDeployment(release_number, db.db_name,
                                                                                       db.trim_host,
                                                                                       schemaname)

                                        if deploymentvo is not None and deploymentvo.deploystatus == 'NOTDEPLOYED':
                                            (scheduled_start_date, completed_date, summary, change_imp, infrastructure_change_id) = getDBpatchDeploymentChange(release_number, db, schemaname)
                                            deploymentvo.deploytime = deploytime
                                            deploymentvo.deploystatus = "DEPLOYED"
                                            deploymentvo.spdeploytime = spdeploytime
                                            deploymentvo.spdeploystatus = "DEPLOYED" if spdeploytime is not None else "NOTDEPLOYED"
                                            deploymentvo.major_number = major_number
                                            deploymentvo.minor_number = minor_number
                                            if completed_date is not None:
                                                deploymentvo.change_id = infrastructure_change_id,
                                                deploymentvo.change_sch_start_date = scheduled_start_date,
                                                deploymentvo.change_completed_date = completed_date,
                                                deploymentvo.change_imp = change_imp
                                        logger.info(
                                            "insert process, update vo  %s with db=%s" % (deploymentvo, db.db_name))
                                except Exception as e:
                                    daomanager.rollback()
                                    if deploymentvo is not None:
                                        logger.error(deploymentvo.releasenumber, deploymentvo.db_name, deploymentvo.schemaname, deploymentvo.cluster_name)
                                    logger.error("Error occurred when insert new release with release number=%d, db_name=%s" % (release_number, db.db_name))
                                    logger.error(e)
                                    logger.error(traceback.format_exc())
                                # Before release lock, we should commit at first
                                defaultDaoManager.commit()
                                _lock.release()
                        else:
                            try:
                                logger.info("update release with release_number=%d on dbid %s" % (release_number, dbid))
                                #         This dbpatch is found on other db before;
                                idx = 0
                                deploymentvo = depotdbDao.getDBPatchDeployment(release_number, db.db_name, db.trim_host,schemaname)
                                if deploymentvo is not None:
                                    if deploymentvo.deploystatus == "NOTDEPLOYED":
                                        deploymentvo.deploytime = deploytime
                                        deploymentvo.deploystatus = "DEPLOYED"

                                        deploymentvo.major_number = major_number
                                        deploymentvo.minor_number = minor_number

                                    if deploymentvo.spdeploystatus == "NOTDEPLOYED":
                                        deploymentvo.spdeploytime = spdeploytime
                                        deploymentvo.spdeploystatus = "DEPLOYED" if spdeploytime is not None else "NOTDEPLOYED"

                                    if deploymentvo.change_completed_date is None:
                                        (scheduled_start_date, completed_date, summary, change_imp,
                                         infrastructure_change_id) = getDBpatchDeploymentChange(release_number, db,
                                                                                                schemaname)
                                        deploymentvo.change_id = infrastructure_change_id
                                        deploymentvo.change_sch_start_date = scheduled_start_date
                                        deploymentvo.change_completed_date = completed_date
                                        deploymentvo.change_imp = change_imp
                                    logger.info("update process, update vo  %s with db=%s" % (deploymentvo, db.db_name))

                                else:
                                    logger.info("update release with release_number=%d, but not find data in wbxdbpatchdeployment table for db %s" % (release_number, db.db_name))

                            except Exception as e:
                                logger.error("Error occurred when update release with release number=%d" % release_number)

                    defaultDaoManager.commit()
                except Exception as e:
                    defaultDaoManager.rollback()
                    logger.error(e)
                    logger.error(traceback.format_exc())
                finally:
                    defaultDaoManager.commit()
            else:
                logger.error("No wbxdatabaseversion table under schema %s on db %s" % (schemaname, db.getdbid()))
    logger.info("getdbpatchDeployment(dbid=%s) end" % dbid)
    getAllScheduledDBChange(dbid)

def getAllScheduledDBChange(dbid):
    daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daomanager = daomanagerfactory.getDefaultDaoManager()
    depotDao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    db = daomanagerfactory.getDatabaseByDBID(dbid)
    starttime = wbxutil.getcurrenttime(31 * 24 * 60 * 60)
    try:
        daomanager.startTransaction()
        deploylist = depotDao.listScheduledChange(db.trim_host, db.db_name, starttime)
        for deploymentvo in deploylist:
            (scheduled_start_date, completed_date, summary, change_imp,
             infrastructure_change_id) = getDBpatchDeploymentChange(deploymentvo.releasenumber, db, deploymentvo.schemaname)
            if completed_date is not None or deploymentvo.change_id is None:
                deploymentvo.change_id = infrastructure_change_id
                deploymentvo.change_sch_start_date = scheduled_start_date
                deploymentvo.change_completed_date = completed_date
                deploymentvo.change_imp = change_imp
        daomanager.commit()
    except Exception as e:
        daomanager.rollback()
        logger.error("Error occurred when executing getAllScheduledDBChange(%s) with %s" % (dbid,e))
        logger.error(traceback.format_exc())


def getDBpatchDeploymentChange(release_number, db, schemaname):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDaoManagerForJobManager()
    jobDao = daoManager.getDao(DaoKeys.DAO_JOBMANAGERDAO)
    searchcondition=""
    env=""
    cluster_name = ""
    if db.appln_support_code ==  wbxdatabasemanager.APPLN_SUPPORT_CODE_GLOOKUPDB:
        searchcondition="%%DB%%%s%%%s%%RLM%%" %(release_number, "GLOBALLOOKUPDB")
        if db.db_type == wbxdatabasemanager.DB_TYPE_PROD:
            cluster_name = "PRODGLOBALLOOKUPDB%s" % ('SJ' if db.trim_host[0:2].upper()=="SJ" else "VA")
        else:
            cluster_name = "BTSGLOBALLOOKUPDB"
    elif db.appln_support_code == wbxdatabasemanager.APPLN_SUPPORT_CODE_WEBDB:
        searchcondition = "%%DB%%%s%%%s%%RLM%%"  % (release_number, db.wbx_cluster.upper())
        cluster_name = db.wbx_cluster.upper()
    elif db.appln_support_code == wbxdatabasemanager.APPLN_SUPPORT_CODE_TAHOEDB:
        domain = db.getDomainBySchemaname(schemaname)
        if domain is not None:
            searchcondition = "%%DB%%%s%%%s%%RLM%%"  % (release_number, domain.domainname.upper())
            cluster_name = domain.domainname.upper()
        else:
            return (None, None, None, None, None)
    elif db.appln_support_code == wbxdatabasemanager.APPLN_SUPPORT_CODE_BILLINGDB:
        searchcondition = "%%DB%%%s%%%s%%RLM%%"  % (release_number, "BILLING")
        if db.db_type == wbxdatabasemanager.DB_TYPE_PROD:
            cluster_name = "PRODBILLINGDB"
        else:
            cluster_name = "BTSBILLINGDB"
    elif db.appln_support_code == wbxdatabasemanager.APPLN_SUPPORT_CODE_CONFIGDB:
        searchcondition = "%%DB%%%s%%%s%%RLM%%"  % (release_number, "CONFIG")
        if db.db_type == wbxdatabasemanager.DB_TYPE_PROD:
            cluster_name = "PRODCONFIGDB"
        else:
            cluster_name = "BTSCONFIGDB"
    elif db.appln_support_code == wbxdatabasemanager.APPLN_SUPPORT_CODE_TEODB:
        searchcondition = "%%DB%%%s%%%s%%RLM%%"  % (release_number, "TEOREPORTDB")
        if db.db_type == wbxdatabasemanager.DB_TYPE_PROD:
            cluster_name = "PRODTEOREPORTDB%s" % ('SJ' if db.trim_host[0:2].upper()=="SJ" else "TX")
        else:
            cluster_name = "BTSTEOREPORTDB"

    if db.db_type==wbxdatabasemanager.DB_TYPE_PROD:
        env="Production"
    elif db.db_type == wbxdatabasemanager.DB_TYPE_BTS:
        env = "BTS/Internal"

    try:
        daoManager.startTransaction()
        wcrlist = jobDao.listStapWCRBySummary(searchcondition, env)
        for wcrvo in wcrlist:
            sumarry = re.split('[-:]', wcrvo.summary)
            if len(sumarry) > 2:
                clslist = sumarry[1].split(",")
                for cls_name in clslist:
                    # The cls_name maybe "IM (Federal)" or IL (China)
                    cls_name= cls_name.replace(" ", "").upper().split("(")[0]
                    if cls_name == cluster_name:
                        # print(wcrvo.scheduled_start_date, wcrvo.completed_date, wcrvo.summary, wcrvo.change_imp, wcrvo.infrastructure_change_id)
                        return (wbxutil.convertStringToGMTString(wcrvo.scheduled_start_date), wbxutil.convertStringToGMTString(wcrvo.completed_date), wcrvo.summary, wcrvo.change_imp, wcrvo.infrastructure_change_id)
            else:
                logger.error("%s" % wcrvo.summary)
    except Exception as e:
        daoManager.rollback()
        logger.error("Error occurred when executing getDBpatchDeploymentChange(db_name=%s, release_number=%s, schemaname=%s)" % ( db.db_name, release_number, schemaname))
        logger.error(traceback.format_exc())
    finally:
        daoManager.commit()
    return (None, None, None, None, None)

# Used to check whether shareplex data in DepotDB is consistent with configuration in shareplex
def monitorShareplexDatainDepotDB(dbid):
    if dbid == "None_None":
        return

    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDaoManager(dbid)
    dbauditdao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
    spDao = daoManager.getDao(DaoKeys.DAO_SHAREPLEXMONITORDAO)
    db = daoManagerFactory.getDatabaseByDBID(dbid)

    sucmsg = ""
    logmsg = ""
    # {19063：{CONFIG_SPLEX,1, RACWEBJT_SID:2},19064:{...}}
    portdict = {}
    spchannels = db.shareplexchanneldict.values()

    for spchannel in spchannels:
        port = spchannel.port
        # if spchannel.tgt_splex_sid != 'KAFKA':
        #     continue

        if spchannel.tgt_db == db.db_name:
            tgt_splex_sid = spchannel.tgt_splex_sid.upper()
            src_splex_sid = spchannel.src_splex_sid.upper()
            table_name = spchannel.getMonitorTableName()
            sucmsg = "%s******Check port %s from %s to %s with monitor_table_name=%s*******\n" % (
                sucmsg, port, spchannel.src_db, spchannel.tgt_db, table_name)
            schemaname = spchannel.getSchemaname()

            if port in portdict:
                portmsg = portdict[port]["msg"]
            else:
                portmsg = ""

            try:
                try:
                    daoManager.startTransaction()
                    tablevo = dbauditdao.getTableByTableName(schemaname, table_name)
                    if tablevo is None:
                        logmsg = "%sError: The monitor table %s.%s does not exist under port %s from %s \n" % (
                            logmsg, schemaname, table_name, port, spchannel.src_db)
                        portmsg = "%sError: The monitor table %s.%s does not exist under port %s from %s" % (
                            portmsg, schemaname, table_name, port, spchannel.src_db)
                    else:
                        vo = spDao.querysplexmonitoradbdata(schemaname, table_name, spchannel.src_db)
                        if vo is None:
                            logmsg = "%sError: The monitor table %s.%s exist but no data\n" % (
                                logmsg, schemaname, table_name)
                            portmsg = "%sError: The monitor table %s.%s exist but no data" % (
                                portmsg, schemaname, table_name)
                        elif (datetime.datetime.now() - vo.logtime).days > 1:
                            logmsg = "%sError: The monitor table %s under port %s from %s exists but the backlog delay more than 1 day\n" % (
                                logmsg, table_name, port, spchannel.src_db)

                            portmsg = "%sError: The monitor table %s under port %s from %s exists but the backlog delay more than 1 day" % (
                                portmsg, table_name, port, spchannel.src_db)
                        else:
                            sucmsg = "%s The monitor table %s exist and backlog is recent\n" % (sucmsg, table_name)
                except Exception as e:
                    logger.error("Error occurred: {0}".format(e))
                    daoManager.rollback()
                finally:
                    daoManager.commit()

                depotDaoManager = daoManagerFactory.getDefaultDaoManager()
                depotDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)

                try:
                    depotDaoManager.startTransaction()
                    direction = spchannel.getDirection()
                    adbmonvo = depotDao.getadbmon(spchannel.src_db, spchannel.src_host, spchannel.port,
                                                  spchannel.tgt_db,
                                                  spchannel.tgt_host, direction)
                    if adbmonvo is None:
                        logmsg = "%sError: No record in wbxadbmon table in DepotDB for this port %d from %s to %s with replication_to=%s and monitor table name %s\n" % (
                            logmsg, spchannel.port, spchannel.src_db, spchannel.tgt_db, direction, table_name)
                        portmsg = "%sError: No record in wbxadbmon table in DepotDB for this port %d from %s to %s with replication_to=%s and monitor table name %s" % (
                            portmsg, spchannel.port, spchannel.src_db, spchannel.tgt_db, direction, table_name)
                    elif (datetime.datetime.now() - adbmonvo.lastreptime).days > 1:
                        logmsg = "%sError: The wbxadbmon.lastreptime value is 1 days ago in depotdb for this port %d from %s to %s\n" % (
                            logmsg, spchannel.port, spchannel.src_db, spchannel.tgt_db)
                        portmsg = "%sError: The wbxadbmon.lastreptime value is 1 days ago in depotdb for this port %d from %s to %s" % (
                            portmsg, spchannel.port, spchannel.src_db, spchannel.tgt_db)
                    else:
                        sucmsg = "%s queue is monitored by monitor tool and wbxadbmon.lastreptime is recent\n" % (sucmsg)
                finally:
                    depotDaoManager.commit()

            except Exception as e:
                logger.error(e)
                logger.error(e.__traceback__)

            if port not in portdict:
                portdict[port] = {src_splex_sid: 1, "msg": portmsg}
            else:
                portinfo = portdict[port]
                portinfo["msg"] = portmsg
                if src_splex_sid in portinfo:
                    portinfo[src_splex_sid] = portinfo[src_splex_sid] + 1
                else:
                    portinfo[src_splex_sid] = 1

    if not wbxutil.isNoneString(logmsg):
        logmsg = "Count of queue to %s from the same source sid are different;\n%s" % (db.db_name, logmsg)

    queuemsg = ""
    daoManager = daoManagerFactory.getDaoManager(dbid)
    spDao = daoManager.getDao(DaoKeys.DAO_SHAREPLEXMONITORDAO)
    daoManager.startTransaction()
    for port, portinfo in portdict.items():
        schemaname = wbxutil.getShareplexSchemanamebyPort(port)
        routingvos = spDao.getRoutingList(schemaname)
        for rvo in routingvos:
            csvlog = ""
            src_splex_sid = rvo.src_splex_sid
            if src_splex_sid in portinfo:
                queuecount = portinfo[src_splex_sid]
                msg = portinfo["msg"]
                if queuecount != rvo.queuecount:
                    msg = "%s The queuecount are different in Shareplex and DepotDB" % msg
                    queuemsg = "%s%-50s%-10s%-30s%-30s\n" % (queuemsg, src_splex_sid, port, rvo.queuecount, queuecount)
                else:
                    sucmsg = "%s queuecount=%d under port %s is the same from %s to %s\n" % (
                        sucmsg, rvo.queuecount, port, src_splex_sid, dbid)
                csvlog = "%s,%s,%s,%d,%d,%s\n" % (
                    src_splex_sid, tgt_splex_sid, port, rvo.queuecount, queuecount, msg)
            else:
                queuemsg = "%s%-50s%-10s%-30s%-30s\n" % (queuemsg, src_splex_sid, port, rvo.queuecount, 0)
                csvlog = "%s,%s,%s,%d,%d,%s\n" % (
                    src_splex_sid, tgt_splex_sid, port, rvo.queuecount, 0, "Error: Not found this channel in DepotDB")

            if not wbxutil.isNoneString(csvlog):
                logger.error(csvlog)

    daoManager.commit()

    if len(portdict) == 0:
        sucmsg = " no post queue \n"
    logger.info(sucmsg)

    if not wbxutil.isNoneString(queuemsg):
        queuemsg = "%-50s%-10s%-30s%-30s\n%s" % (
        "src_splex_sid", "port", "queuecount_in_shareplex", "queuecount_in_depotdb", queuemsg)

    if not wbxutil.isNoneString(logmsg) or not wbxutil.isNoneString(queuemsg):
        logmsg = "%s\n%s" % (logmsg, queuemsg)
        return logmsg

    return ""


def monitorAUDTable(dbid, sizethreshold):
    daoManager = None
    try:
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDaoManager(dbid)
        dbauditdao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
        audTableVO = dbauditdao.getTableSize("SYS", "AUD$")

        if audTableVO.bytes > sizethreshold:
            logmsg = "The %s.%s table size is %0.2f G on database %s which exceed the threshold %d G\n" % \
                     (
                     audTableVO.owner, audTableVO.segment_name, round(audTableVO.bytes / 1024.0 / 1024 / 1024, 2), dbid,
                     round(sizethreshold / 1024.0 / 1024 / 1024))
            logger.info(logmsg)
            return logmsg

    except Exception as e:
        logger.error("Exception occurred on dbid=%s as error: %s" % (dbid, e))
        logger.error(traceback.format_exc())
        if daoManager is not None:
            daoManager.rollback()
    finally:
        if daoManager is not None:
            daoManager.commit()

    return ""

def monitorDBParameter(dbid):
    daoManager = None
    try:
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDaoManager(dbid)
        dbauditdao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
        nlsparamList = dbauditdao.getNLSParameters()
        db = daoManagerFactory.getDatabaseByDBID(dbid)

        if db.appln_support_code not in ('TOOLS', 'AVWATCH'):
            for param in nlsparamList:
                expectedcharacterset = db.getExpectedCharacterSet()
                if param.parameter == "NLS_CHARACTERSET":
                    if param.value != expectedcharacterset:
                        logmsg = "Error: The database character set is %s, but it should be %s\n" % (
                        param.value, expectedcharacterset)

                        logger.info(logmsg)
                        return logmsg
    except Exception as e:
        logger.error("Exception occurred on dbid=%s as error: %s" % (dbid, e))
        logger.error(traceback.format_exc())
        if daoManager is not None:
            daoManager.rollback()
    finally:
        if daoManager is not None:
            daoManager.commit()

    return ""

# We changed CR monitor method, so this method is deprecated
def monitorShareplexCRDeployment(dbid):
    logger.info("start monitorShareplexCRDeployment(%s)" % dbid)
    global dbversiontable
    daoManager = None
    try:
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDaoManager(dbid)
        dbauditdao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
        defaultDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        db = daoManagerFactory.getDatabaseByDBID(dbid)
        if db.appln_support_code not in [wbxdatabasemanager.APPLN_SUPPORT_CODE_CONFIGDB,
                                         wbxdatabasemanager.APPLN_SUPPORT_CODE_WEBDB,
                                         wbxdatabasemanager.APPLN_SUPPORT_CODE_TAHOEDB,
                                         wbxdatabasemanager.APPLN_SUPPORT_CODE_BILLINGDB,
                                         wbxdatabasemanager.APPLN_SUPPORT_CODE_GLOOKUPDB,
                                         wbxdatabasemanager.APPLN_SUPPORT_CODE_TEODB,
                                         wbxdatabasemanager.APPLN_SUPPORT_CODE_MMP,
                                         wbxdatabasemanager.APPLN_SUPPORT_CODE_MEDIATE]:
            return

        ports = []

        redis = wbxredis.getRedis()
        for channelid, spchannel in db.shareplexchanneldict.items():
            if spchannel.tgt_db == db.db_name:
                src_host_name = spchannel.src_host
                sp_port = spchannel.port
                if sp_port in ports:
                    continue
                ports.append(sp_port)

                schemaname="SPLEX%s" % sp_port

                tablevo = dbauditdao.getTableByTableName(schemaname, "WBXDATABASEVERSION")
                if tablevo is not None:
                    if schemaname in dbversiontable:
                        tdbver = dbversiontable[schemaname]
                    else:
                        tdbver = type("WBXDATABASEVERSION", (object,), dict())
                        dbvertbl = Table(
                            "WBXDATABASEVERSION",
                            metadata,
                            Column('release_number', Integer, primary_key=True),
                            Column('major_number', Integer),
                            Column('minor_number', Integer),
                            Column('dbtype', String(56)),
                            Column('description', String(512)),
                            schema=schemaname
                        )
                        mapper(tdbver, dbvertbl)
                        dbversiontable[schemaname] = tdbver

                    dbvervo = dbauditdao.getDatabaseVersion(tdbver)
                    if dbvervo is not None:
                        release_number = dbvervo.release_number
                        major_number = dbvervo.major_number
                    else:
                        release_number = 0
                        major_number = 0
                else:
                    logger.info("no wbxdatabaseversion table with dbid=%s and schemaname=%s" % (db.getdbid(), schemaname))
                    release_number = -1
                    major_number = -1

                crdict = {}
                startdate = wbxutil.getcurrenttime(86400 * 30)
                enddate = wbxutil.getcurrenttime()

                if release_number > 0:
                    crdict = dbauditdao.getCRCountInLastDay(sp_port, startdate, enddate)

                keyname = "%s_%s_%s" % (db.trim_host, db.db_name, sp_port)
                delta = (enddate - startdate).days
                for i in range(delta):
                    crdate = (startdate + datetime.timedelta(days=i)).strftime("%Y%m%d")
                    if crdate in crdict:
                        crcount = crdict[crdate]
                    else:
                        crcount = 0
                    redis.lpush(keyname, "%s:%s" % (crdate, crcount))

                # trim list per week
                if enddate.strftime("%w") == 0:
                    redis.ltrim(keyname, 30)

                # Judge whether the CR workd or not, method is if cr occured in past 1 weeks, then means enabled, otherwise disabled
                recentcrdate = None
                crcntlist = redis.lrange(keyname,30)
                for strcrcnt in crcntlist:
                    crdate = datetime.datetime.strptime (strcrcnt.split(":")[0],"%Y%m%d")
                    crcnt = int((strcrcnt.split(":")[1]))
                    if crcnt > 0:
                        recentcrdate = crdate
                        break

                try:
                    defaultDaoManager.startTransaction()
                    deployvo = depotdbDao.getShareplexCRDeploymentVO(db.trim_host, db.db_name, sp_port)
                    if deployvo is not None:
                        deployvo.release_number = release_number
                        deployvo.major_number = major_number
                        deployvo.monitor_time = wbxutil.getcurrenttime()
                        deployvo.recentcrdate = recentcrdate
                    else:
                        deployvo = ShareplexCRDeploymentVO()
                        deployvo.release_number = release_number
                        deployvo.major_number = major_number
                        deployvo.trim_host = db.trim_host
                        deployvo.db_name = db.db_name
                        deployvo.port = sp_port
                        deployvo.monitor_time = wbxutil.getcurrenttime()
                        deployvo.recentcrdate = recentcrdate
                        depotdbDao.newShareplexCRDeploymentVO(deployvo)

                    logger.info("Check CR deployment with dbid=%s, port=%s, release_number=%s" % (db.getdbid(), sp_port, deployvo.release_number))
                    defaultDaoManager.commit()
                except Exception as e:
                    logger.error("Exception occurred during get data with dbid=%s as error: %s" % (dbid, e))
                    defaultDaoManager.rollback()

    except Exception as e:
        logger.error("Exception occurred on dbid=%s as error: %s" % (dbid, e))
        logger.error(traceback.format_exc())
        if daoManager is not None:
            daoManager.rollback()
    finally:
        if daoManager is not None:
            daoManager.commit()



# Daily job used to check database health
def dbHealthExamination(dbid, args, emailto, sendtospark):
    sucmsg = "dbHealthExamination(dbid=%s) start\n" % (dbid)
    argdict = {}
    if args is not None:
        argdict = json.loads(args)

    try:
        if "AUD_TABLE_SIZE" in argdict:
            sizethreshold = int(argdict["AUD_TABLE_SIZE"])
        else:
            sizethreshold = 1073741824

        # audmsg = monitorAUDTable(dbid, sizethreshold)
        # parammsg = monitorDBParameter(dbid)
        # monitorShareplexCRDeployment(dbid)
        parammsg = ""
        monitordblink(dbid)
        monitorCREnabled(dbid)
        getdbpatchDeployment(dbid)
        logger.info("dbHealthExamination(dbid=%s) succeed" % dbid)
    except Exception as e:
        errmsg = "Error occurred when execute dbHealthExamination(dbid=%s) with error msg %s" % (dbid,e)
        emailtitle = "%s on DB %s" % (wbxemailtype.EMAILTYPE_DBHEALTH_EXAM, dbid)
        emailmsg = wbxemailmessage(emailtitle, errmsg, receiver=emailto, issendtospark=sendtospark)
        sendalert(emailmsg)


