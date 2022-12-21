import logging
import threading
import traceback

from common.wbxchatbot import wbxchatbot
from dao.vo.depotdbvo import DBLinkMonitorResultVO
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from common.wbxutil import wbxutil

from sqlalchemy import Table, Column, MetaData, String, DateTime, Integer, and_, create_engine
from sqlalchemy.orm import mapper
from sqlalchemy.exc import  DBAPIError, DatabaseError
from sqlalchemy.pool import NullPool
from common.wbxexception import wbxexception

logger = logging.getLogger("DBAMONITOR")

def getDBlinkmonitordetail(trim_host,db_name,status):
    logger.info("getDBlinkmonitordetail trim_host=%s, db_name=%s, status=%s" % (trim_host,db_name,status))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rows = dao.getDBlinkmonitordetail(trim_host,db_name,status)
        res['data'] = [dict(vo) for vo in rows]
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    finally:
        if daoManager is not None:
            daoManager.close()
    return res

def checkDBlink(db_name,createby):
    logger.info("checkDBlink db_name=%s,createby=%s" % (db_name,createby))
    res = {"status": "SUCCESS", "errormsg": "", "msg": ""}

    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rows = dao.checkdbName(db_name)
        daoManager.commit()
        if len(rows) == 0:
            res["status"] = "FAILED"
            res["errormsg"] = "The DB Name %s is invalid." %(db_name)
            return res
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    finally:
        if daoManager is not None:
            daoManager.close()

    host_name = "sjdbormt065"
    if not createby:
        res['status'] = 'FAILED'
        res['errormsg'] = "No operator."
        return res
    people = wbxchatbot().get_people_cec(createby)
    if len(people) == 0:
        res['status'] = 'FAILED'
        res['errormsg'] = "The user is invalid."
        return res

    try:
        t = threading.Thread(target=execCheckDBLink, args=(host_name,db_name, createby))
        t.start()
    except Exception as e:
        res['status'] = 'FAILED'
        res['errormsg'] = str(e)
        logger.error("checkDBlink error occurred", exc_info=e, stack_info=True)
    res['msg'] = "Monitor dblink (%s) job has started, and I will send the results to you through Webex Teams. If you have any questions, please contact Le Xing." %(db_name)
    return res

def execCheckDBLink(host_name, db_name, createby):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    server = None
    try:
        server = daoManagerFactory.getServer(host_name)
        if server is not None:
            server.connect()
            cmd = "date"
            res1 = server.exec_command(cmd)
            logger.info(res1)
            cmd = ". /home/oracle/.bash_profile;python3 /home/oracle/project/wbxdbaudit/dbaudit.py DBLinkMonitor %s %s" %(db_name,createby)
            logger.info(cmd)
            server.exec_command(cmd)
    except Exception as e:
        raise e
    finally:
        if server is not None:
            server.close()


def generateDBLinkName(tgtdb, nameformat):
    if nameformat.find("<domainname>") >= 0:
        return nameformat.replace("<domainname>",tgtdb.web_domain)
    elif nameformat.find("<dbname>") >= 0:
        return nameformat.replace("<dbname>", tgtdb.db_name)
    elif nameformat.find("<clustername>") >= 0:
        return nameformat.replace("<clustername>", tgtdb.wbx_cluster.rjust(2,'d'))
    return nameformat

# This function only scan one db's dblink
# It will get all dblink baseline by appln_support_code
# Because each db has many different db link baseline, so it repeat baseline by baseline
# Each db has several schemas which fulfil one baseline, so it need to scan all schemas;
# Each schema may has several db link to different dbs, so it need to get target dbs by baseline target db type and appln_support_code
def monitordblink(dbid):
    logger.debug("start monitordblink(%s)" % dbid)

    try:
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDaoManager(dbid)
        dbauditdao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
        defaultDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        db = daoManagerFactory.getDatabaseByDBID(dbid)
        connectionurl = db.getConnectionURL()
        # Get dblink baseline list

        depotdbDao.deleteDBLinkMonitorResult(db.trim_host, db.db_name)
        defaultDaoManager.commit()

        baselinvoList = depotdbDao.listDBLinkBaseline(db.appln_support_code, db.db_type)
        for baselinevo in baselinvoList:
            apptypelist = baselinevo.application_type.split(",")
            tgtapptypelist = baselinevo.tgt_application_type.split(",")
            logger.info(baselinevo)

            if db.application_type in apptypelist:
                # Get and loop schema list under the schema type
                schemalist = db.getSchemaBySchemaType(baselinevo.appln_support_code, baselinevo.schematype)
                for schema in schemalist:
                    # logger.info(schema.schema)
                    schemaname = schema.schema
                    schemapwd = schema.password
                    # logger.info("start to monitor db link on %s under schema %s" % (dbid, schemaname))
                    tgtdblist = []
                    if baselinevo.schematype == "splex":
                        tgtdbdict = {}
                        for channelid, spchannel in db.shareplexchanneldict.items():
                            srcdbid = spchannel.getSourceDBID()
                            splexuser = "splex%s" % spchannel.port
                            if srcdbid == dbid:
                                continue
                            if schemaname.lower() != splexuser:
                                continue

                            srcdb = daoManagerFactory.getDatabaseByDBID(srcdbid)
                            if srcdb is not None:
                                if srcdb.appln_support_code == baselinevo.tgt_appln_support_code and srcdb.application_type in tgtapptypelist and srcdb.db_type == baselinevo.tgt_db_type:
                                    if baselinevo.dblinktarget is not None and baselinevo.dblinktarget == "failoverdb":
                                        if srcdb.appln_support_code == db.appln_support_code:
                                            tgtdbdict[srcdbid] = srcdb
                                    else:
                                        tgtdbdict[srcdbid] = srcdb
                        tgtdblist = tgtdbdict.values()
                    else:
                        if baselinevo.dblinktarget == "failoverdb":
                            failoverdb = db.failoverdb
                            tgtdblist.append(failoverdb)
                        else:
                            dblist = daoManagerFactory.getDBListByAppCode(baselinevo.tgt_appln_support_code)
                            for tgtdb in dblist:
                                if tgtdb.appln_support_code == baselinevo.tgt_appln_support_code and tgtdb.application_type in tgtapptypelist and tgtdb.db_type == baselinevo.tgt_db_type:
                                    tgtdblist.append(tgtdb)

                    # for tgtdb in tgtdblist:
                    #     print(tgtdb.getdbid())
                    # continue

                    # If above conditions all pass, then it means the user should have dblink, so build connection with the user and try to verify the db link
                    connect = None
                    try:
                        schemastatus="SUCCEED"
                        logger.debug("start monitordblink for dbid=%s, schema=%s" % (dbid, schemaname))
                        engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (schemaname, schemapwd, connectionurl),
                                               poolclass=NullPool, echo=False)
                        connect = engine.connect()
                        for tgtdb in tgtdblist:
                            if tgtdb.application_type in tgtapptypelist and tgtdb.db_type == baselinevo.tgt_db_type:
                                # Generate db link by db link template defined in baseline
                                dblinkName = generateDBLinkName(tgtdb, baselinevo.dblink_name)
                                status = None
                                errormsg = None
                                logger.debug("monitordblink(%s) under schema %s to dblink %s" % (dbid, schemaname, dblinkName))
                                cursor = None
                                try:
                                    SQL = "select sysdate from dual@%s" % dblinkName
                                    cursor = connect.execute(SQL)
                                    row = cursor.fetchone()
                                    curdate = row[0]
                                    status = "SUCCESS"

                                except DatabaseError as e:
                                    logger.error(
                                        "Error occurred: monitordblink(%s) under schema %s to dblink %s. Recreate it" % (dbid, schemaname, dblinkName))
                                    tgtschemaname = None
                                    tgtschema = None
                                    if baselinevo.tgt_schematype == "splex":
                                        if baselinevo.schematype == "splex":
                                            tgtschemaname = schemaname
                                        else:
                                            for channelid, spchannel in db.shareplexchanneldict.items():
                                                if spchannel.tgt_db == db.db_name:
                                                    tgtschemaname = "SPLEX%s" % spchannel.port
                                        if tgtschemaname is not None:
                                            tgtschema = tgtdb.getSchema(tgtschemaname)
                                    else:
                                        tgt_schemalist = tgtdb.getSchemaBySchemaType(baselinevo.tgt_appln_support_code, baselinevo.tgt_schematype)
                                        if tgt_schemalist is not None and len(tgt_schemalist) > 0:
                                            tgtschema = tgt_schemalist[0]
                                    if tgtschema is not None:
                                        try:
                                            dblinkSQL = "drop database link %s" % dblinkName
                                            connect.execute(dblinkSQL)
                                        except Exception as e:
                                            pass

                                        try:
                                            dblinkSQL="CREATE database link %s connect to %s identified by %s using '%s'" % (dblinkName, tgtschema.schema, tgtschema.password, tgtdb.getConnectionURL())
                                            connect.execute(dblinkSQL)
                                            cursor = connect.execute(SQL)
                                            row = cursor.fetchone()
                                            curdate = row[0]
                                            status = "SUCCESS"

                                        except Exception as e:
                                            errormsg = "error msg: %s" % e
                                            logger.error("Error occurred: monitordblink(%s) under schema %s to dbline %s with %s" % (dbid, schemaname, dblinkName, errormsg))
                                            status = "FAILED"

                                except Exception as e:
                                    errormsg = "error msg: %s" % e
                                    logger.error("Error occurred: monitordblink(%s) under schema %s to dbline %s with %s" % (dbid, schemaname, dblinkName, errormsg))
                                    status = "FAILED"
                                finally:
                                    if cursor is not None:
                                        cursor.close()

                                monitorvo = depotdbDao.getDBLinkMonitorResult(db.trim_host, db.db_name, schemaname, dblinkName)
                                if monitorvo is None:
                                    monitorvo = DBLinkMonitorResultVO()
                                    monitorvo.trim_host = db.trim_host
                                    monitorvo.db_name = db.db_name
                                    monitorvo.schema_name = schemaname
                                    monitorvo.dblink_name = dblinkName
                                    monitorvo.monitor_time = wbxutil.getcurrenttime()
                                    monitorvo.status = status
                                    monitorvo.errormsg = errormsg
                                    depotdbDao.insertDBLinkMonitorResult(monitorvo)
                                else:
                                    monitorvo.monitor_time = wbxutil.getcurrenttime()
                                    monitorvo.status = status
                                    monitorvo.errormsg = errormsg
                        logger.debug("end monitordblink for dbid=%s, schema=%s" % (dbid, schemaname))
                    except DatabaseError as e:
                        errormsg = "error msg: %s" % e
                        logger.error("Database Error occurred: monitordblink(%s) under schema %s with %s" % ( dbid, schemaname, errormsg))
                        logger.error(traceback.format_exc())
                        schemastatus = "FAILED"
                        schemaerrormsg = errormsg
                    except Exception as e:
                        errormsg = "error msg: %s" % e
                        logger.error("Exception occurred: monitordblink(%s) under schema %s with %s" % (dbid, schemaname, errormsg))
                        logger.error(traceback.format_exc())
                        schemastatus = "FAILED"
                        schemaerrormsg = errormsg
                    finally:
                        if connect is not None:
                            connect.close()

                    if schemastatus == "FAILED":
                        monitorvo = depotdbDao.getDBLinkMonitorResult(db.trim_host, db.db_name, schemaname, baselinevo.dblink_name)
                        if monitorvo is None:
                            monitorvo = DBLinkMonitorResultVO()
                            monitorvo.trim_host = db.trim_host
                            monitorvo.db_name = db.db_name
                            monitorvo.schema_name = schemaname
                            monitorvo.dblink_name = baselinevo.dblink_name

                            monitorvo.monitor_time = wbxutil.getcurrenttime()
                            monitorvo.status = schemastatus
                            monitorvo.errormsg = schemaerrormsg
                            depotdbDao.insertDBLinkMonitorResult(monitorvo)
                        else:
                            monitorvo.monitor_time = wbxutil.getcurrenttime()
                            monitorvo.status = schemastatus
                            monitorvo.errormsg = schemaerrormsg

            defaultDaoManager.commit()

            logger.info(baselinevo.db_type)
    except Exception as e:
        raise wbxexception("Error occurred in DBLinkMonitor.monitordblink(%s) with error msg: %s" % (dbid, e))
