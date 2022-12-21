import logging
import traceback
import sys
import os
import datetime
import subprocess

from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from dao.vo.depotdbvo import DBPatchReleaseVO, DBPatchDeploymentVO, wbxoraexception
from common.wbxexception import wbxexception
from common.wbxmail import wbxemailmessage, wbxemailtype, sendalert
from dao.vo.jobvo import DBAMonitorJobVO
from common.wbxutil import wbxutil
from dao.vo.depotdbvo import wbxdatabasemanager, wbxschema

from sqlalchemy import Table, Column, MetaData, String, DateTime, Integer, and_, create_engine
from sqlalchemy.orm import mapper
from sqlalchemy.exc import  DBAPIError, DatabaseError
from sqlalchemy.pool import NullPool
import pandas as pd
from collections import OrderedDict
from common.wbxinfluxdb import wbxinfluxdb

logger = logging.getLogger("DBAMONITOR")

def getUserLoginFaileAuditdData(dbid, args, emailto, sendtospark):
    logger.debug("getUserLoginFaileAuditdData(dbid=%s) start" % (dbid))
    timerange = int(args)
    starttime = wbxutil.getcurrenttime(timerange)

    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDaoManager(dbid)
    dbauditdao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
    auditList = dbauditdao.getLoginFailedInfo(starttime)

    if len(auditList) > 0:
        currenttime = wbxutil.gettimestr()

        emailcontent = "The searched time range is from %s to %s\n" \
                       "%-30s%-30s%-30s%-30s\n" \
                       "------------------------------------------------------------------------------\n" % (
            starttime, currenttime, "OS_USERNAME", "USERNAME", "ClientHost", "Login_Failed_Count")

        for auditvo in auditList:
            emailcontent = "%s%-30s%-30s%-30s%-30d\n" % (
                emailcontent, auditvo.os_username, auditvo.username, auditvo.userhost, auditvo.failedcount)

        emailtitle = "%s on DB %s during recent %d minutes" % (
            wbxemailtype.EMAILTYPE_ACCOUNT_LOGIN_FAILED, dbid, timerange / 60)
        emailmsg = wbxemailmessage(emailtitle, emailcontent, receiver=emailto, issendtospark=sendtospark)
        sendalert(emailmsg)
    logger.debug("getUserLoginFaileAuditdData(dbid=%s) end" % (dbid))
    daoManager.commit()

def userLoginVerification(dbid, emailto, sendtospark):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    db = daoManagerFactory.getDatabaseByDBID(dbid)
    connectionurl = db.getConnectionURL()
    schemalist = db.schemadict.values()
    logmsg = ""

    for schema in schemalist:
        connect = None
        trytimes = 0
        schemaname = schema.schema
        schemapwd = schema.password
        schematype = schema.schematype
        # At here we do not  sharelex user, because shareplex monitor job will do it also.
        # We need to reduce the db effort
        if schematype in (wbxdatabasemanager.SCHEMATYPE_SPLEX, wbxdatabasemanager.SCHEMATYPE_BACKUP, wbxdatabasemanager.SCHEMATYPE_STAPRO, wbxdatabasemanager.SCHEMATYPE_DBA, wbxdatabasemanager.SCHEMATYPE_SPLEXDENY):
            continue

        while trytimes < 3:
            trytimes = trytimes + 1
            try:
                logger.debug("userLoginVerification for dbid=%s, schema=%s" % (dbid, schemaname))
                engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (schemaname, schemapwd, connectionurl),
                                       poolclass=NullPool, echo=False)
                connect = engine.connect()
                res = connect.execute("select sysdate as curtime from dual")
                res.close()
                connect.connection.commit()
                # connect.connection.close()
                trytimes = 4
                logmsg = ""
            except DatabaseError as e:
                if connect is not None:
                    connect.connection.rollback()
                errormsg = str(e)
                logger.error("The userLoginVerification job failed for schema %s with url=%s, error msg: %s" % (schema, connectionurl, errormsg))
                if trytimes >=3:
                    raise wbxexception(errormsg)
                if errormsg.find(wbxoraexception.ORA_CONNECTION_TIMEOUT) >= 0:
                    trytimes = 4
                elif errormsg.find(wbxoraexception.ORA_INVALID_USERNAME_PASSWORD) >= 0:
                    trytimes += 1
                    # Once user login failed, this tool will wait 60 seconds, then get the database info from DepotDB again;
                    # time.sleep(60)
                    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
                    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                    try:
                        db = depotdbDao.getDatabaseInfoByDBID(db.trim_host, db.db_name)
                        daoManagerFactory.addDatabase(dbid, db)

                        if schematype == wbxdatabasemanager.SCHEMATYPE_APP:
                            if trytimes == 1:
                                logmsg = "%s Error: The userLoginVerification job failed for schemaname=%s\n" % (
                                    logmsg, schemaname)
                                jobvo = DBAMonitorJobVO(jobname="CheckUserLoginFailedTimes",
                                                        dbnames=dbid,
                                                        job_type="interval",
                                                        frequency=300,
                                                        args="600",
                                                        func="getUserLoginFaileAuditdData",
                                                        start_time=wbxutil.getcurrenttime(),
                                                        end_time=wbxutil.getcurrenttime(-24 * 60 * 60),
                                                        emailto=emailto,
                                                        sendtospark=sendtospark)
                                from common.wbxjob import wbxjobmanager
                                jobmanager = wbxjobmanager.getJobManager()
                                jobmanager.mergejob(jobvo)
                        else:
                            logmsg = "%s Error: The userLoginVerification job failed for schemaname=%s\n" % (logmsg, schemaname)
                            trytimes = 4
                    except Exception as e:
                        trytimes = 4
                        logger.error(e)
                        logger.error(traceback.format_exc())
                    finally:
                        depotDaoManager.commit()

            except Exception as e:
                if connect is not None:
                    connect.connection.rollback()
                logger.error(e)
                logger.error("The userLoginVerification job failed for schema %s with url=%s" % (schema, connectionurl))
            else:
                pass
                logger.debug("userLoginVerification for dbid=%s, schema=%s succeed" % (dbid, schemaname))
            finally:
                if connect is not None:
                    connect.connection.close()
    return logmsg

def getShareplexDelaySRCDBData():
    status = "SUCCEED"
    result_data = None
    influx_db_obj = wbxinfluxdb()
    result_data = influx_db_obj.get_src_db_data()
    if not result_data:
        status = "FAILED"
    return {
        "status": status,
        "data": result_data
    }

def getShareplexDelay(start_time, end_time, src_db_name):
    # start_time = wbxutil.convertStringtoDateTime(start_time)
    # end_time = wbxutil.convertStringtoDateTime(end_time)
    influx_db_obj = wbxinfluxdb()
    dbList = influx_db_obj.get_tgt_db_data_by_src_db_name(src_db_name)
    # dbList = ["RACBWEB","RACAIWEB","RACASWEB","RACAPWEB"]
    dbResList = []
    for tgtdb in dbList:
        try:
            delayList = influx_db_obj.get_delay_by_src_db_name_and_time(start_time, end_time, src_db_name, tgtdb)
            repTimeList = []
            lineList = []
            for delay in delayList:
                repTimeList.append(delay['delayinsecond'])
                lineList.append({delay['time']:delay['delayinsecond']})
            # print(lineList)
            df = pd.DataFrame({"rep_time": repTimeList})
            aggdate = df["rep_time"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
            aggdict = OrderedDict()
            aggdict["count"] = aggdate["count"]
            aggdict["mean"] = round(aggdate["mean"],2)
            aggdict["50%"] = round(aggdate["50%"],2)
            aggdict["90%"] = round(aggdate["90%"],2)
            aggdict["99.5%"] = round(aggdate["99.5%"],2)
            aggdict["99.9%"] = round(aggdate["99.9%"],2)
            df["label"] = pd.cut(df["rep_time"], [0, 2, 4, 10, 60, 120, sys.maxsize*1.0],
                                 labels=["count_2s", "count_4s", "count_10s", "count_60s", "count_120s", "count_more"])
            dbresdict = {"DB_NAME": tgtdb,
                         "subtext": aggdict,
                         "line": lineList,
                         "data": df.groupby("label").count().to_dict()["rep_time"]}
            dbResList.append(dbresdict)
        except Exception as e:
            logger.error("getShareplexDelay(tgtdb=%s) error occurred" % tgtdb, exc_info = e)
    return dbResList

def getConfigDBShareplexDelay(str_start_time, str_end_time):
    logger.info("getConfigDBShareplexDelay from %s to %s" % (str_start_time, str_end_time))
    start_time = wbxutil.convertStringtoDateTime(str_start_time)
    end_time = wbxutil.convertStringtoDateTime(str_end_time)
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    dbList = ["RACBWEB","RACAIWEB","RACASWEB","RACAPWEB"]
    dbResList = []
    for dbid in dbList:
        daoManager = daoManagerFactory.getDaoManager(dbid)
        try:
            daoManager.startTransaction()
            dbauditdao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
            repTimeList = dbauditdao.getConfigDBReplicationDelay(start_time, end_time)
            df = pd.DataFrame({"rep_time": repTimeList})
            aggdate = df["rep_time"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
            aggdict = OrderedDict()
            aggdict["count"] = aggdate["count"]
            aggdict["mean"] = round(aggdate["mean"],2)
            aggdict["50%"] = round(aggdate["50%"],2)
            aggdict["90%"] = round(aggdate["90%"],2)
            aggdict["99.5%"] = round(aggdate["99.5%"],2)
            aggdict["99.9%"] = round(aggdate["99.9%"],2)
            df["label"] = pd.cut(df["rep_time"], [0, 2, 4, 10, 60, 120, sys.maxsize*1.0],
                                 labels=["count_2s", "count_4s", "count_10s", "count_60s", "count_120s", "count_more"])
            dbresdict = {"DB_NAME": dbid,
                         "subtext": aggdict,
                         "data": df.groupby("label").count().to_dict()["rep_time"]}
            dbResList.append(dbresdict)
            daoManager.commit()
        except Exception as e:
            daoManager.rollback()
            logger.error("getConfigDBShareplexDelay(dbid=%s) error occurred" % dbid, exc_info = e)
        finally:
            daoManager.close()
    return dbResList

def getMeetingDataReplicationDelay(cluster_name, str_start_time, str_end_time):
    resDict = {"status":"SUCCEED","errormsg":"", "aggdata":None, "meetingdata":None,"replicationdata":None,"cpudata":None}
    logger.info("getMeetingDataReplicationDelay from %s to %s, cluster_name=%s" % (str_start_time, str_end_time,cluster_name))
    start_time = wbxutil.convertStringtoDateTime(str_start_time)
    end_time = wbxutil.convertStringtoDateTime(str_end_time)
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    gsbdbnameMap = {"AB":"RACAVWEB","AA":"RACVVWEB","W":"RACAUWEB","AS":"RACSYWEB"}
    dbnameMap = {"AB": "RACABWEB", "AA": "RACAAWEB", "W": "RACWWEB", "AS": "RACASWEB"}
    db_name = gsbdbnameMap[cluster_name]
    daoManager = daoManagerFactory.getDaoManager(db_name)
    try:
        daoManager.startTransaction()
        dbauditdao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
        timepointlist, reptimelist, rowcountlist = dbauditdao.listMeetingDataReplicationData(start_time, end_time)
        # logger.info(rows)
        # maxdelaytimelist=[{row[0]:row[1]} for row in rows]
        if cluster_name == "AS":
            # Because AS/SY has 5 second time diff
            reptimelist = [reptime+5 for reptime in reptimelist]

        df = pd.DataFrame({"rep_time": reptimelist})
        aggdate = df["rep_time"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
        if len(reptimelist) == 0:
            aggdict={"count":0,"mean":0,"50%":0,"90%":0,"99.5%":0,"99.9%":0}
        else:
            aggdict = OrderedDict()
            aggdict["count"] = sum(rowcountlist)
            aggdict["mean"] = round(aggdate["mean"], 2)
            aggdict["50%"] = round(aggdate["50%"], 2)
            aggdict["90%"] = round(aggdate["90%"], 2)
            aggdict["99.5%"] = round(aggdate["99.5%"], 2)
            aggdict["99.9%"] = round(aggdate["99.9%"], 2)
        resDict["aggdata"] = aggdict
        resDict["meeting_timepoint"] = timepointlist
        resDict["meeting_reptime"] = reptimelist
        resDict["meeting_rowcount"] = rowcountlist
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        resDict["status"] = "FAILED"
        resDict["errormsg"] = str(e)
        logger.error("getMeetingDataReplicationDelay(cluster name=%s) error occurred" % cluster_name, exc_info=e)
    finally:
        daoManager.close()
    logging.info(resDict)
    logger.info("getMeetingDataReplicationDelay:listMeetingDataReplicationData end from %s to %s" % (str_start_time, str_end_time))

    db_name = dbnameMap[cluster_name]
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        resDict["replicationdata"] = depotdao.getwbxsplexreplicationdelaytime(db_name, start_time, end_time)
        resDict["cpudata"] = {"capture process":[],"read process":[],"export process":[],"import process":[],"post process":[]}
        rowlist = depotdao.getshareplexprocesscputime(db_name, start_time, end_time)
        for row in rowlist:
            process_type = "%s process" % row[0]
            resDict["cpudata"][process_type].append({row[1]: row[2] if row[2] > 0 else 0})

        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        resDict["status"] = "FAILED"
        resDict["errormsg"] = str(e)
        logger.error("getConfigDBShareplexDelay(cluster name=%s) error occurred" % cluster_name, exc_info=e)
    finally:
        daoManager.close()
    # logger.info(resDict)
    logger.info("getMeetingDataReplicationDelay end from %s to %s" % (str_start_time, str_end_time))
    return resDict

def listDBInstanceName(db_name):
    res = {"status": "SUCCESS", "errormsg": "","data":None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    db = daoManagerFactory.getDatabaseByDBName(db_name)
    if db is None:
        res["status"] = "FAILED"
        res["errormsg"] = "Can not find the db with db_name=%s" % db_name
        return res
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDaoManager(db_name)
    try:
        daoManager.startTransaction()
        dbauditdao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
        instanceDict = dbauditdao.getInstanceNameList()
        instanceDict["global"] = 0
        res["data"] = instanceDict
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        logger.error("generateAWRReport(db_name=%s) error occurred" % db_name, exc_info=e)
    finally:
        daoManager.close()
    return res

def generateAWRReport(db_name, str_start_time, str_end_time, instance_number,env):
    res = {"status":"SUCCESS","errormsg":"","data":None}
    if sys.platform == "win32":
        awr_dir = "C:\\Users\\zhiwliu\\Documents\\office\\oracle\\AWR\\AWRReport"
    else:
        awr_dir = ""
        if "china" == env:
            awr_dir = "/usr/share/nginx/html/awrreport"
        else:
            awr_dir = "/usr/local/nginx/html/awrreport"
    if not os.path.isdir(awr_dir):
        res["status"] = "FAILED"
        res["errormsg"] = "%s directory does not exist" % awr_dir
        return res

    timeformat = "%Y-%m-%d-%H-%M-%S"
    start_time = wbxutil.convertStringtoDateTime(str_start_time)
    end_time = wbxutil.convertStringtoDateTime(str_end_time)
    l_start_time = start_time.strftime(timeformat)
    l_end_time = end_time.strftime(timeformat)
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDaoManager(db_name)
    try:
        daoManager.startTransaction()
        dbauditdao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
        row = dbauditdao.getSnapshotID(instance_number,start_time, end_time)
        if row is not None:
            dbid = row[0]
            start_snap_id = row[1]
            end_snap_id = row[2]
            if end_snap_id == start_snap_id:
                end_snap_id += 1
            file_name = "awrreport_%s_%s_%s_%s_%s_%s.html" %(db_name, instance_number, l_start_time, l_end_time, start_snap_id, end_snap_id)
            reportfile = os.path.join(awr_dir, file_name)
            if not os.path.isfile(reportfile):
                rows = dbauditdao.getAWRReport(dbid, instance_number, start_snap_id, end_snap_id)
                with open(reportfile, "w") as f:
                    f.writelines(["" if row[0] is None else row[0] for row in rows])
                logger.info(reportfile)
                os.chmod(reportfile, 0o777)
        else:
            res["status"] = "FAILED"
            res["errormsg"] = "Does not find snapshot in the time range"
        daoManager.commit()

        if sys.platform != "win32":
        #     subprocess.Popen("find %s -mtime +7 -exec rm -rf {} \;" % awr_dir)
            os.system("find %s -mtime +7 -exec rm -rf {} \;" % awr_dir)

    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        logger.error("generateAWRReport(db_name=%s) error occurred" % db_name, exc_info=e)
    finally:
        daoManager.close()
    return res

def generateASHReport(db_name, str_start_time, str_end_time, instance_number,env):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    if sys.platform == "win32":
        ash_dir = "C:\\Users\\zhiwliu\\Documents\\office\\oracle\\ASH\\ASHReport"
    else:
        ash_dir = ""
        if "china" == env:
            ash_dir = "/usr/share/nginx/html/ashreport"
        else:
            ash_dir = "/usr/local/nginx/html/ashreport"
    if not os.path.isdir(ash_dir):
        res["status"] = "FAILED"
        res["errormsg"] = "%s directory does not exist" % ash_dir
        return res

    timeformat1 = "%Y-%m-%d-%H-%M-%S"
    timeformat = "%Y-%m-%d %H:%M:%S"
    start_time = wbxutil.convertStringtoDateTime(str_start_time)
    end_time = wbxutil.convertStringtoDateTime(str_end_time)
    l_start_time = datetime.datetime.strptime(str_start_time, timeformat)
    l_end_time = datetime.datetime.strptime(str_end_time, timeformat)
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDaoManager(db_name)
    try:
        daoManager.startTransaction()
        dbauditdao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
        row = dbauditdao.getSnapshotID(instance_number, start_time, end_time)
        if row is not None:
            dbid = row[0]
            start_snap_id = row[1]
            end_snap_id = row[2]
            if end_snap_id == start_snap_id:
                end_snap_id += 1
            file_name = "ashreport_%s_%s_%s_%s_%s_%s.html" % (
            db_name, instance_number, start_time.strftime(timeformat1), end_time.strftime(timeformat1), start_snap_id, end_snap_id)
            reportfile = os.path.join(ash_dir, file_name)
            if not os.path.isfile(reportfile):
                rows = dbauditdao.getASHReport(dbid, instance_number, l_start_time, l_end_time)
                with open(reportfile, "w") as f:
                    f.writelines(["" if row[0] is None else row[0] for row in rows])
                logger.info(reportfile)
                os.chmod(reportfile, 0o777)
        else:
            res["status"] = "FAILED"
            res["errormsg"] = "Does not find snapshot in the time range"
        daoManager.commit()

        if sys.platform != "win32":
            # subprocess.Popen("find %s -mtime +7 -exec rm -rf {} \;" % ash_dir)
            os.system("find %s -mtime +7 -exec rm -rf {} \;" % ash_dir)
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        logger.error("generateASHReport(db_name=%s) error occurred" % db_name, exc_info=e)
    finally:
        daoManager.close()
    return res

