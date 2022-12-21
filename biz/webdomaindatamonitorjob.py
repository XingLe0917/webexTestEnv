import logging
import traceback
from common.wbxutil import wbxutil
from common.wbxmail import sendalert, wbxemailmessage, wbxemailtype

from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from dao.vo.depotdbvo import wbxdatabasemanager, MeetingDataMonitorVO, WebDomainDataMonitorVO

logger = logging.getLogger("DBAMONITOR")

def getWebDomainMonitorData(dbid, args, emailto, sendtospark):
    daoManager = None
    try:
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDaoManager(dbid)
        dbauditdao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
        defaultDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        db = daoManagerFactory.getDatabaseByDBID(dbid)
        if db.appln_support_code != wbxdatabasemanager.APPLN_SUPPORT_CODE_WEBDB:
            return
        if db.application_type != wbxdatabasemanager.APPLICATION_TYPE_PRI:
            return
        if dbid in wbxdatabasemanager.FEDERAMP_DBS:
            return
        #
        # if dbid != "tadbormt011_RACLWEB":
        #     return

        starttime = wbxutil.getcurrenttime()
        logger.info("start to get Meeting Data with dbid=%s" % dbid)
        (case1, case2, case3, case4, case5) = dbauditdao.getMeetingUUIDDataWithDifferentConfID()
        # (case1, case2, case3, case4, case5) = (0,0,0,0,0)
        # errorcount = dbauditdao.getPasscodeAllocationLog()

        try:
            defaultDaoManager.startTransaction()
            meetingDatavo = depotdbDao.getMeetingDataMonitorVO(db.trim_host, db.db_name)
            if meetingDatavo is not None:
                meetingDatavo.case1 = case1
                meetingDatavo.case2 = case2
                meetingDatavo.case3 = case3
                meetingDatavo.case4 = case4
                meetingDatavo.case5 = case5
                meetingDatavo.monitor_time = wbxutil.getcurrenttime()
            else:
                meetingDatavo = MeetingDataMonitorVO()
                meetingDatavo.trim_host = db.trim_host
                meetingDatavo.db_name = db.db_name
                meetingDatavo.cluster_name = db.wbx_cluster
                meetingDatavo.case1 = case1
                meetingDatavo.case2 = case2
                meetingDatavo.case3 = case3
                meetingDatavo.case4 = case4
                meetingDatavo.case5 = case5
                meetingDatavo.monitor_time = wbxutil.getcurrenttime()
                depotdbDao.newMeetingDataMonitorVO(meetingDatavo)

            itemname = "PasscodeJobError"
            ncount = dbauditdao.getPasscodeAllocationLog()
            cfgvo = depotdbDao.getWebdomainDataMonitorVO(db.wbx_cluster, itemname)
            if cfgvo is not None:
                cfgvo.itemvalue = ncount
                cfgvo.monitortime = wbxutil.getcurrenttime()
            else:
                cfgvo = WebDomainDataMonitorVO()
                cfgvo.clustername = db.wbx_cluster
                cfgvo.itemname = itemname
                cfgvo.itemvalue = ncount
                cfgvo.monitortime = wbxutil.getcurrenttime()
                depotdbDao.newWebDomainDataMonitorVO(cfgvo)

            endtime = wbxutil.getcurrenttime()
            logger.info(
                "Get Meeting Data with dbid=%s, case1=%s, case2=%s, case3=%s, case4=%s, case5=%s, starttime=%s, endtime=%s" % (
                dbid, case1, case2, case3, case4, case5, wbxutil.convertDatetimeToString(starttime),
                wbxutil.convertDatetimeToString(endtime)))
            defaultDaoManager.commit()
        except Exception as e:
            logger.error("Exception occurred during get data with dbid=%s as error: %s" % (dbid, e))
            logger.error(traceback.format_exc())
            defaultDaoManager.rollback()
    except Exception as e:
        logger.error("Exception occurred on dbid=%s as error: %s" % (dbid, e))
        logger.error(traceback.format_exc())
        if daoManager is not None:
            daoManager.rollback()

def sendWebdomainDataMonitorReport(emailto):
    logger.info("sendWebdomainDataMonitorReport with emailto=%s" % emailto)
    daoManager = None
    try:
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        defaultDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        pjoblist = depotdbDao.listPasscodeJobError()
        emailmsg = ""
        for pjob in pjoblist:
            emailmsg="%s" \
                    "<tr><td>%s</td><td>%s</td><td>%s</td></tr>" % (emailmsg, pjob.clustername, "Y" if int(pjob.itemvalue) >0 else "N", pjob.monitortime)
        if not wbxutil.isNoneString(emailmsg):
            emailtitle = wbxemailtype.EMAILTYPE_WEBDOMAIN_DATA_MONITOR
            emailmsg = """<table border="1" bordercolor="black"><tr><td>Cluster Name</td><td>hasError</td><td>Monitor Time</td></tr>%s</table>""" % emailmsg
            emailobj = wbxemailmessage(emailtitle, emailmsg, receiver=emailto, issendtospark="N", emailformat=wbxemailtype.EMAIL_FORMAT_HTML)
            sendalert(emailobj)
    except Exception as e:
        logger.error("Exception occurred on calling sendWebdomainDataMonitorReport() as error: %s" % e, exc_info = True)
        if daoManager is not None:
            daoManager.rollback()