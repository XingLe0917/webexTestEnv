import json
import logging

from dao.vo.alertvo import AlertVo
from dao.wbxdaomanager import wbxdao
from dao.vo.depotdbvo import wbxdatabasemanager, wbxdatabase, wbxshareplexchannel, wbxschema, wbxserver, wbxmappinginfo, \
    wbxadbmon, wbxinstance, DBPatchDeploymentVO, DBPatchReleaseVO, ShareplexBaselineVO, ShareplexCRDeploymentVO, \
    MeetingDataMonitorVO, \
    DBLinkBaselineVO, DBLinkMonitorResultVO, WebDomainDataMonitorVO, wbxloginuser, wbxdbwaiteventvo
from common.wbxexception import wbxexception
from sqlalchemy import text, or_, func, and_, literal_column
from sqlalchemy.exc import DBAPIError
from common.wbxutil import wbxutil

logger = logging.getLogger("DBAMONITOR")


class DepotDBDao(wbxdao):

    def getDatabaseInfo(self):
        session = self.getLocalSession()
        # dbList = session.query(wbxdatabase).filter(and_(wbxdatabase.db_type == "PROD")). \
        dbList = session.query(wbxdatabase).filter(or_(wbxdatabase.db_type == "PROD",wbxdatabase.db_type == "BTS_PROD")). \
            filter(wbxdatabase.db_vendor == "Oracle").all()
        # dbList = session.query(wbxdatabase).filter(wbxdatabase.trim_host.like('%sjdbth39%')).all()
        return dbList

    def getDatabaseInfoByDBID(self, trimhost, dbname):
        try:
            session = self.getLocalSession()
            db = session.query(wbxdatabase).filter(
                and_(wbxdatabase.trim_host == trimhost, wbxdatabase.db_name == dbname)).first()
            if db is None:
                return None

            db.__init__()
            instanceList = session.query(wbxinstance).filter(
                and_(wbxinstance.trim_host == trimhost, wbxinstance.db_name == dbname)).all()
            schemaList = session.query(wbxschema).from_statement(text(
                "select TRIM_HOST,DB_NAME,upper(APPLN_SUPPORT_CODE) as APPLN_SUPPORT_CODE,SCHEMA,f_get_deencrypt(PASSWORD) as password, "
                "DATE_ADDED,LASTMODIFIEDDATE,CREATED_BY,MODIFIED_BY,KM_VERSION,SCHEMATYPE,f_get_deencrypt(NEW_PASSWORD) as NEW_PASSWORD,"
                "TRACK_ID,CHANGE_STATUS "
                "from appln_pool_info "
                "where trim_host='%s'"
                "and db_name='%s'"
                "and upper(schema) != 'GRIDVIEW'"
                "order by db_name, schematype" % (trimhost, dbname))).all()

            # # return schemaList
            #
            # schemalist = session.query(wbxschema).filter(
            #     and_(wbxschema.trim_host == trimhost, wbxschema.db_name == dbname, wbxschema.schematype != wbxdatabasemanager.SCHEMATYPE_BACKUP)).order_by(wbxschema.db_name, wbxschema.schematype).all()
            hostlist = session.query(wbxserver).filter(wbxserver.trim_host == trimhost).all()

            spchannellist = session.query(wbxshareplexchannel).filter(
                or_(wbxshareplexchannel.src_db == dbname, wbxshareplexchannel.tgt_db == dbname)).all()
            for schema in schemaList:
                db.addSchema(schema)

            schema = wbxschema(trim_host=db.trim_host, db_name=db.db_name, appln_support_code=db.appln_support_code,
                               schema="SYSTEM", password="sysnotallow", new_password="sysnotallow", schematype="dba")
            db.addSchema(schema)

            for instance in instanceList:
                db.addInstance(instance)

            for host in hostlist:
                db.addServer(host)
            for spchannel in spchannellist:
                if spchannel.src_host.find(trimhost) >= 0 or spchannel.tgt_host.find(trimhost) >= 0:
                    db.addShareplexChannel(spchannel)
            return db
        except DBAPIError as e:
            logger.error(e)
            raise wbxexception(
                "Error occured when execute depotdbDao.getDatabaseInfoByDBID(%s, %s)" % (trimhost, dbname))

    def addDatabaseInfo(self, db):
        session = self.getLocalSession()
        session.add(db)

    def getDatabaseInfoByDBName(self, db_name):
        session = self.getLocalSession()
        dbvo = session.query(wbxdatabase).filter(wbxdatabase.db_name == db_name).first()
        return dbvo

    def getInstanceInfoByDBName(self, db_name, db_type):
        session = self.getLocalSession()
        rows = session.query(wbxdatabase, wbxinstance, wbxserver). \
            filter(and_(wbxinstance.db_name == wbxdatabase.db_name, wbxinstance.trim_host == wbxdatabase.trim_host,
                        wbxdatabase.db_name == db_name,
                        wbxdatabase.db_type == db_type, wbxinstance.host_name == wbxserver.host_name)) \
            .all()
        return rows

    def getLoginUser(self, hostnameList):
        session = self.getLocalSession()
        userList = session.query(wbxloginuser.host_name, literal_column('f_get_deencrypt(pwd)').label("pwd")).filter(
            wbxloginuser.host_name.in_(hostnameList)).all()
        return userList

    def getInstanceInfo(self):
        session = self.getLocalSession()
        instanceList = session.query(wbxinstance).all()
        return instanceList

    def getShareplexChannel(self):
        session = self.getLocalSession()
        channelList = session.query(wbxshareplexchannel).all()
        return channelList

    def getShareplexChannelByTargetDBAndPort(self, tgt_db, port):
        session = self.getLocalSession()
        channelList = session.query(wbxshareplexchannel).filter(
            and_(wbxshareplexchannel.tgt_db == tgt_db, wbxshareplexchannel.port == port)).all()
        return channelList

    def getShareplexChannelByDBName(self, db_name, hostnameList):
        session = self.getLocalSession()
        spch = session.query(wbxshareplexchannel).filter(
            or_(and_(wbxshareplexchannel.src_db == db_name, wbxshareplexchannel.src_host.in_(hostnameList)),
                and_(wbxshareplexchannel.tgt_db == db_name, wbxshareplexchannel.tgt_host.in_(hostnameList)))).all()
        return spch

    def getShareplexChannelByChannelid(self, channelid):
        session = self.getLocalSession()
        spch = session.query(wbxshareplexchannel).filter(wbxshareplexchannel.channelid == channelid).first()
        return spch

    def addShareplexChannel(self, spchannel):
        session = self.getLocalSession()
        session.add(spchannel)

    def deleteShareplexChannel(self, spchannel):
        session = self.getLocalSession()
        session.delete(spchannel)

    def getSchemaList(self):
        session = self.getLocalSession()
        schemaList = session.query(wbxschema).from_statement(text(
            " select TRIM_HOST,DB_NAME,upper(APPLN_SUPPORT_CODE) as APPLN_SUPPORT_CODE,SCHEMA,f_get_deencrypt(PASSWORD) as password, "
            "        DATE_ADDED,LASTMODIFIEDDATE,CREATED_BY,MODIFIED_BY,KM_VERSION,SCHEMATYPE,f_get_deencrypt(NEW_PASSWORD) as NEW_PASSWORD,"
            "        TRACK_ID,CHANGE_STATUS "
            " from appln_pool_info "
            " WHERE lower(schema) != 'gridview'"
            "order by trim_host, db_name, schematype")).all()
        return schemaList

    def getSchemaListByDBName(self, db_name):
        session = self.getLocalSession()
        schemaList = session.query(wbxschema).from_statement(text(
            " select TRIM_HOST,DB_NAME,upper(APPLN_SUPPORT_CODE) as APPLN_SUPPORT_CODE,SCHEMA,f_get_deencrypt(PASSWORD) as password, "
            "        DATE_ADDED,LASTMODIFIEDDATE,CREATED_BY,MODIFIED_BY,KM_VERSION,SCHEMATYPE,f_get_deencrypt(NEW_PASSWORD) as NEW_PASSWORD,"
            "        TRACK_ID,CHANGE_STATUS "
            " from appln_pool_info "
            " WHERE lower(schema) != 'gridview'"
            " AND db_name='%s'"
            "order by trim_host, db_name, schematype" % db_name)).all()
        return schemaList

    def getLoginUserList(self):
        session = self.getLocalSession()
        userList = session.query(wbxloginuser).from_statement(text(
            " select TRIM_HOST, HOST_NAME, USERNAME, f_get_deencrypt(pwd) as pwd"
            " from host_user_info WHERE lower(username)='oracle'")).all()
        return userList

    def getServerList(self):
        session = self.getLocalSession()
        serverList = session.query(wbxserver).order_by(wbxserver.trim_host).all()
        return serverList

    def addServer(self, server):
        session = self.getLocalSession()
        session.add(server)

    def deleteServer(self, server):
        session = self.getLocalSession()
        session.delete(server)

    def getMappingInfoList(self):
        session = self.getLocalSession()
        # mappingList = session.query(wbxmappinginfo).filter(func.lower(wbxmappinginfo.appln_support_code).in_(['MON_GRID', 'config', 'tel', 'lookup', 'opdb'])).all()
        mappingList = session.query(wbxmappinginfo).all()
        return mappingList

    def deleteSchema(self, schema):
        session = self.getLocalSession()
        session.delete(schema)

    def addSchema(self, schema):
        session = self.getLocalSession()
        session.add(schema)

    def getadbmon(self, src_db, src_host, port, tgt_db, tgt_host, replication_to):
        session = self.getLocalSession()
        adbmonvo = session.query(wbxadbmon).filter(
            and_(wbxadbmon.src_host == src_host, wbxadbmon.src_db == src_db, wbxadbmon.port == port,
                 wbxadbmon.tgt_host == tgt_host, wbxadbmon.tgt_db == tgt_db,
                 wbxadbmon.replication_to == replication_to)). \
            first()
        return adbmonvo

    def listadbmon(self, searchstarttime, delaytime):
        session = self.getLocalSession()
        adbmonvolist = session.query(wbxadbmon, literal_column('trunc((sysdate-lastreptime) * 24 * 60 * 60)').label(
            "delaytimeinsec")). \
            filter(
            and_(wbxadbmon.montime >= searchstarttime, text('(sysdate-lastreptime) * 24 * 60 * 60 > %d' % delaytime),
                 wbxadbmon.port >= 6000)). \
            order_by(wbxadbmon.port). \
            all()
        return adbmonvolist

    def addadbmon(self, adbmonvo):
        session = self.getLocalSession()
        session.add(adbmonvo)

    def deleteadbmon(self, adbmonvo):
        session = self.getLocalSession()
        session.delete(adbmonvo)

    def deleteShareplexBaseline(self, releasenumber, src_appln_support_code, tgt_appln_support_code):
        session = self.getLocalSession()
        session.query(ShareplexBaselineVO).filter(and_(ShareplexBaselineVO.releasenumber == releasenumber,
                                                       ShareplexBaselineVO.src_appln_support_code == src_appln_support_code,
                                                       ShareplexBaselineVO
                                                       .tgt_appln_support_code == tgt_appln_support_code)).delete()

    def getpreviousRelease(self, appln_support_code, schematype):
        session = self.getLocalSession()
        SQL = " select releasenumber " \
              " from (" \
              "     select releasenumber " \
              "     from (" \
              "         select releasenumber, lpad(major_number,4, '0')||lpad(minor_number,4, '0') as curver, createtime,  max(lpad(major_number,4, '0')||lpad(minor_number,4, '0'))  over ()  as maxver " \
              "         from wbxdbpatchrelease " \
              "         where appln_support_code='%s' and schematype='%s'" \
              "         and releasenumber!=15896 " \
              "     ) where curver=maxver" \
              "      order by createtime desc" \
              " ) where rownum=1" % (appln_support_code, schematype)
        row = session.execute(SQL).fetchone()
        if row is None:
            return -1
        else:
            return row[0]

    def mergeShareplexBaseline(self, prevreleasenumber, curreleasenumber, src_appln_support_code, src_schematype):
        session = self.getLocalSession()
        SQL = " insert into wbxshareplexbaseline(releasenumber, SRC_APPLN_SUPPORT_CODE, src_schematype, SRC_TABLENAME, TGT_APPLN_SUPPORT_CODE, tgt_schematype, TGT_TABLENAME, " \
              "   TABLESTATUS, SPECIFIEDKEY, COLUMNFILTER, SPECIFIEDCOLUMN, CHANGERELEASE, TGT_APPLICATION_TYPE)" \
              " select %s,SRC_APPLN_SUPPORT_CODE, src_schematype, SRC_TABLENAME, TGT_APPLN_SUPPORT_CODE, tgt_schematype, TGT_TABLENAME, " \
              "    TABLESTATUS, SPECIFIEDKEY, COLUMNFILTER, SPECIFIEDCOLUMN, CHANGERELEASE, TGT_APPLICATION_TYPE" \
              " from wbxshareplexbaseline" \
              " where releasenumber=%s" \
              " and SRC_APPLN_SUPPORT_CODE='%s'" \
              " and SRC_SCHEMATYPE='%s'" \
              " and (releasenumber,SRC_APPLN_SUPPORT_CODE,TGT_APPLN_SUPPORT_CODE,SRC_TABLENAME,TGT_TABLENAME) " \
              "     not in (" \
              "          select %s,SRC_APPLN_SUPPORT_CODE,TGT_APPLN_SUPPORT_CODE,SRC_TABLENAME,TGT_TABLENAME" \
              "          from wbxshareplexbaseline" \
              "          where releasenumber=%s" \
              "          and SRC_APPLN_SUPPORT_CODE='%s' " \
              "          and SRC_SCHEMATYPE='%s' " \
              "      )" % (
                  curreleasenumber, prevreleasenumber, src_appln_support_code, src_schematype, prevreleasenumber,
                  curreleasenumber, src_appln_support_code, src_schematype)
        logger.info(SQL)
        session.execute(SQL)

    def getDBPatchRelease(self, releasenumber, appln_support_code, schematype):
        session = self.getLocalSession()
        return session.query(DBPatchReleaseVO).filter(and_(DBPatchReleaseVO.releasenumber == releasenumber,
                                                           DBPatchReleaseVO.appln_support_code == appln_support_code,
                                                           DBPatchReleaseVO.schematype == schematype)).first()

    def getDBPatchReleaseByReleaseNumber(self, releasenumber):
        session = self.getLocalSession()
        return session.query(DBPatchReleaseVO).filter(DBPatchReleaseVO.releasenumber == releasenumber).first()

    def addDBPatchRelease(self, dbpatchReleasevo):
        session = self.getLocalSession()
        session.add(dbpatchReleasevo)

    def getDBPatchDeployment(self, releasenumber, dbname, trimhost, schemaname):
        session = self.getLocalSession()
        return session.query(DBPatchDeploymentVO).filter(and_(DBPatchDeploymentVO.releasenumber == releasenumber,
                                                              DBPatchDeploymentVO.db_name == dbname,
                                                              DBPatchDeploymentVO.trim_host == trimhost,
                                                              DBPatchDeploymentVO.schemaname == schemaname)).first()

    def addDBPatchDeployment(self, dbpatchDeploymentvo):
        session = self.getLocalSession()
        session.add(dbpatchDeploymentvo)

    def addDBPatchSPChange(self, spvo):
        session = self.getLocalSession()
        session.add(spvo)

    def getLatestDBPatchRelease(self, limitcnt):
        session = self.getLocalSession()
        SQL = " select ta.releasenumber, min(ta.mindeploytime) as mindeploytime, max(ta.maxdeploytime) as maxdeploytime, " \
              "        count(1) expecteddbcount, sum(ta.isdeployed) deployedcount, decode(count(1), sum(ta.isdeployed),0,1) as ismissed, tb.description" \
              " from (" \
              "     select releasenumber, appln_support_code, trim_host, cluster_name, mindeploytime, maxdeploytime," \
              "            case when noclusterdeploystatus> 0 and clusterdeploystatus > 0 then 1 when noclusterdeploystatus is null and clusterdeploystatus > 0 then 1 else 0 end isdeployed" \
              "     from (" \
              "           select distinct releasenumber, appln_support_code, trim_host, db_name, cluster_name, schematype," \
              "                 min(deploytime) over (partition by releasenumber, db_name) mindeploytime," \
              "                 max(deploytime) over (partition by releasenumber, db_name) maxdeploytime," \
              "                 sum(decode(schematype,'app',NULL, decode(deploystatus,'DEPLOYED',1,'SKIPPED',1,0))) over (partition by releasenumber, trim_host, db_name) noclusterdeploystatus," \
              "                 sum(decode(schematype,'app',decode(deploystatus,'DEPLOYED',1,'SKIPPED',1,0),NULL)) over (partition by releasenumber, trim_host, cluster_name) clusterdeploystatus" \
              "           from wbxdbpatchdeployment" \
              "           where appln_support_code='TEL'" \
              "          ) where schematype='app'" \
              "    union all" \
              "    select releasenumber, appln_support_code, trim_host, cluster_name, min(deploytime) as mindeploytime, max(deploytime) as maxdeploytime," \
              "           case when sum(decode(deploystatus,'DEPLOYED',1,'SKIPPED',1,0)) > 0 then 1 else 0 end as isdeployed" \
              "    from wbxdbpatchdeployment" \
              "    where appln_support_code!='TEL'" \
              "    group by releasenumber, appln_support_code, trim_host, cluster_name" \
              "    ) ta, " \
              "   (select distinct releasenumber,appln_support_code,upper(description) as description from wbxdbpatchrelease tb) tb" \
              " where ta.releasenumber=tb.releasenumber" \
              " and ta.appln_support_code=tb.appln_support_code" \
              " group by ta.releasenumber,tb.description" \
              " order by ismissed desc, maxdeploytime desc"

        releaselist = session.execute(SQL).fetchmany(limitcnt)
        return releaselist
        #
        # subq = session.query(DBPatchDeploymentVO.releasenumber,
        #                      func.max(DBPatchDeploymentVO.deploytime).label("maxdeploytime"),
        #                      func.min(DBPatchDeploymentVO.deploytime).label("mindeploytime"),
        #                      func.count(DBPatchDeploymentVO.releasenumber).label("expecteddbcount"),
        #                      func.sum(text("decode(deploystatus,'DEPLOYED',1,'SKIPPED',1,0)")).label("deployedcount")). \
        #     group_by(DBPatchDeploymentVO.releasenumber). \
        #     subquery()
        # releaselist = session.query(subq.c.releasenumber, subq.c.maxdeploytime, subq.c.mindeploytime,
        #                             subq.c.expecteddbcount, subq.c.deployedcount,
        #                             literal_column("decode(expecteddbcount, deployedcount,0,1)").label("ismissed")). \
        #     order_by(text("ismissed desc"), subq.c.maxdeploytime.desc()).limit(limitcnt).all()
        # return releaselist

    def getDBPatchDeploymentByReleaseNumber(self, releasenumber):
        session = self.getLocalSession()
        deploylist = session.query(DBPatchDeploymentVO.db_type, DBPatchDeploymentVO.appln_support_code,
                                   DBPatchDeploymentVO.cluster_name,
                                   DBPatchDeploymentVO.deploystatus, DBPatchDeploymentVO.spdeploystatus,
                                   DBPatchDeploymentVO.change_id, DBPatchDeploymentVO.change_sch_start_date,
                                   DBPatchDeploymentVO.change_completed_date, DBPatchDeploymentVO.change_imp,
                                   literal_column("decode(deploystatus,'DEPLOYED',0,'SKIPPED',0,1)").label(
                                       "isundelployed")). \
            filter(DBPatchDeploymentVO.releasenumber == releasenumber). \
            order_by(text("isundelployed desc, CHANGE_COMPLETED_DATE")). \
            all()
        return deploylist

    def listScheduledChange(self, trim_host, db_name, starttime):
        session = self.getLocalSession()
        deploylist = session.query(DBPatchDeploymentVO). \
            filter(and_(DBPatchDeploymentVO.db_name == db_name, DBPatchDeploymentVO.trim_host == trim_host,
                        DBPatchDeploymentVO.change_completed_date.is_(None),
                        DBPatchDeploymentVO.createtime > starttime)). \
            all()
        return deploylist

    def getshareplexportlist(self):
        session = self.getLocalSession()
        SQL = " select distinct si.src_db, si.src_splex_sid, si.port, si.src_host, hi.ssh_port " \
              " from database_info di, shareplex_info si, instance_info ii, host_info hi" \
              " WHERE di.db_name=si.src_db" \
              " and di.db_type in ('PROD')" \
              " and di.db_vendor='Oracle'" \
              " and di.db_name=ii.db_name" \
              " and di.trim_host=ii.trim_host" \
              " and ii.host_name=hi.host_name" \
              " and si.src_db=di.db_name" \
              " and si.src_host=hi.host_name" \
              " AND di.appln_support_code in ('CONFIG','WEB')" \
              " AND di.db_name not in ('RACFWEB','RACAFWEB','TSJ35','TTA35','RACINTH','RACFMMP','RACFTMMP')" \
              " AND hi.host_name not in ('sjdbcfg12','tadbth421','tadbth422','tadbth381','sjdbcnt2','rsdbcnt3','rsdbcnt4')" \
              " AND si.port != 15003"
        # " and di.db_name in ('RACIBWEB','RACOPDB','TSJCOMB1','SJLOOKUP')"
        # " and di.appln_support_code in ('CONFIG','WEB','')"

        portlist = session.execute(SQL).fetchall()
        return portlist

    """
    select src_splex_sid, src_appln_support_code,tgt_splex_sid, tgt_appln_support_code, to_char(max(MONITORTIME),'YYYY-MM-DD hh24:mi:ss') monitortime, 
           sum(decode(status,'SAME',0,1)) diffcnt 
    from wbxshareplexmonitordetail 
    group by src_splex_sid, src_appln_support_code,tgt_splex_sid, tgt_appln_support_code 
    order by diffcnt desc ,src_splex_sid,tgt_appln_support_code desc
    """

    def getShareplexMonitorSummary(self):
        session = self.getLocalSession()
        SQL = '''
        select src_splex_sid, src_appln_support_code,tgt_splex_sid, tgt_appln_support_code, to_char(max(MONITORTIME),'YYYY-MM-DD hh24:mi:ss') monitortime, 
           sum(decode(status,'SAME',0,1)) diffcnt 
    from wbxshareplexmonitordetail where tgt_tablename not in ('WBXMEETINGZONE_GSB','WBXSITEMEETINGDOMAIN_GSB','SPLEX_REP_MINITOR','WBXBILLINGGROUP','WBXBILLINGGROUPUSER','WBXBILLINGGROUPCONFIG','GCFGDB_REP_MONITOR')
    group by src_splex_sid, src_appln_support_code,tgt_splex_sid, tgt_appln_support_code 
    order by src_splex_sid, diffcnt desc
        '''
        sumlist = session.execute(SQL).fetchall()
        # return sumlist
        return [dict(row) for row in sumlist]

    def listShareplexMonitorDetail(self):
        session = self.getLocalSession()
        SQL = '''
        SELECT src_splex_sid, src_appln_support_code, src_tablename, tgt_splex_sid, tgt_appln_support_code,
              tgt_tablename, specifiedkey, columnfilter, specifiedcolumn, status
              FROM wbxshareplexmonitordetail
              WHERE status !='SAME'
              and tgt_tablename not in ('WBXMEETINGZONE_GSB','WBXSITEMEETINGDOMAIN_GSB','SPLEX_REP_MINITOR','WBXBILLINGGROUP','WBXBILLINGGROUPUSER','WBXBILLINGGROUPCONFIG','GCFGDB_REP_MONITOR')
        '''
        tablist = session.execute(SQL).fetchall()
        # return tablist
        return [dict(row) for row in tablist]

    def newShareplexCRDeploymentVO(self, crvo):
        session = self.getLocalSession()
        session.add(crvo)

    def getShareplexCRDeploymentVO(self, trim_host, db_name, port):
        session = self.getLocalSession()
        deploylist = session.query(ShareplexCRDeploymentVO). \
            filter(and_(ShareplexCRDeploymentVO.db_name == db_name, ShareplexCRDeploymentVO.trim_host == trim_host,
                        ShareplexCRDeploymentVO.port == port)). \
            first()
        return deploylist

    def listShareplexCRDeploymentDetail(self):
        session = self.getLocalSession()
        SQL = " SELECT trim_host, db_name, port, decode(release_number,15845,'CR2.0','15790','CR1.0','Not Deployed') crversion,decode(release_number,15845,0,15790,1,2) crversionno, " \
              " monitor_time, recentcrdate  " \
              " FROM wbxshareplexcrdeployment " \
              " ORDER BY crversionno desc, recentcrdate, db_name"
        tablist = session.execute(SQL).fetchall()
        return tablist

    def getMeetingDataMonitorVO(self, trim_host, db_name):
        session = self.getLocalSession()
        meetingDatavo = session.query(MeetingDataMonitorVO).filter(
            and_(MeetingDataMonitorVO.db_name == db_name, MeetingDataMonitorVO.trim_host == trim_host)).first()
        return meetingDatavo

    def newMeetingDataMonitorVO(self, meetingDatavo):
        session = self.getLocalSession()
        session.add(meetingDatavo)

    def listMeetingDataMonitorDetail(self):
        session = self.getLocalSession()
        meetingDataList = session.query(MeetingDataMonitorVO).order_by(MeetingDataMonitorVO.case5.desc()).all()
        return meetingDataList

    def getWebdomainDataMonitorVO(self, clustername, itemname):
        session = self.getLocalSession()
        monitorvo = session.query(WebDomainDataMonitorVO).filter(and_(WebDomainDataMonitorVO.clustername == clustername,
                                                                      WebDomainDataMonitorVO.itemname == itemname)).first()
        return monitorvo

    def newWebDomainDataMonitorVO(self, cfgvo):
        session = self.getLocalSession()
        session.add(cfgvo)

    def listPasscodeJobError(self):
        session = self.getLocalSession()
        dataList = session.query(WebDomainDataMonitorVO).filter(
            WebDomainDataMonitorVO.itemname == 'PasscodeJobError').order_by(WebDomainDataMonitorVO.itemvalue).all()
        return dataList

    def listOSWMonitorDetail(self):
        session = self.getLocalSession()
        SQL = " SELECT host_name, oswdir, decode(instr(oswstatus,'Running'),1,'OSW Running','OSW Not Running') oswstatus," \
              "        decode(instr(oswstatus,'has cronjob'),0,'no cronjob','has cronjob') cronjobdeployment," \
              "        decode(instr(oswstatus,'dir is same'),0,'invalid cronjob','valid cronjob') as cronjobstatus ,oswstatus as description" \
              " FROM wbxoswmonitordetail " \
              " ORDER BY oswdir"
        tablist = session.execute(SQL).fetchall()
        return tablist

    def listDBLinkMonitorDetail(self):
        session = self.getLocalSession()
        monitordetaillist = session.query(DBLinkMonitorResultVO).order_by(DBLinkMonitorResultVO.status,
                                                                          DBLinkMonitorResultVO.db_name,
                                                                          DBLinkMonitorResultVO.schema_name).all()
        return monitordetaillist

    def listDBLinkBaseline(self, appln_support_code, db_type):
        session = self.getLocalSession()
        dblinkbaselinelist = session.query(DBLinkBaselineVO).filter(
            and_(DBLinkBaselineVO.appln_support_code == appln_support_code, DBLinkBaselineVO.db_type == db_type,
                 DBLinkBaselineVO.status == 1)).all()
        return dblinkbaselinelist

    def getDBLinkMonitorResult(self, trim_host, db_name, schema_name, dblink_name):
        session = self.getLocalSession()
        dblinkMonitorResultVO = session.query(DBLinkMonitorResultVO).filter(
            and_(DBLinkMonitorResultVO.trim_host == trim_host, DBLinkMonitorResultVO.db_name == db_name,
                 DBLinkMonitorResultVO.schema_name == schema_name,
                 DBLinkMonitorResultVO.dblink_name == dblink_name)).first()
        return dblinkMonitorResultVO

    def insertDBLinkMonitorResult(self, vo):
        session = self.getLocalSession()
        session.add(vo)

    def deleteDBLinkMonitorResult(self, trim_host, db_name):
        session = self.getLocalSession()
        SQL = "DELETE FROM wbxdblinkmonitordetail WHERE trim_host='%s' and db_name='%s'" % (trim_host, db_name)
        session.execute(SQL)

    def getSplexuserPasswordByPort(self, host_name, port):
        session = self.getLocalSession()
        SQL = """select distinct ai.trim_host,ai.db_name, ai.schema, f_get_deencrypt(password) as pwd,
                        case when ii.host_name=si.src_host and ai.db_name = si.src_db then si.src_splex_sid when ii.host_name=si.tgt_host and ai.db_name = si.tgt_db then si.tgt_splex_sid end as splex_sid
                 from instance_info ii, database_info di, appln_pool_info ai, shareplex_info si
                 where ii.host_name='%s'
                 and ii.trim_host=di.trim_host
                 and ii.db_name = di.db_name
                 and di.trim_host=ai.trim_host
                 and di.db_name = ai.db_name
                 and ai.schema='splex%s'
                 and ((si.src_host=ii.host_name and si.src_db=ai.db_name ) or ( si.tgt_host=ii.host_name and si.tgt_db=ai.db_name))
                 and si.port=%s""" % (host_name, port, port)
        userlist = session.execute(SQL).fetchall()
        return userlist

    def getOldSplexuserPasswordByPort(self, db_name, port):
        session = self.getLocalSession()
        SQL = """select distinct ai.trim_host,ai.db_name, ai.schema, f_get_deencrypt(password) as pwd,
                        case when ii.host_name=si.src_host and ai.db_name = si.src_db then si.src_splex_sid when ii.host_name=si.tgt_host and ai.db_name = si.tgt_db then si.tgt_splex_sid end as splex_sid
                 from instance_info ii, database_info di, appln_pool_info ai, shareplex_info si
                 where ii.db_name='%s'
                 and ii.trim_host=di.trim_host
                 and ii.db_name = di.db_name
                 and di.trim_host=ai.trim_host
                 and di.db_name = ai.db_name
                 and ai.schema='splex%s'
                 and ((si.src_host=ii.host_name and si.src_db=ai.db_name ) or ( si.tgt_host=ii.host_name and si.tgt_db=ai.db_name))
                 and si.port=%s""" % (db_name, port, port)
        userlist = session.execute(SQL).fetchall()
        return userlist

    def getOracleUserPwdByHostname(self, host_name):
        session = self.getLocalSession()
        SQL = "select f_get_deencrypt(pwd) from HOST_USER_INFO where host_name='%s' and username='oracle'" % host_name
        row = session.execute(SQL).fetchone()
        if row is None:
            return None
        return row[0]

    def getsplexportbydb(self, db_name, *args):
        session = self.getLocalSession()
        hosts = "','".join(args)
        SQL = '''
        select distinct decode(src_db,'%s','src','tgt') type, src_host||'?'||src_db||'?'||tgt_host||'?'||tgt_db||'?'||port info
        from shareplex_info a
        where (src_db=upper('%s') and src_host in ('%s') ) or (tgt_db=upper('%s') and tgt_host in ('%s'))
        ''' % (db_name, db_name, hosts, db_name, hosts)
        rows = session.execute(SQL).fetchall()
        if rows is None:
            return None
        return rows

    def getDBAppschema(self, db_name):
        session = self.getLocalSession()
        SQL = "select listagg(schema,',') within group(order by schema) from appln_pool_info where db_name ='%s' AND lower(SCHEMATYPE)='app'" % (
            db_name)
        rows = session.execute(SQL).fetchone()
        if rows[0] is None:
            return None
        return rows

    def getdbapplnsupportcode(self, dbname):
        session = self.getLocalSession()
        SQL = "select appln_support_code from database_info where db_name='%s'" % dbname
        row = session.execute(SQL).fetchone()
        if row is None:
            return None
        return row[0]

    def getSitecodeByHostname(self, host_name):
        session = self.getLocalSession()
        SQL = "select site_code from host_info where host_name='%s'" % host_name
        row = session.execute(SQL).fetchone()
        if row is None:
            return None
        return row[0]

    def getHostIPByHostname(self, host_name):
        session = self.getLocalSession()
        SQL = "select host_ip from host_info where host_name='%s'" % host_name
        row = session.execute(SQL).fetchone()
        if row is None:
            raise wbxexception("cannot get the host ip for %s" % host_name)
        return row[0]

    def addCronjobLog(self, logvo):
        session = self.getLocalSession()
        session.add(logvo)

    def getApplnList(self, trim_host, db_name, user_name):
        session = self.getLocalSession()
        SQL = "select trim_host,db_name,schema from appln_pool_info where trim_host = '%s' and db_name = '%s' and schema = '%s' " % (
            trim_host, db_name, user_name)
        logger.info(SQL)
        _list = session.execute(SQL).fetchall()
        return _list

    def updateUserPwd(self, user_name, password):
        session = self.getLocalSession()
        SQL = "alter user %s identified by %s " % (user_name, password)
        logger.info(SQL)
        session.execute(SQL)

    def insertUserPwdInfo(self, password, trim_host, db_name, user_name, appln_support_code, schematype):
        session = self.getLocalSession()
        SQL = "insert into appln_pool_info(trim_host,db_name,APPLN_SUPPORT_CODE,schema,schematype,password,CREATED_BY,MODIFIED_BY,new_password) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s') " \
              % (trim_host, db_name, appln_support_code, user_name, schematype, password, 'auto', 'auto', password)
        logger.info(SQL)
        session.execute(SQL)

    def updateUserPwdInfo(self, password, trim_host, db_name, user_name):
        session = self.getLocalSession()
        SQL = "update appln_pool_info set password='%s', new_password='%s' where trim_host = '%s' and db_name = '%s' and schema = '%s'" % (
            password, password, trim_host, db_name, user_name)
        logger.info(SQL)
        session.execute(SQL)

    def checkInfo(self, trim_host, db_name):
        session = self.getLocalSession()
        SQL = "select db_name,trim_host,trim_host,appln_support_code from database_info where db_name = '%s' and trim_host = '%s' " % (
            db_name, trim_host)
        logger.info(SQL)
        _list = session.execute(SQL).fetchall()
        return _list

    def getTaskList(self, job, host_name, port):
        session = self.getLocalSession()
        SQL = ""
        if port == '':
            SQL = "select taskid,job from task_log where job = '%s' and host_name = '%s' " % (job, host_name)
        else:
            SQL = "select taskid,job from task_log where job = '%s' and host_name = '%s' and port= '%s' " % (
                job, host_name, port)
        logger.info(SQL)
        _list = session.execute(SQL).fetchall()
        return _list

    def addTaskLog(self, job, host_name, port, parameters, status):
        session = self.getLocalSession()
        SQL = "insert into task_log(job,host_name,port,parameters,status,LAST_MODIFIED_TIME) values('%s','%s','%s','%s','%s',systimestamp)" \
              % (job, host_name, port, parameters, status)
        logger.info(SQL)
        session.execute(SQL)

    def updateTaskLog(self, job, host_name, port, parameters, status):
        session = self.getLocalSession()
        SQL = ""
        if port == '':
            SQL = "update task_log set status='%s',LAST_MODIFIED_TIME = systimestamp where job = '%s' and host_name = '%s'  " \
                  % (status, job, host_name)
        else:
            SQL = "update task_log set status='%s',LAST_MODIFIED_TIME = systimestamp where job = '%s' and host_name = '%s' and port = '%s' " % (
                status, job, host_name, port)
        logger.info(SQL)
        session.execute(SQL)

    def applnMappingInfo(self, trim_host, db_name):
        session = self.getLocalSession()
        SQL = "select distinct appln_support_code,mapping_name,schema from appln_mapping_info where trim_host= '%s' and db_name= '%s'" % (
            trim_host, db_name)
        logger.info(SQL)
        _list = session.execute(SQL).fetchall()
        return _list

    def getkafka_monitor_threshold(self, metric_type):
        session = self.getLocalSession()
        if metric_type:
            _list = session.query(AlertVo).filter(AlertVo.metric_type.like("%" + metric_type + "%")).order_by(
                AlertVo.metric_type, AlertVo.db_host, AlertVo.db_name, AlertVo.shareplex_port,
                AlertVo.lastmodifiedtime.desc()).all()
            return _list
        else:
            _list = session.query(AlertVo).order_by(AlertVo.metric_type, AlertVo.db_host, AlertVo.db_name,
                                                    AlertVo.shareplex_port, AlertVo.lastmodifiedtime.desc()).all()
            return _list

    def getkafka_monitor_threshold_by_id(self, id):
        session = self.getLocalSession()
        _list = session.query(AlertVo).filter(
            and_(AlertVo.id == id)).order_by(AlertVo.lastmodifiedtime).all()
        return _list

    def add_kafka_alert(self, alertVo):
        session = self.getLocalSession()
        session.add(alertVo)

    def update_kafka_alert(self, alertVo):
        session = self.getLocalSession()
        vo = session.query(AlertVo).filter(AlertVo.id == alertVo.id).one()
        vo.metric_type = alertVo.metric_type
        vo.metric_name = alertVo.metric_name
        vo.metric_operator = alertVo.metric_operator
        vo.threshold_value = alertVo.threshold_value
        vo.severity = alertVo.severity
        vo.db_host = alertVo.db_host
        vo.db_name = alertVo.db_name
        vo.shareplex_port = alertVo.shareplex_port
        vo.threshold_times = alertVo.threshold_times
        session.commit()

    def delete_alert_threshold(self, id):
        session = self.getLocalSession()
        vo = session.query(AlertVo).filter(AlertVo.id == id).one()
        logger.info(vo)
        session.delete(vo)

    def getDBListForDBHealth(self):
        session = self.getLocalSession()
        SQL = '''
            select distinct ii.host_name, ii.db_name
            from wbxjobmanagerinstance jm, instance_info ii, database_info di
            where jm.host_name=ii.host_name
            and ii.trim_host=di.trim_host
            and ii.db_name=di.db_name
            and jm.lastupdatetime > sysdate-1
            order by ii.db_name, ii.host_name
        '''
        return session.execute(SQL).fetchall()

    def getCheckCronStatus(self):
        session = self.getLocalSession()
        SQL = '''
            with t as  (
            select host_name,status,case when instr(status, 'running') > 0 then '1' else '0' end 
            cron_status,db_agent_exist,to_char(monitor_time,'YYYY-MM-DD hh24:mi:ss') monitor_time
            from CRONJOBSTATUS order by host_name
            )
            select t.host_name,t.status,t.db_agent_exist,t.monitor_time from t 
            where ((t.cron_status = '0' and t.db_agent_exist = '0'  ) or (t.cron_status = '1' and t.db_agent_exist = '1' ))

            union all (select t.host_name,t.status,t.db_agent_exist,t.monitor_time from  t 
            where ( (t.cron_status='1' and t.db_agent_exist= '0') or (t.cron_status='0' and t.db_agent_exist= '1') ) )
              '''
        return session.execute(SQL).fetchall()

    def getDBUserInfo(self, host_name):
        session = self.getLocalSession()
        SQL = """select distinct hi.host_name,hi.site_code,ui.username, f_get_deencrypt(ui.pwd) pwd,decode(w.host_name,NULL,'0','1') is_exist_wbxjobinstance 
          from database_info di, instance_info ii, host_info hi, host_user_info ui,(select distinct host_name from wbxjobinstance where status = 'SUCCEED') w 
          where di.db_type in ('PROD','BTS_PROD')
          and ii.host_name not in ('tadborbf06','sjdborbf06','tadborbf07','sjdborbf07','tadbth351','tadbth352','sjdbwbf1','sjdbwbf2','sjdbth351','sjdbth352','tadbwbf1') 
          and di.db_name=ii.db_name 
          and di.trim_host=ii.trim_host
          and ii.host_name=hi.host_name 
          and ii.host_name=ui.host_name(+) 
          and ii.host_name = w.host_name(+) 
          and hi.host_name = '%s'""" % (host_name)
        logger.info(SQL)
        _list = session.execute(SQL).fetchall()
        return _list

    def updateCronJobStatus(self, host_name, status, is_exist_wbxjobinstance):
        session = self.getLocalSession()
        SQL = "update cronjobstatus set status = '%s',db_agent_exist= '%s',monitor_time= systimestamp where host_name = '%s' " % (
            status, is_exist_wbxjobinstance, host_name)
        logger.info(SQL)
        session.execute(SQL)

    def getadbmonlist(self, src_db, tgt_db, port, db_type):
        session = self.getLocalSession()
        SQL = '''
          with ta as (
         select db_type,src_host,src_db,port,replication_to,tgt_host,tgt_db,temp.src_splex_sid, temp.tgt_splex_sid, 
            to_char(lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,
             to_char(montime,'yyyy-mm-dd hh24:mi:ss') montime,
             diff_secend,diff_day,diff_hour,diff_min,(diff_day||':'||diff_hour||':'||diff_min) lag_by,nvl(wbl.lag_by,10) alert_mins,case when temp.diff_secend>nvl(wbl.lag_by,10)*60  then '1' else '0' end alert_flag
             from ( select distinct ta.*, sdi.db_type,ROUND((ta.montime-ta.lastreptime)*24*60*60) diff_secend,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)) diff_day,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)- trunc(TO_NUMBER(ta.montime - ta.lastreptime))*24 diff_hour,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24*60)-trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)*60 diff_min,sdi.appln_support_code src_appln_support_code,tdi.appln_support_code tgt_appln_support_code,si.src_splex_sid, si.tgt_splex_sid 
                        from wbxadbmon ta, shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii
                        where ta.src_host=si.src_host
                        and ta.src_db=si.src_db
                        and ta.tgt_host=si.tgt_host
                        and ta.tgt_db=si.tgt_db
                        and ta.port=si.port
                        and ta.replication_to = si.replication_to||nvl2(qname,'_'||qname, '')
                        and si.src_host=sii.host_name
                        and si.src_db=sii.db_name
                        and sii.db_name=sdi.db_name
                        and sii.trim_host=sdi.trim_host
                        and si.tgt_host=tii.host_name
                        and si.tgt_db=tii.db_name
                        and tdi.db_name=tii.db_name
                        and tdi.trim_host=tii.trim_host
                        --and tdi.db_type in ('PROD')
                       -- and sdi.db_type in ('PROD')
                       -- and (tdi.db_type in ('PROD') or (ta.src_db='RACBTW2' ))
                       -- and (sdi.db_type in ('PROD') or (ta.tgt_db='BGSBWEB' ))
                        and (tdi.db_type in ('PROD','BTS_PROD') or (ta.src_db='RACBTW6' ) or  (ta.src_db='BGSBWEB') or (si.port in (60063,60064)))
                        and (sdi.db_type in ('PROD','BTS_PROD') or (ta.tgt_db='BGSBWEB' ) or  (ta.tgt_db='RACBTW6') or (si.port in (60063,60064)))
                        ) temp,wbxadbmonlagby wbl 
             where temp.src_appln_support_code= wbl.src_appln_support_code(+)
              and temp.tgt_appln_support_code = wbl.tgt_appln_support_code(+)
              order by temp.diff_secend desc
        ) ,
        ta1 as (
         select db_type,src_host,src_db,port,replication_to,tgt_host,tgt_db,temp.src_splex_sid, temp.tgt_splex_sid,
            to_char(lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,
             to_char(montime,'yyyy-mm-dd hh24:mi:ss') montime,
             diff_secend,diff_day,diff_hour,diff_min,(diff_day||':'||diff_hour||':'||diff_min) lag_by,nvl(wbl.lag_by,10) alert_mins,case when temp.diff_secend>nvl(wbl.lag_by,10)*60  then '1' else '0' end alert_flag
             from ( select distinct ta.*,sdi.db_type,ROUND((ta.montime-ta.lastreptime)*24*60*60) diff_secend,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)) diff_day,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)- trunc(TO_NUMBER(ta.montime - ta.lastreptime))*24 diff_hour,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24*60)-trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)*60 diff_min,sdi.appln_support_code src_appln_support_code,tdi.appln_support_code tgt_appln_support_code,si.src_splex_sid, si.tgt_splex_sid 
                        from wbxadbmon ta, shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii
                        where ta.src_host=si.src_host
                        and ta.src_db=si.src_db
                        and ta.tgt_host=si.tgt_host
                        and ta.tgt_db=si.tgt_db
                        and ta.port=si.port
                        and ta.replication_to = si.replication_to||nvl2(qname,'_'||qname, '')
                        and si.src_host=sii.host_name
                        and si.src_db=sii.db_name
                        and sii.db_name=sdi.db_name
                        and sii.trim_host=sdi.trim_host
                        and si.tgt_host=tii.host_name
                        and si.tgt_db=tii.db_name
                        and tdi.db_name=tii.db_name
                        and tdi.trim_host=tii.trim_host
                        --and tdi.db_type in ('PROD')
                       -- and sdi.db_type in ('PROD')
                       -- and (tdi.db_type in ('PROD') or (ta.src_db='RACBTW2' ))
                       -- and (sdi.db_type in ('PROD') or (ta.tgt_db='BGSBWEB' ))

                        and (tdi.db_type in ('PROD','BTS_PROD') or (ta.src_db='RACBTW6' ) or  (ta.src_db='BGSBWEB' )or (si.port in (60063,60064)))
                        and (sdi.db_type in ('PROD','BTS_PROD') or (ta.tgt_db='BGSBWEB' ) or  (ta.tgt_db='RACBTW6' )or (si.port in (60063,60064)))
                        ) temp,wbxadbmonlagby wbl 
             where temp.src_appln_support_code= wbl.src_appln_support_code(+)
              and temp.tgt_appln_support_code = wbl.tgt_appln_support_code(+)   
              order by temp.src_db 
        ) 
        select ta.* from ta  where ta.alert_flag = '1'
        '''
        if src_db:
            SQL = " %s and ta.src_db like '%%%s%%' " % (SQL, src_db)
        if tgt_db:
            SQL = " %s and ta.tgt_db like '%%%s%%' " % (SQL, tgt_db)
        if port:
            SQL = " %s and ta.port=%s " % (SQL, port)
        if db_type:
            SQL = " %s and ta.db_type='%s' " % (SQL, db_type)
        SQL = ''' %s
        union all
        select ta1.* from ta1  where ta1.alert_flag = '0'
        ''' % (SQL)

        if src_db:
            SQL = " %s and ta1.src_db like '%%%s%%' " % (SQL, src_db)
        if tgt_db:
            SQL = " %s and ta1.tgt_db like '%%%s%%' " % (SQL, tgt_db)
        if port:
            SQL = " %s and ta1.port=%s " % (SQL, port)
        if db_type:
            SQL = " %s and ta1.db_type='%s' " % (SQL, db_type)

        list = session.execute(SQL).fetchall()
        return list

    def getadbmonlistAlert(self, src_db, tgt_db, port, db_type):
        session = self.getLocalSession()
        SQL = '''
         with ta as (
         select db_type,src_host,src_db,port,replication_to,tgt_host,tgt_db,temp.src_splex_sid, temp.tgt_splex_sid,
            to_char(lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,
             to_char(montime,'yyyy-mm-dd hh24:mi:ss') montime,
             diff_secend,diff_day,diff_hour,diff_min,(diff_day||':'||diff_hour||':'||diff_min) lag_by,nvl(wbl.lag_by,10) alert_mins,case when temp.diff_secend>nvl(wbl.lag_by,10)*60  then '1' else '0' end alert_flag
             from ( select distinct ta.*,sdi.db_type,ROUND((ta.montime-ta.lastreptime)*24*60*60) diff_secend,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)) diff_day,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)- trunc(TO_NUMBER(ta.montime - ta.lastreptime))*24 diff_hour,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24*60)-trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)*60 diff_min,sdi.appln_support_code src_appln_support_code,tdi.appln_support_code tgt_appln_support_code,si.src_splex_sid, si.tgt_splex_sid 
                        from wbxadbmon ta, shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii
                        where ta.src_host=si.src_host
                        and ta.src_db=si.src_db
                        and ta.tgt_host=si.tgt_host
                        and ta.tgt_db=si.tgt_db
                        and ta.port=si.port
                        and ta.replication_to = si.replication_to||nvl2(qname,'_'||qname, '')
                        and si.src_host=sii.host_name
                        and si.src_db=sii.db_name
                        and sii.db_name=sdi.db_name
                        and sii.trim_host=sdi.trim_host
                        and si.tgt_host=tii.host_name
                        and si.tgt_db=tii.db_name
                        and tdi.db_name=tii.db_name
                        and tdi.trim_host=tii.trim_host
                        --and tdi.db_type in ('PROD')
                        --and sdi.db_type in ('PROD')
                       -- and (tdi.db_type in ('PROD') or (ta.src_db='RACBTW2' ))
                       -- and (sdi.db_type in ('PROD') or (ta.tgt_db='BGSBWEB' ))
                       and (tdi.db_type in ('PROD','BTS_PROD') or (ta.src_db='RACBTW6' ) or  (ta.src_db='BGSBWEB' ) or (si.port in (60063,60064)))
                       and (sdi.db_type in ('PROD','BTS_PROD') or (ta.tgt_db='BGSBWEB' ) or  (ta.tgt_db='RACBTW6' ) or (si.port in (60063,60064)))
                       

                        ) temp,wbxadbmonlagby wbl 
             where temp.src_appln_support_code= wbl.src_appln_support_code(+)
              and temp.tgt_appln_support_code = wbl.tgt_appln_support_code(+)
              order by temp.diff_secend desc
        ) 
        select ta.* from ta  where ta.alert_flag = '1' or to_date(ta.montime,'yyyy-mm-dd hh24:mi:ss') < sysdate - 1/24
        '''
        if src_db:
            SQL = "%s and ta.src_db like '%%%s%%' " % (SQL, src_db)
        if tgt_db:
            SQL = "%s and ta.tgt_db like '%%%s%%' " % (SQL, tgt_db)
        if port:
            SQL = "%s and ta.port=%s " % (SQL, port)
        if db_type:
            SQL = " %s and ta.db_type='%s' " % (SQL, db_type)
        logger.info(SQL)
        _list = session.execute(SQL).fetchall()
        return _list

    def getTrimhost(self, src_host, tgt_host):
        session = self.getLocalSession()
        SQL = "select distinct trim_host,host_name from instance_info ii where ii.host_name in('%s','%s')" % (
            src_host, tgt_host)
        return session.execute(SQL).fetchall()

    def updatewbxadbmon(self, port, src_db, src_host, tgt_db, tgt_host, replication_to, logtime):
        session = self.getLocalSession()
        SQL = "update wbxadbmon set lastreptime = to_date('%s','yyyy-mm-dd hh24:mi:ss'),montime= sysdate where port= '%s' and src_db = '%s' " \
              "and src_host= '%s' and replication_to = '%s' and tgt_db = '%s' and tgt_host = '%s'" % (
                  logtime, port, src_db, src_host, replication_to, tgt_db, tgt_host)
        logger.info(SQL)
        session.execute(SQL)

    def getadbmonOne(self, port, src_db, src_host, tgt_db, tgt_host, replication_to):
        session = self.getLocalSession()
        SQL = "select port,src_db,src_host,replication_to,tgt_db,tgt_host, to_char(lastreptime,'YYYY-MM-DD hh24:mi:ss') " \
              "lastreptime,to_char(montime,'YYYY-MM-DD hh24:mi:ss') montime " \
              "from wbxadbmon where port = %s and src_db= '%s' and src_host='%s' and tgt_db= '%s' " \
              "AND tgt_host = '%s' and replication_to ='%s' " % (
                  port, src_db, src_host, tgt_db, tgt_host, replication_to)
        logger.info(SQL)
        _list = session.execute(SQL).fetchall()
        return _list

    def getshareplexprocesscputime(self, db_name, start_time, end_time):
        session = self.getLocalSession()
        SQL = """
            select process_type, to_char(monitortime,'YYYY-MM-DD hh24:mi:ss'), NVL(costtime-lag(costtime) over (partition by process_type order by monitortime),0) costtime 
            from wbxsplex_performance_monitor 
            where db_name=:db_name 
            and monitortime between :start_time and :end_time
            order by monitortime
            """
        rows = session.execute(SQL, {"db_name": db_name, "start_time": start_time, "end_time": end_time}).fetchall()
        return rows
        # if rows is not None:
        #     return [{row[0]:row[1]} for row in rows]
        # else:
        #     return  None

    def getwbxsplexreplicationdelaytime(self, db_name, start_time, end_time):
        session = self.getLocalSession()
        SQL = ''' select to_char(lastreptime,'YYYY-MM-DD hh24:mi:ss'), round((montime-lastreptime)*24*60*60)
                  from wbxadbmon_history
                  where src_db=:db_name
                  and montime between :start_time and :end_time '''
        rows = session.execute(SQL, {"db_name": db_name, "start_time": start_time, "end_time": end_time}).fetchall()
        return [{row[0]: row[1]} for row in rows]

    def getQnames(self, port, src_db, src_host, tgt_db, tgt_host):
        session = self.getLocalSession()
        SQL = '''
        select distinct replication_to||nvl2(qname,'_'||qname, '') replication_to,qname
              from shareplex_info 
              where port = :port and src_db= :src_db and src_host=:src_host and tgt_db= :tgt_db 
              AND tgt_host = :tgt_host '''
        # logger.info(SQL)
        _list = session.execute(SQL, {"port": port, "src_db": src_db, "src_host": src_host, "tgt_db": tgt_db,
                                      "tgt_host": tgt_host}).fetchall()
        return _list

    def adbmondetail(self, port, src_db, src_host, tgt_db, tgt_host, replication_to):
        session = self.getLocalSession()
        SQL = '''
        select src_db,src_host,port,replication_to,tgt_db,tgt_host,to_char(lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,
        to_char(montime,'yyyy-mm-dd hh24:mi:ss') montime ,trunc(TO_NUMBER(montime - lastreptime)*24*60)-trunc(TO_NUMBER(montime - lastreptime)*24)*60 diff_min 
        from wbxadbmonhistory where  src_db=:src_db and src_host =:src_host  and tgt_db=:tgt_db and  tgt_host =:tgt_host 
        and replication_to =:replication_to and port =:port and lastreptime > sysdate-7
        order by montime
        '''
        logger.info(SQL)
        list = session.execute(SQL, {"port": port, "src_db": src_db, "src_host": src_host, "tgt_db": tgt_db,
                                     "tgt_host": tgt_host, "replication_to": replication_to}).fetchall()
        return list

    def getarchivelog(self, start_time, end_time):
        session = self.getLocalSession()
        SQL = '''
        select to_char(trunc(first_time, 'hh'),'YYYY-MM-DD hh24') first_time,count(1) ARC
        from v$archived_log
        where first_time between sysdate -7 and sysdate
        group by to_char(trunc(first_time, 'hh'),'YYYY-MM-DD hh24')
        order by 1
        '''
        logger.info(SQL)
        list = session.execute(SQL, {"start_time": start_time, "end_time": end_time}).fetchall()
        return list

    def getwbxggreplicationdelaytime(self, start_time, end_time):
        session = self.getLocalSession()
        SQL = ''' select to_char(monitortime, 'YYYY-MM-DD hh24:mi'), sum(lag_by)
                from wbxogglagby
                where monitortime between :start_time and :end_time
                group by to_char(monitortime, 'YYYY-MM-DD hh24:mi')
                order by 1 '''
        rows = session.execute(SQL, {"start_time": start_time, "end_time": end_time}).fetchall()
        result = {}
        for row in rows:
            result[row[0]] = row[1]
        return result

    def getwbxggcpuconsumption(self, start_time, end_time):
        session = self.getLocalSession()
        SQL = ''' select monitortime, host_name, sum(case when costtime < 0 then 0 else costtime end) from (
                select to_char(monitertime, 'YYYY-MM-DD hh24:mi') as monitortime, host_name, process_type, process_name, costtime-lag(costtime) over ( partition by host_name, process_type,process_name order by monitertime) costtime
                from wbxgg_performance_monitor
                where monitertime between :start_time and :end_time and process_type not in ('mgr')
                ) group by monitortime, host_name
                order by monitortime'''
        rows = session.execute(SQL, {"start_time": start_time, "end_time": end_time}).fetchall()
        result = {}
        if not rows:
            return result
        for row in rows:
            keyname = row[1]
            if keyname in result.keys():
                if row[2] is not None and row[2] <= 30:
                    result[keyname].append({
                        row[0]: row[2]
                    })
            else:
                if row[2] is not None and row[2] <= 30:
                    result[keyname] = [{
                        row[0]: row[2]
                    }]
        return result

    def getwbxsplexcpuconsumption(self, start_time, end_time):
        session = self.getLocalSession()
        SQL = ''' select monitortime, host_name, db_name,sum(case when costtime < 0 then 0 else costtime end) from (
                select to_char(monitortime, 'YYYY-MM-DD hh24:mi') monitortime, host_name, db_name, process_type,
                costtime-lag(costtime) over ( partition by process_type order by monitortime) costtime
                from wbxsplex_performance_monitor
                where port=24522 and monitortime between :start_time and :end_time
                and costtime > 0
                ) group by monitortime, host_name, db_name
                order by monitortime '''
        rows = session.execute(SQL, {"start_time": start_time, "end_time": end_time}).fetchall()
        result = {}
        if not rows:
            return result
        for row in rows:
            keyname = row[1]
            if keyname in result.keys():
                if row[3] is not None:
                    result[keyname].append({
                        row[0]: row[3]
                    })
            else:
                if row[3] is not None:
                    result[keyname] = [{
                        row[0]: row[3]
                    }]
        return result

    def getwbxsplexdelaytime(self, start_time, end_time):
        session = self.getLocalSession()
        SQL = ''' select montime, ceil(avg(delaytime)) from (
            select to_char(montime, 'YYYY-MM-DD hh24:mi') montime, round((montime-lastreptime)*24*60*60) delaytime
            from wbxadbmonhistory where port=24522 and src_host='tadbrpt2'
            and montime between :start_time and :end_time
            ) group by montime
            order by montime'''
        rows = session.execute(SQL, {"start_time": start_time, "end_time": end_time}).fetchall()
        result = {}
        for row in rows:
            result[row[0]] = row[1]
        return result

    def getDBTnsInfo(self, db_name):
        session = self.getLocalSession()
        SQL = '''
              with tmp as (
        select distinct di.db_name, di.service_name,di.listener_port,ii.trim_host,hi.scan_ip1,hi.scan_ip2,hi.scan_ip3
        from database_info di ,instance_info ii,host_info hi
        where di.db_name= ii.db_name
        and di.trim_host = ii.trim_host
        and ii.host_name = hi.host_name
        and di.db_type in ('PROD')
        and hi.scan_ip1 is not null
        order by di.db_name,trim_host
        )
        select * from (
        select tmp.*, row_number() over(PARTITION BY db_name,trim_host order by db_name,trim_host ) as rn from  tmp
        ) where rn = 1 and db_name=:db_name order by db_name
               '''
        list = session.execute(SQL, {"db_name": db_name}).fetchall()
        return list

    def getMeetingDBBaseInfo(self):
        session = self.getLocalSession()
        SQL = """
        select distinct mydbname, mydbschema from wbxbackup.dailymtgdbdata
        """
        rows = session.execute(SQL).fetchall()
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                "mydbname": row[0],
                "mydbschema": row[1],
            })
        return result

    def getMeetingDBDBNameByHostName(self, host_name):
        session = self.getLocalSession()
        SQL = """
                select distinct mydbschema from wbxbackup.dailymtgdbdata where  mydbname = :host_name
                """
        rows = session.execute(SQL, {'host_name': host_name}).fetchall()
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({'mydbschemas': row[0]})
        return result

    def getMeetingDBTableList(self, db_name, host_name):
        session = self.getLocalSession()

        SQL = """
                select * from wbxbackup.dailymtgdbdata where  mydbschema=:db_name and mydbname=:host_name and to_date(processdate,'YYYY-MM-DD') > sysdate - 180  order by processdate asc
            """
        rows = session.execute(SQL, {"db_name": db_name, "host_name": host_name}).fetchall()
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                'mydbschema': row[3],
                'mydbname': row[1],
                'mydbip': row[2],
                'processdate': row[4],
                'processrec': row[5]
            })
        return result

    def getCannelTableListByTwoApplicationSupportCode(self, src_appln_support_code, tgt_appln_support_code):
        session = self.getLocalSession()
        SQL = ''
        if src_appln_support_code == '' and tgt_appln_support_code != '':
            SQL = """
                            select decode(src_appln_support_code,'CONFIG','CONFIGDB','WEB','WEBDB','OPDB','OPDB','TEL','TAHOEDB','TEO','TEODB','MEDIATEDB','MEDIATEDB','GLOOKUP','LOOKUP','SYSTOOLDB','TOOLS',src_appln_support_code) as src_appln_support_code, 
            src_schematype, decode(tgt_appln_support_code,'CONFIG','CONFIGDB','WEB','WEBDB','OPDB','OPDB','TEL','TAHOEDB','TEO','TEODB','MEDIATEDB','MEDIATEDB','GLOOKUP','LOOKUP','SYSTOOLDB','TOOLS',tgt_appln_support_code) as tgt_appln_support_code, tgt_schematype, src_tablename, tgt_tablename  
            from wbxshareplexbaseline 
            where tgt_appln_support_code=:tgt_appln_support_code
            and releasenumber=(select releasenumber from (select releasenumber from wbxshareplexbaseline
                           where tgt_appln_support_code=:tgt_appln_support_code order by createtime desc) where rownum =1
                           )
            and tablestatus!='remove_table'
                            """
        elif src_appln_support_code != '' and tgt_appln_support_code == '':
            SQL = """
                            select decode(src_appln_support_code,'CONFIG','CONFIGDB','WEB','WEBDB','OPDB','OPDB','TEL','TAHOEDB','TEO','TEODB','MEDIATEDB','MEDIATEDB','GLOOKUP','LOOKUP','SYSTOOLDB','TOOLS',src_appln_support_code) as src_appln_support_code, 
            src_schematype, decode(tgt_appln_support_code,'CONFIG','CONFIGDB','WEB','WEBDB','OPDB','OPDB','TEL','TAHOEDB','TEO','TEODB','MEDIATEDB','MEDIATEDB','GLOOKUP','LOOKUP','SYSTOOLDB','TOOLS',tgt_appln_support_code) as tgt_appln_support_code, tgt_schematype, src_tablename, tgt_tablename  
            from wbxshareplexbaseline 
            where src_appln_support_code=:src_appln_support_code
            and releasenumber=(select releasenumber from (select releasenumber from wbxshareplexbaseline
                           where src_appln_support_code=:src_appln_support_code order by createtime desc) where rownum =1
                           )
            and tablestatus!='remove_table'
                            """
        else:
            SQL = """
                            select decode(src_appln_support_code,'CONFIG','CONFIGDB','WEB','WEBDB','OPDB','OPDB','TEL','TAHOEDB','TEO','TEODB','MEDIATEDB','MEDIATEDB','GLOOKUP','LOOKUP','SYSTOOLDB','TOOLS',src_appln_support_code) as src_appln_support_code, 
            src_schematype, decode(tgt_appln_support_code,'CONFIG','CONFIGDB','WEB','WEBDB','OPDB','OPDB','TEL','TAHOEDB','TEO','TEODB','MEDIATEDB','MEDIATEDB','GLOOKUP','LOOKUP','SYSTOOLDB','TOOLS',tgt_appln_support_code) as tgt_appln_support_code, tgt_schematype, src_tablename, tgt_tablename  
            from wbxshareplexbaseline 
            where tgt_appln_support_code=:tgt_appln_support_code and src_appln_support_code=:src_appln_support_code
            and releasenumber=(select releasenumber from (select releasenumber from wbxshareplexbaseline
                           where src_appln_support_code=:src_appln_support_code and tgt_appln_support_code=:tgt_appln_support_code order by createtime desc) where rownum =1
                           )
            and tablestatus!='remove_table'
                            """
        rows = session.execute(SQL, {'src_appln_support_code': src_appln_support_code,
                                     'tgt_appln_support_code': tgt_appln_support_code}).fetchall()
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({'SRC_APPLN_SUPPORT_CODE': row[0],
                           'SRC_SCHEMATYPE': row[1],
                           'TGT_APPLN_SUPPORT_CODE': row[2],
                           'TGT_SCHEMATYPE': row[3],
                           'SRC_TABLENAME': row[4],
                           'TGT_TABLENAME': row[5]
                           })
        return result

    def getReplicationTableListByTableName(self, table_name):
        session = self.getLocalSession()
        SQL = """
                select decode(src_appln_support_code,'CONFIG','CONFIGDB','WEB','WEBDB','OPDB','OPDB','TEL','TAHOEDB','TEO','TEODB','MEDIATEDB','MEDIATEDB','GLOOKUP','LOOKUP','SYSTOOLDB','TOOLS',src_appln_support_code) as src_appln_support_code, 
       src_schematype, src_tablename, 
	   decode(tgt_appln_support_code,'CONFIG','CONFIGDB','WEB','WEBDB','OPDB','OPDB','TEL','TAHOEDB','TEO','TEODB','MEDIATEDB','MEDIATEDB','GLOOKUP','LOOKUP','SYSTOOLDB','TOOLS',tgt_appln_support_code) as tgt_appln_support_code, 
	   tgt_schematype 
from (
select distinct src_appln_support_code, src_schematype, src_tablename, tgt_appln_support_code, tgt_schematype, decode(tablestatus,'remove_table',-1,0) cnt
from wbxshareplexbaseline 
where src_tablename=:table_name
)
where cnt =0
                """
        rows = session.execute(SQL, {'table_name': table_name}).fetchall()
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({'SRC_APPLN_SUPPORT_CODE': row[0],
                           'SRC_SCHEMATYPE': row[1],
                           'TGT_APPLN_SUPPORT_CODE': row[3],
                           'TGT_SCHEMATYPE': row[4],
                           'SRC_TABLENAME': row[2]
                           })
        return result

    def getNotifyList(self):
        session = self.getLocalSession()
        SQL = """select * from wbxmonitoralertnotifychannel"""
        rows = session.execute(SQL).fetchall()
        print(rows)
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                "CHANNELID": row[0],
                "CHANNELNAME": row[1],
                'NOTIFYTYPE': row[2],
                'RECEIVERLIST': row[3]
            })
        return result

    def addNotifyChannel(self, channel_name, channel_type, emails, teams):
        session = self.getLocalSession()
        SQL = ''
        if channel_type == 'EMAIL':
            SQL = '''
                    insert into wbxmonitoralertnotifychannel(channelname, notifytype, receiverlist)values('%s', '%s', '%s')
                    ''' % (channel_name, channel_type, emails)
        else:
            SQL = '''
                    insert into wbxmonitoralertnotifychannel(channelname, notifytype, receiverlist)values('%s', '%s', '%s')
                    ''' % (channel_name, channel_type, teams)

        logger.info(SQL)
        session.execute(SQL)

    def updateNotifyChannel(self, channel_name, channel_type, emails, teams, channel_id):
        session = self.getLocalSession()
        SQL = ''
        if channel_type == 'EMAIL':
            SQL = '''
                    update wbxmonitoralertnotifychannel set channelname = '%s', notifytype='%s', receiverlist='%s' where channelid='%s'
                    ''' % (channel_name, channel_type, emails, channel_id)
        else:
            SQL = '''
                    update wbxmonitoralertnotifychannel set channelname = '%s', notifytype='%s', receiverlist='%s' where channelid='%s'
                    ''' % (channel_name, channel_type, teams, channel_id)

        logger.info(SQL)
        session.execute(SQL)

    def getMetricList(self):
        session = self.getLocalSession()
        SQL = """select * from wbxmonitoralertthreshold"""
        rows = session.execute(SQL).fetchall()
        print(rows)
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                "thresholdid": row[0],
                "metric_name": row[2],
                "job_name": row[11],
                'alerttype': row[1],
                'object_name': '',
                'operator': row[3],
                'isnullable': row[10],
                'warning_value': row[7],
                'warning_channels': row[12],
                'critical_value': row[8],
                'critical_channels': row[13],
                'dn_name': row[4],
                'host_name': row[5],
                'instance_name': row[9],
                'appln_support_code': row[6]
            })
        return result

    def addMetricSetting(self, metric_name, job_name, warning_value, warning_channels, critical_value,
                         critical_channels, operator, alerttype, db_name, instance_name, host_name, db_type):
        session = self.getLocalSession()
        SQL = '''
                insert into wbxmonitoralertthreshold(metric_name,jobname,warning_value,warning_channels,critical_value,critical_channels,operator,alerttype,db_name,instance_name,host_name,db_type)values('%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')
                ''' % (
        metric_name, job_name, warning_value, warning_channels, critical_value, critical_channels, operator, alerttype,
        db_name, instance_name, host_name, db_type)
        logger.info(SQL)
        session.execute(SQL)

    def updateMetricSeeting(self, metric_name, job_name, warning_value, warning_channels, critical_value,
                            critical_channels, operator, alerttype, db_name, instance_name, host_name, db_type,
                            thresholdid):
        session = self.getLocalSession()
        SQL = '''
                update wbxmonitoralertthreshold set metric_name = '%s', jobname = '%s',warning_value = '%s', warning_channels='%s',critical_value = '%s', critical_channels='%s', operator='%s',alerttype = '%s',db_name='%s',instance_name = '%s',host_name='%s',db_type='%s' where thresholdid='%s'
                ''' % (
        metric_name, job_name, warning_value, warning_channels, critical_value, critical_channels, operator, alerttype,
        db_name, instance_name, host_name, db_type, thresholdid)
        print('sql', SQL)
        logger.info(SQL)
        session.execute(SQL)

    def getFailedJobList(self):
        session = self.getLocalSession()
        SQL = """select jb.jobid,jb.errormsg, jb.host_name, jb.commandstr,to_char(jb.last_run_time,'YYYY-MM-DD hh24:mi:ss') last_run_time, to_char(jb.next_run_time,'YYYY-MM-DD hh24:mi:ss') next_run_time,jb.jobruntime, to_char(sysdate,'YYYY-MM-DD hh24:mi:ss') currenttime, jb.status from wbxjobinstance jb, wbxjobmanagerinstance jbm
where jb.host_name = jbm.host_name
and jbm.status='RUNNING'
and jbm.lastupdatetime > sysdate-3/60/24
and (jb.status in ('FAILED', 'PAUSE') or (jb.status in ('SUCCEED','RUNNING') and jb.next_run_time < sysdate-5/60/24)) order by jb.host_name,jb.status"""
        rows = session.execute(SQL).fetchall()
        print(rows)
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                "JOBID": row[0],
                "ERRORMSG": row[1],
                'HOST_NAME': row[2],
                'COMMENDSTR': row[3],
                'LAST_RUN_TIME': row[4],
                'NEXT_RUN_TIME': row[5],
                'JOBRUNTIME': row[6],
                'CURRENTTIME': row[7],
                'STATUS': row[8]
            })
        return result

    def getTelegrafMonList(self):
        session = self.getLocalSession()
        SQL = """
        select a.host_name,a.db_vendor, nvl(b.status,2) status, b.logrecordtime, b.logmsg, b.monitortime, b.installed
        from (select distinct hi.host_name, di.db_vendor from host_info hi, database_info di, instance_info ii
        where di.db_name=ii.db_name
        and di.trim_host=ii.trim_host
        and ii.host_name=hi.host_name
        and ii.trim_host=hi.trim_host
        and upper(di.db_vendor)='ORACLE'
        and di.db_type<> 'DECOM') a full outer join (select  t.host_name, t.status, to_char(t.logrecordtime,'YYYY-MM-DD hh24:mi:ss') logrecordtime, t.logmsg, 
        to_char(t.MONITORTIME,'YYYY-MM-DD hh24:mi:ss') monitortime, nvl(t.status,0) installed
        from (select a.*, row_number() over(partition by host_name order by monitortime desc) rw 
        from telegraf_performance_monitor a) t
        where t.rw = 1) b on a.host_name=b.host_name order by installed, host_name"""
        rows = session.execute(SQL).fetchall()
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                "host_name": row[0],
                "db_vendor": row[1],
                "status": row[2],
                "error_log": "%s : %s" % (row[3], row[4]) if row[3] and row[4] else "",
                "monitortime": row[5]
            })
        return result

    def inserttelegrafstatus(self, host_name, status, errortime, errorlog):
        session = self.getLocalSession()
        SQL = '''
        insert into telegraf_performance_monitor(host_name, status, logrecordtime, logmsg)values('%s', %s, to_date('%s','YYYY-MM-DD HH24:mi:ss'), '%s')
        ''' % (host_name, status, errortime, errorlog.replace("\'", "\""))
        logger.info(SQL)
        session.execute(SQL)

    def getDBConnectionURL(self, db_name):
        session = self.getLocalSession()
        SQL = """
        select '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = '|| hi.scan_ip1 ||')(PORT = '|| db.listener_port ||'))(ADDRESS = (PROTOCOL = TCP)(HOST = '|| hi.scan_ip2 ||')(PORT = '|| db.listener_port ||'))(ADDRESS = (PROTOCOL = TCP)(HOST = '|| hi.scan_ip3 ||')(PORT = '|| db.listener_port ||'))(LOAD_BALANCE = yes)(FAILOVER = on)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = '|| db.service_name ||'.webex.com)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5)))' connectionurl
from database_info db, instance_info ii, host_info hi  
where db.trim_host=ii.trim_host  
AND db.db_name=ii.db_name
AND ii.trim_host=hi.trim_host
AND ii.host_name=hi.host_name
AND db.db_name = '%s' 
AND upper(db.db_vendor)='ORACLE'
and db.db_type<> 'DECOM'""" % db_name.upper()
        rows = session.execute(SQL).fetchone()
        if not rows:
            return None
        return rows[0]

    def getShareplexConfigTable(self, src_appln_support_code, tgt_appln_support_code, src_db_name, tgt_db_name):
        session = self.getLocalSession()
        SQL = '''
                with
                src_b as (select distinct schematype,schema from appln_pool_info where db_name =:src_db_name),
                tgt_b as (select distinct schematype,schema from appln_pool_info where db_name =:tgt_db_name)
                select src_b.schema src_schema,tgt_b.schema tgt_schema,w.src_tablename,w.tgt_tablename,w.src_schematype,w.tgt_schematype
                from  wbxshareplexbaseline w,src_b,tgt_b
                where lower(w.src_schematype) = src_b.schematype(+)
                and lower(w.tgt_schematype) = tgt_b.schematype(+)
                and w.releasenumber= (
                select releasenumber from (select releasenumber from wbxshareplexbaseline
                where src_appln_support_code=:src_appln_support_code and tgt_appln_support_code=:tgt_appln_support_code order by createtime desc) where rownum =1
                )
                and w.src_appln_support_code=:src_appln_support_code and w.tgt_appln_support_code=:tgt_appln_support_code and w.tablestatus !='remove_table'
                '''
        rows = session.execute(SQL, {"src_appln_support_code": src_appln_support_code,
                                     "tgt_appln_support_code": tgt_appln_support_code, "src_db_name": src_db_name,
                                     "tgt_db_name": tgt_db_name}).fetchall()
        return rows

    def getReplicationTableList(self, src_appln_support_code, tgt_appln_support_code, src_db_name, tgt_db_name):
        session = self.getLocalSession()
        SQL = '''
               select distinct sai.schema as src_schema,ta.src_tablename,ta.src_appln_support_code, tai.schema as tgt_schema,ta.tgt_tablename, ta.tgt_appln_support_code
               from wbxshareplexbaseline ta, appln_pool_info sai, appln_pool_info tai
               where ta.src_appln_support_code=:src_appln_support_code
               and ta.tgt_appln_support_code=:tgt_appln_support_code
               and ta.releasenumber=(
               select releasenumber from (select releasenumber from wbxshareplexbaseline
               where src_appln_support_code=:src_appln_support_code and tgt_appln_support_code=:tgt_appln_support_code order by createtime desc) where rownum =1
               )
               and upper(ta.src_appln_support_code)=upper(sai.appln_support_code)
               and upper(ta.tgt_appln_support_code)=upper(tai.appln_support_code)
               and upper(ta.src_schematype)=upper(sai.schematype)
               and upper(ta.tgt_schematype) = upper(tai.schematype)
               and ta.tablestatus !='remove_table'
               and sai.db_name=:src_db_name
               and tai.db_name=:tgt_db_name
               '''
        logger.info(SQL)
        rows = session.execute(SQL, {"src_appln_support_code": src_appln_support_code,
                                     "tgt_appln_support_code": tgt_appln_support_code, "src_db_name": src_db_name,
                                     "tgt_db_name": tgt_db_name}).fetchall()
        return rows

    def getApplnPoolInfo(self, src_db_name, tgt_db_name, schematype):
        session = self.getLocalSession()
        SQL = '''
        select ai.appln_support_code,ai.db_name,ai.schema,f_get_deencrypt(ai.password) password,ai.km_version,ai.schematype,
        f_get_deencrypt(ai.new_password) new_password,ai.track_id,ai.change_status 
        from appln_pool_info ai 
        where db_name in (:src_db_name,:tgt_db_name) and schematype =:schematype
        '''
        rows = session.execute(SQL, {"src_db_name": src_db_name, "tgt_db_name": tgt_db_name,
                                     "schematype": schematype}).fetchall()
        return rows

    def getApplnPoolInfoByDBName(self, db_name, schematype, new_tahol_schema_name):
        session = self.getLocalSession()
        if new_tahol_schema_name:
            SQL = '''
                    select ai.appln_support_code,ai.db_name,ai.schema,f_get_deencrypt(ai.password) password,ai.km_version,ai.schematype,
                    f_get_deencrypt(ai.new_password) new_password,ai.track_id,ai.change_status 
                    from appln_pool_info ai 
                    where db_name=:db_name and schematype=:schematype and schema=:new_tahol_schema_name
                    '''
            rows = session.execute(SQL, {"db_name": db_name, "schematype": schematype,
                                         "new_tahol_schema_name": new_tahol_schema_name}).fetchall()
            return rows
        else:
            SQL = '''
                                select ai.appln_support_code,ai.db_name,ai.schema,f_get_deencrypt(ai.password) password,ai.km_version,ai.schematype,
                                f_get_deencrypt(ai.new_password) new_password,ai.track_id,ai.change_status 
                                from appln_pool_info ai 
                                where db_name=:db_name and schematype=:schematype 
                                '''
            rows = session.execute(SQL, {"db_name": db_name, "schematype": schematype}).fetchall()
            return rows

    def createUserToApplnPoolInfo(self, tgt_trim_host, tgt_db_name, src_vo):
        session = self.getLocalSession()
        appln_support_code = src_vo['appln_support_code']
        schema = src_vo['schema']
        password = src_vo['password']
        km_version = src_vo['km_version']
        schematype = src_vo['schematype']
        new_password = src_vo['new_password']
        track_id = src_vo['track_id']
        change_status = src_vo['change_status']
        SQL = '''
        insert into appln_pool_info(trim_host,db_name,appln_support_code,schema,password,
        date_added,lastmodifieddate,created_by,modified_by,km_version,schematype,new_password,track_id,change_status) values
        (:tgt_trim_host,:tgt_db_name,:appln_support_code,:schema,:password,sysdate,sysdate,'auto','auto',:km_version,:schematype,:new_password,:track_id,:change_status)
        '''
        logger.info(SQL)
        session.execute(SQL, {"tgt_trim_host": tgt_trim_host, "tgt_db_name": tgt_db_name,
                              "appln_support_code": appln_support_code, "schema": schema,
                              "password": password, "km_version": km_version, "schematype": schematype,
                              "new_password": new_password, "track_id": track_id, "change_status": change_status})

    def getRandomDBHostName(self):
        session = self.getLocalSession()
        SQL = '''
        select db_name,min(host_name) host_name from (
        select distinct di.db_name,ii.host_name
        from database_info di,instance_info ii
        where di.db_name in('CONFIGDB','RACPSYT','RACOPDB','GCFGDB') and di.db_type = 'PROD'
        and di.db_name = ii.db_name(+)
        and di.trim_host = ii.trim_host(+)
        order by di.db_name
        ) group by db_name 
        '''
        rows = session.execute(SQL).fetchall()
        return rows

    def getBuildTahoeTableByType(self, type):
        session = self.getLocalSession()
        sql = ""
        if type == "test":
            sql = "select schema,tablename from wbx_new_tahoe_build where schema in ('test','wbx11')"
        if type == "tahoe":
            sql = "select schema,tablename from wbx_new_tahoe_build where schema in ('tahoe')"
        rows = session.execute(sql).fetchall()
        return rows

    def updateJobHostNameForConfig(self, jobid, description):
        session = self.getLocalSession()
        sql = '''
        update wbxautotaskjob set description ='%s' where jobid= '%s'
        ''' % (description, jobid)
        session.execute(sql)

    def getConfigHostNameByJobid(self, jobid):
        session = self.getLocalSession()
        sql = '''
        select description from wbxautotaskjob where taskid = (select taskid from wbxautotaskjob where jobid =:jobid ) and processorder =1
        '''
        rows = session.execute(sql, {"jobid": jobid}).fetchall()
        return rows

    def insertShareplex_info(self, src_host_name, src_db, port, replication_to, tgt_host_name, tgt_db, qname,
                             src_splex_sid, tgt_splex_sid, src_schema, tgt_schema):
        session = self.getLocalSession()
        sql = '''

        insert into shareplex_info(src_host,src_db,port,replication_to,tgt_host,tgt_db,qname,
        date_added,lastmodifieddate,created_by,modified_by,src_splex_sid,tgt_splex_sid,src_schema,tgt_schema) VALUES
        (:src_host_name,:src_db,:port,:replication_to,:tgt_host_name,:tgt_db,:qname,sysdate,sysdate,'auto','auto',:src_splex_sid,:tgt_splex_sid,:src_schema,:tgt_schema)
        '''
        session.execute(sql, {"src_host_name": src_host_name, "src_db": src_db,
                              "port": port, "replication_to": replication_to,
                              "tgt_host_name": tgt_host_name, "tgt_db": tgt_db, "qname": qname,
                              "src_splex_sid": src_splex_sid, "tgt_splex_sid": tgt_splex_sid, "src_schema": src_schema,
                              "tgt_schema": tgt_schema})

    def getAppln_mapping_info(self, trim_host, db_name, appln_support_code, mapping_name, schema):
        session = self.getLocalSession()
        SQL = '''
                select * from appln_mapping_info where db_name =:db_name and appln_support_code =:appln_support_code and mapping_name =:mapping_name and schema =:schema
                '''
        rows = session.execute(SQL, {"db_name": db_name,
                                     "appln_support_code": appln_support_code, "mapping_name": mapping_name,
                                     "schema": schema}).fetchall()
        return rows

    def insertAppln_mapping_info(self, trim_host, db_name, appln_support_code, mapping_name, schema, service_name):
        session = self.getLocalSession()
        sql = '''
        insert into appln_mapping_info(trim_host,db_name,appln_support_code,mapping_name,date_added,created_by,modified_by,lastmodifieddate,schema,service_name)
        values (:trim_host,:db_name,:appln_support_code,:mapping_name,sysdate,'auto','auto',sysdate,:schema,:service_name)
        '''
        logger.info(sql)
        session.execute(sql, {"trim_host": trim_host, "db_name": db_name,
                              "appln_support_code": appln_support_code, "mapping_name": mapping_name,
                              "schema": schema, "service_name": service_name})

    def getTrimhostByHostName(self, host_name):
        session = self.getLocalSession()
        SQL = "select distinct trim_host from instance_info where host_name = '%s' " % (host_name)
        return session.execute(SQL).fetchall()

    def getShareplex_info(self, src_db, tgt_db, port, replication_to):
        session = self.getLocalSession()
        SQL = '''
                    select src_db,tgt_db,port,replication_to from shareplex_info where src_db=:src_db and tgt_db=:tgt_db and port=:port and replication_to=:replication_to
                    '''
        rows = session.execute(SQL, {"src_db": src_db,
                                     "tgt_db": tgt_db, "port": port,
                                     "replication_to": replication_to}).fetchall()
        return rows

    def getRedoCheckSharePlex(self, src_db, tgt_db):
        session = self.getLocalSession()
        sql = '''
        select src_host,src_db,port,replication_to,tgt_host,tgt_db from wbxadbmon w where w.tgt_db in (:src_db,:tgt_db) or w.src_db in (:src_db,:tgt_db)
        '''
        rows = session.execute(sql, {"src_db": src_db, "tgt_db": tgt_db}).fetchall()
        return rows

    def getSiteCodeByHostName(self, host_name):
        session = self.getLocalSession()
        sql = '''
                select site_code from host_info  where host_name =:host_name
                '''
        rows = session.execute(sql, {"host_name": host_name}).fetchall()
        return rows

    def getApplnMappingInfo(self, db_name, appln_support_code, mapping_name):
        session = self.getLocalSession()
        SQL = '''
                select * from appln_mapping_info where db_name =:db_name and appln_support_code =:appln_support_code and mapping_name=:mapping_name
                '''
        rows = session.execute(SQL, {"db_name": db_name, "appln_support_code": appln_support_code,
                                     "mapping_name": mapping_name}).fetchall()
        return rows

    def getWbxadbmon(self, src_db, tgt_db, port, replication_to):
        session = self.getLocalSession()
        sql = '''
        select src_host,src_db,port,replication_to,tgt_host,tgt_db from wbxadbmon  
        where src_db=:src_db and port =:port and tgt_db =:tgt_db and replication_to =:replication_to
        '''
        rows = session.execute(sql, {"src_db": src_db, "tgt_db": tgt_db, "port": port,
                                     "replication_to": replication_to}).fetchall()
        return rows

    def listwaitevent(self, search_type, db_name):
        session = self.getLocalSession()
        condition = []
        if search_type == "NEW_EVENT":
            condition.append(wbxdbwaiteventvo.monitor_time >= wbxutil.getcurrenttime(300))
            orderby = wbxdbwaiteventvo.db_name
        else:
            condition.append(wbxdbwaiteventvo.monitor_time > wbxutil.getcurrenttime(24 * 60 * 60 * 90))
            orderby = wbxdbwaiteventvo.sql_exec_start
        if db_name is not None and db_name != "":
            condition.append(wbxdbwaiteventvo.db_name == db_name)
        rows = session.query(wbxdbwaiteventvo).filter(*condition).order_by(orderby).all()
        return rows

    def getPwdByUserDB(self, db_name, schema):
        session = self.getLocalSession()
        sql = '''
        select db_name,schema,f_get_deencrypt(password) password,f_get_deencrypt(new_password) new_password 
        from appln_pool_info where db_name ='%s' and schema ='%s'
        ''' % (db_name, schema)
        logger.debug(sql)
        rows = session.execute(sql).fetchall()
        return rows

    def listjobs(self, search_type, job_name, curpage, pagesize):
        session = self.getLocalSession()
        rows = {}
        if search_type == "LISTJOB":
            SQL = '''
            select a.jobname, starttime, nvl(endtime,sysdate+99) ,a.status ,duration,nvl(scheduletime,sysdate),b.comments
            from pccpjobmonitor a,pccpjob b 
            where a.jobname = b.jobname  order by a.jobname
            '''
        else:
            SQL = '''
            select jobname,starttime,endtime,status,duration from (
            select ROWNUM rn,jobname, starttime, nvl(endtime,sysdate+99) endtime, status, duration 
            from pccpjobmonitor_his where starttime >  trunc(sysdate-7,'DD') and jobname='%s'
            order by status,starttime
            ) where rn >(%s-1)*%s and rn < %s*%s
            ''' % (job_name, curpage, pagesize, curpage, pagesize)
        datalist = session.execute(SQL).fetchall()

        if search_type == "LISTJOB":
            rows["datalist"] = [{"jobname": row[0],
                                 "starttime": row[1].strftime('%Y-%m-%d %H:%M:%S'),
                                 "endtime": row[2].strftime('%Y-%m-%d %H:%M:%S'),
                                 "status": row[3],
                                 "duration": str(row[4]),
                                 "scheduletime": row[5].strftime('%Y-%m-%d %H:%M:%S'),
                                 "comments": row[6],
                                 } for row in datalist]
        elif search_type == "HISJOB":
            SQL = '''
                select  count(1) count,nvl(sum(decode(status,'SUCCESS',1,0)),0) as succount,nvl(sum(decode(status,'FAILED',1,0)),0) as failcount
                from pccpjobmonitor_his where jobname = '%s'
                ''' % (job_name)
            cnts = session.execute(SQL).fetchone()
            rows["datalist"] = [{"jobname": row[0],
                                 "starttime": row[1].strftime('%Y-%m-%d %H:%M:%S'),
                                 "endtime": row[2].strftime('%Y-%m-%d %H:%M:%S'),
                                 "status": row[3],
                                 "duration": str(row[4]),
                                 } for row in datalist]
            rows["count"] = "%s" % str(cnts[0])
            rows["succount"] = "%s" % str(cnts[1])
            rows["failcount"] = "%s" % str(cnts[2])

        return rows

    def chatbotjobmonitor(self, job_name, datadate):
        session = self.getLocalSession()
        rows = {}
        if not job_name:
            SQL = '''
                select jobalias,jobmon.jobname, starttime, nvl(endtime,sysdate+99) ,jobmon.status ,duration,nvl(scheduletime,sysdate),job.comments
                from pccpjobmonitor jobmon,pccpjob job
                where jobmon.jobname = job.jobname  order by jobalias
            '''
        else:
            SQL = '''
                select his.jobname,jobalias, starttime, nvl(endtime,sysdate+99) endtime, his.status, duration          
                from pccpjobmonitor_his his,pccpjob job
                where his.jobname = job.jobname and starttime > to_date('%s','yyyy-mm-dd') and endtime < to_date('%s','yyyy-mm-dd') +1
                and (his.jobname='%s' or JOBALIAS='%s')
                order by his.status asc ,starttime desc
            ''' % (datadate, datadate, job_name, job_name)
        datalist = session.execute(SQL).fetchall()

        if not job_name:
            rows["datalist"] = [{"jobalias": row[0],
                                 "jobname": row[1],
                                 "starttime": row[2].strftime('%Y-%m-%d %H:%M:%S'),
                                 "endtime": row[3].strftime('%Y-%m-%d %H:%M:%S'),
                                 "status": row[4],
                                 "duration": str(row[5]),
                                 "scheduletime": row[6].strftime('%Y-%m-%d %H:%M:%S'),
                                 "comments": row[7],
                                 } for row in datalist]
        else:
            SQL = '''
            select  count(1) count,nvl(sum(decode(his.status,'SUCCESS',1,0)),0) as succount,nvl(sum(decode(his.status,'FAILED',1,0)),0) as failcount
                    from pccpjobmonitor_his his,pccpjob job
            where his.jobname = job.jobname and starttime > to_date('%s','yyyy-mm-dd') and endtime < to_date('%s','yyyy-mm-dd') +1
                 and job.jobname = his.jobname  and (his.jobname='%s' or JOBALIAS='%s')
            ''' % (datadate, datadate, job_name, job_name)
            cnts = session.execute(SQL).fetchone()
            rows["datalist"] = [{"starttime": row[2].strftime('%Y-%m-%d %H:%M:%S'),
                                 "endtime": row[3].strftime('%Y-%m-%d %H:%M:%S'),
                                 "status": row[4],
                                 "duration": str(row[5]),
                                 } for row in datalist]
            rows["jobname"] = datalist[0][0]
            rows["jobalias"] = datalist[0][1]
            rows["count"] = "%s" % str(cnts[0])
            rows["succount"] = "%s" % str(cnts[1])
            rows["failcount"] = "%s" % str(cnts[2])
        return rows

    def testConnect(self):
        session = self.getLocalSession()
        sql = '''select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') sys_date from dual'''
        rows = session.execute(sql).fetchall()
        return rows

    def getCCPUserRole(self, cec):
        session = self.getLocalSession()
        sql = '''select * from ccp_user_role_info where username =:cec'''
        rows = session.execute(sql, {"cec": cec}).fetchall()
        return rows

    def get_OtherUser(self, username):
        session = self.getLocalSession()
        sql = '''select username,f_get_deencrypt(password) password from CCP_USER_OTHER_INFO where username =:username and isvalid = '1' '''
        rows = session.execute(sql, {"username": username}).fetchall()
        return rows

    def getsrctgthostname(self, src_host, port):
        session = self.getLocalSession()
        sql = '''
        select level leveid ,src_host,src_db,tgt_host,tgt_db
        from SHAREPLEX_INFO
        start with tgt_host = '%s' and port = %s
        connect by nocycle (prior src_host) = tgt_host and (prior src_db)=tgt_db and (prior port)=port
        union all
        select level leveid,src_host,src_db,tgt_host,tgt_db
        from SHAREPLEX_INFO
        start with src_host = '%s' and port = %s
        connect by nocycle (prior tgt_host) = src_host and (prior tgt_db)=src_db and (prior port)=port
        ''' % (src_host, port, src_host, port)
        rows = session.execute(sql).fetchall()
        src_host_list = []
        tgt_host_list = []
        other_host = None
        for row in rows:
            src_host_list.append(row[1])
            tgt_host_list.append(row[3])
            if row[1] != src_host and other_host == None:
                other_host = row[1]
            if row[3] != src_host and other_host == None:
                other_host = row[3]
        sql = '''
        select level leveid ,src_host,src_db,tgt_host,tgt_db
        from SHAREPLEX_INFO
        start with tgt_host = '%s' and port = %s
        connect by nocycle (prior src_host) = tgt_host and (prior src_db)=tgt_db and (prior port)=port
        union all
        select level leveid,src_host,src_db,tgt_host,tgt_db
        from SHAREPLEX_INFO
        start with src_host = '%s' and port = %s
        connect by nocycle (prior tgt_host) = src_host and (prior tgt_db)=src_db and (prior port)=port
        ''' % (other_host, port, other_host, port)
        rows = session.execute(sql).fetchall()
        for row in rows:
            src_host_list.append(row[1])
            tgt_host_list.append(row[3])
        src_host_list = list(set(src_host_list))
        tgt_host_list = list(set(tgt_host_list))
        return src_host_list, tgt_host_list

    def getAllWebDB(self):
        session = self.getLocalSession()
        sql = '''
        select db_name from database_info di 
        where di.db_type in ('PROD','BTS_PROD') and di.appln_support_code='WEB'
        '''
        rows = session.execute(sql).fetchall()
        return rows

    def commitprocedure(self, id, procedure_name, db_name, created_by, args):
        session = self.getLocalSession()
        sql = '''
        insert into wbxprocedurejob(JOBID,PROCEDURE_NAME,DB_NAME,STATUS,ARGS,CREATEN_BY,MODIFIED_BY,RESULTMSG1,RESULTMSG2,RESULTMSG3) values('%s','%s','%s','PENDING','%s','%s','%s','','','')
        ''' % (id, procedure_name, db_name, args, created_by, created_by)
        logger.info(sql)
        session.execute(sql)

    def getDBNameByPoolname(self, pool_name):
        session = self.getLocalSession()
        sql = '''
            select distinct sm.mapping_name, tm.mapping_name, sm.db_name, tm.db_name, sm.schema
            from shareplex_info si, appln_mapping_info sm, appln_mapping_info tm
            where sm.mapping_name='%s'
            and lower(sm.appln_support_code)='tel'
            and sm.db_name=si.src_db
            and si.tgt_db=tm.db_name
            and lower(tm.appln_support_code)='tel'
            and sm.schema=tm.schema
            and tm.mapping_name like '%%%s'
            ''' % (pool_name.upper(), "".join(pool_name[3:]))
        logger.info(sql)
        rows = session.execute(sql).fetchall()
        if not rows:
            raise wbxexception("can not get tahoedb failover pool_name by inputed poolname")
        # print(rows)
        return rows[0][2], rows[0][3], rows[0][1], rows[0][4]

    def getHostNameOraclePwdByDBname(self, db_name):
        session = self.getLocalSession()
        sql = '''
            select distinct ii.host_name, ii.db_name, f_get_deencrypt(hui.pwd) from instance_info ii, host_user_info hui, database_info db
            where db.db_name in ('%s') 
            and hui.host_name=ii.host_name
            and ii.db_name=db.db_name
            and ii.trim_host=db.trim_host
            and db.db_type <> 'DECOM'
            ''' % db_name.upper()
        rows = session.execute(sql).fetchall()
        # print(rows)
        host_list = []
        for row in rows:
            host_list.append({
                "host_name": row[0],
                "password": row[2]
            })
        return host_list

    def getProcedureInfo(self, id):
        session = self.getLocalSession()
        sql = "select procedure_name,db_name,status,args from wbxprocedurejob where jobid = '%s' " % (id)
        rows = session.execute(sql).fetchall()
        return rows

    def updateProcedureStatus(self, jobvo):
        session = self.getLocalSession()
        resultmsg1 = str(jobvo['resultmsg1'])
        resultmsg1 = resultmsg1.replace("'", "''")
        resultmsg2 = str(jobvo['resultmsg2'])
        resultmsg2 = resultmsg2.replace("'", "''")
        resultmsg3 = str(jobvo['resultmsg3'])
        resultmsg3 = resultmsg3.replace("'", "''")
        sql = '''update wbxprocedurejob set status ='%s',modified_by='%s',lastmodified_date=sysdate,resultmsg1='%s',resultmsg2='%s',resultmsg3='%s' where jobid ='%s' ''' \
              % (jobvo['status'], jobvo['modified_by'], resultmsg1, resultmsg2, resultmsg3, jobvo['jobid'])
        session.execute(sql)

    def procedureRunning(self):
        session = self.getLocalSession()
        sql = "select * from wbxprocedurejob where status = 'RUNNING' "
        rows = session.execute(sql).fetchall()
        return rows

    def getProcedureList(self, procedure_name):
        session = self.getLocalSession()
        sql = '''select jobid,procedure_name,db_name,status,args,to_char(created_date,'YYYY-MM-DD hh24:mi:ss') created_date ,
        to_char(lastmodified_date,'YYYY-MM-DD hh24:mi:ss') lastmodified_date,createn_by created_by,modified_by
         from wbxprocedurejob where procedure_name='%s' order by created_date desc ''' % (procedure_name)
        rows = session.execute(sql).fetchall()
        return rows

    def getProcedureJobByJobid(self, jobid):
        session = self.getLocalSession()
        sql = '''
        select jobid,procedure_name,db_name,status,args,to_char(created_date,'YYYY-MM-DD hh24:mi:ss') created_date,
       to_char(lastmodified_date,'YYYY-MM-DD hh24:mi:ss') lastmodified_date,createn_by,modified_by,resultmsg1,resultmsg2,resultmsg3
       from wbxprocedurejob where jobid = '%s' 
        ''' % (jobid)
        rows = session.execute(sql).fetchall()
        return rows
        result = []
        if not rows:
            return []
        for row in rows:
            result.append({
                "host_name": row[0],
                "instance_name": row[1],
                "db_name": row[2]
            })
        return result

    def getshareplexportbydb(self, db_name):
        session = self.getLocalSession()
        sql = "select distinct port from shareplex_info where src_db='%s' or tgt_db='%s'" % (
        db_name.upper(), db_name.upper())
        rows = session.execute(sql).fetchall()
        port_list = []
        for row in rows:
            port_list.append(row[0])
        return port_list

    def gethosttopologybyhostname(self, host_name):
        session = self.getLocalSession()
        sql = """
        select distinct iin.host_name, iin.instance_name, ii.db_name from host_info hi, instance_info ii, database_info db, instance_info iin, database_info dbi
where ii.host_name='%s'
and db.db_name=ii.db_name
and db.trim_host=ii.trim_host
and ii.host_name=hi.host_name
and ii.trim_host=hi.trim_host
and db.db_type <> 'DECOM'
and iin.db_name=ii.db_name
and iin.trim_host=ii.trim_host
and iin.db_name=dbi.db_name
and iin.trim_host=dbi.trim_host
and dbi.db_type <> 'DECOM'
order by db_name, instance_name""" % host_name
        rows = session.execute(sql).fetchall()
        result = []
        if not rows:
            return []
        for row in rows:
            result.append({
                "host_name": row[0],
                "instance_name": row[1],
                "db_name": row[2]
            })
        return result

    def listJobManagerInstance(self):
        session = self.getLocalSession()
        sql = """
        select distinct host_name,nvl(opstatus, ''),nvl(status, ''),to_char(lastupdatetime,'YYYY-MM-DD hh24:mi:ss') lastupdatetime from wbxjobmanagerinstance order by lastupdatetime, host_name"""
        rows = session.execute(sql).fetchall()
        result = []
        if not rows:
            return []
        for row in rows:
            result.append({
                "host_name": row[0],
                "opstatus": row[1],
                "status": "SHUTDOWN" if row[3] and wbxutil.convertStringtoDateTime(row[3]) < wbxutil.getcurrenttime(
                    180) else row[2],
                "lastupdatetime": row[3]
            })
        return result

    def getdbhostnameinstancename(self, db_name):
        session = self.getLocalSession()
        sql = '''select distinct ii.host_name, iin.instance_name, iin.db_name from host_info hi, instance_info ii, database_info db, instance_info iin, database_info dbi
        where db.db_name='%s'
        and db.db_name=ii.db_name
        and db.trim_host=ii.trim_host
        and ii.host_name=hi.host_name
        and ii.trim_host=hi.trim_host
        and db.db_type <> 'DECOM'
        and iin.host_name=ii.host_name
        and iin.trim_host=ii.trim_host
        and iin.db_name=dbi.db_name
        and iin.trim_host=dbi.trim_host
        and dbi.db_type <> 'DECOM'
        order by db_name, instance_name''' % db_name.upper()
        rows = session.execute(sql).fetchall()
        result = []
        if not rows:
            return []
        for row in rows:
            result.append({
                "host_name": row[0],
                "instance_name": row[1],
                "db_name": row[2]
            })
        return result

    def getpurgemydbinfo(self, mydbname, mydbschema):
        session = self.getLocalSession()
        SQL = '''
        select mydbip,mydbport,mydbuser,mydbpassword,mydbschema,location,mydbname from TEST.WBXSCHMYSQLDB where active=1 
        and mydbname = '%s' and mydbschema = '%s'
        ''' % (mydbname, mydbschema)
        rows = session.execute(SQL).fetchone()
        return rows

    def InitWbxMySqlDBPurgeStatus(self, mydbname, mydbschema):
        session = self.getLocalSession()
        SQL = '''
        UPDATE wbxmysqldbpurge SET status='RUNNING',duration=0,drows=0,errormsg='',starttime=sysdate,endtime=sysdate
            where mydbname='%s' and mydbschema='%s'
        ''' % (mydbname, mydbschema)
        session.execute(SQL)

    def updmydbpurgestatus(self, mydbname, mydbschema, **kargs):
        session = self.getLocalSession()
        SQL = '''
        begin
            UPDATE wbxmysqldbpurge SET status='%s',duration=%s,drows=%s,errormsg='%s',endtime=sysdate
            where mydbname='%s' and mydbschema='%s';
            insert into wbxmysqldbpurge_his(JOBID,DCNAME,MYDBNAME,MYDBSCHEMA,MYDBIP,STATUS,DURATION,DROWS,DNUM,ERRORMSG,STARTTIME,ENDTIME)  
            select JOBID,DCNAME,MYDBNAME,MYDBSCHEMA,MYDBIP,STATUS,DURATION,DROWS,DNUM,ERRORMSG,STARTTIME,ENDTIME 
            from wbxmysqldbpurge where mydbip='%s' and mydbschema = '%s';
        EXCEPTION when others then rollback ;
        end;
        ''' % (kargs["status"], kargs["Duration"], kargs["drows"], kargs["errormsg"], mydbname, mydbschema, mydbname,
               mydbschema)
        session.execute(SQL)

    def getmydbpurgeslog(self, mydbname=None, mydbschema=None):
        session = self.getLocalSession()
        if mydbname and mydbschema:
            SQL = '''
            select to_char(starttime,'yyyy-mm-dd'),sum(drows),sum(decode(drows,0,0,CEIL(duration)))
            from wbxmysqldbpurge_his 
            where starttime >trunc(sysdate-30,'dd') and status = 'SUCCESS' 
              and mydbname = '%s' and mydbschema = '%s'
            group by to_char(starttime,'yyyy-mm-dd')
            order by to_char(starttime,'yyyy-mm-dd')
            ''' % (mydbname, mydbschema)
        else:
            SQL = '''
            select mydbname,dcname,mydbschema,mydbip,status,decode(drows,0,0,CEIL(duration)) duration,drows,errormsg,
            to_char(starttime,'yyyy-mm-dd hh24:mi:ss'),to_char(endtime,'yyyy-mm-dd hh24:mi:ss') from wbxmysqldbpurge
            order by status asc,drows desc
            '''
        rows = session.execute(SQL).fetchall()
        return rows

    def getdbconfigmonitordata(self, db_name):
        session = self.getLocalSession()
        SQL = """
            select ta.metric_name,tb.description, ta.instance_name, tb.operator, tb.critical_value, ta.status
            from wbxdbconfiguremonitorresult ta, wbxmonitoralertthreshold tb
            where ta.metric_name=tb.metric_name
            and ta.db_name='%s'
                """ % (db_name.upper())
        rows = session.execute(SQL).fetchall()
        if not rows:
            raise wbxexception("getdbconfigmonitordata returned None")
        rst = []
        for row in rows:
            rst.append({
                "metric_name": row[0],
                "description": row[1],
                "instance_name": row[2],
                "operator": row[3],
                "critical_value": row[4],
                "status": row[5]
            })
        return rst

    def get_shareplex_port_install_history(self):
        session = self.getLocalSession()
        SQL = """
                    select taskid,host_name, port, db_name, status, createtime, createby from (select att.taskid, sp.host_name, sp.port, sp.db_name, sp.status, to_char(att.createtime,'yyyy-mm-dd hh24:mi:ss') createtime, att.createby
                    from wbxautotask att, wbxsplex_port_install_log sp 
                    where att.task_type='SHAREPLEXPORTINSTALL_TASK'
                    and att.taskid=sp.taskid
                    and sp.process_type='SHAREPLEX_INSTALL'
                    order by att.createtime desc) 
                    where rownum<=50
                        """
        rows = session.execute(SQL).fetchall()
        if not rows:
            raise wbxexception("get_shareplex_port_install_history returned None")
        rst = []
        for row in rows:
            rst.append({
                "taskid": row[0],
                "host_name": row[1],
                "port": row[2],
                "datasource": row[3],
                "status": row[4],
                "createtime": row[5],
                "createby": row[6]
            })
        return rst

    def get_shareplex_port_install_detail_log(self, taskid):
        session = self.getLocalSession()
        SQL = """
        select process_type, status, nvl(logstr, ' '), nvl(logstr1, ' '), nvl(logstr2, ' '), nvl(logstr3, ' ')
        from wbxsplex_port_install_log
        where process_type in ('PREVERIFY', 'EXECUTE_INSTALL')
        and taskid='%s'
            """ % taskid
        rows = session.execute(SQL).fetchall()
        if not rows:
            raise wbxexception("get_shareplex_port_install_detail_log returned None")
        rst = []
        for row in rows:
            rst.append({
                "process_type": row[0],
                "status": row[1],
                "logstr": (row[2] + row[3] + row[4] + row[5]).split("\n") if row[2] else None
            })
        return rst

    def getshareplexportlog(self, taskid, process_type):
        session = self.getLocalSession()
        SQL = """
                    select status from  wbxsplex_port_install_log
                    where taskid='%s'
                    and process_type='%s'
                        """ % (taskid, process_type)
        rows = session.execute(SQL).fetchall()
        if not rows:
            return None
        return rows[0][0]

    def updateshareplexportlog(self, taskid, process_type, status, logstr):
        session = self.getLocalSession()
        SQL = """
        update wbxsplex_port_install_log set status='%s', logstr='%s', logstr1='%s', logstr2='%s', logstr3='%s' where taskid = '%s' and process_type = '%s'
        """ % (
            status, logstr[0: 3501].replace("'", "\""), logstr[3501: 7001].replace("'", "\""),
            logstr[7001: 10501].replace("'", "\""), logstr[10501: 14001].replace("'", "\""), taskid, process_type)
        logger.info(SQL)
        session.execute(SQL)

    def insertshareplexportlog(self, taskid, host_name, db_name, port, process_type, status, logstr):
        session = self.getLocalSession()
        SQL = """
        insert into wbxsplex_port_install_log(taskid, host_name, db_name, port, process_type, status, logstr, logstr1, logstr2, logstr3) values ('%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s')
        """ % (taskid, host_name, db_name, port, process_type, status, logstr[0: 3501].replace("'", "\""),
               logstr[3501: 7001].replace("'", "\""), logstr[7001: 10501].replace("'", "\""),
               logstr[10501: 14001].replace("'", "\""))
        logger.info(SQL)
        session.execute(SQL)

    def getPCCPParameterList(self, db_name):
        session = self.getLocalSession()
        SQL = """select name, value, instance_name, to_char(monitortime,'YYYY-MM-DD hh24:mi:ss') monitortime from pccp_parameter where db_name='%s' order by name""" % db_name
        rows = session.execute(SQL).fetchall()
        print(rows)
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                "name": row[0],
                "value": row[1],
                'instance_name': row[2],
                'monitortime': row[3]
            })
        return result

    def get_pccp_dba_hist_wr_control_list(self, db_name):
        session = self.getLocalSession()
        SQL = """select snap_interval, retention,to_char(monitortime,'YYYY-MM-DD hh24:mi:ss') monitortime from pccp_dba_hist_wr_control where db_name='%s'""" % db_name
        rows = session.execute(SQL).fetchall()
        print(rows)
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                "snap_interval": row[0],
                "retention": row[1],
                'monitortime': row[2]
            })
        return result

    def get_pccp_option_list(self, db_name):
        session = self.getLocalSession()
        SQL = """select parameter, value, to_char(monitortime,'YYYY-MM-DD hh24:mi:ss') monitortime from pccp_option where db_name='%s'""" % db_name
        rows = session.execute(SQL).fetchall()
        print(rows)
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                "parameter": row[0],
                "value": row[1],
                'monitortime': row[2]
            })
        return result

    def get_pccp_dba_registry_list(self, db_name):
        session = self.getLocalSession()
        SQL = """select comp_name, status, version, to_char(monitortime,'YYYY-MM-DD hh24:mi:ss') monitortime from pccp_dba_registry where db_name='%s'""" % db_name
        rows = session.execute(SQL).fetchall()
        print(rows)
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                "comp_name": row[0],
                "status": row[1],
                'version': row[2],
                "monitortime": row[3]
            })
        return result

    def get_pccp_dba_autotask_client_list(self, db_name):
        session = self.getLocalSession()
        SQL = """select client_name, status, to_char(monitortime,'YYYY-MM-DD hh24:mi:ss') monitortime from pccp_dba_autotask_client where db_name='%s'""" % db_name
        rows = session.execute(SQL).fetchall()
        print(rows)
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                "client_name": row[0],
                "status": row[1],
                "monitortime": row[2]
            })
        return result

    def get_parameter_in_db_list_list(self, type, name):
        session = self.getLocalSession()
        SQL = ''
        if type == 1:
            SQL = """select trim_host, db_name, supplemental_log_data_min,supplemental_log_data_pk,supplemental_log_data_ui,supplemental_log_data_fk, supplemental_log_data_all from pccp_database order by trim_host, db_name"""
        elif type == 2:
            if name == '':
                SQL = """select trim_host, db_name, instance_name, name as parameter_name, value as parameter_value from pccp_parameter order by trim_host, db_name, instance_name"""
            else:
                SQL = """select trim_host, db_name, instance_name, name as parameter_name, value as parameter_value from pccp_parameter where name='%s' order by trim_host, db_name, instance_name""" % name
        elif type == 3:
            SQL = """select trim_host, db_name, snap_interval, retention from pccp_dba_hist_wr_control order by trim_host, db_name"""
        elif type == 4:
            if name == '':
                SQL = """select trim_host, db_name, parameter, value from pccp_option order by trim_host,db_name"""
            else:
                SQL = """select trim_host, db_name, parameter, value from pccp_option where db_name='%s' order by trim_host,db_name""" % name
        elif type == 5:
            SQL = """select trim_host, db_name, client_name, status from pccp_dba_autotask_client order by trim_host,db_name"""
        else:
            SQL = """select trim_host, db_name, instance_name, count(1) as log_file_count from pccp_log group by trim_host, db_name, instance_name"""
        rows = session.execute(SQL).fetchall()
        print(rows)
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            if type == 1:
                result.append(({
                    "trim_host": row[0],
                    "db_name": row[1],
                    "supplemental_log_data_min": row[2],
                    "supplemental_log_data_pk": row[3],
                    "supplemental_log_data_ui": row[4],
                    "supplemental_log_data_fk": row[5],
                    "supplemental_log_data_all": row[6]
                }))
            elif type == 2:
                result.append({
                    "trim_host": row[0],
                    "db_name": row[1],
                    "instance_name": row[2],
                    "parameter_name": row[3],
                    "parameter_value": row[4]
                })
            elif type == 3:
                result.append({
                    "trim_host": row[0],
                    "db_name": row[1],
                    "snap_interval": row[2],
                    "retention": row[3]
                })
            elif type == 4:
                result.append({
                    "trim_host": row[0],
                    "db_name": row[1],
                    "parameter": row[2],
                    "value": row[3]
                })
            elif type == 5:
                result.append({
                    "trim_host": row[0],
                    "db_name": row[1],
                    "client_name": row[2],
                    "status": row[3]
                })
            else:
                result.append({
                    "trim_host": row[0],
                    "db_name": row[1],
                    "instance_name": row[2],
                    "log_file_count": row[3]
                })
        return result

    def get_failed_parameter_list(self):
        session = self.getLocalSession()
        SQL = """select trim_host, db_name, instance_name, metric_name, '' as db_value,operator, baseline_value, to_char(monitortime,'YYYY-MM-DD hh24:mi:ss') monitortime from wbxdbconfiguremonitorresult where status='Failed' order by db_name
        """
        rows = session.execute(SQL).fetchall()
        print(rows)
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                "trim_host": row[0],
                "db_name": row[1],
                "instance_name": row[2],
                "metric_name": row[3],
                "db_value": row[4],
                "operator": row[5],
                "baseline_value": row[6],
                "monitortime": row[7]
            })
        return result

    def get_drop_partition_new_status(self, env):
        session = self.getLocalSession()
        SQL = """
SELECT di.db_name, di.trim_host, dp.host_name, to_char(dp.starttime,'YYYY-MM-DD hh24:mi:ss'), to_char(dp.endtime,'YYYY-MM-DD hh24:mi:ss'), ceil(((dp.endtime - dp.starttime)) * 24 * 60) duration, NVL(dp.status,'NOTRUN'), dp.errormsg 
FROM database_info di, wbxdroppartjob dp
WHERE di.trim_host=dp.trim_host(+)
AND di.db_name=dp.db_name(+)
AND di.appln_support_code in ('CONFIG','WEB','TEL','TEO','OPDB','MEDIATE','CI','CSP','DIAGS','CALENDAR','TRANS','LOOKUP','MCT','MMP','TOOLS','DIAGNS')
and di.db_type in ('BTS_PROD','PROD')"""
        if env != "china":
            SQL += """
and di.catalog_db='COMMERCIAL'"""
        SQL += """
order by status, endtime desc"""
        rows = session.execute(SQL).fetchall()
        rst = []
        for row in rows:
            rst.append({
                "db_name": row[0],
                "trim_host": row[1],
                "host_name": row[2],
                "starttime": row[3],
                "endtime": row[4],
                "duration": row[5],
                "status": row[6],
                "errormsg": row[7]
            })
        return rst

    def get_drop_partition_status_by_dbname(self, db_name):
        session = self.getLocalSession()
        SQL = "select db_name, trim_host, host_name, to_char(starttime,'YYYY-MM-DD hh24:mi:ss'), to_char(endtime,'YYYY-MM-DD hh24:mi:ss'), ceil(((endtime - starttime)) * 24 * 60) duration, status, errormsg from wbxdroppartjobhistory where db_name='%s' and rownum < 21 order by endtime desc" % db_name.upper()
        rows = session.execute(SQL).fetchall()
        rst = []
        for row in rows:
            rst.append({
                "db_name": row[0],
                "trim_host": row[1],
                "host_name": row[2],
                "starttime": row[3],
                "endtime": row[4],
                "duration": row[5],
                "status": row[6],
                "errormsg": row[7]
            })
        return rst

    def get_db_info(self, db_name, host_name):
        session = self.getLocalSession()
        sql = '''
               select distinct hi.trim_host,di.db_name,di.db_version, di.db_type, di.application_type, di.appln_support_code,
                   di.service_name, di.listener_port,di.monitor,di.wbx_cluster,di.web_domain ,to_char(di.date_added,'yyyy-mm-dd hh24:mi:ss') date_added,
                   to_char(di.lastmodifieddate,'yyyy-mm-dd hh24:mi:ss') lastmodifieddate
                   from host_info hi,database_info di,instance_info ii
                   where di.db_name=ii.db_name
                   and di.trim_host=ii.trim_host
                   and ii.trim_host=hi.trim_host
                   and ii.host_name=hi.host_name
                   and di.db_type in ('PROD','BTS_PROD')
               '''
        if db_name:
            sql = "%s and di.db_name='%s'" % (sql, db_name)
        if host_name:
            sql = "%s and hi.host_name='%s'" % (sql, host_name)
        sql = ''' %s
        order by di.db_name
        ''' % (sql)
        logger.info(sql)
        rows = session.execute(sql).fetchall()
        return rows

    def get_rac_info(self, db_name, host_name):
        session = self.getLocalSession()
        sql = '''
                select distinct h2.trim_host, h2.host_name, h2.site_code, h2.host_ip, h2.ssh_port, h2.vip_name, 
                h2.vip_ip, h2.priv_name, h2.priv_ip, h2.scan_name, h2.scan_ip1,h2.scan_ip2,h2.scan_ip3,h2.processor,h2.kernel_release, h2.physical_cpu, h2.CORES,
                to_char(h2.install_date,'yyyy-mm-dd hh24:mi:ss') install_date,
                to_char(h2.date_added,'yyyy-mm-dd hh24:mi:ss') date_added,
                to_char(h2.lastmodifieddate,'yyyy-mm-dd hh24:mi:ss') lastmodifieddate
                from host_info h1, host_info h2
                where h1.scan_name=h2.scan_name
                '''
        if db_name:
            sql = "%s and h1.host_name in (select host_name from instance_info where db_name = '%s')" % (sql, db_name)
            sql = "%s and h2.host_name in (select host_name from instance_info where db_name = '%s')" % (sql, db_name)
        if host_name:
            sql = "%s and h1.host_name = '%s'" % (sql, host_name)
        sql = "%s order by h2.host_name " % (sql)
        logger.info(sql)
        rows = session.execute(sql).fetchall()
        return rows

    def get_depot_manage_user_info(self, db_name, trim_host):
        session = self.getLocalSession()
        sql = '''
                    select distinct schema as username, trim_host,schematype, appln_support_code, f_get_deencrypt(password) as password 
                        from appln_pool_info where db_name='%s' and trim_host='%s'
                        order by schema
                    ''' % (db_name, trim_host)
        logger.info(sql)
        rows = session.execute(sql).fetchall()
        return rows

    def get_depot_manage_splexplex_info(self, db_name):
        session = self.getLocalSession()
        sql = '''
        select src_host, src_db, port, replication_to, tgt_host, tgt_db, qname, src_splex_sid, tgt_splex_sid, src_schema, tgt_schema,
        to_char(si.date_added,'yyyy-mm-dd') date_added,
        to_char(si.lastmodifieddate,'yyyy-mm-dd') lastmodifieddate
        from shareplex_info si, database_info di
        where (si.tgt_db='%s' or si.src_db='%s')
        and si.src_db=di.db_name
        and di.db_type='PROD'
        order by port,tgt_db,qname
        ''' % (db_name, db_name)
        logger.info(sql)
        rows = session.execute(sql).fetchall()
        return rows

    def get_host_info_by_hostname(self, host_name):
        session = self.getLocalSession()
        sql = '''
        SELECT trim_host,lc_code FROM host_info WHERE host_name='%s'
        ''' % (host_name)
        rows = session.execute(sql).fetchall()
        return rows

    def get_host_info_by_scanname(self, scan_name):
        session = self.getLocalSession()
        sql = " SELECT distinct host_name FROM host_info hi WHERE scan_name like '%%%s%%' " % (scan_name)
        logger.info(sql)
        rows = session.execute(sql).fetchall()
        return rows

    def delete_instance_info(self, host_name):
        session = self.getLocalSession()
        sql = '''
                DELETE FROM instance_info WHERE host_name='%s'
                ''' % (host_name)
        logger.info(sql)
        session.execute(sql)

    def delete_host_info(self, host_name):
        session = self.getLocalSession()
        sql = '''
                DELETE FROM host_info WHERE host_name='%s'
                ''' % (host_name)
        logger.info(sql)
        session.execute(sql)

    def get_site_code(self, lc_code):
        session = self.getLocalSession()
        sql = '''
        select site_code from (select rownum as rown, site_code from site_info where lc_code='%s' order by site_code) where rown=1
        ''' % (lc_code)
        logger.info(sql)
        rows = session.execute(sql).fetchall()
        return rows

    def insert_host_info(self, hostInfoVo):
        session = self.getLocalSession()
        trim_host = hostInfoVo.trim_host
        host_name = hostInfoVo.host_name
        domain = hostInfoVo.domain
        site_code = hostInfoVo.site_code
        host_ip = hostInfoVo.host_ip
        vip_name = hostInfoVo.vip_name
        vip_ip = hostInfoVo.vip_ip
        priv_name = hostInfoVo.priv_name
        priv_ip = hostInfoVo.priv_ip
        scan_name = hostInfoVo.scan_name
        scan_ip1 = hostInfoVo.scan_ip1
        scan_ip2 = hostInfoVo.scan_ip2
        scan_ip3 = hostInfoVo.scan_ip3
        os_type_code = hostInfoVo.os_type_code
        processor = hostInfoVo.processor
        kernel_release = hostInfoVo.kernel_release
        hardware_platform = hostInfoVo.hardware_platform
        physical_cpu = hostInfoVo.physical_cpu
        cores = hostInfoVo.cores
        cpu_model = hostInfoVo.cpu_model
        flag_node_virtual = hostInfoVo.flag_node_virtual
        comments = hostInfoVo.comments
        lc_code = hostInfoVo.lc_code
        ssh_port = hostInfoVo.ssh_port
        sql = '''
        INSERT INTO host_info(TRIM_HOST, HOST_NAME, DOMAIN, SITE_CODE, HOST_IP, 
                VIP_NAME, VIP_IP, PRIV_NAME, PRIV_IP, SCAN_NAME, SCAN_IP1, SCAN_IP2, SCAN_IP3, OS_TYPE_CODE, 
                PROCESSOR, KERNEL_RELEASE, HARDWARE_PLATFORM, PHYSICAL_CPU, CORES, CPU_MODEL, 
                FLAG_NODE_VIRTUAL, INSTALL_DATE, DATE_ADDED, LASTMODIFIEDDATE, COMMENTS, LC_CODE, 
                SSH_PORT, CREATED_BY, MODIFIED_BY)
            VALUES(:trim_host,:host_name,:domain,:site_code,:host_ip,:vip_name,:vip_ip,
                :priv_name,:priv_ip,:scan_name,:scan_ip1,:scan_ip2,:scan_ip3,:os_type_code,
                :processor,:kernel_release,:hardware_platform,:physical_cpu,:cores,:cpu_model,
                :flag_node_virtual,sysdate,sysdate,sysdate,:comments,:lc_code,
                :ssh_port,'AutomationTool','AutomationTool')
        '''
        logger.info(sql)
        session.execute(sql, {"trim_host": trim_host, "host_name": host_name,
                              "domain": domain, "site_code": site_code,
                              "host_ip": host_ip, "vip_name": vip_name, "vip_ip": vip_ip,
                              "priv_name": priv_name, "priv_ip": priv_ip, "scan_name": scan_name, "scan_ip1": scan_ip1,
                              "scan_ip2": scan_ip2, "scan_ip3": scan_ip3, "os_type_code": os_type_code,
                              "processor": processor, "kernel_release": kernel_release,
                              "hardware_platform": hardware_platform, "physical_cpu": physical_cpu, "cores": cores,
                              "cpu_model": cpu_model, "flag_node_virtual": flag_node_virtual, "comments": comments,
                              "lc_code": lc_code, "ssh_port": ssh_port})

    def isExistDeport(self, inst_name, host_name):
        session = self.getLocalSession()
        sql = '''
        SELECT instance_name FROM instance_info WHERE host_name='%s' and instance_name='%s'
        ''' % (host_name, inst_name)
        rows = session.execute(sql).fetchall()
        return rows

    def insert_instance_info(self, trim_host, host_name, db_name, instance_name):
        session = self.getLocalSession()
        sql = '''
        INSERT INTO instance_info(trim_host, host_name, db_name, instance_name, created_by, modified_by, date_added, lastmodifieddate)
                        VALUES('%s','%s','%s','%s','AutomationTool','AutomationTool',sysdate,sysdate)
        ''' % (trim_host, host_name, db_name, instance_name)
        logger.info(sql)
        session.execute(sql)

    def get_inst_list(self, host_name):
        session = self.getLocalSession()
        sql = '''
                SELECT instance_name FROM instance_info WHERE host_name = '%s'
                ''' % (host_name,)
        rows = session.execute(sql).fetchall()
        return rows

    def delete_instance_info_by(self, host_name, instance_name):
        session = self.getLocalSession()
        sql = '''
        delete from instance_info where instance_name= '%s' and host_name = '%s'
        ''' % (instance_name, host_name)
        logger.info(sql)
        session.execute(sql)

    def get_database_info(self, db_name):
        session = self.getLocalSession()
        sql = '''
               select * from database_info where db_name= '%s'
                ''' % (db_name)
        rows = session.execute(sql).fetchall()
        return rows

    def insert_database_info(self, databaseVo):
        session = self.getLocalSession()
        trim_host = databaseVo.trim_host
        db_vendor = databaseVo.db_vendor
        db_version = databaseVo.db_version
        db_type = databaseVo.db_type
        application_type = databaseVo.application_type
        appln_support_code = databaseVo.appln_support_code
        db_home = databaseVo.db_home
        db_name = databaseVo.db_name
        # failover_trim_host = databaseVo.failover_trim_host
        # failover_db = Column(String(30))
        service_name = databaseVo.service_name
        listener_port = int(databaseVo.listener_port)
        monitor = databaseVo.monitor
        contents = databaseVo.contents
        web_domain = databaseVo.web_domain
        wbx_cluster = databaseVo.wbx_cluster

        # print(databaseVo)
        # sql = '''
        # INSERT INTO database_info(trim_host,db_vendor,db_version,db_type,db_patch,application_type,appln_support_code,
        # db_home,db_name,failover_trim_host,failover_db,service_name,listener_port,backup_method,backup_server,
        # catalog_trim_host,catalog_db,monitor,appln_contact,contents,createddate,wbx_cluster,
        # date_added,lastmodifieddate,web_domain,created_by,modified_by)
        # VALUES(:trim_host,:db_vendor,:db_version,:db_type,:db_patch,:application_type,:appln_support_code,
        # :db_home,:db_name,:failover_trim_host,:failover_db,:service_name,:listener_port,:backup_method,:backup_server,
        # :catalog_trim_host,:catalog_db,:monitor,:appln_contact,:contents,sysdate,:wbx_cluster,
        # sysdate,sysdate,:web_domain,:created_by,:modified_by)
        # ''' %(trim_host, db_vendor, db_version, db_type,"",application_type,appln_support_code,
        #       db_home,db_name,"","",service_name,listener_port,"","","","",monitor,"",contents,wbx_cluster,web_domain,'AutomationTool','AutomationTool')

        sql = '''
        INSERT INTO database_info(trim_host,db_vendor,db_version,db_type,db_patch,application_type,appln_support_code,
        db_home,db_name,failover_trim_host,failover_db,service_name,listener_port,backup_method,backup_server,
        catalog_trim_host,catalog_db,monitor,appln_contact,contents,createddate,wbx_cluster,
        date_added,lastmodifieddate,web_domain,created_by,modified_by)
        VALUES('%s','%s','%s','%s','%s','%s','%s',
        '%s','%s','%s','%s','%s',%s,'%s','%s',
        '%s','%s','%s','%s','%s',sysdate,'%s',
        sysdate,sysdate,'%s','%s','%s')
        ''' % (trim_host, db_vendor, db_version, db_type, '', application_type, appln_support_code,
               db_home, db_name, '', '', service_name, listener_port, '', '', '', '', monitor, '', contents,
               wbx_cluster, web_domain, 'AutomationTool', 'AutomationTool')

        logger.info(sql)
        session.execute(sql)

    def get_shareplex_list(self, src_host_name, port):
        session = self.getLocalSession()
        sql = '''
        select * from shareplex_info where src_host='%s' and port = %s
        ''' % (src_host_name, port)
        rows = session.execute(sql).fetchall()
        return rows

    def deleteShareplex_info(self, src_db, tgt_db, port, qname):
        session = self.getLocalSession()
        if len(qname) > 0:
            sql = '''
            delete from shareplex_info where src_db='%s' and tgt_db= '%s' and port = %s and qname = '%s'
            ''' % (src_db, tgt_db, port, qname)
            logger.info(sql)
            session.execute(sql)
        else:
            sql = '''
                       delete from shareplex_info where src_db='%s' and tgt_db= '%s' and port = %s
                       ''' % (src_db, tgt_db, port)
            logger.info(sql)
            session.execute(sql)

    def get_shareplex_info(self, src_db, tgt_db, port, qname, src_splex_sid, tgt_splex_sid, src_schema, tgt_schema):
        session = self.getLocalSession()
        if len(qname) > 0:
            SQL = '''
            select src_db,tgt_db,port,replication_to from shareplex_info 
            where src_db=:src_db and tgt_db=:tgt_db and port=:port and qname=:qname
            and src_splex_sid=:src_splex_sid and tgt_splex_sid=:tgt_splex_sid
            and lower(src_schema)=:src_schema and lower(tgt_schema)=:tgt_schema
            '''
            rows = session.execute(SQL, {"src_db": src_db,
                                         "tgt_db": tgt_db, "port": port,
                                         "qname": qname, "src_splex_sid": src_splex_sid, "tgt_splex_sid": tgt_splex_sid,
                                         "src_schema": src_schema, "tgt_schema": tgt_schema}).fetchall()
            return rows
        else:
            SQL = '''
                        select src_db,tgt_db,port,replication_to from shareplex_info 
                        where src_db=:src_db and tgt_db=:tgt_db and port=:port
                        and src_splex_sid=:src_splex_sid and tgt_splex_sid=:tgt_splex_sid
                        and lower(src_schema)=:src_schema and lower(tgt_schema)=:tgt_schema
                        '''
            rows = session.execute(SQL, {"src_db": src_db,
                                         "tgt_db": tgt_db, "port": port, "src_splex_sid": src_splex_sid,
                                         "tgt_splex_sid": tgt_splex_sid,
                                         "src_schema": src_schema, "tgt_schema": tgt_schema}).fetchall()
            return rows

    def getdbtype(self, **kargs):
        session = self.getLocalSession()
        new_host_name = kargs["new_host_name"]
        old_host_name = kargs["old_host_name"]
        db_name = kargs["db_name"]
        SQL = '''
select db_type,db.appln_support_code from database_info db, instance_info ii, host_info hi 
where db.trim_host=ii.trim_host AND db.db_name=ii.db_name
AND ii.trim_host=hi.trim_host AND ii.host_name=hi.host_name
AND db.db_name ='%s' AND hi.host_name in ('%s','%s')
AND upper(db.db_vendor)='ORACLE' and db.db_type<> 'DECOM'
        ''' % (db_name, new_host_name, old_host_name)
        row = session.execute(SQL).fetchone()
        res = dict(zip(row.keys(), row))
        return res

    def getdblicences(self, **kargs):
        session = self.getLocalSession()
        site_code = '' if kargs["dc_name"] == "ALL" else "'" + "','".join(kargs["dc_name"].split(',')) + "'"
        host_name = '' if kargs["host_name"] == "ALL" else "'" + "','".join(kargs["host_name"].split(',')) + "'"
        db_type = "'BTS_PROD','PROD'" if kargs["db_type"] == "ALL" else "'" + "','".join(
            kargs["db_type"].split(',')) + "'"
        SQL = '''
select distinct city||','||country||'('||site_code||')' "Data Center Location"
,decode(regexp_substr(db_version,'[^.]+'),
    '11','Oracle DB 11g EE Release 11.2.0.4.0 - 64bit Production With Partitioning, RAC and ASM options',
    '19','Oracle DB 19c EE Release 19.0.0.0.0 - Production Version 19.8.0.0.0',
    db_version) "Product"
    ,feature as "More Packs and Options"
    ,host_name "Physical Machine Name",physical_cpu "# Physical CPUs",cores "# Cores per Physical CPU"
    ,'N/A' "Virtualization Technology"
    ,listagg(db_type,',') within group(order by instance_name) over(partition by host_name) "Environment Usage"
    ,listagg(appln_support_code,',') within group(order by instance_name) over(partition by host_name) "Application/Project Name"
    ,install_date "Installation Date"
    ,listagg(instance_name,',') within group(order by instance_name) over(partition by host_name) "Instance(s)" from(
select distinct si.city,si.country,si.site_code
  ,regexp_substr(db.db_version,'[^.]+') db_version
  ,hi.host_name,hi.physical_cpu,hi.cores,hi.install_date
  ,db.db_type,db.appln_support_code,ii.instance_name
  ,listagg(case when ex.feature is not null then ex.feature||'('||ex.current_used||')' end,',') within group(order by feature) over(partition by hi.host_name)  as feature
 from database_info db
 inner join instance_info ii on db.trim_host=ii.trim_host AND db.db_name=ii.db_name
 inner join host_info hi on ii.trim_host=hi.trim_host AND ii.host_name=hi.host_name
 inner join site_info si on hi.lc_code=si.lc_code and hi.site_code=si.site_code
 left join wbxoradbfeatures ex on db.trim_host=ex.trim_host AND db.db_name=ex.db_name
 where upper(db.db_vendor)='ORACLE'
 '''
        if site_code:
            SQL += " and hi.site_code in (%s)" % site_code
        if host_name:
            SQL += " and hi.host_name in (%s)" % host_name
        SQL += " and db.db_type in (%s) ) order by host_name" % db_type
        rows = session.execute(SQL).fetchall()
        res = [dict(zip(row.keys(), row)) for row in rows]
        return res

    def getdbliclabelinfo(self):
        session = self.getLocalSession()
        res = {}
        SQL = '''
select  distinct
   tp.name,
   decode(tp.name,'host_name',hi.host_name,hi.site_code) val
 from database_info db, instance_info ii, host_info hi ,( select 'site_code' name from dual union select 'host_name' name from dual) tp
 where db.trim_host=ii.trim_host AND db.db_name=ii.db_name
 AND ii.trim_host=hi.trim_host AND ii.host_name=hi.host_name
 AND upper(db.db_vendor)='ORACLE'
 and db.db_type in ('BTS_PROD','PROD') 
 order by name,val
        '''
        rows = session.execute(SQL).fetchall()
        for item in rows:
            if item[0] not in res.keys():
                res[item[0]] = []
            res[item[0]].append(item[1])
        res["db_type"] = ["BTS_PROD", "PROD"]
        return res

    def get_adbmon_list_DCName(self, dc_name, delay_min):
        session = self.getLocalSession()
        sql = '''
                        with ta as (
         select src_host,src_db,port,replication_to,tgt_host,tgt_db,src_site_code,tgt_site_code,
         temp.src_splex_sid, temp.tgt_splex_sid,
            to_char(lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,
             to_char(montime,'yyyy-mm-dd hh24:mi:ss') montime,
             diff_secend,diff_day,diff_hour,diff_min,(diff_day||':'||diff_hour||':'||diff_min) lag_by,
             case when temp.diff_secend>nvl(wbl.lag_by,%s)*60  then '1' else '0' end alert_flag
             from ( select distinct ta.*,ROUND((ta.montime-ta.lastreptime)*24*60*60) diff_secend,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)) diff_day,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)- trunc(TO_NUMBER(ta.montime - ta.lastreptime))*24 diff_hour,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24*60)-trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)*60 diff_min,
                        sdi.appln_support_code src_appln_support_code,tdi.appln_support_code tgt_appln_support_code,si.src_splex_sid, si.tgt_splex_sid,
                        shi.site_code src_site_code,thi.site_code tgt_site_code
                        from wbxadbmon ta, shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii,host_info shi,host_info thi
                        where ta.src_host=si.src_host
                        and ta.src_db=si.src_db
                        and ta.tgt_host=si.tgt_host
                        and ta.tgt_db=si.tgt_db
                        and ta.port=si.port
                        and ta.replication_to = si.replication_to||nvl2(qname,'_'||qname, '')
                        and si.src_host=sii.host_name
                        and si.src_db=sii.db_name
                        and sii.db_name=sdi.db_name
                        and sii.trim_host=sdi.trim_host
                        and si.tgt_host=tii.host_name
                        and si.tgt_db=tii.db_name
                        and tdi.db_name=tii.db_name
                        and tdi.trim_host=tii.trim_host
                        and sii.host_name = shi.host_name
                        and tii.host_name = thi.host_name
                        and tdi.db_type in ('PROD')
                        and sdi.db_type in ('PROD')

                        ) temp,wbxadbmonlagby wbl 
             where temp.src_appln_support_code= wbl.src_appln_support_code(+)
              and temp.tgt_appln_support_code = wbl.tgt_appln_support_code(+)
              order by temp.diff_secend desc
        ) 
        select 
        ta.src_host,ta.src_db,ta.port,ta.replication_to,ta.tgt_host,ta.tgt_db,ta.src_site_code,ta.tgt_site_code,ta.src_splex_sid,ta.tgt_splex_sid,ta.lastreptime,ta.montime,ta.lag_by
        from ta  
        where ta.alert_flag = '1'
        and (ta.src_site_code = '%s' or ta.tgt_site_code = '%s')
                        ''' % (delay_min, dc_name, dc_name)
        rows = session.execute(sql).fetchall()
        return rows

    def getschemabydbNameandpoolName(self, pool_name, db_name):
        session = self.getLocalSession()
        SQL = """
        select distinct schema from appln_mapping_info where db_name='%s' and mapping_name='%s'
        """ % (db_name, pool_name)
        rows = session.execute(SQL).fetchall()
        if len(rows) > 1:
            raise wbxexception("Found more than 1 target schema : %s" % rows)
        return rows[0][0]

    def checktahoedbisuseless(self, db_name):
        session = self.getLocalSession()
        SQL = """
        select distinct mapping_name from appln_mapping_info where db_name='%s'
        """ % (db_name)
        rows = session.execute(SQL).fetchall()
        if len(rows) > 1:
            return False
        return True

    def getShareplexChannelByDBNameandPort(self, db_name, port):
        session = self.getLocalSession()
        rst_dict = []
        SQL = """
        select src_host, src_db, port,tgt_host, tgt_db from shareplex_info where (src_db='%s' or tgt_db='%s') and port=%s
        """ % (db_name, db_name, port)
        rows = session.execute(SQL).fetchall()
        if not rows:
            raise wbxexception("getShareplexChannelByDBNameandPort get None!!")
        for row in rows:
            rst_dict.append(row[0])
            rst_dict.append(row[3])
        return list(set(rst_dict))

    def getCfgShareplexChannelByDBName(self, db_name):
        session = self.getLocalSession()
        rst_dict = []
        SQL = """
        select src_host, src_db, port,tgt_host, tgt_db from shareplex_info where (src_db='%s' and tgt_db in ('GCFGDB','CONFIGDB')) or (tgt_db='%s' and src_db in ('GCFGDB','CONFIGDB'))
        """ % (db_name, db_name)
        rows = session.execute(SQL).fetchall()
        if not rows:
            raise wbxexception("getCfgShareplexChannelByDBName get None!!")
        for row in rows:
            rst_dict.append({
                "src_host": row[0],
                "src_db": row[1],
                "port": row[2],
                "tgt_host": row[3],
                "tgt_db": row[4]
            })
        return rst_dict

    def getoldprigsbdbbypoolName(self, pri_pool_name, gsb_pool_name):
        session = self.getLocalSession()
        SQL = """
        select distinct pm.mapping_name, gm.mapping_name, pm.db_name, gm.db_name, pm.schema
            from shareplex_info si, appln_mapping_info pm, appln_mapping_info gm
            where pm.mapping_name='%s' 
            and gm.mapping_name='%s' 
            and lower(pm.appln_support_code)='tel'
            and pm.db_name=si.src_db
            and si.tgt_db=gm.db_name
            and lower(gm.appln_support_code)='tel'
        """ % (pri_pool_name.upper(), gsb_pool_name.upper())
        rows = session.execute(SQL).fetchall()
        if len(rows) != 1:
            raise wbxexception("the pri schema and gsb schema donot match or cannot get the matched db pair!")
        return rows[0][2], rows[0][3], rows[0][4]

    def getgsbdbbypridbName(self, db_name):
        session = self.getLocalSession()
        SQL = """
                select distinct gm.db_name
            from shareplex_info si, appln_mapping_info pm, appln_mapping_info gm
            where lower(pm.appln_support_code)='tel'
            and pm.db_name=si.src_db
            and si.tgt_db=gm.db_name
            and lower(gm.appln_support_code)='tel'
            and pm.schema=gm.schema
            and si.src_db='%s'
                """ % (db_name.upper())
        rows = session.execute(SQL).fetchall()
        if len(rows) != 1:
            raise wbxexception("cannot get the new gsb db by pri db_name %s!" % db_name)
        return rows[0][0]

    def gethostNamebyport(self, port, db_name, host_list):
        session = self.getLocalSession()
        SQL = """
        select distinct src_host from shareplex_info where src_db='%s' and port=%s and src_host in ('%s', '%s')
        """ % (db_name.upper(), port, host_list[0].lower(), host_list[1].lower())
        rows = session.execute(SQL).fetchall()
        if len(rows) != 1:
            raise wbxexception("cannot get old gsb host for replication setup script")
        return rows[0][0]

    def getconfighostbydbNameandPort(self, db_name, port):
        pri_configdb_host = None
        gsb_configdb_host = None
        session = self.getLocalSession()
        SQL = """
        select distinct src_host, src_db from shareplex_info where tgt_db='%s' and src_db in ('CONFIGDB', 'GCFGDB') and port=%s
        """ % (db_name.upper(), port)
        rows = session.execute(SQL).fetchall()
        if len(rows) != 2:
            raise wbxexception("cannot get config host info")
        for row in rows:
            if row[1] == "CONFIGDB":
                pri_configdb_host = row[0]
            elif row[1] == "GCFGDB":
                gsb_configdb_host = row[0]
        return pri_configdb_host, gsb_configdb_host

    def getinstanceNamebydbNameandhost(self, db_name, host_name):
        session = self.getLocalSession()
        SQL = """
         select distinct instance_name from instance_info where db_name='%s' and host_name='%s'
         """ % (db_name.upper(), host_name.lower())
        rows = session.execute(SQL).fetchall()
        if len(rows) != 1:
            raise wbxexception("cannot get the instance name by db_name %s and host_name %s !" % (db_name, host_name))
        return rows[0][0]

    def isportshared(self, src_host, port):
        session = self.getLocalSession()
        SQL = """select count(1) from (select distinct lower(src_splex_sid), lower(src_schema) from shareplex_info where port=:port and src_host=:src_host)"""
        rows = session.execute(SQL, {"src_host": src_host, "port": port}).fetchall()
        vCount = rows[0][0]
        return False if rows[0][0] == 1 else True

    # def getChannelCount(self, host_name, port, tgt_schema):
    #     session = self.getLocalSession()
    #     SQL = """select count(1) from (select distinct lower(src_splex_sid), lower(src_schema) from shareplex_info where port=:port and src_host=:src_host)"""
    #     rows = session.execute(SQL, {"src_host": src_host, "port": port}).fetchall()
    #     vCount = rows[0][0]
    #     return False if rows[0][0] == 1 else True

    def getTahoePoolCountInDB(self, db_name, pool_name):
        session = self.getLocalSession()
        SQL = """ select count(1) from appln_mapping_info where db_name=:db_name and mapping_name != :pool_name """
        row = session.execute(SQL, {"db_name": db_name, "pool_name": pool_name}).fetchone()
        vCount = row[0]
        return vCount

    def getTgtdbCountByPort(self, tgt_host, splex_port):
        session = self.getLocalSession()
        SQL = """ select count(1) FROM (
                    select distinct si.tgt_host, si.port, si.tgt_db 
                    from shareplex_info si, database_info sdi, instance_info sii
                    where si.tgt_host=:tgt_host 
                    and si.port=:splex_port
                    and si.src_host=sii.host_name
                    and si.src_db=sii.db_name
                    and sii.trim_host=sdi.trim_host
                    and sii.db_name=sdi.db_name
                    and sdi.appln_support_code='CONFIG')  """
        row = session.execute(SQL, {"tgt_host": tgt_host, "splex_port": splex_port}).fetchone()
        vCount = row[0]
        return vCount

    def updateDepotDBForTahoeCutover(self, db_name, pool_name, port_from_configdb, port_to_opdb,
                                     new_trim_host, new_db_name, isdbdecomm, isRemoveChannel):
        session = self.getLocalSession()
        if isdbdecomm:
            SQL = "UPDATE database_info SET db_type='DECOM' WHERE db_name=:db_name"
            session.execute(SQL, {"db_name": db_name})
        SQL = "UPDATE appln_mapping_info SET db_name=:new_db_name, trim_host=:new_trim_host, schema='tahoe', service_name=lower(:pool_name)||'ha' WHERE mapping_name=:pool_name"
        session.execute(SQL, {"new_db_name": new_db_name, "new_trim_host": new_trim_host, "pool_name": pool_name})
        SQL = ''' DELETE FROM shareplex_info WHERE src_db=:src_db and port=:splex_port '''
        session.execute(SQL, {"src_db": db_name, "splex_port": port_to_opdb})
        if isRemoveChannel:
            SQL = ''' DELETE FROM shareplex_info WHERE src_db in ('CONFIGDB','GCFGDB') and port=:splex_port and tgt_db=:tgt_db '''
            session.execute(SQL, {"tgt_db": db_name, "splex_port": port_from_configdb})

    def getpoolinfobydbNameorpoolName(self, db_name):
        rst_list = []
        session = self.getLocalSession()
        SQL = """
        select distinct mi1.db_name, mi2.mapping_name, lower(mi2.mapping_name) || 'ha' as service_name 
        from appln_mapping_info mi1, appln_mapping_info mi2, database_info db
        where (mi1.mapping_name='%s' or mi1.db_name='%s')
        and mi1.db_name=mi2.db_name
        and mi1.db_name=db.db_name
        and db.db_type <> 'DECOM'
        """ % (db_name.upper(), db_name.upper())
        rows = session.execute(SQL).fetchall()
        for row in rows:
            if row[0] == row[1] + "DB":
                continue
            rst_list.append({
                "db_name": row[0],
                "pool_name": row[1],
                "service_name": row[2]
            })
        return rst_list

    def getWbxJobMgStatusByHost(self, host_name):
        session = self.getLocalSession()
        row = ""
        SQL = "select host_name||','||status from wbxjobmanagerinstance where host_name in ('%s')" % host_name
        row = session.execute(SQL).fetchone()
        if row is None or row[0] is None:
            return None
        return row

    def getRmanBackupStatusList(self):
        session = self.getLocalSession()
        SQL = """select bk.db_id,bk.db_name,bk.db_host,NVL(to_char(bk.full_backup_start,'YYYY-MM-DD hh24:mi:ss'),'2001-01-01 00:00') full_backup_start, NVL(to_char(bk.full_backup_end,'YYYY-MM-DD hh24:mi:ss'),'2001-01-01 00:00') full_backup_end,bk.full_backup_status,bk.full_backup_behind, NVL(to_char(bk.arch_backup_start,'YYYY-MM-DD hh24:mi:ss'),'2001-01-01 00:00') arch_backup_start,NVL(to_char(bk.arch_backup_end,'YYYY-MM-DD hh24:mi:ss'),'2001-01-01 00:00') arch_backup_end, bk.arch_backup_status,bk.arch_backup_behind,bk.restore_validate,NVL(to_char(bk.rest_val_start,'YYYY-MM-DD hh24:mi:ss'),'2001-01-01 00:00') rest_val_start, NVL(to_char(bk.rest_val_end,'YYYY-MM-DD hh24:mi:ss'),'2001-01-01 00:00') rest_val_end from dd_backup_status bk, database_info di, instance_info ii where bk.db_name=ii.db_name and bk.db_host=ii.host_name and ii.trim_host=di.trim_host and ii.db_name=di.db_name and di.db_type in ('PROD','BTS_PROD') order by decode(full_backup_status,'FAILED',1,'Notrun',1,0)+decode(arch_backup_status,'FAILED',1,'Notrun',1,0) desc, bk.db_name"""
        rows = session.execute(SQL).fetchall()
        if not rows:
            return None
        result = []
        for row in rows:
            if not row[0]:
                continue
            result.append({
                "db_id": row[0],
                "db_name": row[1],
                'db_host': row[2],
                'full_backup_start': row[3],
                'full_backup_end': row[4],
                'full_backup_status': row[5],
                'full_backup_behind': row[6],
                'arch_backup_start': row[7],
                'arch_backup_end': row[8],
                'arch_backup_status': row[9],
                'arch_backup_behind': row[10],
                'restore_validate': row[11],
                'rest_val_start': row[12],
                'rest_val_end': row[13]
            })
        return result

    def getSplexDenyUserList(self, curpage, pagesize):
        session = self.getLocalSession()
        rows = {}
        SQL = """
        select Snumber,host_name,port_number,check_stat,user_name,userid_db,userid_sp,to_char(check_time,'YYYY-MM-DD hh24:mi:ss') check_time,comments from (select  ROW_NUMBER() OVER (ORDER BY SP_OCT_DENIED_USERID_LOG.check_stat,SP_OCT_DENIED_USERID_LOG.host_name,SP_OCT_DENIED_USERID_LOG.port_number) AS Snumber,SP_OCT_DENIED_USERID_LOG.* from SP_OCT_DENIED_USERID_LOG) where Snumber >(%s-1)*%s and Snumber <= %s*%s
            """ % (curpage, pagesize, curpage, pagesize)
        datalist = session.execute(SQL).fetchall()
        rows['datalist'] = [{
            "number": row[0],
            "host_name": row[1],
            "port_number": row[2],
            'check_stat': row[3],
            'user_name': row[4],
            'userid_db': row[5],
            'userid_sp': row[6],
            'check_time': row[7],
            'comments': row[8]
        } for row in datalist]
        print(rows['datalist'])
        SQL = '''select  count(1) count from SP_OCT_DENIED_USERID_LOG '''
        count = session.execute(SQL).fetchone()
        rows["count"] = "%s" % str(count[0])
        return rows

    def get_depot_manage_instance(self, db_name):
        session = self.getLocalSession()
        sql = '''
        select trim_host,db_name,instance_name,host_name,
        to_char(date_added,'yyyy-mm-dd hh24:mi:ss') date_added,to_char(lastmodifieddate,'yyyy-mm-dd hh24:mi:ss') lastmodifieddate 
        from instance_info where db_name= '%s' 
        ''' % (db_name)
        _list = session.execute(sql).fetchall()
        return _list

    def get_depot_manage_pool_info(self, db_name):
        session = self.getLocalSession()
        sql = '''
              select trim_host,db_name,appln_support_code,mapping_name,schema,service_name,
            to_char(date_added,'yyyy-mm-dd hh24:mi:ss') date_added,to_char(lastmodifieddate,'yyyy-mm-dd hh24:mi:ss') lastmodifieddate 
            from appln_mapping_info 
            where db_name= '%s'
               ''' % (db_name)
        list = session.execute(sql).fetchall()
        return list

    def get_cpu_info_for_chatbot(self):
        cpu_info = {}
        session = self.getLocalSession()
        SQL = "select host_name, physical_cpu, cores from host_info"
        rows = session.execute(SQL).fetchall()
        for row in rows:
            cpu_info.update({
                row[0]: {
                    "physical_cpu": row[1],
                    "core_cpu": row[2]
                }
            })
        return cpu_info

    def get_db_info_for_chatbot(self):
        db_info = {}
        session = self.getLocalSession()
        SQL = "select db_name, instance_name, sga_target, sga_max_size, pga_aggregate_target from wbxoracledbinfo"
        rows = session.execute(SQL).fetchall()
        for row in rows:
            if row[0] not in db_info.keys():
                db_info.update({
                    row[0]: [{
                        "instance_name": row[1],
                        "sga_target": row[2],
                        "sga_max_size": row[3],
                        "pga_aggregate_target": row[4]
                    }]
                })
            else:
                db_info[row[0]].append({
                    "instance_name": row[1],
                    "sga_target": row[2],
                    "sga_max_size": row[3],
                    "pga_aggregate_target": row[4]
                })
        return db_info

    def getdbhostnameinstancenameanddbconfig(self, db_name):
        session = self.getLocalSession()
        sql = """
        select distinct ii.host_name, ii.instance_name, ii.db_name, dbcfg.sga_target, dbcfg.sga_max_size, dbcfg.pga_aggregate_target from host_info hi, instance_info ii, database_info db, wbxoracledbinfo dbcfg
        where db.db_name='%s'
        and db.db_name=ii.db_name
        and db.trim_host=ii.trim_host
        and dbcfg.instance_name=ii.instance_name
        and ii.host_name=hi.host_name
        and ii.trim_host=hi.trim_host
        and db.db_type <> 'DECOM'
        order by db_name, instance_name
        """ % db_name.upper()
        rows = session.execute(sql).fetchall()
        result = []
        if not rows:
            return []
        for row in rows:
            result.append({
                "host_name": row[0],
                "instance_name": row[1],
                "db_name": row[2],
                "sga_target": row[3],
                "sga_max_size": row[4],
                "pga_aggregate_target": row[5]
            })
        return result

    def gethosttopologybyhostnameanddbconfig(self, host_name):
        session = self.getLocalSession()
        sql = """
        select distinct iin.host_name, iin.instance_name, ii.db_name, dbcfg.sga_target, dbcfg.sga_max_size, dbcfg.pga_aggregate_target from host_info hi, instance_info ii, database_info db, instance_info iin, database_info dbi, wbxoracledbinfo dbcfg
where ii.host_name='%s'
and db.db_name=ii.db_name
and db.trim_host=ii.trim_host
and ii.host_name=hi.host_name
and dbcfg.instance_name=iin.instance_name
and ii.trim_host=hi.trim_host
and db.db_type <> 'DECOM'
and iin.db_name=ii.db_name
and iin.trim_host=ii.trim_host
and iin.db_name=dbi.db_name
and iin.trim_host=dbi.trim_host
and dbi.db_type <> 'DECOM'
order by db_name, instance_name""" % host_name
        rows = session.execute(sql).fetchall()
        result = []
        if not rows:
            return []
        for row in rows:
            result.append({
                "host_name": row[0],
                "instance_name": row[1],
                "db_name": row[2],
                "sga_target": row[3],
                "sga_max_size": row[4],
                "pga_aggregate_target": row[5]
            })
        return result

    def getteohostByDBName(self, db_name):
        session = self.getLocalSession()
        sql = """
select ii.host_name
from database_info db, instance_info ii
where db.appln_support_code='TEO'
and db.db_type <> 'DECOM'
and db.db_name='%s'
and db.trim_host=ii.trim_host
and db.db_name=ii.db_name""" % db_name.upper()
        rows = session.execute(sql).fetchall()
        result = []
        if not rows:
            return []
        for row in rows:
            result.append(row[0])
        return result

    def get_replication_to(self, src_db, tgt_db, src_schema, tgt_schema):
        session = self.getLocalSession()
        sql = '''
        select replication_to from direction_info2 
       where ( src_appln_support_code in ('WEB','CI','CSP') or tgt_appln_support_code in ('WEB','CI','CSP'))
       and src_appln_support_code = (select distinct appln_support_code from database_info di where di.db_name = '%s' and di.db_type= 'PROD')
       and tgt_appln_support_code = (select distinct appln_support_code from database_info di where di.db_name = '%s' and di.db_type= 'PROD')
       and src_application_type = (select distinct application_type from database_info di where di.db_name = '%s' and di.db_type= 'PROD')
       and tgt_application_type = (select distinct application_type from database_info di where di.db_name = '%s' and di.db_type= 'PROD')
       and lower(src_schema) = '%s'
       and lower(tgt_schema) = '%s'
        ''' % (src_db, tgt_db, src_db, tgt_db, src_schema, tgt_schema)
        rows = session.execute(sql).fetchall()
        return rows

    def get_all_site_code(self):
        session = self.getLocalSession()
        sql = """
select distinct hi.site_code 
from host_info hi, instance_info ii, database_info db
where hi.trim_host=ii.trim_host
and hi.host_name=ii.host_name
and ii.trim_host=db.trim_host
and ii.db_name=db.db_name
and db.db_type<>'DECOM'
and db.db_vendor='Oracle'"""
        rows = session.execute(sql).fetchall()
        result = []
        if not rows:
            return []
        for row in rows:
            result.append(row[0])
        return result

    def add_alertdetail(self, wbxmonitoralertdetailVo):
        session = self.getLocalSession()
        sql = '''
        insert into wbxmonitoralertdetail(alerttitle,host_name,db_name,splex_port,instance_name,alert_type,job_name,parameter,alerttime,createtime,lastmodifiedtime)
                                      values('%s','%s','%s','%s','%s','%s','%s','%s',sysdate,sysdate,sysdate)
        ''' % (wbxmonitoralertdetailVo.alerttitle, wbxmonitoralertdetailVo.host_name, wbxmonitoralertdetailVo.db_name,
               wbxmonitoralertdetailVo.splex_port,
               wbxmonitoralertdetailVo.instance_name, wbxmonitoralertdetailVo.alert_type,
               wbxmonitoralertdetailVo.job_name, wbxmonitoralertdetailVo.parameter)
        logger.info(sql)
        session.execute(sql)

    def delete_shareplex_info(self, src_db, src_host, port):
        session = self.getLocalSession()
        sql = '''
        delete from shareplex_info where src_db='%s' and src_host='%s' and port=%s
        ''' % (src_db, src_host, port)
        logger.info(sql)
        session.execute(sql)

    def getWbxmonitoralert(self, db_name, status, host_name, alert_type, start_date, end_date):
        session = self.getLocalSession()
        sql = '''
        select t1.alertid,t1.alerttitle,t1.status,t1.autotaskid,t1.host_name,t1.db_name,t1.splex_port,t1.instance_name,
        t1.alert_type,t1.job_name,t1.parameter,
        to_char(t1.first_alert_time,'yyyy-MM-dd HH24:mi:ss') first_alert_time, 
        to_char(t1.last_alert_time,'yyyy-MM-dd HH24:mi:ss') last_alert_time, 
        t1.alert_count,
        t1.attemptcount,
        to_char(t1.fixtime,'yyyy-MM-dd HH24:mi:ss') fixtime, 
        to_char(t1.createtime,'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(t1.lastmodifiedtime,'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
        from wbxmonitoralert2 t1 
        where t1.alert_type not in('TEST')
        and t1.first_alert_time>=to_date('%s', 'YYYY-MM-DD') 
        and t1.first_alert_time<to_date('%s', 'yyyy-MM-DD') 
        ''' % (start_date, end_date)
        if db_name:
            sql += " and t1.db_name like '%%%s%%' " % (db_name)
        if host_name:
            sql += " and t1.host_name like '%%%s%%' " % (host_name)
        if status:
            sql += " and t1.status = '%s' " % (status)
        if alert_type:
            sql += " and t1.alert_type = '%s' " % (alert_type)
        sql += " order by t1.last_alert_time desc"
        logger.info(sql)
        rows = session.execute(sql).fetchall()
        return rows

    def getWbxmonitoralertdetail(self, alertid):
        session = self.getLocalSession()
        sql = '''
               select alertdetailid,alerttitle,host_name,db_name,splex_port,instance_name,alert_type,job_name,parameter,status,
                to_char(alerttime,'yyyy-MM-dd HH24:mi:ss') alerttime,
                to_char(createtime,'yyyy-MM-dd HH24:mi:ss') createtime,
                to_char(lastmodifiedtime,'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
                from wbxmonitoralertdetail 
                where alertid = '%s'
                order by alerttime desc
               ''' % (alertid)
        rows = session.execute(sql).fetchall()
        return rows

    def getWbxautotask(self, autotaskid):
        session = self.getLocalSession()
        sql = '''
        select t1.task_type,t2.jobid,t2.db_name,t2.host_name,t2.splex_port,t2.job_action,t2.processorder,t2.status,t2.execute_method,t2.description,t2.parameter,
        to_char(t2.createtime,'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(t2.lastmodifiedtime,'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
        from wbxautotask t1,wbxautotaskjob t2
        where t1.taskid = t2.taskid
        and t1.taskid = '%s'
        ''' % (autotaskid)
        rows = session.execute(sql).fetchall()
        return rows

    def get_instance(self):
        session = self.getLocalSession()
        sql = '''
        select instance_name,host_name from gv$instance
        '''
        rows = session.execute(sql).fetchall()
        return rows

    def getDBlinkmonitordetail(self, trim_host, db_name, status):
        session = self.getLocalSession()
        sql = '''
        select trim_host,db_name,schema_name,dblink_name,to_char(monitor_time,'YYYY-MM-DD hh24:mi:ss') monitor_time,status,errormsg
        from wbxdblinkmonitordetail
        where monitor_time>sysdate-30
        '''
        if trim_host:
            sql += " and trim_host like '%%%s%%' " % (trim_host)
        if db_name:
            sql += " and db_name like '%%%s%%' " % (db_name)
        if status:
            sql += " and status= '%s' " % (status)
        sql += " order by db_name"
        rows = session.execute(sql).fetchall()
        return rows

    def getSplexParamsList(self, host_name, port, param_name, ismodified, curpage, pagesize):
        session = self.getLocalSession()
        rows = {}
        SQL = """
                select rn,host_name,to_char(port_number) port_number, param_category,param_name,queue_name, actual_value, default_value, to_char(collect_time,'YYYY-MM-DD hh24:mi:ss') collect_time
                from (
    select ROWNUM rn,host_name,port_number, param_category,param_name,queue_name, actual_value, default_value, collect_time
    from (
    select host_name,port_number,param_category, param_name,queue_name, actual_value, default_value, collect_time,
    row_number() over(partition by host_name,port_number,param_name order by collect_time desc) sp
    from splex_param_detail
    where host_name !='0' and collect_time >= sysdate-2
                    """
        if host_name != "":
            SQL = "%s and host_name = '%s'" % (SQL, host_name)
        if port != "":
            SQL = "%s and port_number = '%s'" % (SQL, port)
        if param_name != "":
            SQL = "%s and param_name = '%s'" % (SQL, param_name)
        if ismodified != "":
            SQL = "%s and ismodified = '%s'" % (SQL, ismodified)

        SQL_F = "%s order by ismodified desc,host_name,port_number,param_name" \
                ") t where t.sp <=1 and ROWNUM<=%s*%s) where rn > (%s-1)*%s " % (
                    SQL, curpage, pagesize, curpage, pagesize)
        paramslist = session.execute(SQL_F).fetchall()
        rows['paramslist'] = [{
            "number": row[0],
            "host_name": row[1],
            "port_number": row[2],
            'param_category': row[3],
            'param_name': row[4],
            'queue_name': row[5],
            'actual_value': row[6],
            'default_value': row[7],
            'collect_time': row[8]
        } for row in paramslist]
        print(rows['paramslist'])
        SQL_C = ''' select  count(1) count from (%s) t where t.sp <=1)) ''' % (SQL)
        count = session.execute(SQL_C).fetchone()
        rows["count"] = "%s" % str(count[0])
        SQL_P = ''' select  distinct port_number from (%s) t where t.sp <=1)) ''' % (SQL)
        ports = session.execute(SQL_P).fetchall()
        rel = []
        for port in ports:
            rel.append(port[0])
        rows['ports_num'] = rel
        return rows

    def checkdbName(self, db_name):
        session = self.getLocalSession()
        sql = '''
                select db_name,trim_host
                from database_info where DB_TYPE = 'PROD' 
                and appln_support_code in ('TEL','WEB','TEO','CONFIG','OPDB','LOOKUP','DIAGNS','CSP','CI','TRANS')
                and db_name not in ('TTA136')
                and db_type = 'PROD'
                and db_name='%s'
                ''' % (db_name)
        rows = session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def addMonitorSplexParam(self, param_category, param_name):
        session = self.getLocalSession()
        SQL = "insert into splex_param_detail(host_name, port_number, param_category, param_name, queue_name, actual_value,default_value, collect_time, ismodified) values('0','0','%s','%s','0','0','0',systimestamp,'N')" \
              % (param_category, param_name)
        logger.info(SQL)
        session.execute(SQL)

    def get_newest_teodb_failover_taskid(self, db_name):
        session = self.getLocalSession()
        sql = """
        select parameter from
(select row_number() over(partition by a.taskid order by decode(status,'FAILED',1,'RUNNING',2,'PENDING',3,'SUCCEED',4,5), a.createtime desc ) rum,
        a.taskid,a.task_type,a.parameter,a.createtime, a.lastmodifiedtime, a.createby, b.status 
from wbxautotask a , wbxautotaskjob b 
where a.task_type='TEODB_FAILOVER_TASK'
and a.taskid=b.taskid
and b.db_name='%s'
and a.createtime > sysdate - 1
) where rum=1""" % db_name.upper()
        rows = session.execute(sql).fetchone()
        if not rows:
            return None
        return rows[0]

    def get_teodb_failover_list(self):
        session = self.getLocalSession()
        SQL = """
        select jobid, taskid, parameter, status, to_char(createtime,'YYYY-MM-DD hh24:mi:ss') createtime from wbxautotaskjob where taskid in (select taskid from (select taskid from wbxautotask where task_type in ('TEODB_FAILOVER', 'TEODB_FAILBACK') and rownum< 51 order by createtime desc)) order by createtime desc
        """
        rows = session.execute(SQL).fetchall()
        result = []
        if not rows:
            raise wbxexception("get_teodb_failover_list from depot failed!")
        for row in rows:
            parameter_dict = eval(json.loads(row[2]))
            result.append({
                "jobid": row[0],
                "taskid": row[1],
                "db_name": parameter_dict["pri_db_name"],
                "failover_to": parameter_dict["gsb_db_name"],
                "status": row[3],
                "createtime": row[4]
            })
        return result

    def get_teodb_failover_detail_log(self, taskid, jobid):
        session = self.getLocalSession()
        SQL = """
        select nvl(resultmsg1, ' '), nvl(resultmsg2, ' '), nvl(resultmsg3, ' ')
        from wbxautotaskjob
        where taskid='%s' and jobid='%s'
        """ % (taskid, jobid)
        rows = session.execute(SQL).fetchone()
        if not rows:
            raise wbxexception(
                "get_teodb_failover_detail_log taskid:{0} jobid:{1} from depot failed!".format(taskid, jobid))
        return "".join(rows)

    def getShplexParamsServerHostname(self):
        session = self.getLocalSession()
        sql = '''
              select rownum num_id,host_name from(
                select distinct ii.host_name
                from shareplex_info si, instance_info ii, database_info di, host_user_info ui
                where ((si.tgt_host=ii.host_name and si.tgt_db=ii.db_name) or (si.src_host=ii.host_name and si.src_db=ii.db_name))
                and ii.db_name=di.db_name
                and ii.trim_host=di.trim_host
                and di.db_type in ('BTS_PROD','PROD')
                and ii.host_name=ui.host_name
                and ui.username='oracle'
                and di.catalog_db='COMMERCIAL' order by 1)
                '''
        rows = session.execute(sql).fetchall()
        rst = []
        for row in rows:
            rst.append({
                "num_id": row[0],
                "host_name": row[1]
            })
        return rst

    def get_db_failoverdb(self, db_name):
        session = self.getLocalSession()
        sql = '''
           select si.src_db db_name, sdi.application_type application_type, si.tgt_db failover_db_name,tdi.application_type failover_db_application_type
           from shareplex_info si, instance_info sii, database_info sdi, instance_info tii, database_info tdi
           where si.src_host=sii.host_name
           and si.src_db=sii.db_name
           and sii.db_name=sdi.db_name
           and sii.trim_host=sdi.trim_host
           and si.tgt_host=tii.host_name
           and si.tgt_db=tii.db_name
           and tii.db_name=tdi.db_name
           and tii.trim_host=tdi.trim_host
           and sdi.appln_support_code=tdi.appln_support_code
           and src_db  = '%s' ''' % (db_name)
        rows = session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def get_schema_password(self, schemaname, src_dbid, trim_host_src):
        session = self.getLocalSession()
        schemaname = str(schemaname).lower()
        sql = '''select distinct schema as username, trim_host,schematype, appln_support_code, f_get_deencrypt(password) as password 
                        from appln_pool_info where db_name='%s' and trim_host='%s' and lower(schema)='%s'
                        ''' % (src_dbid, trim_host_src, schemaname)
        rows = session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def getSplexCRStatus(self, host_name, port_number):
        session = self.getLocalSession()
        SQL = '''
        select distinct trim_host, host_name, to_char(splex_port) splex_port, db_name, status, errormsg, to_char(collecttime,'YYYY-MM-DD hh24:mi:ss') collecttime from wbxcrmonitordetail
        where collecttime > sysdate-2
        '''
        if host_name:
            SQL += " and host_name = '%s' " % (host_name)
        if port_number:
            SQL += " and splex_port = '%s' " % (port_number)
        SQL += " order by trim_host "
        rows = session.execute(SQL).fetchall()
        return [dict(row) for row in rows]

    def getSplexCRLogCount(self):
        session = self.getLocalSession()
        SQL = '''
        select trim_host,host_name,db_name, splex_port, conflictdate, to_char(crcount) cr_count, collecttime from (
select trim_host,host_name,db_name, to_char(splex_port) splex_port, to_char(conflictdate,'YYYY-MM-DD') conflictdate, crcount, to_char(collecttime,'YYYY-MM-DD hh24:mi:ss') collecttime, row_number() over(partition by host_name,db_name,splex_port order by collecttime desc) rn
from wbxcrlog ) where rn =1 order by crcount desc
        '''
        rows = session.execute(SQL).fetchall()
        return [dict(row) for row in rows]

    def getCRLogCountHistory(self, host_name, db_name, splex_port):
        session = self.getLocalSession()
        SQL = '''
        select host_name,db_name, to_char(splex_port) splex_port, to_char(conflictdate,'YYYY-MM-DD') conflictdate, to_char(crcount) crcount, to_char(collecttime,'YYYY-MM-DD hh24:mi:ss') collecttime from wbxcrlog where host_name='%s' and db_name='%s' and splex_port='%s' and collecttime > sysdate-15
        ''' % (host_name, db_name, splex_port)
        rows = session.execute(SQL).fetchall()
        return [dict(row) for row in rows]

    def getSplexCRFailed(self, host_name, port_number):
        session = self.getLocalSession()
        SQL = '''
        select distinct cr.host_name, cr.splex_port, cr.db_name, cr.status, cr.errormsg, cr.collecttime,f_get_deencrypt(ui.pwd) password,si.tgt_splex_sid splex_sid
        from wbxcrmonitordetail cr,shareplex_info si,host_user_info ui 
        where cr.status='FAILED'
        and cr.host_name=ui.host_name
        and cr.host_name=si.tgt_host
        and cr.db_name=si.tgt_db
        '''
        if host_name:
            SQL += " and cr.host_name = '%s' " % (host_name)
        if port_number:
            SQL += " and cr.splex_port = '%s' " % (port_number)
        SQL += " order by host_name "
        row = session.execute(SQL).first()
        return row

    def getGatherStats(self, trim_host, db_name, schema_name, curpage, pagesize):
        session = self.getLocalSession()
        rows = {}
        SQL = '''
        select rn,trim_host,db_name,schema_name,job_name,last_start_date,last_run_duration,job_action,repeat_interval,collect_time 
        from (select ROWNUM rn, trim_host,db_name, schema_name, job_name, to_char(last_start_date,'YYYY-MM-DD hh24:mi:ss') last_start_date, REGEXP_SUBSTR(last_run_duration,'\d{2}:\d{2}:\d{2}') last_run_duration,job_action,repeat_interval, to_char(collect_time,'YYYY-MM-DD hh24:mi:ss') collect_time 
        from (select  ROW_NUMBER() OVER (ORDER BY gatherstatsjob.trim_host,gatherstatsjob.db_name,gatherstatsjob.schema_name) AS Snumber,gatherstatsjob.* from gatherstatsjob where job_name !='GATHER_STATS_FIXED_OBJECTS' and collect_time >= sysdate-2 
        '''
        if trim_host != "":
            SQL = "%s and trim_host = '%s'" % (SQL, trim_host)
        if db_name != "":
            SQL = "%s and db_name = '%s'" % (SQL, db_name)
        if schema_name != "":
            SQL = "%s and schema_name = '%s'" % (SQL, schema_name)

        SQL_F = "%s order by last_run_duration desc) where ROWNUM<=%s*%s) where rn > (%s-1)*%s " % (
            SQL, curpage, pagesize, curpage, pagesize)
        datalist = session.execute(SQL_F).fetchall()
        rows['datalist'] = [{
            "num_id": row[0],
            "trim_host": row[1],
            "db_name": row[2],
            "schema_name": row[3],
            'job_name': row[4],
            'last_start_date': row[5],
            'last_run_duration': row[6],
            'job_action': row[7],
            'repeat_interval': row[8],
            'collect_time': row[9]
        } for row in datalist]
        print(rows['datalist'])
        SQL = '''select  count(1) count from gatherstatsjob where job_name !='GATHER_STATS_FIXED_OBJECTS' and collect_time >= sysdate-2 '''
        count = session.execute(SQL).fetchone()
        rows["count"] = "%s" % str(count[0])
        return rows

    def get_homepage_server_info(self, db_version=None):
        db_version_map = {
            "19c_db": "19.0.0.0.0",
            "11g_db": "11.2.0.4.0"
        }
        session = self.getLocalSession()
        sql = """
            select count(1) from 
            (select distinct db.db_name
            from database_info db, instance_info ii, host_info hi
            where db.db_name=ii.db_name
            and db.trim_host=ii.trim_host
            and ii.host_name=hi.host_name
            and ii.trim_host=hi.trim_host
            and lower(db.db_vendor)='oracle'"""
        if db_version:
            sql += """
            and db.db_version='%s'""" % db_version_map[db_version]
        sql += """       
            and db_type='PROD')"""
        rows = session.execute(sql).fetchall()
        return rows[0][0]

    def get_db_count_info(self, db_vendor=None):
        db_vendor_map = {
            "kafka_db": "kafka",
            "oracle_db": "oracle",
            "postgres_db": "postgres",
            "mysql_db": "mysql",
            "cassandra_db": "cassandra"
        }
        session = self.getLocalSession()
        sql = """
            select count(1)
            from database_info
            where db_type='PROD'"""
        if db_vendor:
            sql += """
            and lower(db_vendor)='%s'""" % db_vendor_map[db_vendor]
        rows = session.execute(sql).fetchall()
        return rows[0][0]

    def get_db_type_count_info(self):
        session = self.getLocalSession()
        sql = """
            select distinct appln_support_code, count(1)
            from database_info
            where db_type='PROD'
            and lower(db_vendor)='oracle'
            group by appln_support_code"""
        rows = session.execute(sql).fetchall()
        data = []
        for row in rows:
            data.append({
                "name": "%s_db" % row[0].lower(),
                "value": row[1]
            })
        return data

    def get_rencent_alert(self):
        session = self.getLocalSession()
        sql = """
select * from (select alertid,alerttitle,autotaskid,host_name,db_name,splex_port,instance_name,alert_type,job_name,to_char(first_alert_time,'YYYY-MM-DD hh24:mi:ss') first_alert_time,to_char(last_alert_time,'YYYY-MM-DD hh24:mi:ss') last_alert_time,alert_count,attemptcount,to_char(fixtime,'YYYY-MM-DD hh24:mi:ss') fixtime 
            from wbxmonitoralert2 where last_alert_time> sysdate-7 order by last_alert_time desc) where rownum <6
        """
        rows = session.execute(sql).fetchall()
        data = []
        for row in rows:
            data.append({
                "alertid": row[0],
                "alerttitle": row[1],
                "autotaskid": row[2],
                "host_name": row[3],
                "db_name": row[4],
                "splex_port": row[5],
                "instance_name": row[6],
                "alert_type": row[7],
                "job_name": row[8],
                "first_alert_time": row[9],
                "last_alert_time": row[10],
                "alert_count": row[11],
                "attemptcount": row[12],
                "fixtime": row[13]
            })
        return data

    def get_shareplex_count_info(self):
        session = self.getLocalSession()
        sql = """
        select actual_value, count(1) from splex_param_detail where param_name='version' group by actual_value"""
        rows = session.execute(sql).fetchall()
        data = []
        for row in rows:
            data.append({
                "name": row[0],
                "value": row[1]
            })
        return data

    def get_shareplex_count_total(self):
        session = self.getLocalSession()
        sql = "select count(1) from splex_param_detail where param_name='version'"
        rows = session.execute(sql).fetchall()
        return rows[0][0]

    def get_wbxmonitoralert_type(self):
        session = self.getLocalSession()
        sql = '''select distinct alert_type from wbxmonitoralert2'''
        rows = session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def get_wbxmonitoralert2(self, top):
        session = self.getLocalSession()
        sql = '''
        select to_char(lastmodifiedtime,'YYYY-MM-DD hh24:mi:ss') lastmodifiedtime ,alerttitle
        from (select lastmodifiedtime,alerttitle from wbxmonitoralert2 where createtime>sysdate-7 order by createtime desc)
        where rownum <= %s''' % (top)
        rows = session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def get_wbxdbauditlist(self, modelid):
        session = self.getLocalSession()
        sql = '''select wat.templetype,wai.templeid from wbxauditinstance wai, wbxaudittemplate wat
                    where wai.templeid=wat.templeid and wai.status='VAILD' and wat.status='VAILD'
                    and wai.modelid='%s' order by templetype ''' % (modelid)
        rows = session.execute(sql).fetchall()
        if len(rows) == 0:
            return None
        data = {}
        for item in [dict(row) for row in rows]:
            if item["templetype"] not in data.keys():
                data[item["templetype"]] = [item["templeid"]]
            else:
                data[item["templetype"]].append(item["templeid"])
        return data

    def get_wbxdbauditparam(self):
        session = self.getLocalSession()
        sql = "select key ,val from wbxauditcfg where upper(codetype)='DBAUDIT' order by key"
        rows = session.execute(sql).fetchall()
        if len(rows) == 0:
            return None
        data = {}
        data["model"] = [dict(row) for row in rows]
        return data

    def get_oncall_handover(self, start_time, end_time):
        session = self.getLocalSession()
        sql = "select to_char(oncall_date,'YYYY-MM-DD hh24:mi:ss'), classification || '/' || severity classification_severity, from_shift || '/' || to_shift from_to, modified_by created_by, category, wbx_cluster, status, description from oncall_handover where oncall_date between :starttime and :endtime order by created desc"
        rows = session.execute(sql, {"starttime": start_time, "endtime": end_time}).fetchall()
        result = []
        if not rows:
            raise wbxexception("Failed to get_oncall_handover")
        for row in rows:
            result.append({
                "oncall_date": row[0],
                "classification_severity": row[1] or "",
                "from_to": row[2] or "",
                "created_by": row[3] or "",
                "category": row[4] or "",
                "cluster": row[5] or "",
                "status": row[6] or "",
                "description": row[7] or ""
            })
        return result

    def insert_oncall_handover(self, created_by, from_shift, to_shift, classification, severity,
                               status, description, category, cluster):
        session = self.getLocalSession()
        sql = """
        insert into oncall_handover(classification, oncall_date, from_shift, to_shift, severity, category, wbx_cluster, 
        description, status, created_by, modified_by, created, lastmodified) 
        values('%s', sysdate, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', sysdate, sysdate)""" \
              % (classification, from_shift, to_shift, severity, category, cluster, description, status, "PCCP",
                 created_by.upper())
        logger.info(sql)
        session.execute(sql)

    def get_connection_endpoint(self, db_type, appln_support_code, web_domain, schema, db_name):
        session = self.getLocalSession()
        sql = '''select distinct hi.site_code,db.web_domain,db.db_name,db.appln_support_code,db.service_name,hi.scan_ip1,db.listener_port,
            ai.schema,f_get_deencrypt(ai.password) password,ai.schematype
                from host_info hi, instance_info ii, database_info db,appln_pool_info ai
                where hi.trim_host=ii.trim_host
                and hi.host_name=ii.host_name
                and ii.trim_host=db.trim_host
                and ii.db_name=db.db_name
                and db.db_name = ai.db_name
                and db.trim_host = ai.trim_host
                and db.db_type = '%s'
                and db.db_vendor='Oracle'
                and db.appln_support_code = '%s'
                 and upper(ai.schema) = '%s'
                                ''' % (db_type, appln_support_code, schema)
        if web_domain:
            sql = " %s and db.web_domain= '%s' " % (sql, web_domain)
        if db_name:
            sql = " %s and db.db_name= '%s' " % (sql, db_name)
        logger.info(sql)
        rows = session.execute(sql).fetchone()
        if rows is None:
            return None
        return rows

    def get_role_list(self):
        session = self.getLocalSession()
        sql = "select distinct role_name from ccp_role_page_info"
        rows = session.execute(sql).fetchall()
        result = []
        if not rows:
            raise wbxexception("Failed to get_role_list")
        for row in rows:
            result.append(row[0])
        return result

    def get_user_list_by_rolename(self, role_name):
        session = self.getLocalSession()
        sql = "select distinct username from ccp_user_role_info where role_name='" + role_name + "'"
        rows = session.execute(sql).fetchall()
        result = []
        for row in rows:
            result.append(row[0])
        return result

    def assign_role_to_user(self, username, role_name):
        session = self.getLocalSession()
        sql = "insert into ccp_user_role_info (username, role_name) values ('" + username + "','" + role_name + "')"
        logger.info(sql)
        session.execute(sql)

    def delete_user_from_role(self, username, role_name):
        session = self.getLocalSession()
        sql = "delete from ccp_user_role_info where username = '" + username + "' and role_name = '" + role_name + "'"
        logger.info(sql)
        session.execute(sql)

    def get_existed_url_list(self):
        session = self.getLocalSession()
        sql = "select distinct parent_page_dir, page_dir, page_name from ccp_role_page_info"
        rows = session.execute(sql).fetchall()
        rst_dict = {}
        for item in rows:
            rst_dict.update({item[2]: "".join([item[0], item[1]])})
        return rst_dict

    def add_page_to_depot(self, page_dir, parent_page_dir, page_name):
        session = self.getLocalSession()
        sql = "insert into ccp_role_page_info (page_dir, parent_page_dir, page_name, role_name, permission) values ('%s','%s', '%s', 'visitor', '1')" % (
        page_dir, parent_page_dir, page_name)
        logger.info(sql)
        session.execute(sql)
        sql = "insert into ccp_page_info (page_dir, parent_page_dir, page_name) values ('%s','%s', '%s')" % (
            page_dir, parent_page_dir, page_name)
        logger.info(sql)
        session.execute(sql)

    def delete_page_from_depot(self, page_dir, parent_page_dir, page_name):
        session = self.getLocalSession()
        sql = "delete from ccp_role_page_info where page_dir = '%s' and parent_page_dir = '%s' and page_name='%s'" % (
            page_dir, parent_page_dir, page_name)
        logger.info(sql)
        session.execute(sql)
        sql = "select count(1) from ccp_page_info where page_dir = '%s' and parent_page_dir = '%s' and page_name='%s'" % (
            page_dir, parent_page_dir, page_name)
        rows = session.execute(sql).fetchall()
        if int(rows[0][0]) > 0:
            sql = "delete from ccp_page_info where page_dir = '%s' and parent_page_dir = '%s' and page_name='%s'" % (
                page_dir, parent_page_dir, page_name)
            logger.info(sql)
            session.execute(sql)

    def get_page_role_list(self):
        session = self.getLocalSession()
        sql = "select distinct parent_page_dir, page_dir, role_name, permission from ccp_role_page_info"
        rows = session.execute(sql).fetchall()
        return rows

    def update_role_permission(self, page_dir, parent_page_dir, role_name, permission):
        session = self.getLocalSession()
        sql = "select count(1) from ccp_role_page_info where page_dir='%s' and parent_page_dir='%s' and role_name='%s'" % (
        page_dir, parent_page_dir, role_name)
        rows = session.execute(sql).fetchall()
        if int(rows[0][0]) == 0:
            self.add_role_to_page(page_dir, parent_page_dir, role_name, permission)
        else:
            sql = "update ccp_role_page_info set permission=" + permission + " where page_dir='" + page_dir + \
                  "' and parent_page_dir='" + parent_page_dir + "' and role_name='" + role_name + "'"
            logger.info(sql)
            session.execute(sql)

    def add_role_to_page(self, page_dir, parent_page_dir, role_name, permission):
        session = self.getLocalSession()
        sql = "insert into ccp_role_page_info (page_dir, parent_page_dir, role_name, permission) values ('" + page_dir + "','" + parent_page_dir + "','" + role_name + "', '" + permission + "')"
        logger.info(sql)
        session.execute(sql)

    def get_role_list_by_name(self, username):
        session = self.getLocalSession()
        sql = "select role_name from ccp_user_role_info where username='" + username + "'"
        rows = session.execute(sql).fetchall()
        rst_list = []
        for item in rows:
            rst_list.append(item[0])
        return rst_list

    def check_login_user(self, username):
        session = self.getLocalSession()
        sql = "select distinct username from ccp_user_role_info where username='%s'" % username
        rows = session.execute(sql).fetchall()
        return rows

    def get_access_dir(self, username):
        session = self.getLocalSession()
        sql = "select distinct p.parent_page_dir, p.page_dir from ccp_role_page_info p, ccp_user_role_info r where r.username='%s' and r.role_name=p.role_name and p.permission=1" % username
        rows = session.execute(sql).fetchall()
        if not rows:
            raise wbxexception("find no page info by username %s" % username)
        return rows

    def get_access_favourite_dir(self, username):
        session = self.getLocalSession()
        sql = """
        select distinct p.parent_page_dir, p.page_dir 
from ccp_role_page_info p, ccp_user_role_info r, ccp_user_page_info u
where r.username='%s' 
and r.username=u.username
and p.parent_page_dir=u.parent_page_dir
and p.page_dir=u.page_dir
and r.role_name=p.role_name 
and p.permission=1""" % username
        rows = session.execute(sql).fetchall()
        if not rows:
            raise wbxexception("find no page info by username %s" % username)
        return rows

    def add_user_favourite_page(self, username, page_name, page_dir, parent_page_dir):
        session = self.getLocalSession()
        sql = "insert into ccp_user_page_info (username, page_name, page_dir, parent_page_dir) values ('" + username + "','" + page_name + "','" + page_dir + "', '" + parent_page_dir + "')"
        logger.info(sql)
        session.execute(sql)

    def delete_user_favourite_page(self, username, page_name, page_dir, parent_page_dir):
        session = self.getLocalSession()
        sql = "delete from ccp_user_page_info where username='%s' and page_name='%s' and page_dir='%s' and parent_page_dir='%s'" % (
        username, page_name, page_dir, parent_page_dir)
        logger.info(sql)
        session.execute(sql)

    # def getadbmonlistAlertForTemp(self):
    #     session = self.getLocalSession()
    #     SQL = '''
    #    select t.src_host,t.src_db,t.port,t.replication_to,t.tgt_db,t.tgt_host,
    #         to_char(t.lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,
    #          to_char(t.montime,'yyyy-mm-dd hh24:mi:ss') montime, t.diff_secend,t.diff_day,t.diff_hour,t.diff_min,
    #          (t.diff_day||':'||t.diff_hour||':'||t.diff_min) lag_by from (
    #     select ta.*,ROUND((ta.montime-ta.lastreptime)*24*60*60) diff_secend,
    #                         trunc(TO_NUMBER(ta.montime - ta.lastreptime)) diff_day,
    #                         trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)- trunc(TO_NUMBER(ta.montime - ta.lastreptime))*24 diff_hour,
    #                         trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24*60)-trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)*60 diff_min
    #     from wbxadbmon ta where ta.port = 13006)t
    #     '''
    #     _list = session.execute(SQL).fetchall()
    #     return _list

    def get_all_server_info(self):
        session = self.getLocalSession()
        sql = """
with temp as (
select ii.host_name,db.db_type,row_number() over(partition by ii.host_name order by db.db_type desc) rn
from instance_info ii, database_info db
where ii.trim_host=db.trim_host
and ii.db_name=db.db_name
and db.db_type<>'DECOM'
group by ii.host_name,db.db_type)
select hi.trim_host, hi.host_name, hi.domain, hi.site_code, hi.host_ip, hi.vip_name,
hi.vip_ip, hi.priv_name, hi.priv_ip, hi.scan_name, hi.scan_ip1, hi.scan_ip2, hi.scan_ip3,hi.os_type_code,db_type from temp tp ,host_info hi
where tp.host_name=hi.host_name
and rn=1 order by site_code,host_name"""
        rows = session.execute(sql).fetchall()
        if not rows:
            raise wbxexception("find no server info")
        return [dict(row) for row in rows]

    def checkDBNameByPoolname(self, db_name, pool_name):
        session = self.getLocalSession()
        sql = '''
            select count(1) from appln_mapping_info where db_name='%s' and mapping_name='%s'
            ''' % (db_name.upper(), pool_name.upper())
        logger.info(sql)
        rows = session.execute(sql).fetchone()
        if int(rows[0]) < 1:
            raise wbxexception("cannot find pool %s in db %s" % (pool_name, db_name))

    def getGSBPoolByPoolname(self, pool_name):
        session = self.getLocalSession()
        sql = '''
                    select distinct sm.mapping_name, tm.mapping_name, sm.db_name, tm.db_name, sm.schema
                    from shareplex_info si, appln_mapping_info sm, appln_mapping_info tm
                    where sm.mapping_name='%s'
                    and lower(sm.appln_support_code)='tel'
                    and sm.db_name=si.src_db
                    and si.tgt_db=tm.db_name
                    and lower(tm.appln_support_code)='tel'
                    and sm.schema=tm.schema
                    and upper(tm.mapping_name) like '%%%s'
                    ''' % (pool_name.upper(), "".join(pool_name[3:]).upper())
        logger.info(sql)
        row = session.execute(sql).fetchone()
        if not row:
            raise wbxexception("cannot find gsb pool by pool_name %s" % pool_name)
        return row[0]

    def getDBNamebyPoolName(self, pool_name):
        session = self.getLocalSession()
        db_name = session.query(wbxmappinginfo).filter(wbxmappinginfo.mapping_name == pool_name,
                                                       wbxmappinginfo.schema == 'tahoe').first().getdb()
        return db_name

    def getSqlExecutionPlan(self, trim_host, db_name, sql_id, curpage, pagesize):
        session = self.getLocalSession()
        rows = {}
        SQL = '''
        select rn,trim_host,db_name,sql_id,sql_plan_id,cost_time,time_increase,first_problem_time,last_problem_time,monitor_time,problem_label,sql_text,statement
        from (select ROWNUM rn, trim_host,db_name, sql_id, sql_plan_id, cost_time,time_increase, to_char(create_time,'YYYY-MM-DD hh24:mi:ss') first_problem_time, to_char(modify_time,'YYYY-MM-DD hh24:mi:ss') last_problem_time,  to_char(monitor_time,'YYYY-MM-DD hh24:mi:ss') monitor_time,problem_label,sql_text,statement
        from (select  ROW_NUMBER() OVER (ORDER BY sql_multi_plan_mon.trim_host,sql_multi_plan_mon.db_name,sql_multi_plan_mon.sql_id) AS Snumber,sql_multi_plan_mon.* from sql_multi_plan_mon 
        where problem_label = 'Y' and fix_label='N' and modify_time > sysdate-7 and create_time != modify_time  and executions_delta >30
        '''
        if trim_host != "":
            SQL = "%s and trim_host = '%s'" % (SQL, trim_host)
        if db_name != "":
            SQL = "%s and db_name = '%s'" % (SQL, db_name)
        if sql_id != "":
            SQL = "%s and sql_id = '%s'" % (SQL, sql_id)

        SQL_F = "%s order by db_name, sql_id , cost_time desc) where ROWNUM<=%s*%s) where rn > (%s-1)*%s " % (
            SQL, curpage, pagesize, curpage, pagesize)
        datalist = session.execute(SQL_F).fetchall()
        rows['datalist'] = [{
            "num_id": row[0],
            "trim_host": row[1],
            "db_name": row[2],
            "sql_id": row[3],
            'sql_plan_id': row[4],
            'cost_time': row[5],
            'time_increase': row[6],
            'first_problem_time': row[7],
            'last_problem_time': row[8],
            'monitor_time': row[9],
            'problem_label': row[10],
            'sql_text': row[11],
            'statement': row[12]
        } for row in datalist]
        print(rows['datalist'])
        SQL = ''' select count(1) count from sql_multi_plan_mon  where problem_label = 'Y' and fix_label='N' and modify_time > sysdate-7 and create_time != modify_time  and executions_delta >30 '''
        if trim_host != "":
            SQL = "%s and trim_host = '%s'" % (SQL, trim_host)
        if db_name != "":
            SQL = "%s and db_name = '%s'" % (SQL, db_name)
        if sql_id != "":
            SQL = "%s and sql_id = '%s'" % (SQL, sql_id)
        count = session.execute(SQL).fetchone()
        rows["count"] = "%s" % str(count[0])
        return rows

    def getSqlExecutionPlanDetail(self, trim_host, db_name, sql_id):
        session = self.getLocalSession()
        rows = {}
        SQL = '''
        SELECT trim_host,db_name,sql_id,sql_plan_id,executions_delta,cost_time,time_increase,to_char(create_time,'YYYY-MM-DD hh24:mi:ss') first_problem_time, to_char(modify_time,'YYYY-MM-DD hh24:mi:ss') last_problem_time, to_char(monitor_time,'YYYY-MM-DD hh24:mi:ss') monitor_time,problem_label,sql_text,statement
        FROM sql_multi_plan_mon 
        WHERE trim_host='%s' and db_name='%s' and sql_id='%s' and nvl(problem_label,'xx')<>'Y' order by db_name, sql_id , cost_time desc
        ''' % (trim_host, db_name, sql_id)
        datalist = session.execute(SQL).fetchall()
        rows['datalist'] = [{
            "trim_host": row[0],
            "db_name": row[1],
            "sql_id": row[2],
            'sql_plan_id': row[3],
            'executions_delta': row[4],
            'cost_time': row[5],
            'time_increase': row[6],
            'first_problem_time': row[7],
            'last_problem_time': row[8],
            'monitor_time': row[9],
            'problem_label': row[10],
            'sql_text': row[11],
            'statement': row[12]
        } for row in datalist]
        return rows

    def get_wbxCassClusterName(self, casscluster):
        session = self.getLocalSession()
        sql = ''' select distinct cassclustername from dis.wbxCassClusterinfo where cassclustername like '%%%s%%' ''' % (
            casscluster)
        rows = session.execute(sql).fetchall()
        return rows

    def get_WbxCassClusterInfo(self, casscluster, localdc):
        session = self.getLocalSession()
        sql = '''select clusterid,localdc,contactpoints,port,cassclustername,to_char(createtime,'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime,'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime from dis.wbxCassClusterinfo '''
        if casscluster and localdc:
            sql += " where cassclustername = '%s' and localdc = '%s' " % (casscluster, localdc)
        elif casscluster:
            sql += " where cassclustername = '%s'  " % (casscluster)
        elif localdc:
            sql += " where localdc = '%s'  " % (localdc)
        rows = session.execute(sql).fetchall()
        return rows

    def get_wbxCassUser(self, casscluster):
        session = self.getLocalSession()
        sql = ''' select userid,username,userrole,password,casscluster from dis.wbxCassUser where casscluster = '%s' ''' % (
            casscluster)
        rows = session.execute(sql).fetchall()
        return rows

    def get_WbxCassClusterInfoByClusterid(self, clusterid):
        session = self.getLocalSession()
        sql = '''
        select clusterid,localdc,contactpoints,port,cassclustername from dis.wbxCassClusterInfo where clusterid = '%s'
        ''' % (clusterid)
        rows = session.execute(sql).fetchall()
        return rows

    def get_wbxCassAppKeyspaceConnInfo(self, keyspaceid, casscluster, localdc):
        session = self.getLocalSession()
        sql_1 = '''
        select  distinct k.keyspaceid,k.keyspacename,k.localdc,k.casscluster,k.contactpoints,k.port,u.userid,u.username,cci.clusterid
        from dis.wbxCassAppKeyspaceConnInfo k ,dis.wbxCassUser u,dis.wbxCassClusterinfo cci
        where k.userid = u.userid 
        and k.localdc = cci.localdc
         and k.contactpoints = cci.contactpoints
         and k.port = cci.port
         and k.casscluster = cci.cassclustername
        '''
        if casscluster:
            sql_1 += " and k.casscluster = '%s' " % (casscluster)
        if keyspaceid:
            sql_1 += " and k.keyspaceid = '%s' " % (keyspaceid)
        if localdc:
            sql_1 += " and k.localdc = '%s' " % (localdc)

        sql = '''
        select a.*,c.num service_num from (%s)a,(  select keyspaceid,count(1) num from dis.wbxCassKeyspaceEnvServiceMap 
         group by keyspaceid) c
        where a.keyspaceid = c.keyspaceid
        ''' % (sql_1)
        rows = session.execute(sql).fetchall()
        return rows

    def add_wbxCassAppService(self, servicename, servicelevel):
        session = self.getLocalSession()
        sql = '''
        insert into dis.wbxCassAppService(servicename, servicelevel) values ('%s', '%s')
        ''' % (servicename, servicelevel)
        session.execute(sql)

    def update_wbxCassAppService(self, serviceid, servicename, servicelevel):
        session = self.getLocalSession()
        sql = '''
        update dis.wbxCassAppService set servicename= '%s',servicelevel= '%s' where serviceid='%s'
        ''' % (servicename, servicelevel, serviceid)
        session.execute(sql)

    def add_wbxCassUser(self, username, userrole, password, casscluster):
        session = self.getLocalSession()
        sql = '''
               insert into dis.WBXCASSUSER(userName, userrole, password, cassCluster) 
               values('%s','%s','%s','%s')
                ''' % (username, userrole, password, casscluster)
        session.execute(sql)

    def update_wbxCassUser(self, userid, username, userrole, password, casscluster):
        session = self.getLocalSession()
        sql = '''
        update dis.wbxCassUser set username = '%s', userrole = '%s',password = '%s',casscluster= '%s',lastmodifiedtime = sysdate where userid = '%s'
        ''' % (username, userrole, password, casscluster, userid)
        logger.info(sql)
        session.execute(sql)

    def delete_wbxCassUser(self, userid):
        session = self.getLocalSession()
        sql = '''
        delete from dis.wbxCassUser where userid= '%s'
        ''' % (userid)
        session.execute(sql)

    def get_wbxCassClusterinfo(self, localdc, cassclustername):
        session = self.getLocalSession()
        sql = '''
        select localdc,contactpoints,port from dis.wbxCassClusterInfo where cassclustername='%s' and localdc= '%s'
        ''' % (cassclustername, localdc)
        rows = session.execute(sql).fetchall()
        return rows

    def add_wbxCassClusterinfo(self, localdc, contactpoints, port, cassclustername):
        session = self.getLocalSession()
        sql = '''
                insert into dis.wbxCassClusterinfo(localdc,contactpoints,port,cassclustername) 
                values('%s','%s','%s','%s')
        ''' % (localdc, contactpoints, port, cassclustername)
        session.execute(sql)

    def update_wbxCassClusterinfo(self, clusterid, localdc, contactpoints, port, cassclustername):
        session = self.getLocalSession()
        sql = '''
        update dis.wbxCassClusterinfo set localdc='%s',contactpoints='%s',port='%s',cassclustername='%s',lastmodifiedtime=sysdate where clusterid='%s'
        ''' % (localdc, contactpoints, port, cassclustername, clusterid)
        session.execute(sql)

    def get_wbxCassAppKeyspaceConnInfo_by_userid(self, userid):
        session = self.getLocalSession()
        sql = '''
         select keyspaceid,keyspacename,localdc,contactpoints,port,userid,casscluster from dis.wbxCassAppKeyspaceConnInfo where userid = '%s'
        ''' % (userid)
        rows = session.execute(sql).fetchall()
        return rows

    def delete_wbxCassClusterinfo(self, clusterid):
        session = self.getLocalSession()
        sql = ''' delete from dis.wbxCassClusterinfo where clusterid='%s' 
                ''' % (clusterid)
        session.execute(sql)

    def get_cassUserByCasscluster(self, casscluster):
        session = self.getLocalSession()
        sql = ''' select userid,username,userrole from dis.wbxCassUser where casscluster = '%s' ''' % (casscluster)
        rows = session.execute(sql).fetchall()
        return rows

    def get_cassLocaldcByCasscluster(self, casscluster):
        session = self.getLocalSession()
        sql = ''' select localdc,contactpoints,port from dis.wbxCassClusterInfo where cassclustername='%s' ''' % (
            casscluster)
        rows = session.execute(sql).fetchall()
        return rows

    def add_wbxCassAppKeyspaceConnInfo(self, keyspacename, localdc, contactpoints, port, userid, casscluster):
        session = self.getLocalSession()
        sql = '''
        insert into dis.wbxCassAppKeyspaceConnInfo(keyspacename, localdc, contactpoints, port, userid, casscluster)
        values('%s','%s','%s','%s','%s','%s')
        ''' % (keyspacename, localdc, contactpoints, port, userid, casscluster)
        session.execute(sql)

    def update_wbxCassAppKeyspaceConnInfo(self, keyspaceid, keyspacename, localdc, contactpoints, port, userid,
                                          casscluster):
        session = self.getLocalSession()
        sql = '''
        update dis.wbxCassAppKeyspaceConnInfo set keyspacename='%s',localdc='%s',contactpoints='%s',port='%s',userid='%s',casscluster='%s' where keyspaceid='%s'
        ''' % (keyspacename, localdc, contactpoints, port, userid, casscluster, keyspaceid)
        session.execute(sql)

    def get_wbxCassAppService(self, servicename):
        session = self.getLocalSession()
        sql = ''' select distinct servicename from dis.wbxCassAppService where servicename like '%%%s%%' 
        ''' % (servicename)
        rows = session.execute(sql).fetchall()
        return rows

    def get_WbxCassAppService(self, servicename):
        session = self.getLocalSession()
        sql = '''
        select serviceid,servicename,servicelevel from dis.wbxCassAppService where servicename = '%s'
        ''' % (servicename)
        rows = session.execute(sql).fetchall()
        return rows

    def get_wbxCassAppServiceByServiceid(self, serviceid):
        session = self.getLocalSession()
        # sql = '''
        #         select t1.envid,t1.serviceid,t2.servicename,t1.envtype,t1.applocation,t1.webdomain
        #         from dis.wbxCassEnvAppServiceMap t1,dis.wbxCassAppService t2
        #         where t1.serviceid = t2.serviceid
        #         and t1.serviceid = '%s'
        #         ''' % (serviceid)

        sql = '''
        select serviceid,servicename,servicelevel from dis.wbxCassAppService where serviceid = '%s'
        ''' % (serviceid)
        rows = session.execute(sql).fetchall()
        return rows

    def get_wbxCassEndpoints(self, envid):
        session = self.getLocalSession()
        sql = '''
         select t3.servicename,t3.serviceid,t3.servicelevel,t2.envtype,t2.envid,t2.applocation,t1.keyspaceid,t4.keyspacename,t4.localdc,t4.contactpoints,t4.port,t4.casscluster,t4.userid,t5.username,t5.password
        from dis.wbxCassKeyspaceEnvServiceMap t1,dis.wbxCassEnvAppServiceMap t2,dis.wbxCassAppService t3,dis.wbxCassAppKeyspaceConnInfo t4,dis.wbxCassUser t5
        where t1.envid = t2.envid
        and t2.serviceid = t3.serviceid
        and t1.keyspaceid = t4.keyspaceid
        and t4.userid= t5.userid
        and t1.envid= '%s'
        ''' % (envid)
        rows = session.execute(sql).fetchall()
        return rows

    def add_wbxCassEnvAppServiceMap(self, serviceid, envtype, applocation, webdomain):
        session = self.getLocalSession()
        sql = '''
        insert into dis.wbxCassEnvAppServiceMap(serviceid, envtype,applocation,webdomain) 
        values ('%s','%s','%s','%s')
        ''' % (serviceid, envtype, applocation, webdomain)
        session.execute(sql)

    def update_wbxCassEnvAppServiceMap(self, envid, serviceid, envtype, applocation, webdomain):
        session = self.getLocalSession()
        sql = '''
        update dis.wbxCassEnvAppServiceMap set envtype= '%s',applocation='%s',webdomain='%s' where envid= '%s' 
        ''' % (envtype, applocation, webdomain, envid)
        session.execute(sql)

    def add_wbxCassKeyspaceEnvServiceMap(self, keyspaceid, envid, serviceid):
        session = self.getLocalSession()
        sql = '''
               insert into dis.wbxCassKeyspaceEnvServiceMap(keyspaceid, envid, serviceid) 
               values ('%s','%s','%s')
               ''' % (keyspaceid, envid, serviceid)
        session.execute(sql)

    def update_wbxCassKeyspaceEnvServiceMap(self, keyspaceid, envid, serviceid):
        session = self.getLocalSession()
        sql = '''
        update dis.wbxCassKeyspaceEnvServiceMap set keyspaceid='%s' where envid='%s' and serviceid= '%s'
        ''' % (keyspaceid, envid, serviceid)
        session.execute(sql)

    def get_wbxCassKeyspaceEnvServiceMap(self, envid, serviceid):
        session = self.getLocalSession()
        sql = '''
        select keyspaceid,envid,serviceid from dis.wbxCassKeyspaceEnvServiceMap where envid = '%s' and serviceid= '%s'
        ''' % (envid, serviceid)
        rows = session.execute(sql).fetchall()
        return rows

    def get_wbxCassKeyspaceidBycasscluster(self,casscluster):
        session = self.getLocalSession()
        sql = '''
        select distinct localdc 
        from dis.wbxCassAppKeyspaceConnInfo 
        where casscluster = '%s' order by localdc
        '''% (casscluster)
        rows = session.execute(sql).fetchall()
        return rows

    def get_wbxCassKeyspaceidBycassclusterDC(self,casscluster, localdc):
        session = self.getLocalSession()
        sql = '''
        select distinct keyspaceid,keyspacename
        from dis.wbxCassAppKeyspaceConnInfo 
        where casscluster = '%s' and localdc = '%s'
                ''' % (casscluster,localdc)
        rows = session.execute(sql).fetchall()
        return rows

    def get_wbxCassKeyspaceid(self, casscluster, localdc, keyspacename):
        session = self.getLocalSession()
        sql = '''
        select keyspaceid,keyspacename,localdc,contactpoints,port,userid,casscluster from dis.wbxCassAppKeyspaceConnInfo 
        where casscluster = '%s' 
        ''' % (casscluster)
        if localdc:
            sql += " and localdc = '%s' " % (localdc)
        if keyspacename:
            sql += " and keyspacename = '%s' " % (keyspacename)
        rows = session.execute(sql).fetchall()
        return rows

    def delete_wbxCassAppKeyspaceConnInfo(self, keyspaceid):
        session = self.getLocalSession()
        sql = '''
        delete from dis.wbxCassAppKeyspaceConnInfo where keyspaceid= '%s'
        ''' % (keyspaceid)
        session.execute(sql)

    def get_wbxCasscluster(self):
        session = self.getLocalSession()
        sql = '''
        select distinct casscluster from dis.wbxCassAppKeyspaceConnInfo
        '''
        rows = session.execute(sql).fetchall()
        return rows

    def get_wbxCassEnvAppServiceMap(self, serviceid, applocation, envtype):
        session = self.getLocalSession()
        sql = '''
        select serviceid,envid,envtype,applocation,webdomain from dis.wbxCassEnvAppServiceMap where applocation= '%s' and envtype = '%s'
        ''' % (applocation, envtype)
        if serviceid:
            sql += " and serviceid='%s' " % (serviceid)
        rows = session.execute(sql).fetchall()
        return rows

    def delete_wbxCassEnvAppServiceMap(self, envid, serviceid):
        session = self.getLocalSession()
        sql = '''
        delete from dis.wbxCassEnvAppServiceMap where envid='%s' and serviceid='%s'
        ''' % (envid, serviceid)
        session.execute(sql)

    def delete_wbxCassKeyspaceEnvServiceMap(self, keyspaceid, envid, serviceid):
        session = self.getLocalSession()
        sql = '''
        delete from dis.wbxCassKeyspaceEnvServiceMap where keyspaceid ='%s' and envid ='%s' and serviceid ='%s'
        ''' % (keyspaceid, envid, serviceid)
        session.execute(sql)

    def ifhasport(self, port):
        session = self.getLocalSession()
        SQL = """select count(1) from (select distinct lower(src_splex_sid), lower(src_schema) from shareplex_info where port=:port)"""
        rows = session.execute(SQL, {"port": port}).fetchall()
        vCount = rows[0][0]
        return False if rows[0][0] == 0 else True

    def get_rac_host_list(self, host_name):
        session = self.getLocalSession()
        SQL = """
select distinct hi.host_name from host_info hi, instance_info ii, database_info db, host_info hi2
where hi.host_name=ii.host_name
and hi.trim_host=ii.trim_host
and ii.trim_host=db.trim_host
and ii.db_name=db.db_name
and lower(db.db_vendor)='oracle'
and db.db_type <> 'DECOM'
and hi2.host_name='%s'
and hi2.scan_name=hi.scan_name""" % host_name
        rows = session.execute(SQL).fetchall()
        return [ row[0] for row in rows ]

    def get_deploy_channel(self,db_name, target_schema_type,env):
        session = self.getLocalSession()
        sql = """
        select a.*,t1.schematype src_schematype,t2.schematype tgt_schematype from (
        select si.*,sdi.db_type src_db_type,tdi.db_type tgt_db_type, 
        sdi.appln_support_code src_appln_support_code,tdi.appln_support_code tgt_appln_support_code
        from shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii
        where si.src_host=sii.host_name
        and si.src_db=sii.db_name
        and sii.db_name=sdi.db_name
        and sii.trim_host=sdi.trim_host
        and si.tgt_host=tii.host_name
        and si.tgt_db=tii.db_name
        and tdi.db_name=tii.db_name
        and tdi.trim_host=tii.trim_host
        and si.src_db = '%s'
        and tdi.appln_support_code= '%s'
        )a , appln_pool_info t1, appln_pool_info t2
        where a.src_db= t1.db_name
        and a.tgt_db= t2.db_name
        and a.src_schema = t1.schema
        and a.tgt_schema = t2.schema
        """ %(db_name,target_schema_type)
        rows = session.execute(sql).fetchall()
        return rows

    def get_pg_alert_rule(self,alert_type,host_name,db_name,splex_port,alert_channel_type,alert_channel_value):
        session = self.getLocalSession()
        sql = '''
                select alert_type,host_name,db_name,splex_port,is_public,alert_channel_type,alert_channel_value from WBXMONITORALERTCONFIG 
                where alert_type = '%s' 
                ''' % (alert_type)
        if host_name:
            sql += " and host_name = '%s' " % (host_name)
        if db_name:
            sql += " and db_name = '%s' " % (db_name)
        if splex_port:
            sql += " and splex_port = %s " % (splex_port)
        if alert_channel_type:
            sql += " and alert_channel_type = '%s' " % (alert_channel_type)
        if alert_channel_value:
            sql += " and alert_channel_value = '%s' " % (alert_channel_value)
        rows = session.execute(sql).fetchall()
        return rows

    def set_pg_alert_rule(self,alert_id,alert_type,host_name,db_name,splex_port,is_public,alert_channel_type,alert_channel_value,comments,alert_title):
        session = self.getLocalSession()
        sql = '''insert into WBXMONITORALERTCONFIG(ALERT_ID,ALERT_TYPE,HOST_NAME,DB_NAME,SPLEX_PORT,ALERT_CHANNEL_TYPE,ALERT_CHANNEL_VALUE,IS_PUBLIC,COMMENTS,ALERT_TITLE) 
        values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')
        ''' %(alert_id,alert_type,host_name,db_name,splex_port,alert_channel_type,alert_channel_value,is_public,comments,alert_title)
        logger.info(sql)
        session.execute(sql)

    def update_pg_alert_rule(self,alert_id,host_name,db_name,splex_port,is_public,alert_channel_type,alert_channel_value,comments,alert_title):
        session = self.getLocalSession()
        sql = '''
        update WBXMONITORALERTCONFIG set host_name ='%s',db_name='%s',splex_port='%s',is_public='%s',alert_channel_type='%s',alert_channel_value='%s',comments='%s',alert_title='%s' where alert_id='%s'
        ''' %(host_name,db_name,splex_port,is_public,alert_channel_type,alert_channel_value,comments,alert_title,alert_id)
        logger.info(sql)
        session.execute(sql)

    def list_pg_alert_rule(self,alert_type):
        session = self.getLocalSession()
        sql = '''select alert_id,alert_type,host_name,db_name,splex_port,alert_channel_type,alert_channel_value,is_public,comments,
                   to_char(createtime,'yyyy-mm-dd hh24:mi:ss') createtime,to_char(lastmodifiedtime,'yyyy-mm-dd hh24:mi:ss') lastmodifiedtime 
                   from wbxmonitoralertconfig 
        '''
        if alert_type:
            sql += " where is_public = '0' and alert_type='%s' order by alert_type " % (alert_type)
        else:
            sql += " where is_public = '1' order by alert_type"
        rows = session.execute(sql).fetchall()
        return rows

    def delete_pg_alert_rule(self,alert_id):
        session = self.getLocalSession()
        sql = '''delete from WBXMONITORALERTCONFIG where alert_id='%s' ''' % (alert_id)
        logger.info(sql)
        session.execute(sql)

    def get_pg_alert_rule_types(self):
        session = self.getLocalSession()
        sql = '''
          select distinct alert_type
        from WBXMONITORALERT2 w,host_info hi,database_info di
        where w.host_name =hi.host_name
        and hi.trim_host = di.trim_host
        and di.db_vendor in ('POSTGRESQL')
        '''
        rows = session.execute(sql).fetchall()
        return rows
