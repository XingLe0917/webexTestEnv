import logging

from dao.wbxdaomanager import wbxdao
from dao.vo.auditvo import AuditVO, DBSessionVO, DBProcessVO, DBLockVO, DBATableVO, NLSParameterVO
from dao.vo.tablespacevo import SegmentVO
from datetime import datetime, timedelta
from common.wbxutil import wbxutil
from sqlalchemy import literal_column

from sqlalchemy import func, and_, text
logger = logging.getLogger("DBAMONITOR")

class DBAuditDao(wbxdao):
    def getLoginFailedInfo(self, starttime):
        session = self.getLocalSession()
        auditList = session.query(AuditVO.os_username, AuditVO.username, AuditVO.userhost, func.count(AuditVO.returncode).label("failedcount")).\
            filter(and_(AuditVO.timestamp > starttime, AuditVO.returncode == 1017)). \
            group_by(AuditVO.os_username, AuditVO.username, AuditVO.userhost).\
            all()
        return auditList

    def getTableSize(self, tableowner, tablename):
        session = self.getLocalSession()
        segvo = session.query(SegmentVO).\
            filter(and_(SegmentVO.owner == tableowner, SegmentVO.segment_name == tablename)).first()
        return segvo


    def getBlockedSession(self, timethreshold):
        session = self.getLocalSession()
        subq = session.query(DBLockVO.id1, DBLockVO.id2, DBLockVO.type).filter(DBLockVO.request > 0).subquery()
        lockq = session.query(DBLockVO).join(subq, and_(subq.c.id1 == DBLockVO.id1, subq.c.id2 == DBLockVO.id2,
                                                        subq.c.type == DBLockVO.type, DBLockVO.ctime > timethreshold)).subquery()
        locklist = session.query(DBSessionVO, lockq). \
            join(lockq, and_(lockq.c.inst_id == DBSessionVO.inst_id, lockq.c.sid == DBSessionVO.sid)).order_by(lockq.c.id1, lockq.c.request).all()
        return locklist

    def getTableByTableName(self, schemaname, tablename):
        session = self.getLocalSession()
        tablevo = session.query(DBATableVO).filter(and_(DBATableVO.owner == schemaname, DBATableVO.table_name==tablename)).first()
        return tablevo

    def getNLSParameters(self):
        session = self.getLocalSession()
        paramList = session.query(NLSParameterVO).all()
        return paramList

    def getDatabaseVersion(self, clz):
        session = self.getLocalSession()
        vo = session.query(clz).first()
        return vo

    def getCRCountInLastDay(self, spport, starttime, endtime):
        session = self.getLocalSession()
        strstarttime = starttime.strftime("%Y%m%d")
        strendtime = endtime.strftime("%Y%m%d")
        SQL="select trunc(conflicttime) as crdate,count(1) as CRCOUNT from splex%s.wbxcrlog where conflicttime between to_date('%s','YYYYMMDD') and to_date('%s','YYYYMMDD') group by trunc(conflicttime) order by 1" % (spport,strstarttime, strendtime)

        volist = session.execute(SQL).fetchall()
        crdict = {}
        for vo in volist:
            crdict[vo.crdate.strftime("%Y%m%d")] = vo.crcount
        return crdict

    def getReleseNumber(self, schemaname):
        session = self.getLocalSession()
        SQL="SELECT RELEASE_NUMBER FROM %s.wbxdatabaseversion" % schemaname
        vo = session.execute(SQL).first()
        return vo.RELEASE_NUMBER
    '''
    select distinct version as releasenumber,min(decode(instr(description,'shareplex_type'),0,createtime, null)) over (partition by version) as dbcreatetime,
        min(decode(instr(description,'shareplex_type'),0,null, createtime)) over (partition by version) as spcreatetime
    from test.wbxdatabase
    where createtime > sysdate- 3
    '''
    def getDatabase(self, tdb, timerange):
        session = self.getLocalSession()
        starttime = wbxutil.getcurrenttime(timerange * 24 * 60 * 60)
        volist = session.query(tdb.version.label("release_number"),
                               literal_column("min(decode(instr(description,'shareplex_type'),0,createtime, sysdate+100)) over (partition by version)").label("deploytime"),
                               literal_column("min(decode(instr(description,'shareplex_type'),0,sysdate+100, createtime)) over (partition by version)").label("spdeploytime")). \
            filter(tdb.createtime > starttime). \
            distinct(). \
            all()
        return volist

    '''select trunc(first_time, 'hh') as logtime, count(*), round(sum(blocks * block_size) / 1024 / 1024) mbsize
          from v$archived_log a
         where a.DEST_ID = 1
           and a.FIRST_TIME between to_date('2018-08-01','YYYY-MM-DD') and to_date('2018-08-30','YYYY-MM-DD')
           and to_number(to_char(FIRST_TIME,'HH24')) between 2 and 5
           group by trunc(first_time, 'hh')
           order by 1'''
    def getArchiveLog(self, start_date, end_date, start_time, end_time):
        session = self.getLocalSession()
        starth = start_time.split(":")[0]
        endh = end_time.split(":")[0]

        SQL=" SELECT to_char(trunc(first_time, 'hh'),'YYYY-MM-DD hh24') as logtime, round(sum(blocks * block_size) / 1024 / 1024) mbsize" \
            " FROM v$archived_log" \
            " WHERE DEST_ID = 1" \
            " AND FIRST_TIME between to_date('%s','YYYY-MM-DD') and to_date('%s','YYYY-MM-DD')" \
            " AND to_number(to_char(FIRST_TIME,'HH24')) between %s and %s " \
            " group by trunc(first_time, 'hh')" \
            " order by 1" % (start_date, end_date, starth, endh)
        return session.execute(SQL).fetchall()

    '''
    select to_char(begin_interval_time,'YYYY-MM-DD hh24:mi') as logtime, stat_name, round((value-nextvalue)/1000000/60,2) val  from (
select ta.snap_id, ta.begin_interval_time, ta.instance_number, tc.stat_name, tc.value,
     lag(value) over (partition by stat_name, trunc(ta.end_interval_time,'DD')  order by ta.instance_number,ta.snap_id) nextvalue
from dba_hist_snapshot ta, dba_hist_sys_time_model tc,
(select to_date('2018-09-04 13:38:00','YYYY-MM-DD hh24:mi:ss') + level-1 as starttime, 
 to_date('2018-09-04 14:15:00','YYYY-MM-DD hh24:mi:ss') + level-1 as endtime
from dual 
connect by level<=5) tb
where (ta.end_interval_time between tb.starttime and tb.endtime or ta.begin_interval_time between tb.starttime and tb.endtime)
and ta.snap_id=tc.snap_id
and ta.instance_number=tc.instance_number
and ta.instance_number=1
and tc.stat_name in ('DB time','sql execute elapsed time','sequence load elapsed time','background elapsed time','connection management call elapsed time')
order by snap_id, stat_name
) where nextvalue is not null
    '''
    def getDBStat(self, start_date, end_date, start_time, end_time):
        session = self.getLocalSession()
        wbxutil.convertDatetimeToString()
        startdatetime = "%s %s" % (start_date, start_time)
        enddatetime = "%s %s" % (end_date, end_time)
        daydelta = (wbxutil.convertStringtoDateTime(enddatetime) - wbxutil.convertStringtoDateTime(startdatetime)).days

        SQL="select to_char(begin_interval_time,'YYYY-MM-DD hh24:mi') as logtime, stat_name, round((value-nextvalue)/1000000/60,2) val  " \
            "from (" \
            "    select ta.snap_id, ta.begin_interval_time, ta.instance_number, tc.stat_name, tc.value, " \
            "            lag(value) over (partition by stat_name, trunc(ta.end_interval_time,'DD')  order by ta.instance_number,ta.snap_id) nextvalue" \
            "    from dba_hist_snapshot ta, dba_hist_sys_time_model tc," \
            "         (select to_date('%s','YYYY-MM-DD hh24:mi:ss') + level-1 as starttime, " \
            "          to_date('%s','YYYY-MM-DD hh24:mi:ss') + level-1 as endtime" \
            "          from dual " \
            "         connect by level<=%s) tb" \
            "    where (ta.end_interval_time between tb.starttime and tb.endtime or ta.begin_interval_time between tb.starttime and tb.endtime)" \
            "    and ta.snap_id=tc.snap_id" \
            "    and ta.instance_number=tc.instance_number" \
            "    and ta.instance_number=1" \
            "    and tc.stat_name in ('DB time','sql execute elapsed time','sequence load elapsed time','background elapsed time','connection management call elapsed time')" \
            "    order by snap_id, stat_name" \
            " ) where nextvalue is not null" % (startdatetime, enddatetime, daydelta)

        return session.execute(SQL).fetchall()

    '''select startpoint, endpoint,begin_interval_time,end_interval_time,snap_interval   from (
select snap_id endpoint, lead(snap_id)  over (order by begin_interval_time desc) startpoint,begin_interval_time,end_interval_time , snap_interval
from (
select distinct snap_id, trunc(begin_interval_time,'mi') begin_interval_time , trunc(end_interval_time,'mi') end_interval_time,
      extract(hour from (end_interval_time - begin_interval_time)) * 60 + extract(minute from (end_interval_time - begin_interval_time)) as snap_interval
from dba_hist_snapshot where begin_interval_time > sysdate -1/24
) order by 1 desc
) where rownum < 2'''

    def getLatestSnapshot(self):
        session = self.getLocalSession()
        SQL=" select startpoint, endpoint,begin_interval_time,end_interval_time,snap_interval   " \
            " from (" \
            "     select snap_id endpoint, lead(snap_id)  over (order by begin_interval_time desc) startpoint,begin_interval_time,end_interval_time,snap_interval " \
            "     from (" \
            "         select distinct snap_id, trunc(begin_interval_time,'mi') begin_interval_time , trunc(end_interval_time,'mi') end_interval_time," \
            "                extract(hour from (end_interval_time - begin_interval_time)) * 60 + extract(minute from (end_interval_time - begin_interval_time)) as snap_interval" \
            "         from dba_hist_snapshot where begin_interval_time > sysdate -3/24" \
            "     ) order by 1 desc" \
            " ) where rownum < 2"

        row = session.execute(SQL).fetchone()
        return row

    def getInstanceNameList(self):
        session = self.getLocalSession()
        SQL = '''SELECT instance_number, instance_name from gv$instance'''
        rows = session.execute(SQL).fetchall()
        resDict = {}
        for row in rows:
            resDict[row[1]] = row[0]
        return resDict

    def getSnapshotID(self, instance_number, starttime, endtime):
        session = self.getLocalSession()
        SQL = '''select ta.dbid, min(ta.snap_id) as minsnapid, max(ta.snap_id) as maxsnapid 
                from dba_hist_snapshot ta, v$database tb 
                where trunc(ta.begin_interval_time,'mi') <= :end_time 
                and trunc(ta.end_interval_time,'mi') >= :start_time
                and ta.dbid=tb.dbid
            '''
        if instance_number != 0:
            SQL = SQL + " and ta.instance_number=%s" % instance_number
        SQL = SQL + " group by ta.dbid"
        params = {"start_time":starttime,"end_time":endtime}
        row = session.execute(SQL, {"start_time":starttime,"end_time":endtime}).fetchone()
        return row

    def getAWRReport(self, dbid, instance_number, start_snap_id, end_snap_id):
        session = self.getLocalSession()
        if instance_number == 0:
            SQL = "select output from table(dbms_workload_repository.AWR_GLOBAL_REPORT_HTML(L_DBID=>:dbid,l_inst_num=>CAST(null AS VARCHAR2(10)),l_bid=>:start_snap_id, l_eid=>:end_snap_id))"
            params= {"dbid":dbid, "start_snap_id":start_snap_id, "end_snap_id":end_snap_id}
            rows = session.execute(SQL, params).fetchall()
        else:
            SQL = "select output from table(dbms_workload_repository.AWR_GLOBAL_REPORT_HTML(L_DBID=>:dbid,l_inst_num=>:instance_number,l_bid=>:start_snap_id, l_eid=>:end_snap_id))"
            params = {"dbid": dbid, "instance_number":instance_number, "start_snap_id": start_snap_id, "end_snap_id": end_snap_id}
            rows = session.execute(SQL, params).fetchall()
        return rows

    def getASHReport(self,dbid, instance_number, l_start_time, l_end_time):
        session = self.getLocalSession()
        if instance_number == 0:
            SQL = "select output from table(dbms_workload_repository.ASH_GLOBAL_REPORT_HTML(L_DBID=>:dbid,l_inst_num=>CAST(null AS VARCHAR2(10)),l_btime=>:l_start_time, l_etime=>:l_end_time))"
            params = {"dbid": dbid, "l_start_time": l_start_time, "l_end_time": l_end_time}
            rows = session.execute(SQL, params).fetchall()
        else:
            SQL = "select output from table(dbms_workload_repository.ASH_GLOBAL_REPORT_HTML(L_DBID=>:dbid,l_inst_num=>:instance_number,l_btime=>:l_start_time, l_etime=>:l_end_time))"
            params = {"dbid": dbid, "instance_number": instance_number, "l_start_time": l_start_time,
                      "l_end_time": l_end_time}
            rows = session.execute(SQL, params).fetchall()
        return rows

    def getMeetingUUIDDataWithDifferentConfID(self):
        session = self.getLocalSession()
        SQL = "select count(1) as mtgcount from test.wbxmeetinguuidmap where confid<>joinconfid or joinconfid is null"
        row = session.execute(SQL).first()
        case5 = row.mtgcount

        SQL = " select count(1) as mtgcount " \
              " from test.wbxcalendar a," \
              "      test.wbxmeetinguuidmap b," \
              "      test.wbxsite c," \
              "      test.wbxsitewebdomain d," \
              "      test.wbxdatabaseversion e" \
              " where a.siteid=b.siteid(+) and a.eventid=b.confid(+)" \
              " and  a.eventtype=0" \
              " and a.siteid=c.siteid and c.active=1 and c.siteid not in (512665,573002,314178,12351279,12351263,12351259,12352979,12358687)" \
              " and c.siteid=d.siteid and d.domainid=e.webdomainid" \
              " and ((a.serviceid in (6,7,9) and b.confid IS NULL) or (a.serviceid = 1 and ( a.mtguuid<>b.mtguuid or b.mtguuid is null)))"
        row = session.execute(SQL).first()
        case1 = row.mtgcount

        SQL = " select count(1) as mtgcount " \
              " from test.mtgconference a," \
              "      test.wbxmeetinguuidmap b," \
              "      test.wbxsite c," \
              "      test.wbxsitewebdomain d," \
              "      test.wbxdatabaseversion e" \
              " where a.siteid=b.siteid(+) and a.confid=b.confid(+)" \
              " and a.siteid=c.siteid and c.active=1 and c.siteid not in (512665,573002,314178,12351279,12351263,12351259,12352979,12358687)" \
              " and c.siteid=d.siteid and d.domainid=e.webdomainid" \
              " and ((a.companyid in (6,7,9) and b.confid IS NULL) or (a.companyid = 1 and ( a.mtguuid<>b.mtguuid or b.mtguuid is null)))"
        row = session.execute(SQL).first()
        case2 = row.mtgcount

        SQL = " select count(1) as mtgcount " \
              " from test.mtgconference a, " \
              "      test.wbxsite c," \
              "      test.wbxsitewebdomain d," \
              "      test.wbxdatabaseversion e" \
              " where a.companyid=1" \
              " and a.siteid=c.siteid and c.active=1 and c.siteid not in (512665,573002,314178,12351279,12351263)" \
              " and c.siteid=d.siteid and d.domainid=e.webdomainid" \
              " and a.mtguuid is null"
        row = session.execute(SQL).first()
        case3 = row.mtgcount

        return (case1, case2, case3, 0, case5)

    def getPasscodeAllocationLog(self):
        session = self.getLocalSession()
        SQL = "SELECT count(1) as errorcount FROM test.WBXALLOCATEPASSCODEMONITOR  WHERE ACTION IN ('MonitorPasscodeError','AllocatePasscodeError','AllocateRangeJobError') AND CREATETIME BETWEEN SYSDATE-7 AND SYSDATE"
        row = session.execute(SQL).first()
        return row.errorcount

    def isDBPatchInstalled(self, username, releaseNumber):
        session = self.getLocalSession()
        SQL = """ SELECT count(1)  FROM dba_tables WHERE table_name='WBXDATABASE' and owner=upper('%s') """ % username
        row = session.execute(SQL).first()
        if row[0] == 1:
            SQL = """ SELECT count(1)  FROM %s.wbxdatabase  WHERE version='%s' """ % (username, releaseNumber)
            row = session.execute(SQL).first()
            return True if row[0] > 0 else False
        else:
            return False

    def getConfigDBReplicationDelay(self, start_time, end_time):
        session = self.getLocalSession()
        SQL = """ select case when arrive_time < create_time then 2 else round(extract(day from arrive_time-create_time)*24 * 60 * 60 + extract(hour from arrive_time-create_time)*60 * 60 + extract(minute from arrive_time-create_time)*60 + extract(second from arrive_time-create_time), 2) end as rep_time 
                  from test.configdb_rep_monitor
                  where create_time between :start_time and :end_time 
             """
        rows = session.execute(SQL, {"start_time": start_time, "end_time":end_time}).fetchall()
        return [float(row[0]) for row in rows]

    def listMeetingDataReplicationData(self, start_time, end_time):
        session = self.getLocalSession()
        SQL = """ 
                select to_char(lastmodifiedtime,'YYYY-MM-DD hh24:mi:ss'), round(avg((arrivetime-lastmodifiedtime)*24 * 60* 60)) avgtime, count(distinct confid) as row_count 
                from wbxbackup.MEETINGDATA_SPLEX 
                WHERE lastmodifiedtime between :start_time and :end_time
                and tablename='wbxmmconference'
                group by lastmodifiedtime
                order by 1
             """
        rows = session.execute(SQL, {"start_time": start_time, "end_time":end_time}).fetchall()
        timepointlist = []
        avgtimelist = []
        rowcountlist = []
        for row in rows:
            timepointlist.append(row[0])
            avgtimelist.append(row[1])
            rowcountlist.append(row[2])
        return timepointlist,avgtimelist,rowcountlist

    def isDataguardEnabled(self):
        session = self.getLocalSession()
        SQL = """ SELECT count(1) FROM gv$managed_standby WHERE process='LNS' and status='WRITING' """
        row = session.execute(SQL).one()
        return True if row[0] > 0 else False

    def exec_procedure(self,sql):
        session = self.getLocalSession()
        session.execute(sql)

    def gettahoemachineconnection(self, schema, pri_pool_name, gsb_pool_name):
        session = self.getLocalSession()
        SQL = """ 
            select 'alter system kill session ''' ||sid || ',' || serial# || ''' immediate;' from %s where USERNAME='%s' and regexp_like(machine,'^%s[[:alpha:]]')
            union 
            select 'alter system kill session ''' ||sid || ',' || serial# || ''' immediate;' from %s where USERNAME='%s' and regexp_like(machine,'^%s[[:alpha:]]')
            """% ("gv$session", schema.upper(), pri_pool_name.lower(), "gv$session", schema.upper(), gsb_pool_name.lower() )
        logging.info("starting to execute: \n %s" % SQL)
        rows = session.execute(SQL).fetchall()
        machine_connection_list = []
        for row in rows:
            machine_connection_list.append(row[0])
        return machine_connection_list

    def killmachine_connection(self, _machine_connection_list):
        SQL = "\n".join(_machine_connection_list)
        session = self.getLocalSession()
        print("starting to execute: \n %s" % SQL)
        session.execute(SQL)

    def getTablespaceSize(self):
        session = self.getLocalSession()
        res={}
        SQL="""
        select ta.tablespace_name,nvl(ceil(tb.usedsize/1024.0/1024/1024/24),1)
        from (
            select ta.tablespace_name, sum(decode(ta.maxbytes,0,ta.user_bytes, ta.maxbytes)) totalsize, count(1) filecnt
            from dba_data_files ta, dba_tablespaces tb
            where ta.tablespace_name=tb.tablespace_name
            and tb.tablespace_name not in ('SYSTEM','SYSAUX','USERS') and contents='PERMANENT'
            group by ta.tablespace_name
        ) ta,
        (select tablespace_name, sum(bytes) usedsize
         from dba_segments
         group by tablespace_name ) tb
        where ta.tablespace_name=tb.tablespace_name(+)
        """
        rows = session.execute(SQL).fetchall()
        if rows is None:
            return None
        for item in rows:
            res[item[0]]=item[1]
        return res

    def getDBMemoSize(self):
        session = self.getLocalSession()
        SQL="""select * from (select name,value/1024/1024/1024 as val from v$parameter where name in ('sga_target' ,'pga_aggregate_target'))
        pivot (max(val) for name in ('sga_target' as sga_target,'pga_aggregate_target' AS pga_aggregate_target))
        """
        rows = session.execute(SQL).fetchone()
        if rows is None:
            return None
        res = dict(zip(rows.keys(), rows))
        return res

    def checkwbxbackendjoblog(self):
        session = self.getLocalSession()
        sql = "select logid,logcontent from test.wbxbackendjoblog where dbprocname='PKGREPAIRMTG.ProcRepairMtgCoreTables' and logtime>sysdate-1 order by logtime desc"
        row = session.execute(sql).first()
        return row

    def get_WBXBACKENDJOBDETAILLOG(self,logid):
        session = self.getLocalSession()
        sql = '''
        select itemid,logid,itemname,itemvalue from test.WBXBACKENDJOBDETAILLOG where logid = '%s'
        ''' %(logid)
        row = session.execute(sql).fetchall()
        return row

    def getteodbmachineconnection(self):
        session = self.getLocalSession()
        SQL = """ 
        select count(1) from gv$session where type='USER' and schemaname in ('BOSSRPT','XXRPTH')
        """
        rows = session.execute(SQL).fetchall()
        return rows[0][0]
