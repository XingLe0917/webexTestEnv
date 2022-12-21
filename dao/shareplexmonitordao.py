import logging

from sqlalchemy import text
from sqlalchemy.orm import mapper
from dao.wbxdaomanager import wbxdao


logger = logging.getLogger("DBAMONITOR")
class ShareplexMonitorDao(wbxdao):

    # src_host is not in WHERE clause because it is possible the src_host is trimhost
    def querysplexmonitoradbdata(self, owner, tablename, src_db):
        session = self.getLocalSession()
        SQL="SELECT logtime FROM (SELECT logtime FROM %s.%s WHERE src_db='%s' order by logtime desc) where rownum <=1 " % (owner, tablename, src_db)
        adbvo = session.execute(SQL).fetchone()
        # adbvo = session.query(spvomapper).filter(and_(spvomapper.DIRECTION ==direction, spvomapper.SRC_HOST==src_host)).first()
        return adbvo

    def getsplexmonitoradbdata(self, schemaname, table_name, datadict):
        session = self.getLocalSession()
        SQL = text(
            "SELECT logtime FROM %s.%s WHERE direction=:DIRECTION and src_db=:SRC_DB and src_host=:SRC_HOST" % (schemaname, table_name))
        vo = session.execute(SQL, datadict).first()
        return vo

    def updatesplexmonitoradbdata(self, schemaname, table_name, datadict):
        session = self.getLocalSession()
        SQL = text("UPDATE %s.%s SET logtime=:CURRENTTIME WHERE direction=:DIRECTION and src_db=:SRC_DB and src_host=:SRC_HOST" % (schemaname, table_name))
        logger.info(SQL)
        iresult = session.execute(SQL, datadict)
        return iresult.rowcount

    def addsplexmonitoradbdata(self, schemaname, table_name, datadict):
        session = self.getLocalSession()
        SQL = text("INSERT INTO %s.%s VALUES(:DIRECTION, :SRC_HOST, :SRC_DB, :CURRENTTIME, :PORT_NUMBER) " % (schemaname, table_name))
        session.execute(SQL, datadict)

    def addmonitordata(self, spvo):
        session = self.getLocalSession()
        session.add(spvo)

    def getRoutingList(self, schemaname):
        session = self.getLocalSession()
        SQL = "select distinct LISTAGG(queuename,';') within group (order by rownum) over (partition by src_splex_sid) queuenames, src_splex_sid, count(1) over (partition by src_splex_sid) queuecount from ( select substr(qrouting,0,pos) queuename,  upper(substr(qrouting, pos+2)) src_splex_sid from (select length(qrouting)-instr(reverse(qrouting),'-',1) pos, qrouting from (select distinct substr(combo,1,instr(combo,':')-1) as qrouting from %s.SHAREPLEX_TRANS WHERE scn > timestamp_to_scn(sysdate-4))))" % schemaname
        routingvos = session.execute(SQL).fetchall()
        return routingvos

    def getLastSCNByChannel(self, splexuser, monitor_table_name, src_splex_sid):
        session = self.getLocalSession()
        if monitor_table_name is not None:
            SQL = "with routing " \
                  "      as " \
                  "     (select replace(substr(combo,instr(combo,'.')+1),'\"','') table_name,substr(combo,1, instr(combo,':')-1) sprouting, scn  " \
                  "     from %s.shareplex_trans" \
                  "     where combo like '%%%s%%')" \
                  " select max(tb.scn) as maxscn from routing ta, routing tb " \
                  " where ta.table_name='%s'" \
                  " and ta.sprouting=tb.sprouting" % (splexuser, src_splex_sid, monitor_table_name)
        else:
            SQL = "SELECT max(scn) as maxscn FROM %s.shareplex_trans WHERE combo like '%%%s%%'" % (splexuser, src_splex_sid)

        rec = session.execute(SQL).first()
        if rec is not None:
            return rec.maxscn
        return None

    def getTimestampBySCN(self, scn):
        session = self.getLocalSession()
        SQL = " select scn_to_timestamp(%d) as lasttimestamp FROM dual" % scn
        rec = session.execute(SQL).first()
        if rec is not None:
            return rec.lasttimestamp
        return None

    ''' select instance_number, count(1) laglogcount 
              from 
                   (select sp.seqno, inst.instance_number
                   from splex18022.shareplex_actid sp, gv$instance inst
                   where sp.instance_name=inst.instance_name
                   ) ta, 
                  (select sequence#, thread#  
                   from v$archived_log 
                   union all
                   select sequence#,thread#  from v$log where status = 'CURRENT'
                  ) tb 
               where ta.instance_number=tb.thread# 
               and ta.seqno < tb.sequence# 
               group by instance_number'''

    def getCaptureDelayLogCount(self, splexuser):
        session = self.getLocalSession()
        SQL = " select instance_number, count(1) laglogcount " \
              "from " \
              "     (select sp.seqno, inst.instance_number" \
              "     from %s.shareplex_actid sp, gv$instance inst" \
              "     where sp.instance_name=inst.instance_name" \
              "     ) ta, " \
              "    (select sequence#, thread#  " \
              "     from v$archived_log " \
              "     union all" \
              "     select sequence#,thread#  from v$log where status = 'CURRENT'" \
              "    ) tb " \
              " where ta.instance_number=tb.thread# " \
              " and ta.seqno < tb.sequence# " \
              " group by instance_number" % splexuser
        rows = session.execute(SQL).fetchall()
        return rows

    def updatesplexmonitoradbdata1(self, schemaname,table_name, direction,src_host,src_db,logtime):
        session = self.getLocalSession()
        if logtime:
            SQL = "UPDATE %s.%s SET logtime=to_date('%s','YYYY-MM-DD hh24:mi:ss') WHERE direction='%s' and src_db='%s' and src_host='%s'" % (
            schemaname, table_name, logtime, direction, src_db, src_host)
        else:
            SQL = "UPDATE %s.%s SET logtime=sysdate WHERE direction='%s' and src_db='%s' and src_host='%s'" % (schemaname, table_name,direction,src_db,src_host)
        logger.info(SQL)
        iresult = session.execute(SQL)
        return iresult.rowcount

    def deletesplexmonitoradbdata1(self,schemaname,table_name, replication_to,src_host,src_db):
        session = self.getLocalSession()
        SQL = "delete from %s.%s where direction='%s' and src_db='%s' and src_host='%s'" % (schemaname, table_name, replication_to, src_db, src_host)
        logger.info(SQL)
        iresult = session.execute(SQL)
        return iresult.rowcount

    def getsplexmonitoradbdata1(self, schemaname,table_name, replication_to,src_host,src_db):
        session = self.getLocalSession()
        # SQL = "select to_char(logtime,'YYYY-MM-DD hh24:mi:ss') logtime from %s.%s where direction = '%s' and src_host='%s' and src_db= '%s' "% (schemaname, table_name,replication_to,src_host,src_db)
        SQL = "select logtime from %s.%s where direction = '%s' and src_host='%s' and src_db= '%s' " % (
        schemaname, table_name, replication_to, src_host, src_db)
        logger.info(SQL)
        rows = session.execute(SQL).fetchall()
        return rows

    def insertsplexmonitoradbdata1(self,schemaname,table_name, direction,src_host,src_db,port,logtime):
        session = self.getLocalSession()
        if logtime:
            SQL = "insert into %s.%s (direction,src_host,src_db,logtime,port_number) values ('%s','%s','%s',to_date('%s','YYYY-MM-DD hh24:mi:ss'),%s)" %(schemaname, table_name,direction,src_host,src_db,logtime,port)
        else:
            SQL = "insert into %s.%s (direction,src_host,src_db,logtime,port_number) values ('%s','%s','%s',sysdate,%s)" %(schemaname, table_name,direction,src_host,src_db,port)
        logger.info(SQL)
        iresult = session.execute(SQL)
        return iresult.rowcount

    def delete_splex_monitor_adb(self,schemaname,table_name, direction,src_host,src_db):
        session = self.getLocalSession()
        SQL = '''
        delete from %s.%s WHERE direction='%s' and src_db='%s' and src_host='%s'
        ''' %(schemaname,table_name,direction,src_db, src_host)
        logger.info(SQL)
        iresult = session.execute(SQL)
        return iresult.rowcount

    def getSplexMonitorAdb(self, schemaname,table_name, direction,src_host,src_db):
        session = self.getLocalSession()
        SQL = "select * from %s.%s where direction='%s' and src_db='%s' and src_host='%s' " % (
        schemaname, table_name, direction, src_db, src_host)
        logger.info(SQL)
        rows = session.execute(SQL).fetchall()
        return rows

