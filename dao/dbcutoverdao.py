from dao.wbxdaomanager import wbxdao
from dao.vo.dbcutover import wbxdbcutovervo, wbxdbcutoverprocessvo, wbxdbcutoverspmappingvo
from common.wbxutil import wbxutil
from sqlalchemy import desc, and_

class DBCutoverDao(wbxdao):

    def getDBInfo(self, host_name, db_name):
        try:
            session = self.getLocalSession()
            SQL = " SELECT hi.host_name, hi.ssh_port, hi.site_code           " \
                  " from database_info di, instance_info ii, host_info hi    " \
                  " WHERE di.db_name='%s'                                    " \
                  " AND ii.host_name='%s'                                      " \
                  " AND di.trim_host=ii.trim_host                            " \
                  " AND di.db_name=ii.db_name                                " \
                  " AND ii.host_name=hi.host_name" % (db_name, host_name)
            rows = session.query(SQL).all()
            if rows is None or len(rows) == 0:
                raise Exception("WBXERROR: Can not get db from DepotDB by db_name=%s,host_name=%s" % (db_name, host_name))

            host_names = []
            ssh_port = 0
            for row in rows:
                host_names.append(row[0].split(".")[0])
                ssh_port = row[1]

            SQL = " select distinct src_db, src_host, port , hi.ssh_port" \
                  " from shareplex_info si, database_info di, host_info hi" \
                  " where tgt_db='%s' " \
                  " and si.src_db=di.db_name" \
                  " and di.trim_host='%s'" \
                  " and si.src_host=hi.host_name" % (db_name, host_name)
            rows = session.query(SQL).all()

            srcdict = {}
            for row in rows:
                port = row[2]
                rowdict = {"db_name": row[0],"port":row[2],"host_name": row[1],"ssh_port": row[3]}
                if port not in srcdict:
                    srcdict[port] = []
                srcdict[port].append(rowdict)

            tgtdict = {}
            SQL = " select distinct si.src_host, si.port, tgtdb.db_name, tgthi.host_name, tgthi.ssh_port" \
                  " from shareplex_info si, database_info srcdi , database_info tgtdb, host_info tgthi" \
                  " where si.src_db='%s' " \
                  " and si.src_db=srcdi.db_name" \
                  " and srcdi.trim_host='%s'" \
                  " and si.tgt_db=tgtdb.db_name" \
                  " and si.tgt_host=tgthi.host_name" % (db_name, host_name)

            for row in rows:
                port = row[1]
                rowdict = {"src_host": row[0],"port": row[1], "tgt_db":row[2], "tgt_host":row[3], "tgt_ssh_port": row[4]}
                if port not in tgtdict:
                    tgtdict[port] = []
                tgtdict[port].append(rowdict)

            schemadict = {}
            SQL=" select  schema, f_get_deencrypt(password) as pwd " \
                " from appln_pool_info ai, database_info di " \
                " where di.db_name=:db_name " \
                " and di.db_type=:db_env " \
                " and di.trim_host=ai.trim_host" \
                " and di.db_name=ai.db_name"
            rows = session.query(SQL).all()
            for row in rows:
                schemaname = row[0]
                pwd = row[1]
                schemadict[schemaname] = pwd
            return host_names, ssh_port, srcdict, tgtdict, schemadict
        except Exception as e:
            print(e)

    def getSplexuserPasswordByDBName(self, host_name, db_name):
        session = self.getLocalSession()
        SQL = """ select distinct src_host, src_db, port, tgt_host, tgt_db, src_schema, tgt_schema, src_splex_sid, tgt_splex_sid, ii.trim_host, f_get_deencrypt(ai.password)
                  from shareplex_info si, instance_info ii, instance_info ii2,appln_pool_info ai, database_info di
                  where ii2.db_name='%s'
                  and ii2.host_name='%s'
                  and ii.db_name=ii2.db_name
                  and ii.trim_host=ii2.trim_host
                  and ii.db_name=di.db_name
                  and ii.trim_host=di.trim_host
                  and di.db_type in ('PROD','BTS_PROD')
                  and si.src_host not in ('tadborbf06','sjdborbf06','tadborbf07','sjdborbf07','tadbth351','tadbth352','sjdbwbf1','sjdbwbf2','sjdbth351','sjdbth352')
                  and ((ii.host_name=si.src_host and ii.db_name=si.src_db) or (ii.host_name=si.tgt_host and ii.db_name=si.tgt_db))
                  and ii.db_name=ai.db_name
                  and ii.trim_host=ai.trim_host
                  and lower(ai.schema)='splex'||si.port""" \
              % (db_name, host_name)
        userlist = session.execute(SQL).fetchall()
        return userlist

    def getCutoverStepByProcessid(self, processid):
        session = self.getLocalSession()
        processvo = session.query(wbxdbcutoverprocessvo).filter(wbxdbcutoverprocessvo.processid == processid).one()
        processvo.initLock()
        return processvo

    def addLog(self, vo):
        session = self.getLocalSession()
        session.add(vo)

    def deleteLog(self, vo):
        session = self.getLocalSession()
        session.delete(vo)

    def getDBCutoverByCutoverid(self, dbcutoverid):
        session = self.getLocalSession()
        cutovervo = session.query(wbxdbcutovervo).filter(wbxdbcutovervo.cutoverid == dbcutoverid).one()
        return cutovervo

    def getDBCutoverByDBName(self, db_name, old_hostname):
        session = self.getLocalSession()
        cutovervo = session.query(wbxdbcutovervo).filter(and_(wbxdbcutovervo.db_name == db_name, wbxdbcutovervo.old_host_name == old_hostname)).first()
        return cutovervo

    def listCutoverDetail(self, db_name, old_host_name):
        session = self.getLocalSession()
        SQL='''
            select d.db_name,p.db_name as src_db_name, p.host_name, p.server_type, p.module, p.port, p.action, p.status, m.old_host_name,m.new_host_name
            from wbxdbcutover d, wbxdbcutoverprocess p, wbxdbcutoverspmapping m
            where d.db_name='%s' and d.old_host_name='%s'
            and d.cutoverid = p.cutoverid
            and p.cutoverid = m.cutoverid(+)
            and p.port = m.port(+)
            order by p.order
        ''' % (db_name, old_host_name)
        userlist = session.execute(SQL).fetchall()
        return userlist

    def listAllCutoverDB(self):
        session = self.getLocalSession()
        volist = session.query(wbxdbcutovervo).filter(wbxdbcutovervo.createtime > wbxutil.getcurrenttime(3*24 * 60 * 60)).order_by(desc(wbxdbcutovervo.createtime)).all()
        return volist

    def listCutoverStep(self, cutoverid):
        session = self.getLocalSession()
        volist = session.query(wbxdbcutoverprocessvo).filter(wbxdbcutoverprocessvo.cutoverid == cutoverid).order_by(wbxdbcutoverprocessvo.processorder).all()
        return volist

    def listCutoverspMapping(self, cutoverid):
        session = self.getLocalSession()
        volist = session.query(wbxdbcutoverspmappingvo).filter(wbxdbcutoverspmappingvo.cutoverid == cutoverid).all()
        return volist

    def getUserSessionCount(self):
        session = self.getLocalSession()
        SQL = "select count(1) from gv$session where type='USER' and schemaname like 'TAHOE%'"
        row = session.execute(SQL).fetchone()
        return row[0]
