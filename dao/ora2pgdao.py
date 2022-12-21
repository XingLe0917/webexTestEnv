import json
import logging
from datetime import datetime
from dao.wbxdaomanager import wbxdao
from dao.vo.ora2pgvo import wbxora2pgtablevo
from sqlalchemy import or_

'''
This dao contains all SQL for ora2pg;
'''
class ORA2PGDao(wbxdao):
    def listPGTables(self,schemaname):
        session = self.getLocalSession()
        SQL = ''' SELECT relname, reltype,relkind,relispartition 
                  FROM pg_class pc left join pg_namespace pn on pc.relnamespace=pn.oid 
                  WHERE pn.nspname=:schemaname 
                  AND pc.relkind='r' 
                  AND relispartition='f'
                      '''
        tableList = session.execute(SQL,{"schemaname":schemaname}).fetchall()
        return tableList

    def isSchemaExist(self, schemaname, db_vendor):
        session = self.getLocalSession()
        if db_vendor=="Oracle":
            SQL = '''SELECT count(1) FROM DBA_USERS WHERE username=:schemaname '''
        elif db_vendor=="POSTGRESQL":
            SQL = '''SELECT count(1) FROM information_schema.schemata WHERE schema_name = :schemaname '''
        row = session.execute(SQL, {"schemaname": schemaname.upper()}).fetchone()
        return True if row is not None else False

    def listTableColumns(self, schemaname, db_vendor,*tablelist):
        session = self.getLocalSession()
        # table_names = "','".join(tablelist)
        if db_vendor == "Oracle":
            # SQL = '''SELECT owner as table_schema, table_name, column_name, data_type, data_length as character_maximum_length,
            #                 data_precision as numeric_precision, data_scale as numeric_scale,0 as datetime_precision, nullable  as is_nullable
            #         FROM dba_tab_columns
            #         WHERE owner=:schemaname and table_name in :tablelist '''
            SQL = '''SELECT owner as table_schema, table_name, column_name, data_type, data_length as character_maximum_length, 
                                        data_precision as numeric_precision, data_scale as numeric_scale,0 as datetime_precision, nullable  as is_nullable
                                FROM dba_tab_columns 
                                WHERE owner='%s' and table_name in %s '''
            schemaname=schemaname.upper()
        elif db_vendor == "POSTGRESQL":
            SQL = '''SELECT table_schema, table_name, column_name,data_type,character_maximum_length,
                            numeric_precision,numeric_scale,datetime_precision,is_nullable 
                    FROM information_schema.columns 
                    WHERE table_schema='%s' and table_name in %s'''
            schemaname = schemaname.lower()
        SQL=SQL %(schemaname, tuple(tablelist))
        rows = session.execute(SQL, {"schemaname": schemaname,"tablelist":tuple(tablelist)}).fetchall()
        tableDict = {}
        for row in rows:
            table_name = row[1]
            if table_name not in tableDict:
                tableDict[table_name] = {}
            colDict = tableDict[table_name]
            colDict[row[2]] = row
        return tableDict

    def listTableForMigration(self, schemaname, db_vendor):
        session = self.getLocalSession()
        res=[]
        if db_vendor == "Oracle":
            schemaname=schemaname.upper()
            SQL = '''
                with top_table as 
                (
                select distinct parent_table as table_name, pri 
                from (
                    select pri, parent_table,max(pri) over (partition by parent_table) top_pri 
                    from (
                        select level+1 as pri, child_table, parent_table 
                        from (
                            select fck.table_name child_table, pck.table_name parent_table
                            from dba_constraints fck, dba_constraints pck 
                            where fck.owner=:schemaname 
                            and fck.constraint_type='R'
                            and fck.r_constraint_name=pck.constraint_name
                            ) connect by prior parent_table=child_table
                        )
                    )  where pri=top_pri
                )
                select dt.owner as table_owner, dt.table_name, 1 as priority, par.partition_name, decode(partitioned,'YES',par.num_rows, dt.num_rows) as num_rows
                from dba_tables dt, dba_tab_partitions par  
                where dt.owner=:schemaname  
                and dt.owner=par.table_owner(+) 
                and dt.table_name=par.table_name(+)
                and not exists (select 1 from top_table tt where dt.table_name=tt.table_name)
                union all
                select  dt.owner as table_owner, tt.table_name, tt.pri as priority, par.partition_name, decode(dt.partitioned,'YES',par.num_rows, dt.num_rows) as num_rows
                from top_table tt, dba_tables dt, dba_tab_partitions par
                where dt.owner=:schemaname  
                and dt.owner=par.table_owner(+) 
                and dt.table_name=par.table_name(+)
                and dt.table_name=tt.table_name
                order by priority desc
        '''
        starttime=datetime.now()
        rows = session.execute(SQL, {"schemaname": schemaname}).fetchall()
        endtime=datetime.now()
        print((endtime-starttime).seconds)
        for row in rows:
            tbvo=wbxora2pgtablevo(table_owner=row[0],table_name=row[1],priority=row[2],partition_name=row[3],estimate_count=row[4])
            res.append(tbvo)
        # if rows is not None:
        #     res=[dict(zip(row.keys(), row)) for row in rows]
        return res

    def listFilterTables(self, taskid, schemaname):
        session = self.getLocalSession()
        SQL = ''' SELECT table_owner, table_name, partition_name FROM WBXORA2PGFILTERTABLE WHERE taskid=:taskid and table_owner=:schemaname'''
        rows = session.execute(SQL, {"taskid": taskid,"schemaname":schemaname}).fetchall()
        return {row.table_name:row for row in rows}

    def insertOra2pgTable(self, taskid, table_owner, table_name, partition_name, num_rows, priority):
        session = self.getLocalSession()
        SQL = '''INSERT INTO WBXORA2PGTABLE(taskid, table_owner, table_name, partition_name, estimate_count, priority )
                 VALUES(:taskid, :table_owner, :table_name, :partition_name, :num_rows, :priority )
                 '''
        session.execute(SQL,{"taskid":taskid,"table_owner":table_owner,"table_name":table_name,
                             "partition_name":partition_name,"num_rows":num_rows,"priority":priority})

    def batchInsertOra2pgTable(self, *objs):
        session = self.getLocalSession()
        session.bulk_save_objects(objs)

    def getAvailableSlaveServer(self):
        session = self.getLocalSession()
        SQL = '''SELECT ui.host_name,ui.username, f_get_deencrypt(ui.pwd) as pwd, pg.load, pg.cpu_count
                 FROM wbxora2pgserver pg, host_user_info ui 
                 WHERE pg.host_name=ui.host_name
                 AND pg.lastupdatetime > sysdate - 3/60/24
                 AND pg.status='RUNNING' '''
        rows = session.execute(SQL).fetchall()
        return rows

    def getOra2pgTaskTableByTabStatus(self, taskid, status):
        session = self.getLocalSession()
        return session.query(wbxora2pgtablevo).filter(wbxora2pgtablevo.taskid == taskid, wbxora2pgtablevo.table_status == status).order_by(wbxora2pgtablevo.table_status
                ,wbxora2pgtablevo.extract_status,wbxora2pgtablevo.load_status).all()

    def getOra2pgTaskTabWithStatus(self,  taskid ,owners , status ):
        session = self.getLocalSession()
        owner_list=owners.split(",")
        return session.query(wbxora2pgtablevo).filter(wbxora2pgtablevo.taskid == taskid, wbxora2pgtablevo.table_owner.in_(owner_list),
                                                      wbxora2pgtablevo.table_status == status).order_by(wbxora2pgtablevo.table_status , wbxora2pgtablevo.extract_status, wbxora2pgtablevo.load_status).all()

    def getOra2pgTaskTabByTabQuery(self,  taskid, schema=None,table_name=None, table_status=None, page_size=None, page_index=None):
        session = self.getLocalSession()
        query = session.query(wbxora2pgtablevo).filter(wbxora2pgtablevo.taskid == taskid,
                                                      or_(wbxora2pgtablevo.table_owner == schema,schema is None),
                                                      or_(wbxora2pgtablevo.table_name.like('%{0}%'.format(table_name)),table_name is None),
                                                      or_(wbxora2pgtablevo.table_status == table_status, table_status is None)
                                                      ).order_by(wbxora2pgtablevo.table_status ,
                                                                 wbxora2pgtablevo.extract_status,
                                                                 wbxora2pgtablevo.load_status)
        if page_size is not None and page_index is not None:
            query = query.limit(page_size).offset((page_index-1)*page_size)
        tabvo_list=query.all()
        return tabvo_list

    def updBatchOra2pgTabStatusByTableID(self, taskid, status, *tabidlist):
        session = self.getLocalSession()
        # tbvolist = session.query(wbxora2pgtablevo).filter(wbxora2pgtablevo.taskid == taskid, wbxora2pgtablevo.table_owner == owner,
        #                                                   wbxora2pgtablevo.table_name.in_(tableid)).all()
        tbvo_upd_mapping = [{"tableid": tableid, "table_status": status} for tableid in tabidlist]
        session.bulk_update_mappings(wbxora2pgtablevo, tbvo_upd_mapping)
