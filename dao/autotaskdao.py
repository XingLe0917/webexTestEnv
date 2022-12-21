from dao.wbxdaomanager import wbxdao
from dao.vo.dbcutover import wbxdbcutovervo, wbxdbcutoverprocessvo, wbxdbcutoverspmappingvo
from common.wbxutil import wbxutil
from sqlalchemy import desc, and_

from dao.vo.autotaskvo import wbxautotaskvo, wbxautotaskjobvo

class autoTaskDao(wbxdao):

    def addAutoTask(self, taskvo):
        session = self.getLocalSession()
        session.add(taskvo)

    def addAutoTaskJob(self,jobvo):
        session = self.getLocalSession()
        session.add(jobvo)

    def getAutoTaskByParameter(self, task_type, parameter):
        session = self.getLocalSession()
        return session.query(wbxautotaskvo).filter(wbxautotaskvo.task_type == task_type, wbxautotaskvo.parameter == parameter).first()

    def getAutoTaskByTaskid(self, taskid):
        session = self.getLocalSession()
        return session.query(wbxautotaskvo).filter(wbxautotaskvo.taskid == taskid).first()

    def getAutoTaskJobByTaskid(self, taskid):
        session = self.getLocalSession()
        return session.query(wbxautotaskjobvo).filter(wbxautotaskjobvo.taskid == taskid).order_by(wbxautotaskjobvo.processorder
                ,wbxautotaskjobvo.host_name,wbxautotaskjobvo.splex_port).all()

    def getAutoTaskJobByJobid(self, jobid):
        session = self.getLocalSession()
        return session.query(wbxautotaskjobvo).filter(wbxautotaskjobvo.jobid == jobid).first()

    def listAutoTaskByTasktype(self, task_type):
        session = self.getLocalSession()
        return session.query(wbxautotaskvo).filter(wbxautotaskvo.task_type == task_type).all()
            # order_by(wbxautotaskvo.lastmodifiedtime).all()

    def getAutoTaskByTasktype(self, task_type):
        session = self.getLocalSession()
        SQL='''
select taskid,task_type,parameter,to_char(createtime,'yyyy-MM-dd HH24:mi:ss') createtime,to_char(lastmodifiedtime,'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime,createby,status from
(
select row_number() over(partition by a.taskid order by decode(status,'FAILED',1,'RUNNING',2,'PENDING',3,'SUCCEED',4,5) ) rum,
a.taskid,a.task_type,a.parameter,a.createtime,a.lastmodifiedtime,a.createby
,b.status from wbxautotask a , wbxautotaskjob b
where task_type='%s'
and a.taskid=b.taskid
) where rum=1 order by createtime desc
        '''%(task_type)
        rows=session.execute(SQL).fetchall()
        res = [dict(zip(row.keys(), row)) for row in rows]
        return res

    def getlastjob(self,task_type,db_name,host_name,splex_port):
        session = self.getLocalSession()
        SQL = ''' 
        select * from (
        select t1.taskid,t2.jobid,t2.db_name,t2.host_name,t2.splex_port,t2.status,t2.createtime,t2.lastmodifiedtime
        from wbxautotask t1,wbxautotaskjob t2
        where t1.taskid = t2.taskid
        and t1.task_type = '%s'
        ''' %(task_type)
        if db_name:
            SQL = SQL + " and t2.db_name = '%s'" % db_name
        if host_name:
            SQL = SQL + " and t2.host_name = '%s'" % host_name
        if splex_port:
            SQL = SQL + " and t2.splex_port = %s " % splex_port
        SQL = SQL + " order by t2.lastmodifiedtime desc) where rownum=1"
        rows = session.execute(SQL).fetchall()
        return rows

    def update_wbxautotask_lastmodifiedtime(self,taskid):
        session = self.getLocalSession()
        SQL = '''
        update wbxautotask set lastmodifiedtime = sysdate where taskid = '%s'
        ''' %(taskid)
        iresult = session.execute(SQL)
        return iresult.rowcount

    def update_wbxautotaskjob_lastmodifiedtime(self,jobid):
        session = self.getLocalSession()
        SQL = '''
              update wbxautotaskjob set lastmodifiedtime = sysdate where jobid = '%s'
               ''' % (jobid)
        iresult = session.execute(SQL)
        return iresult.rowcount

    def getSelfHealingList(self):
        session = self.getLocalSession()
        SQL = '''
         select t1.taskid,t2.jobid,t1.task_type,t2.db_name,t2.host_name,t2.splex_port,t2.status,to_char(t2.createtime,'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(t2.lastmodifiedtime,'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
        from wbxautotask t1,wbxautotaskjob t2
        where t1.taskid = t2.taskid
        and t1.self_heal = '1'
        order by t2.lastmodifiedtime desc
        '''
        rows = session.execute(SQL).fetchall()
        return rows

    def getJobTimedelta(self,taskid,processorder):
        session = self.getLocalSession()
        SQL = '''
        select nvl(ceil(TO_NUMBER(sysdate - max(end_time))*24*60+2),0) from wbxautotaskjob where taskid = '%s' and processorder=%s
        ''' %(taskid,processorder)
        rows = session.execute(SQL).fetchone()
        return rows

    def getAutoTaskJobByProcessOrder(self, taskid, processorder):
        session = self.getLocalSession()
        return session.query(wbxautotaskjobvo).filter(wbxautotaskjobvo.processorder == processorder, wbxautotaskjobvo.taskid == taskid).first()
