from dao.wbxdaomanager import wbxdao
from dao.vo.jobvo import DBAMonitorJobVO, StapCronJobDefinition, StapCronjobConfig, StapCronjobLog,Stawcr
from sqlalchemy import and_, literal_column, text
from sqlalchemy import func

class JobManagerDao(wbxdao):

    def searchJobs(self):
        session = self.getLocalSession()
        jobList = session.query(DBAMonitorJobVO).all()
        return jobList

    def getJobByJobname(self, jobname):
        session = self.getLocalSession()
        jobvo = session.query(DBAMonitorJobVO).filter(DBAMonitorJobVO.jobname == jobname).first()
        return jobvo

    def getJobByJobnameAndDBNames(self, jobname, dbnames):
        session = self.getLocalSession()
        jobvo = session.query(DBAMonitorJobVO).filter(and_(DBAMonitorJobVO.jobname == jobname, DBAMonitorJobVO.dbnames==dbnames)).first()
        return jobvo

    def addJob(self, jobvo):
        session = self.getLocalSession()
        session.add(jobvo)

    def deleteJob(self, jobvo):
        session = self.getLocalSession()
        session.delete(jobvo)

    def getAllStapCronJobDefinition(self):
        session = self.getLocalSession()
        return  session.query(StapCronJobDefinition).all()

    def getAllStapCronJobConfig(self):
        session = self.getLocalSession()
        return session.query(StapCronjobConfig).all()

    '''
    select * from(
select distinct ta.*, tb.start_time, tb.end_time from (
select  hostname, name, nvl(custom_id,0) as custom_id, cron_sch, last_runtime, next_runtime from stapuser.stap_crontab_conf
) ta,
(
select  hostname, name, nvl(custom_id,0) as custom_id, start_time, end_time from stapuser.stap_crontab_log
) tb
where ta.hostname=tb.hostname(+)
and ta.name=tb.name(+)
and ta.custom_id=tb.custom_id(+)
) where hostname='abdblkup1'
    '''
    def getAllCronJobConf(self):
        session = self.getLocalSession()
        SQL=" select distinct ta.hostname,ta.name, ta.custom_id,ta.cron_sch, ta.last_runtime, ta.next_runtime, tb.start_time, tb.end_time " \
            " from (select  hostname, name, nvl(custom_id,0) as custom_id, cron_sch, last_runtime, next_runtime from stapuser.stap_crontab_conf) ta," \
            "      (select  hostname, name, nvl(custom_id,0) as custom_id, start_time, end_time from stapuser.stap_crontab_log) tb" \
            " where ta.hostname=tb.hostname(+)" \
            " and ta.name=tb.name(+)" \
            " and ta.custom_id=tb.custom_id(+)" \
            " order by ta.hostname, ta.name, ta.custom_id"
        return session.execute(SQL).fetchall()

    def deleteStapCronjobConfig(self, cronjobcfgvo):
        session = self.getLocalSession()
        session.delete(cronjobcfgvo)

    '''select scheduled_start_date, completed_date, summary, type_of_environment, change_imp, infrastructure_change_id 
       from stap_cp_wcr where CHANGE_IMP_SUPPORT_GROUP in ('Production DBA','China DBA') and upper(summary) like '%DB%15826%L%RLM%
       '''
    def listStapWCRBySummary(self, searchstr, dbenv):
        session = self.getLocalSession()
        SQL=" select scheduled_start_date, completed_date, summary, type_of_environment, change_imp, infrastructure_change_id " \
            " from stapuser.stap_cp_wcr " \
            " where CHANGE_IMP_SUPPORT_GROUP in ('Production DBA','China DBA') " \
            " and to_date(SCHEDULED_START_DATE,'YYYY-MM-DD hh24:mi:ss') > sysdate - 360 " \
            " and type_of_environment='%s'" \
            " and upper(summary) like '%s'" % (dbenv, searchstr)
        return session.execute(SQL).fetchall()
