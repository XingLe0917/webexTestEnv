from sqlalchemy import Column,Integer, BigInteger,String, DateTime, text, func
from dao.vo.wbxvo import Base
from common.wbxutil import wbxutil

class DBAMonitorJobVO(Base):
    __tablename__ = "wbxdbamonitorjob2"
    jobid = Column(String(64), primary_key=True, default=func.sys_guid(), server_default=text("SYS_GUID()"))
    jobname = Column(String(64))
    appln_support_code = Column(String(64))
    dbnames = Column(String(1024))
    db_type = Column(String(32))
    func = Column(String(64))
    reportfunc  = Column(String(64))
    args = Column(String(4000))
    job_type = Column(String(32))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    frequency = Column(Integer)
    jobargs = Column(String(4000))
    runstatus = Column(String(16), default='pending')
    laststarttime =  Column(DateTime)
    lastendtime =  Column(DateTime)
    failedcount =  Column(Integer)
    errormsg = Column(String(4000))
    emailto = Column(String(4000))
    sendtospark = Column(String(1), default='N')


    def __str__(self):
        return "jobname=%s, func=%s, appln_support_code=%s, db_type=%s, job_type=%s, frequency=%s start_time=%s, end_time=%s, jobargs=%s" % \
               (self.jobname, self.func, self.appln_support_code, self.db_type, self.job_type, self.frequency, wbxutil.convertDatetimeToString(self.start_time),
                wbxutil.convertDatetimeToString(self.end_time), self.jobargs)

    def __repr__(self):
        return "jobname=%s, func=%s, appln_support_code=%s, db_type=%s, job_type=%s, frequency=%s" % (
        self.jobname, self.func, self.appln_support_code, self.db_type, self.job_type, self.frequency)

class StapCronJobDefinition(Base):
    __tablename__ = "STAP_CRONTAB_JOB_DEFINITION"
    __table_args__ = {'schema': 'stapuser'}
    jobid = Column(String(64), nullable=False, primary_key=True, default=func.sys_guid())
    jobname = Column(String(300), nullable=False)
    joblevel = Column(String(32), nullable=False)
    createtime = Column(DateTime, nullable=False, default=func.now())
    lastmodifiedtime = Column(DateTime, nullable=False, default=func.now())


class StapCronjobConfig(Base):
    __tablename__ = "STAP_CRONTAB_CONF"
    __table_args__ = {'schema': 'stapuser'}
    hostname = Column(String(100), nullable=False, primary_key=True)
    name = Column(String(300), nullable=False, primary_key=True)
    custom_id = Column(String(20), primary_key=True)
    create_date = Column(DateTime)
    cron_sch = Column(String(100))
    next_runtime = Column(String(30))
    last_runtime = Column(String(30))
    is_del = Column(String(1))
    id = Column(BigInteger)

    def getJobID(self):
        return "%s_%s" % (self.hostname.split('.')[0], self.name)

    def getDBID(self):
        return "%s_%s" % (self.hostname.split('.')[0][0:-1].lower(), self.custom_id[0:-1].upper())

class StapCronjobLog(Base):
    __tablename__ = "STAP_CRONTAB_LOG"
    __table_args__ = {'schema': 'stapuser'}
    hostname = Column(String(100), nullable=False, primary_key=True)
    name = Column(String(300), nullable=False, primary_key=True)
    custom_id = Column(String(20), primary_key=True)
    start_time = Column(String(50))
    end_time = Column(String(50))
    result = Column(String(500))
    status = Column(String(20))
    create_date = Column(DateTime)

class Stawcr(Base):
    __tablename__ = "STAP_CP_WCR"
    __table_args__ = {'schema': 'stapuser'}
    infrastructure_change_id = Column(String(30), primary_key=True)
    scheduled_start_date = Column(String(30))
    completed_date = Column(String(30))
    summary = Column(String(100))
    type_of_environment = Column(String(30))
    change_imp  = Column(String(50))
    change_imp_support_group = Column(String(50))
