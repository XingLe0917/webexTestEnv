from sqlalchemy import Column, Integer,String, DateTime, and_, ForeignKey, literal_column, text, func
from sqlalchemy.orm import object_session
from sqlalchemy.sql import case
from dao.vo.wbxvo import Base
from datetime import datetime
from common.wbxutil import wbxutil

class JobTemplateVO(Base):
    __tablename__ = "wbxjobtemplate"
    templateid = Column(String(64), primary_key=True,server_default=text("SYS_GUID()"))
    jobname = Column(String(64))
    job_level = Column(String(32))
    db_vendor = Column(String(16))
    appln_support_code = Column(String(16))
    application_type = Column(String(16))
    db_type = Column(String(16))
    db_names = Column(String(4000))
    job_type = Column(String(16))
    filename = Column(String(4000))
    parameter = Column(String(4000))
    jobruntime = Column(String(4000))
    status = Column(String(16))
    description = Column(String(4000))

class JobManagerInstanceVO(Base):
    __tablename__ = "wbxjobmanagerinstance"
    host_name = Column(String(35), primary_key=True)
    opstatus = Column(Integer)
    status = Column(String(16))
    lastupdatetime = Column(DateTime)

    @property
    def finalstatus(self):
        return "SHUTDOWN" if self.lastupdatetime < wbxutil.getcurrenttime(180) else self.status

class JobInstanceVO(Base):
    __tablename__ = "wbxjobinstance"
    jobid = Column(String(64), primary_key=True, server_default=text("SYS_GUID()"))
    templateid = Column(String(64))
    host_name = Column(String(35))
    jobname = Column(String(64))
    job_type = Column(String(16))
    commandstr = Column(String(4000))
    jobruntime = Column(String(4000))
    status = Column(String(16))
    errormsg = Column(String(4000))
    next_run_time = Column(DateTime)
    last_run_time = Column(DateTime)