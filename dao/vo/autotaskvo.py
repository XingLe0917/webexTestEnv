from sqlalchemy import Column,Integer,String, DateTime, func, text, JSON, select
from dao.vo.wbxvo import Base, JSONEncodedDict
import threading

class wbxautotaskvo(Base):
    __tablename__ = "wbxautotask"
    taskid = Column(String(64), primary_key=True)
    task_type = Column(String(32))
    parameter = Column(JSONEncodedDict)
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())
    self_heal = Column(String(8))
    createby = Column(String(50),default="AutomationTool")


class wbxautotaskjobvo(Base):
    __tablename__ = "wbxautotaskjob"
    jobid = Column(String(64), primary_key=True)
    taskid = Column(String(64))
    db_name = Column(String(30))
    host_name = Column(String(30))
    splex_port = Column(Integer)
    job_action = Column(String(30))
    execute_method = Column(String(30))
    processorder = Column(Integer, default=1)
    parameter = Column(JSONEncodedDict)
    status = Column(String(16))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    resultmsg1 = Column(String)
    resultmsg2 = Column(String)
    resultmsg3 = Column(String)
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())

    def initLock(self):
        self._lock = threading.Lock()

    def acquire(self):
        return self._lock.acquire()

    def release(self):
        self._lock.release()
