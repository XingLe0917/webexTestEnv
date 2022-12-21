from sqlalchemy import Column,Integer,String, DateTime, func, text, select
from sqlalchemy.orm import column_property
from dao.vo.wbxvo import Base
import threading

class wbxdbcutovervo(Base):
    __tablename__ = "wbxdbcutover"
    cutoverid = Column(String(64), primary_key=True)
    db_name = Column(String(30))
    old_host_name = Column(String(22))
    new_host_name = Column(String(22))
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())

class wbxdbcutoverprocessvo(Base):
    __tablename__ = "wbxdbcutoverprocess"
    processid = Column(String(64), primary_key=True, server_default=text("sys_guid()"))
    cutoverid = Column(String(64))
    db_name = Column(String(30))
    db_splex_sid = Column(String(30))
    host_name = Column(String(22))
    server_type = Column(String(30))
    module = Column(String(64))
    port = Column(Integer)
    action = Column(String(30))
    status = Column(String(30))
    processorder = Column(Integer)
    starttime = Column(DateTime)
    endtime = Column(DateTime)
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

    def __str__(self):
        return "cutoverid=%s, db_name=%s, host_name=%s, server_type=%s, module=%s, action=%s, status=%s" %\
               (self.cutoverid, self.db_name, self.host_name, self.server_type, self.module, self.action, self.status)

class wbxdbcutoverspmappingvo(Base):
    __tablename__ = "wbxdbcutoverspmapping"
    mappingid = Column(String(64), primary_key=True, server_default=text("SYS_GUID()"))
    cutoverid = Column(String(64))
    db_name = Column(String(30))
    port = Column(Integer)
    old_host_name = Column(String(22))
    new_host_name = Column(String(22))



