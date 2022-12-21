from sqlalchemy import Column,Integer,String, DateTime, func, text, JSON, select
from dao.vo.wbxvo import Base, JSONEncodedDict
import threading

class wbxora2pgtablevo(Base):
    __tablename__ = "wbxora2pgtable"
    tableid = Column(String(64), primary_key=True,  server_default=text("SYS_GUID()"))
    taskid = Column(String(64))
    # host_name = Column(String(60))
    table_owner = Column(String(30))
    table_name = Column(String(30))
    partition_name = Column(String(30))
    priority = Column(Integer)
    table_status = Column(String(16))
    estimate_count = Column(Integer)
    extract_count = Column(Integer)
    extract_status = Column(String(16))
    extract_errormsg = Column(String)
    load_succeed_count = Column(Integer)
    load_failed_count = Column(Integer)
    load_status = Column(String(16))
    load_errormsg = Column(String)
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())

    def setTaskid(self, taskid):
        self.taskid=taskid

    def getTaskid(self):
        return self.taskid

    def setTable_status(self, status):
        self.table_status=status

    def getTable_status(self):
        return  self.table_status



