from sqlalchemy import Column, Integer,String, DateTime, and_, ForeignKey, literal_column
from sqlalchemy.orm import relationship
from dao.vo.wbxvo import Base

class AuditVO(Base):
    __tablename__ = "dba_audit_trail"
    os_username = Column(String(30))
    username = Column(String(30), primary_key=True)
    userhost = Column(String(1024), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)
    returncode = Column(Integer)



class DBProcessVO(Base):
    __tablename__ = "gv$process"
    addr = Column(String, primary_key=True)
    inst_id = Column(Integer)
    pid = Column(Integer)
    spid = Column(Integer)

    sessionvo = relationship("DBSessionVO", back_populates="processvo")


class DBSessionVO(Base):
    __tablename__ = "gv$session"
    inst_id = Column(Integer, primary_key=True)
    sid = Column(Integer, primary_key=True)
    serial_id = literal_column('serial#', Integer)
    audsid = Column(Integer)
    paddr = Column(String, ForeignKey('gv$process.addr'))
    status = Column(String)
    osuser = Column(String)
    username = Column(String)
    program = Column(String)
    event = Column(String)
    state = Column(String)
    sql_id = Column(String)
    p1 =Column(Integer)
    p2 = Column(Integer)
    p3 = Column(Integer)

    processvo = relationship("DBProcessVO", uselist=False, back_populates="sessionvo")

class DBLockVO(Base):
    __tablename__ = "gv$lock"
    inst_id = Column(Integer)
    sid = Column(Integer)
    addr = Column(String, primary_key=True)
    type = Column(String)
    id1 = Column(Integer)
    id2 = Column(Integer)
    lmode = Column(Integer)
    request = Column(Integer)
    ctime = Column(Integer)
    block = Column(Integer)

class DBATableVO(Base):
    __tablename__ = "dba_tables"
    owner = Column(String(30), primary_key=True)
    table_name = Column(String(30), primary_key=True)
    tablespace_name = Column(String(30))
    num_rows = Column(Integer)


class NLSParameterVO(Base):
    __tablename__ = "nls_database_parameters"
    parameter = Column(String(128), primary_key=True)
    value = Column(String(128))


class wbxdatabaseversionvo(Base):
    __tablename__ = "wbxdatabaseversion"
    release_number = Column(Integer, primary_key=True)
    major_number = Column(Integer)
    minor_number = Column(Integer)
    dbtype = Column(String(56))
    description = Column(String(512))
    __table_args__ = {'schema': 'test'}







