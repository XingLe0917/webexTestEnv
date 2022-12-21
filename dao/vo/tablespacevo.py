from sqlalchemy import Column,Integer, BigInteger,String, column
from dao.vo.wbxvo import Base


class SegmentVO(Base):
    __tablename__ = "dba_segments"
    tablespace_name = Column(String(100))
    owner = Column(String(30), primary_key=True)
    segment_name = Column(String(30), primary_key=True)
    bytes = Column(Integer)

class DatafileVO(Base):
    __tablename__ = "dba_data_files"
    tablespace_name = Column(String(100), primary_key=True)
    maxbytes = Column(Integer)
    user_bytes = Column(Integer)
