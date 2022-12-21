import datetime

from dao.vo.wbxvo import Base
from sqlalchemy import Column, Integer,String, DateTime, and_, ForeignKey, literal_column, text, func

class AlertVo(Base):
    __tablename__ = "kafka_alert"
    id = Column(String(64), primary_key=True)
    metric_type = Column(String(200))
    metric_name = Column(String(200))
    metric_operator = Column(String(16))
    threshold_value = Column(String(200))
    severity = Column(String(2))
    db_host = Column(String(200))
    db_name = Column(String(200))
    shareplex_port = Column(String(200))
    threshold_times = Column(String(20))
    createtime = Column(DateTime,default=datetime.datetime.now)
    lastmodifiedtime = Column(DateTime,default=datetime.datetime.now,onupdate=datetime.datetime.now)