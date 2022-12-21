from sqlalchemy import Column, Integer,String, DateTime, and_, ForeignKey, literal_column, func
from sqlalchemy.orm import relationship
from dao.vo.wbxvo import Base

class WebDomainVO(Base):
    __tablename__ = "wbxwebdomain"
    domainid = Column(Integer, primary_key=True)
    domainname = Column(String(64))
    active = Column(Integer)
    confidsegment = Column(Integer)
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())
    itemlist = relationship("WebDomainConfigVO")

    def getItemValue(self, itemname):
        for item in self.itemlist:
            if item.itemname == itemname:
                return item.itemvalue
        return None

class WebDomainConfigVO(Base):
    __tablename__ = "wbxwebdomainconfig"
    domainid = Column(Integer, ForeignKey('wbxwebdomain.domainid'), primary_key=True)
    itemname = Column(String(32), primary_key=True)
    blockorder = Column(Integer)
    itemvalue = Column(String(4000))
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())

