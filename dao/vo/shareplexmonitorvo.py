from sqlalchemy import Column, String, BigInteger, Integer, DateTime, func, literal_column, text

from dao.vo.wbxvo import Base

class ShareplexRoutingVO(Base):
    __tablename__ = "shareplex_trans"
    # __tablename__ = "shareplex_trans"
    trans_num = Column(BigInteger, primary_key=True)
    scn = Column(BigInteger, primary_key=True)
    routings=literal_column("substr(combo,1,instr(combo,':')-1)", String(256))
    # schema = "splex19063"
