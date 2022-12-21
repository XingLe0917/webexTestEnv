from sqlalchemy import Column, Integer, Sequence, String
from dao.vo import wbxvo


class uservo(wbxvo):
    __tablename__ = "testuser"

    userid = Column(Integer, Sequence("seq_testuser_userid"), primary_key=True)
    username = Column(String(100))

