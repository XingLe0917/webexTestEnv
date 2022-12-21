from biz.dbmanagement.wbxdbserver import wbxdbserver
from sqlalchemy import Table, Column, MetaData, String, DateTime, Integer, and_, create_engine, func
from sqlalchemy.exc import  DBAPIError, DatabaseError

# instance can not inherit from wbxdbserver, because one db server may have multiple dbs
class wbxdbinstance:
    def __init__(self, db, instance_name, dbserver):
        self._db = db
        self._instance_name = instance_name
        self._dbserver = dbserver

    def getInstanceName(self):
        return self._instance_name

    def getServer(self):
        return self._dbserver

    def getDatabase(self):
        return self._db
