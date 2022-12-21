from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TypeDecorator, VARCHAR
from common.wbxutil import wbxutil
from datetime import datetime
import json

Base = declarative_base()

class JSONEncodedDict(TypeDecorator):
    """Representes an immutable structure as a json-encoded string"""
    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        if value:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value:
            value = json.loads(value)
        return value or dict()

def to_dict(self):
    objdict = {}
    for c in self.__table__.columns:
        val = getattr(self, c.name, None)
        if isinstance(val, dict):
            for k,v in val.items():
                objdict[k]=v
        else:
            if isinstance(val, datetime):
                val = wbxutil.convertDatetimeToString(val)
            objdict[c.name] = val
    return objdict
    # return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}

@classmethod
def loadFromJson(cls, dict):
    obj = cls()
    for c in obj.__table__.columns:
        if c.name in dict:
            val = dict[c.name]
            setattr(obj, c.name, val)
    return obj

Base.to_dict = to_dict
Base.loadFromJson = loadFromJson