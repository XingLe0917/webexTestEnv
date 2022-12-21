import os
from common.Config import Config
import base64
import cx_Oracle
import logging
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from common.wbxcache import curcache
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from biz.dbmanagement.wbxdb import wbxdb
from sqlalchemy import Table, Column, MetaData, String, DateTime, Integer, and_, create_engine
from sqlalchemy.exc import  DBAPIError, DatabaseError
from sqlalchemy.pool import NullPool
from biz.getserveroraclepwd import get_db_metadata

logger = logging.getLogger("DBAMONITOR")


def get_jira_issue_list():
    status = "SUCCESS"
    rst = []
    errormsg = ""
    try:
        tns = get_db_metadata("db", "stapdb")
        rst, rst_bool = ConnDb(tns).get_issue_list()
        if not rst_bool:
            raise wbxexception(rst)
    except Exception as e:
        errormsg = "get_issue_list failed with errormsg %s" % (str(e))
        logger.error(errormsg)
        status = "FAIL"

    return {
        "status": status,
        "errormsg": errormsg,
        "data": rst
    }


def get_to_do_jira_issue_list():
    status = "SUCCESS"
    rst = []
    errormsg = ""
    try:
        tns = get_db_metadata("db", "stapdb")
        rst, rst_bool = ConnDb(tns).get_issue_list()
        if not rst_bool:
            raise wbxexception(rst)
    except Exception as e:
        errormsg = "get_issue_list failed with errormsg %s" % (str(e))
        logger.error(errormsg)
        status = "FAIL"

    return {
        "status": status,
        "errormsg": errormsg,
        "data": rst
    }

class ConnDb(object):
    def __init__(self, tns):
        self._sourcedb_usr = "system"
        self._sourcedb_pwd = b"c3lzbm90YWxsb3c="
        self._tns = """(DESCRIPTION =
(ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.9.199)(PORT = 1701))
(ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.9.197)(PORT = 1701))
(ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.9.198)(PORT = 1701))
(LOAD_BALANCE = yes)
(FAILOVER = on)
(CONNECT_DATA =
(SERVER = DEDICATED)
(SERVICE_NAME = stapdbha.webex.com)
(FAILOVER_MODE =
(TYPE = SELECT)
(METHOD = BASIC)
(RETRIES = 3)
(DELAY = 5)
)
)
)"""
        self.cursor = None

    def conn_source_db(self):
        if self.cursor:
            return self.cursor
        conn = cx_Oracle.connect("%s/%s@%s" % (self._sourcedb_usr, str(base64.b64decode(self._sourcedb_pwd), "utf-8"), self._tns))
        self.cursor = conn.cursor()

    def disconn_source_db(self, cursor):
        cursor.close()

    def get_issue_list(self):
        self.conn_source_db()
        sql = """
select changeid, env, summary, taskimplementer, schstartdate, statestage, UUID, INPROGRESS from stapuser.STAP_CHANGEDBINFO 
where implementergroup='Production DBA' 
and statestage in ('Completed','In Progress','Scheduled')
and to_date(ACTENDDATE, 'YYYY-MM-DD hh24:mi:ss') > sysdate - 2 and to_date(ACTENDDATE, 'YYYY-MM-DD hh24:mi:ss') > sysdate - 1
and summary like '%RLSE%'
order by actenddate desc
        """
        try:# "and a.host_name in ('tadbth351', 'tadbormt030', 'sjdbth352', 'sydbor13', 'sydbor14', 'sjdbwbf1', 'tadbwbf1', 'sjdbth351', 'sydbor11', 'tadbth352')"
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
        except Exception as e:
            print(str(e))
            return str(e), False
        self.disconn_source_db(self.cursor)
        rst = []
        for row in rows:
            rst.append({
            "changeid": row[0],
            "env": row[1],
            "summary": row[2],
            "implementer": row[3],
            "schstartdate": row[4],
            "statestage": row[5],
            "uuid": row[6],
            "inprogress": row[7]
            })
        return rst, True  # 2 types:List: server name; Boolen: False


