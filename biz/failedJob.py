import json
import logging
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from dao.vo.cronjobmanagementvo import JobManagerInstanceVO, JobTemplateVO, JobInstanceVO
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from sqlalchemy.exc import IntegrityError
from common.wbxssh import wbxssh
import cx_Oracle
from common.Config import Config
from sqlalchemy.exc import  DBAPIError, DatabaseError
import time

logger = logging.getLogger("DBAMONITOR")

failed_job_tablelist = ''
def get_failed_job_tablelist():
    global failed_job_tablelist
    status = "SUCCEED"
    errormsg = ""
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        failed_job_tablelist = depotdbDao.getFailedJobList()
        if not failed_job_tablelist:
            status = "FAIL"
            errormsg = "No Data"
        # parameter = json.dumps(kwargs)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAIL"
    finally:
        depotDaoManager.close()

    return {"failed_job_tablelist": failed_job_tablelist,
            "status": status,
            "errormsg": errormsg}

connect = ''
def execute_failed_job(jsondata):
    global connect
    # print(jsondata)
    status = "SUCCEED"
    errormsg = ""
    sp = None
    c = wbxFailedJobLog(jsondata)
    config = Config.getConfig()
    schemaname, schemapwd, connectionurl = config.getDepotConnectionurl()
    connectionurl = "%s/%s@%s"% (schemaname, schemapwd, connectionurl)
    print('----',c.get_last_run_time(),c.get_next_run_time(),jsondata["JOBID"])
    try:
        connect = cx_Oracle.connect(connectionurl)
        cursor = connect.cursor()
        SQL = "UPDATE wbxjobinstance SET status='RUNNING', last_run_time=to_date('"+c.get_last_run_time()+"','YYYY-MM-DD hh24:mi:ss'), next_run_time=to_date('"+str(c.get_next_run_time())+"','YYYY-MM-DD hh24:mi:ss') WHERE jobid="+"'"+str(jsondata["JOBID"])+"'"
        # SQL ="rollback;"
        print('sql-----', SQL)
        cursor.execute(SQL)
        connect.commit()
    except Exception as e:
        logger.error(str(e))
        logger.error("start job failed with jobid=%s" % jsondata["JOBID"], exc_info=e)
    try:
        sp = wbxFailedJob.newInstance(jsondata)
        sp.login()
        sp.start_script(jsondata)

    except Exception as e:
        errormsg = str(e)
        status = "FAIL"
    try:
        cursor = connect.cursor()
        SQL = ''' UPDATE wbxjobinstance SET status=:status, errormsg=:errormsg WHERE jobid=:jobid and status not in ('PAUSE','DELETED') '''
        paramdict = {"status": c.getStatus(errormsg), "jobid": jsondata["JOBID"], "errormsg": errormsg}
        cursor.execute(SQL, paramdict)
        connect.commit()
    except Exception as e:
        logger.error(str(e))
        logger.error("update failed with jobid=%s" % jsondata["JOBID"], exc_info=e)

    finally:
        if sp is not None:
            sp.close()
    return {
        "status": status,
        "errormsg": errormsg
    }

def skip_failed_job(jsondata):
    # print(jsondata)
    status = "SUCCEED"
    errormsg = ""
    sp = None
    c = wbxFailedJobLog(jsondata)
    config = Config.getConfig()
    schemaname, schemapwd, connectionurl = config.getDepotConnectionurl()
    connectionurl = "%s/%s@%s"% (schemaname, schemapwd, connectionurl)
    print('----',c.get_last_run_time(),c.get_next_run_time(),jsondata["JOBID"])
    try:
        connect = cx_Oracle.connect(connectionurl)
        cursor = connect.cursor()
        SQL = "UPDATE wbxjobinstance SET status='RUNNING', last_run_time=to_date('"+c.get_last_run_time()+"','YYYY-MM-DD hh24:mi:ss'), next_run_time=to_date('"+str(c.get_next_run_time())+"','YYYY-MM-DD hh24:mi:ss') WHERE jobid="+"'"+str(jsondata["JOBID"])+"'"
        # SQL ="rollback;"
        print('sql-----', SQL)
        cursor.execute(SQL)
        connect.commit()
    except Exception as e:
        logger.error(str(e))
        logger.error("start job failed with jobid=%s" % jsondata["JOBID"], exc_info=e)

    try:
        cursor = connect.cursor()
        SQL = ''' UPDATE wbxjobinstance SET status=:status, errormsg=:errormsg WHERE jobid=:jobid and status not in ('PAUSE','DELETED') '''
        paramdict = {"status": 'SKIP', "jobid": jsondata["JOBID"], "errormsg": errormsg}
        cursor.execute(SQL, paramdict)
        connect.commit()
    except Exception as e:
        logger.error(str(e))
        logger.error("update failed with jobid=%s" % jsondata["JOBID"], exc_info=e)

    finally:
        if sp is not None:
            sp.close()
    return {
        "status": status,
        "errormsg": errormsg
    }

class wbxFailedJob:
    def __init__(self, server):
        self.host_name = server.host_name
        self.server = server

    def close(self):
        self.server.close()

    def login(self):
        self.server.connect()

    @staticmethod
    def newInstance(jsondata):
        if not jsondata["HOST_NAME"]:
            raise wbxexception("host_name can not be null")
        host_name = jsondata["HOST_NAME"].split(".")[0]

        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daoManagerFactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            depotDaoManager.startTransaction()
            ssh_pwd = depotdbDao.getOracleUserPwdByHostname(host_name)
            site_code = depotdbDao.getSitecodeByHostname(host_name)
            depotDaoManager.commit()
        except DatabaseError as e:
            logger.error("getUserPasswordByHostname getSitecodeByHostname met error %s" % e)
            raise wbxexception(
                "Error ocurred when get oracle user password and sitecode on the server %s in DepotDB with msg %s" % (
                    host_name, e))
        if wbxutil.isNoneString(ssh_pwd) or wbxutil.isNoneString(site_code):
            raise wbxexception(
                "Can not get oracle user password and site_code on the server %s in DepotDB" % host_name)

        servervo = daoManagerFactory.getServer(host_name)
        if servervo is None:
            raise wbxexception("can not get server info with hostname %s" % host_name)
        ssh_port = servervo.ssh_port
        server = wbxssh(host_name, ssh_port, "oracle", ssh_pwd)
        try:
            server.connect()
        except Exception as e:
            raise wbxexception("cannot login the server %s with password in depot" % host_name)
        sp = wbxFailedJob(server)
        return sp

    def start_script(self,jsondata):
        # print('----',jsondata["COMMENDSTR"],type(jsondata))
        cmd = "sh "+jsondata["COMMENDSTR"]
        self.server.exec_command(cmd)

class wbxFailedJobLog:
    def __init__(self, jsondata):
        self.commend_str = jsondata["CURRENTTIME"]
        self.last_run_time = jsondata["LAST_RUN_TIME"]
        self.next_run_time = jsondata["NEXT_RUN_TIME"]
        self.jobid = jsondata["JOBID"]
        self.jobruntime = jsondata['JOBRUNTIME']

    def get_last_run_time(self):
        # return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return wbxutil.gettimestr()

    def get_next_run_time(self):
        # print( "select to_date(pkg_jenkins_cron.fn_getnexttimeafter('"+self.next_run_time+"'),'yyyy-mm-dd hh24:mi:ss') from dual")
        config = Config.getConfig()
        schemaname, schemapwd, connectionurl = config.getDepotConnectionurl()
        connectionurl = "%s/%s@%s" % (schemaname, schemapwd, connectionurl)
        db = cx_Oracle.Connection(connectionurl)
        cursor = db.cursor()
        time = ''
        # print(type(self.jobruntime))
        if isinstance(self.jobruntime, str):
           self.jobruntime = eval(self.jobruntime)

        if 'minute' in self.jobruntime:
            time = str(self.jobruntime['minute'])
        else:
            time = '*'
        if 'hour' in self.jobruntime:
            time = time + ' '+ str(self.jobruntime['hour'])
        else:
            time = time + ' *'
        if 'day' in self.jobruntime:
            time = time + ' ' + str(self.jobruntime['day'])
        else:
            time = time + ' *'
        if 'month' in self.jobruntime:
            time = time + ' '+ str(self.jobruntime['month'])
        else:
            time = time + ' *'
        if 'day_of_week' in self.jobruntime:
            time = time + ' ' + str((int(self.jobruntime['day_of_week'])+1)%7)
        else:
            time  = time + ' *'

        print('time------',time)
        try:
            sql = "select to_date(pkg_jenkins_cron.fn_getnexttimeafter('"+time+"'),'yyyy-mm-dd hh24:mi:ss') from dual"
            cursor.execute(sql)
            rows = cursor.fetchall()
            row = rows[0]
            print(type(row[0]))
        finally:
            cursor.close()
            db.close()
        return row[0]

    def getStatus(self, errormsg):
        if errormsg:
            return 'FAILED'
        else:
            return 'SUCCEED'

    def getJobid(self):
        return self.jobid









