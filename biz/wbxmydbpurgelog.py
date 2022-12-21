import logging
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
import pymysql
from datetime import datetime
import time

logger = logging.getLogger("DBAMONITOR")

def wbxmydbpurgelog(mydbname,mydbschema):
    res = {
        "status": "SUCCEED",
        "data": None
    }
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    opdtDaoManager = daoManagerFactory.getOPDBDaoManager()
    depotdbDao = opdtDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        opdtDaoManager.startTransaction()
        row = depotdbDao.getpurgemydbinfo(mydbname,mydbschema)
        opdtDaoManager.commit()
    except Exception as e:
        logger.error("Error occurred in getpurgemydbinfo:%s" %str(e) )
        opdtDaoManager.rollback()
        res["status"] = "FAILED"
        res["data"] = str(e)
    finally:
        opdtDaoManager.close()

    defaultDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)

    try:
        defaultDaoManager.startTransaction()
        depotdbDao.InitWbxMySqlDBPurgeStatus(mydbname,mydbschema)
        defaultDaoManager.commit()
    except Exception as e:
        logger.error("Error occurred in InitWbxMySqlDBPurgeStatus:%s" %str(e) )
        defaultDaoManager.rollback()
        res["status"] = "FAILED"
        res["data"] = str(e)
    finally:
        defaultDaoManager.close()

    hostname = row[0]
    port = int(row[1])
    username = row[2]
    passwd = row[3]
    dbschema = row[4]
    kargs = {"hostname": hostname, "port": port, "username": username, "passwd": passwd, "dbschema": dbschema}
    mydbret = runOnEachDBSchema(**kargs)


    try:
        defaultDaoManager.startTransaction()
        depotdbDao.updmydbpurgestatus(mydbname,mydbschema,**mydbret)
        defaultDaoManager.commit()
    except Exception as e:
        logger.error("Error occurred in updmydbpurgestatus:%s" %str(e) )
        defaultDaoManager.rollback()
        res["status"] = "FAILED"
        res["data"] = str(e)
    finally:
        defaultDaoManager.close()

    return res

def runOnEachDBSchema(**kargs):
    status="SUCCESS"
    flag=True
    dnum = 200000
    drows=0
    errormsg=""
    hostname = kargs["hostname"]
    port = int(kargs["port"])
    username = kargs["username"]
    passwd = kargs["passwd"]
    dbschema =kargs["dbschema"]
    res={}
    start = time.time()
    try:
        db = pymysql.connect(host=hostname,
                             port=port,
                             user=username, passwd=passwd, db=dbschema)
    except Exception as e:
        errormsg=str(e)
        status = "FAILED"

    if db:
        # sql = "select 1 "
        sql = "delete from wbxmeetinglog where INTFFLAG='C' and GWINTFFLAG='C' and TIMESTAMP < now()-interval 6 hour limit %s" % dnum
        try:
            cursor = db.cursor()
            while flag:
                cnt = cursor.execute(sql)
                drows = drows + cnt
                if cnt < dnum:
                    flag = False
            sql = "OPTIMIZE table wbxmeetinglog"
            # print('%s ++++++ %s'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),sql))
            cursor.execute(sql)
            # print('%s ++++++ end' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        except Exception as e:
            errormsg = str(e)
            status = "FAILED"
        finally:
            cursor.close()
            db.close()
        end = time.time()
        Duration = (end - start)  # sec
        res["status"]=status
        res["drows"]=drows
        res["errormsg"]=errormsg
        res["Duration"]=Duration
    return res

def getmydbdellog(mydbname=None,mydbschema=None):
    res = {
        "status": "SUCCEED",
        "data": None
    }
    data = {}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    defaultDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        defaultDaoManager.startTransaction()

        rows = depotdbDao.getmydbpurgeslog(mydbname, mydbschema)
        if mydbname and mydbschema:
            datalist=[{"datadate": row[0],
                      "drows": row[1],
                       "duration": row[2],
                      } for row in rows]
            data["datalist"] = datalist
            data["mydbname"] = mydbname
            data["mydbschema"] = mydbschema
            print("get history log")
        else:
            datalist = [{"mydbname": row[0],
                         "dcname": row[1],
                         "mydbschema": row[2],
                         "mydbip": row[3],
                         "status": row[4],
                         "duration": str(row[5]),
                         "drows": str(row[6]),
                         "errormsg": row[7],
                         "starttime": row[8],
                         "endtime": row[9],
                         } for row in rows]
            data["datalist"] = datalist
            print("get current log")
        res["data"]=data
        defaultDaoManager.commit()
    except Exception as e:
        logger.error("Error occurred in getmydbpurgelog:%s" %str(e))
        defaultDaoManager.rollback()
        res["status"] = "FAILED"
    finally:
        defaultDaoManager.close()
    return res