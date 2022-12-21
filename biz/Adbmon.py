import datetime
import logging
import re
import time

from cx_Oracle import DatabaseError
from dateutil.parser import parse
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from biz.TaskLog import get_db_tns
from common.Config import Config
from common.wbxutil import wbxutil
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

def getadbmonlist(src_db,tgt_db,port,db_type):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.getadbmonlist(str(src_db).upper(),str(tgt_db).upper(),port,db_type)
        # list_temp = depotdbDao.getadbmonlistAlertForTemp()
        # new_list = list+list_temp
        daoManager.commit()
        return [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        logger.error("getadbmonlist error occurred", exc_info=e, stack_info=True)
    finally:
        daoManager.close()
    return None

def adbmon_check_new(port,src_db,src_host,tgt_db,tgt_host,replication_to,env):
    logger.info(
        "adbmoncheck, port={0},src_db={1},src_host={2},tgt_db={3},tgt_host={4},replication_to{5},env={6}".format(port, src_db,
                                                                                                         src_host,
                                                                                                         tgt_db,
                                                                                                         tgt_host,
                                                                                                         replication_to,env))
    if "china" == env:
        return adbmoncheck2(port,src_db,src_host,tgt_db,tgt_host,replication_to)
        # return adbmoncheck_china(port,src_db,src_host,tgt_db,tgt_host,replication_to)
    else:
        return adbmoncheck_global(port,src_db,src_host,tgt_db,tgt_host,replication_to)

def adbmoncheck_china(port,src_db,src_host,tgt_db,tgt_host,replication_to):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    trim_host_src = ''
    trim_host_tgt = ''
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        ls = depotdbDao.getTrimhost(src_host,tgt_host)
        qnames = depotdbDao.getQnames(port,src_db,src_host,tgt_db,tgt_host)
        qname = ""
        for vo in qnames:
            qname=vo['qname']
            if vo['replication_to'] == replication_to:
                qname = vo['qname']
                break
        daoManager.commit()
        for vo in ls:
            item = dict(vo)
            if(item['host_name'] == src_host):
                trim_host_src = item['trim_host']
            if(item['host_name'] == tgt_host):
                trim_host_tgt = item['trim_host']
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = "find trim host fail"
        logger.error("find trim host fail", exc_info=e, stack_info=True)
        logger.info(res)
        return res

    logger.info("trim_host_src:" + trim_host_src)
    logger.info("trim_host_tgt:" + trim_host_tgt)
    schemaname = "splex"+str(port)
    table_name = "splex_monitor_adb"
    if qname:
        table_name += "_"+qname
    dbid = "%s" % (src_db)
    logger.info("src bdid:" + dbid)
    daoManager1 = None
    try:
        daoManager1 = daoManagerFactory.getDaoManager(dbid)
    except Exception as e:
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    try:
        daoManager1.startTransaction()
        spDao = daoManager1.getDao(DaoKeys.DAO_SHAREPLEXMONITORDAO)
        vo = spDao.getsplexmonitoradbdata1(schemaname,table_name, replication_to,trim_host_src,src_db)  # updatesplexmonitoradbdata1(schemaname,table_name, replication_to,trim_host_src,src_db,None)
        print("select splexmonitoradbdata done ,num:{0}".format(vo))
        if len(vo)==0:
            a = spDao.insertsplexmonitoradbdata1(schemaname, table_name, replication_to, trim_host_src, src_db,port,"")
            print("insert splexmonitoradbdata done ,num:{0}".format(a))
        else:
            a = spDao.updatesplexmonitoradbdata1(schemaname,table_name, replication_to,trim_host_src,src_db,None)
            print("update splexmonitoradbdata done ,num:{0}".format(a))
        # vo2 = spDao.deletesplexmonitoradbdata1(schemaname, table_name, replication_to, trim_host_src, src_db)
        # print("delete splexmonitoradbdata done ,num:{0}".format(vo2))
        # a = spDao.insertsplexmonitoradbdata1(schemaname, table_name, replication_to, trim_host_src, src_db, port)
        # print("insert splexmonitoradbdata done ,num:{0}".format(a))
        daoManager1.commit()
    except Exception as e:
        daoManager1.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = "update/insert src splex_monitor_adb fail"
        logger.error("update/insert src splex_monitor_adb fail", exc_info=e, stack_info=True)
        return res

    dbid = "%s" % (tgt_db)
    logger.info("tgt bdid:" + dbid)
    daoManager2 = None
    try:
        daoManager2 = daoManagerFactory.getDaoManager(dbid)
    except Exception as e:
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    logtime = None
    try:
        daoManagerFactory.getDaoManager(dbid)
        daoManager2.startTransaction()
        spDao = daoManager2.getDao(DaoKeys.DAO_SHAREPLEXMONITORDAO)
        time = spDao.getsplexmonitoradbdata1(schemaname,table_name, replication_to,trim_host_src,src_db)
        logger.info(time)
        if time:
            logtime = time[0][0]
        else:
            logging.debug("no logtime and set logtime=1970-01-01")
            logtime = datetime.datetime.strptime("1970-01-01", '%Y-%m-%d')
        logger.info(logtime)
        daoManager2.commit()
    except Exception as e:
        daoManager2.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = "query tgt splex_monitor_adb fail"
        logger.error("query tgt splex_monitor_adb fail", exc_info=e, stack_info=True)
        return res

    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        logger.info("updatewbxadbmon")
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        depotdbDao.updatewbxadbmon(port,src_db,src_host,tgt_db,tgt_host,replication_to,logtime)
        daoManager.commit()
        ls = depotdbDao.getadbmonOne(port,src_db,src_host,tgt_db,tgt_host,replication_to)
        daoManager.commit()
        item = dict(ls[0])
        lastreptime = parse(item['lastreptime'])
        montime = parse(item['montime'])
        times = (montime - lastreptime).seconds
        m, s = divmod(times, 60)
        h, m = divmod(m, 60)
        d = (montime - lastreptime).days
        item['lag_by'] = str(d) + ":" + str(h) + ":" + str(m)
        res["data"] = item
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = "update wbxadbmon fail"
        logger.error("update wbxadbmon fail", exc_info=e, stack_info=True)
    finally:
        daoManager.close()
        daoManager1.close()
        daoManager2.close()
    logger.info(res)
    return res

def adbmondetail(port,src_db,src_host,tgt_db,tgt_host,replication_to):
    config = Config.getConfig()
    influxDBclient = config.getInfluxDB_SJC_client()
    res_list = []
    try:
        sql = '''
             SELECT * FROM wbxadbmon_history
             where port='%s' and src_db='%s' and src_host='%s' and tgt_db='%s' and tgt_host='%s' and replication_to='%s'
             and time > now() - 7d
            ''' % (port, src_db, src_host, tgt_db, tgt_host, replication_to)
        results = influxDBclient.query(sql)
        points = results.get_points()
        for data in points:
            res = {}
            res['src_db'] = data['src_db']
            res['src_host'] = data['src_host']
            res['port'] = data['port']
            res['tgt_db'] = data['tgt_db']
            res['tgt_host'] = data['tgt_host']
            logtime = str(data['logtime']).split(".")[0]
            time = str(data['time']).split(".")[0]

            lastreptime = re.match(r'(.*)T(.*)Z', logtime).group(1) + " " + re.match(r'(.*)T(.*)Z', logtime).group(2)
            montime = re.match(r'(.*)T(.*)', time).group(1) + " " + re.match(r'(.*)T(.*)', time).group(2)
            res['lastreptime'] = lastreptime
            res['montime'] = montime
            diff = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S") - datetime.datetime.strptime(logtime,
                                                                                                      "%Y-%m-%dT%H:%M:%SZ")
            res['diff_seconds'] = diff.seconds
            minutes = divmod(diff.seconds, 60)[0]
            res['diff_min'] = minutes
            res_list.append(res)
        return res_list
    except Exception as e:
        logger.error("adbmondetail error occurred", exc_info=e, stack_info=True)



# def adbmondetail(port,src_db,src_host,tgt_db,tgt_host,replication_to):
#     daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
#     daoManager = daoManagerFactory.getDefaultDaoManager()
#     try:
#         daoManager.startTransaction()
#         depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
#         list = depotdbDao.adbmondetail(port,src_db,src_host,tgt_db,tgt_host,replication_to)
#         daoManager.commit()
#         return [dict(vo) for vo in list]
#     except Exception as e:
#         daoManager.rollback()
#         logger.error("adbmondetail error occurred", exc_info=e, stack_info=True)
#     return None

def getadbmonlistAlert(src_db,tgt_db,port,db_type):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.getadbmonlistAlert(str(src_db).upper(),str(tgt_db).upper(),port,db_type)
        daoManager.commit()
        return [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        logger.error("getadbmonlistAlert error occurred", exc_info=e, stack_info=True)
    finally:
        daoManager.close()
    return None

def getAdbmonlistByDCName(dc_name,delay_min):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    logger.info("getAdbmonlistByDCName,dc_name={0},delay_min={1}".format(dc_name,delay_min))
    try:
        delay_min = int(delay_min)
    except Exception as e:
        logger.error("getAdbmonlistByDCName error occurred", exc_info=e, stack_info=True)
        res['errormsg']= "parameter delay_min must be a positive integer"
        return res
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.get_adbmon_list_DCName(dc_name,delay_min)
        daoManager.commit()
        res['data'] = [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        logger.error("getAdbmonlistByDCName error occurred", exc_info=e, stack_info=True)
    finally:
        daoManager.close()
    return res

def adbmonClean(port,src_db,src_host,tgt_db,tgt_host,replication_to):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    logger.info(
        "adbmonClean, port={0},src_db={1},src_host={2},tgt_db={3},tgt_host={4},replication_to{5}".format(port, src_db,
                                                                                                         src_host,
                                                                                                         tgt_db,
                                                                                                         tgt_host,
                                                                                                         replication_to))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    src_trim_host = ''
    tgt_trim_host = ''
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        ls = depotdbDao.getTrimhost(src_host, tgt_host)
        qnames = depotdbDao.getQnames(port, src_db, src_host, tgt_db, tgt_host)
        qname = ""
        for vo in qnames:
            qname = vo['qname']
            if vo['replication_to'] == replication_to:
                qname = vo['qname']
                break
        daoManager.commit()
        for vo in ls:
            item = dict(vo)
            if (item['host_name'] == src_host):
                src_trim_host = item['trim_host']
            if (item['host_name'] == tgt_host):
                tgt_trim_host = item['trim_host']
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = "find trim host fail"
        logger.error("find trim host fail", exc_info=e, stack_info=True)
        return res

    logger.info("src_trim_host:" + src_trim_host)
    logger.info("tgt_trim_host:" + tgt_trim_host)
    schemaname = "splex" + str(port)
    table_name = "splex_monitor_adb"
    if qname:
        table_name += "_" + qname
    dbid = "%s" % (src_db)
    logger.info("src bdid:" + dbid)
    daoManager1 = None
    try:
        daoManager1 = daoManagerFactory.getDaoManager(dbid)
    except Exception as e:
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    try:
        daoManager1.startTransaction()
        spDao = daoManager1.getDao(DaoKeys.DAO_SHAREPLEXMONITORDAO)
        vo = spDao.delete_splex_monitor_adb(schemaname, table_name, replication_to, src_trim_host, src_db)
        logger.info("delete src %s.%s done ,count:%s" %(schemaname,table_name,vo))
        daoManager1.commit()
    except Exception as e:
        daoManager1.rollback()
        res["status"] = "FAILED"
        errormsg = "delete src %s.%s fail, e=%s" %(schemaname,table_name,str(e))
        res["errormsg"] = errormsg
        logger.error(errormsg, exc_info=e, stack_info=True)
        return res

    dbid = "%s" % (tgt_db)
    logger.info("tgt bdid:" + dbid)
    daoManager2 = None
    try:
        daoManager2 = daoManagerFactory.getDaoManager(dbid)
    except Exception as e:
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    logtime = None
    try:
        daoManagerFactory.getDaoManager(dbid)
        daoManager2.startTransaction()
        spDao = daoManager2.getDao(DaoKeys.DAO_SHAREPLEXMONITORDAO)
        tgt_vo = spDao.delete_splex_monitor_adb(schemaname, table_name, replication_to, src_trim_host, src_db)
        logger.info("delete tgt %s.%s done ,count:%s" % (schemaname, table_name, tgt_vo))
        daoManager2.commit()
    except Exception as e:
        daoManager2.rollback()
        res["status"] = "FAILED"
        errormsg = "delete tgt %s.%s fail, e=%s" % (schemaname, table_name, str(e))
        res["errormsg"] = errormsg
        logger.error(errormsg, exc_info=e, stack_info=True)
        return res
    return res

def adbmoncheck_global(port,src_db,src_host,tgt_db,tgt_host,replication_to):
    logger.info("adbmoncheck")
    host_name = "sjdbormt065"
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    server = None
    try:
        server = daoManagerFactory.getServer(host_name)
        logger.info(server)
        if server is not None:
            server.connect()
            params = "{0}-{1}-{2}-{3}-{4}-{5}" .format(src_db,tgt_db,port,src_host,tgt_host,replication_to)
            logger.info(host_name)
            cmd = ". /home/oracle/.bash_profile;python3 /home/oracle/project/wbxdbaudit/dbaudit.py SHAREPLEXADBMONCHECK_JOB %s" % (params)
            logger.info(cmd)
            result = server.exec_command(cmd)
            logger.info(result)
            res = get_adbMonOne(port, src_db, src_host, tgt_db, tgt_host, replication_to, None)
            logger.info(res)
            return res
        else:
            logger.error("getServer None, host_name={0}".format(host_name))
            return None
    except Exception as e:
        logger.error(str(e))
        raise e
    finally:
        if server is not None:
            server.close()

def get_adbMonOne(port, src_db, src_host, tgt_db, tgt_host, replication_to):
    logger.info("get_adbMonOne")
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        ls = depotdbDao.getadbmonOne(port, src_db, src_host, tgt_db, tgt_host, replication_to)
        daoManager.commit()
        item = dict(ls[0])
        lastreptime = parse(item['lastreptime'])
        montime = parse(item['montime'])
        times = (montime - lastreptime).seconds
        m, s = divmod(times, 60)
        h, m = divmod(m, 60)
        d = (montime - lastreptime).days
        item['lag_by'] = str(d) + ":" + str(h) + ":" + str(m)
        res["data"] = item
        return res
    except Exception as e:
        daoManager.rollback()
        logger.error("get_adbMonOne error occurred", exc_info=e, stack_info=True)



# 1. Test channel status
# 2. If channel status is ok, get src and tgt abdmon data, otherwise ignore
# 3. According to the above 4 cases
#    case 1 : src True,  tgt False  -> sync data from sourcedb
#    case 2 : src True,  tgt True   -> sync data from sourcedb
#    case 3 : src False, tgt True   -> delete tgt data, go to case 4
#    case 4 : src False, tgt False  -> insert data into sourcedb ; query from targetdb; update wbxadbmon table in depotdb
def adbmoncheck2(port,src_db,src_host,tgt_db,tgt_host,replication_to):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    logger.info(
        "Call api adbmoncheck, port={0},src_db={1},src_host={2},tgt_db={3},tgt_host={4},replication_to={5}".format(port, src_db,
                                                                                                         src_host,
                                                                                                         tgt_db,
                                                                                                         tgt_host,
                                                                                                         replication_to))
    # test channel status
    try:
        channel_status = test_channel(port, src_db, src_host, tgt_db, tgt_host,replication_to)
        logger.info("Test channel status done, channel_status={0}".format(channel_status))
    except Exception as e:
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        logger.error(str(e), exc_info=e, stack_info=True)
        return res

    if not channel_status:
        res = get_adbMonOne(port,src_db,src_host,tgt_db,tgt_host,replication_to,None)
        return res

    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    trim_host_src = ''
    trim_host_tgt = ''
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        ls = depotdbDao.getTrimhost(src_host,tgt_host)
        qnames = depotdbDao.getQnames(port,src_db,src_host,tgt_db,tgt_host)
        qname = ""
        for vo in qnames:
            qname=vo['qname']
            if vo['replication_to'] == replication_to:
                qname = vo['qname']
                break
        daoManager.commit()
        for vo in ls:
            item = dict(vo)
            if(item['host_name'] == src_host):
                trim_host_src = item['trim_host']
            if(item['host_name'] == tgt_host):
                trim_host_tgt = item['trim_host']
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = "find trim host fail"
        logger.error("find trim host fail", exc_info=e, stack_info=True)
        return res

    logger.info("trim_host_src:" + trim_host_src)
    logger.info("trim_host_tgt:" + trim_host_tgt)
    schemaname = "splex"+str(port)
    table_name = "splex_monitor_adb"
    if qname:
        table_name += "_"+qname
    src_dbid = "%s" % (src_db)
    tgt_dbid = "%s" % (tgt_db)
    src_schema_password_info = depotdbDao.get_schema_password(schemaname,src_dbid,trim_host_src)
    src_schema_password = ''
    if len(src_schema_password_info)>0:
        src_schema_password = src_schema_password_info[0]['password']

    tgt_schema_password_info = depotdbDao.get_schema_password(schemaname, tgt_dbid, trim_host_tgt)
    tgt_schema_password = ''
    if len(tgt_schema_password_info) > 0:
        tgt_schema_password = tgt_schema_password_info[0]['password']
    src_vo = opt_splex_monitor_abd("select",src_dbid,port,schemaname, src_schema_password,table_name,replication_to,src_db,trim_host_src,None)
    tgt_vo = opt_splex_monitor_abd("select", tgt_dbid, port, schemaname, tgt_schema_password,table_name, replication_to, src_db,trim_host_src,None)
    logger.info("The source and target query results are: src={0},tgt={1}" .format(len(src_vo),len(tgt_vo)))
    logtime = None
    try:
        if len(src_vo) == 1 and len(tgt_vo) == 0:
            item = dict(src_vo[0])
            logtime = item['logtime']
            opt_splex_monitor_abd("insert",tgt_dbid,port,schemaname,tgt_schema_password,table_name,replication_to,src_db,trim_host_src,logtime)
        if len(src_vo) == 1 and len(tgt_vo) == 1:
            item = dict(src_vo[0])
            # logtime = item['logtime']
            logtime = wbxutil.gettimestr()
            opt_splex_monitor_abd("update", src_dbid, port, schemaname, src_schema_password,table_name, replication_to,src_db, trim_host_src,logtime)
            opt_splex_monitor_abd("update", tgt_dbid, port, schemaname, tgt_schema_password,table_name,replication_to, src_db, trim_host_src,logtime)
        if len(src_vo) == 0 and len(tgt_vo) == 1:
            opt_splex_monitor_abd("delete", tgt_dbid, port, schemaname, tgt_schema_password,table_name,replication_to, src_db, trim_host_src,None)
            opt_splex_monitor_abd("insert", src_dbid, port, schemaname, src_schema_password,table_name,replication_to, src_db, trim_host_src,
                                  None)
            tgt = opt_splex_monitor_abd("select", tgt_dbid, port, schemaname, tgt_schema_password,table_name,replication_to, src_db,trim_host_src,None)
            if tgt:
                logtime = dict(tgt[0])['logtime']
            else:
                logtime = datetime.datetime.strptime("1970-01-01", '%Y-%m-%d')
        if len(src_vo) == 0 and len(tgt_vo) == 0:
            opt_splex_monitor_abd("insert", src_dbid, port, schemaname, src_schema_password,table_name,replication_to, src_db, trim_host_src,None)
            tgt = opt_splex_monitor_abd("select", tgt_dbid, port, schemaname, tgt_schema_password,table_name,replication_to, src_db, trim_host_src,None)
            if tgt:
                logtime = dict(tgt[0])['logtime']
            else:
                logtime = datetime.datetime.strptime("1970-01-01", '%Y-%m-%d')
    except Exception as e:
        res["status"] = "FAILED"
        res["errormsg"] = "check fail, e={0}".format(e)
        logger.error("check fail", exc_info=e, stack_info=True)
        return res
    res = get_adbMonOne(port,src_db,src_host,tgt_db,tgt_host,replication_to,logtime)
    return res

def get_adbMonOne(port,src_db,src_host,tgt_db,tgt_host,replication_to,logtime):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        if logtime:
            depotdbDao.updatewbxadbmon(port, src_db, src_host, tgt_db, tgt_host, replication_to, logtime)
            daoManager.commit()
        ls = depotdbDao.getadbmonOne(port, src_db, src_host, tgt_db, tgt_host, replication_to)
        daoManager.commit()
        item = dict(ls[0])
        lastreptime = parse(item['lastreptime'])
        montime = parse(item['montime'])
        times = (montime - lastreptime).seconds
        m, s = divmod(times, 60)
        h, m = divmod(m, 60)
        d = (montime - lastreptime).days
        item['lag_by'] = str(d) + ":" + str(h) + ":" + str(m)
        res["data"] = item
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        logger.error(str(e), exc_info=e, stack_info=True)
    finally:
        daoManager.close()
    return res

def test_channel(port,src_db,src_host,tgt_db,tgt_host,replication_to):
    logger.info(
        "Call api test_channel, port={0},src_db={1},src_host={2},tgt_db={3},tgt_host={4},replication_to={5}".format(port, src_db,
                                                                                                           src_host,
                                                                                                           tgt_db,
                                                                                                           tgt_host,
                                                                                                           replication_to))
    flag = False
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    trim_host_src = ''
    trim_host_tgt = ''
    qname = ""
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        ls = depotdbDao.getTrimhost(src_host, tgt_host)
        qnames = depotdbDao.getQnames(port, src_db, src_host, tgt_db, tgt_host)
        for vo in qnames:
            qname = vo['qname']
            if vo['replication_to'] == replication_to:
                qname = vo['qname']
                break
        daoManager.commit()
        for vo in ls:
            item = dict(vo)
            if (item['host_name'] == src_host):
                trim_host_src = item['trim_host']
            if (item['host_name'] == tgt_host):
                trim_host_tgt = item['trim_host']
    except Exception as e:
        daoManager.rollback()
        logger.error(e, exc_info=e, stack_info=True)
        raise e

    schemaname = "splex" + str(port)
    table_name = "splex_monitor_adb"
    if qname:
        table_name += "_" + qname
    src_dbid = "%s" % (src_db)
    tgt_dbid = "%s" % (tgt_db)
    direction = "checktest"
    src_schema_password_info = depotdbDao.get_schema_password(schemaname, src_dbid, trim_host_src)
    src_schema_password = ''
    if len(src_schema_password_info) > 0:
        src_schema_password = src_schema_password_info[0]['password']
    tgt_schema_password_info = depotdbDao.get_schema_password(schemaname, tgt_dbid, trim_host_tgt)
    tgt_schema_password = ''
    if len(tgt_schema_password_info) > 0:
        tgt_schema_password = tgt_schema_password_info[0]['password']
    try:
        opt_splex_monitor_abd("delete", src_dbid, port, schemaname, src_schema_password,table_name, direction, src_db, trim_host_src, None)
        time.sleep(1)
        opt_splex_monitor_abd("insert", src_dbid, port, schemaname, src_schema_password,table_name, direction, src_db, trim_host_src, None)
        res = opt_splex_monitor_abd("select",tgt_dbid,port, schemaname, tgt_schema_password,table_name, direction, src_db, trim_host_src,None)
        num1 = 0
        while len(res) == 0 and num1 < 5:
            time.sleep(1)
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            logger.info("Try to query data on target, time={0}" .format(now))
            res = opt_splex_monitor_abd("select", tgt_dbid, port, schemaname, tgt_schema_password,table_name, direction, src_db,trim_host_src, None)
            num1 += 1
        num2 = 0
        while len(res) == 0 and num2 < 5:
            time.sleep(3)
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            logger.info("Try to query data on target, time={0}" .format(now))
            res = opt_splex_monitor_abd("select", tgt_dbid, port, schemaname, tgt_schema_password,table_name, direction, src_db,trim_host_src, None)
            num2 += 1
        if len(res) > 0:
            flag = True

        logger.info("Clean source test data")
        opt_splex_monitor_abd("delete", src_dbid, port, schemaname, src_schema_password,table_name, direction, src_db, trim_host_src,None)
        logger.info("Clean target test data")
        opt_splex_monitor_abd("delete", tgt_dbid, port, schemaname, tgt_schema_password,table_name, direction, src_db, trim_host_src,None)
    except Exception as e:
        logger.error(e, exc_info=e, stack_info=True)
        raise e
    return flag

# opt: select/insert/update/delete
def opt_splex_monitor_abd(opt,dbid,port,schemaname,schemapwd, table_name,direction,src_db,trim_host_src,logtime):
    logger.info(
        "opt={0},dbid={1},port={2},schemaname={3},schemapwd={4},able_name={5},direction={6},src_db={7},trim_host_src={8},logtime={9}".format(
            opt,dbid,port,schemaname,schemapwd, table_name,direction,src_db,trim_host_src,logtime))

    count = None
    res = None
    connectionurl = ""
    db_tns_info = get_db_tns(dbid)
    if db_tns_info:
        connectionurl = db_tns_info['tns']
    try:
        engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (schemaname, schemapwd, connectionurl),poolclass=NullPool, echo=False)
        connect = engine.connect()
        sql = ""
        if "delete" == opt:
            sql = '''
                   delete from %s.%s WHERE direction='%s' and src_db='%s' and src_host='%s'
                   ''' % (schemaname, table_name, direction, src_db, trim_host_src)
        if "insert" == opt:
            if logtime:
                sql = "insert into %s.%s (direction,src_host,src_db,logtime,port_number) values ('%s','%s','%s',to_date('%s','YYYY-MM-DD hh24:mi:ss'),%s)" % (
                schemaname, table_name, direction, trim_host_src, src_db, logtime, port)
            else:
                sql = "insert into %s.%s (direction,src_host,src_db,logtime,port_number) values ('%s','%s','%s',sysdate,%s)" % (
                schemaname, table_name, direction, trim_host_src, src_db, port)
        if "update" == opt:
            if logtime:
                sql = "UPDATE %s.%s SET logtime=to_date('%s','YYYY-MM-DD hh24:mi:ss') WHERE direction='%s' and src_db='%s' and src_host='%s'" % (
                    schemaname, table_name, logtime, direction, src_db, trim_host_src)
            else:
                sql = "UPDATE %s.%s SET logtime=sysdate WHERE direction='%s' and src_db='%s' and src_host='%s'" % (
                schemaname, table_name, direction, src_db, trim_host_src)
        if "select" == opt:
            sql = "select * from %s.%s where direction='%s' and src_db='%s' and src_host='%s' " % (
            schemaname, table_name, direction, src_db, trim_host_src)
        logger.info(sql)
        cursor = connect.execute(sql)
        if "select" == opt:
            res = cursor.fetchall()
            logger.info("res={0}" .format(res))
        connect.connection.commit()
    except DatabaseError as e:
        if connect is not None:
            connect.connection.rollback()
        logger.error(e, exc_info=e, stack_info=True)
        raise e
    return res






    # daoManager = None
    # try:
    #     daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    #     daoManager = daoManagerFactory.getDaoManager(dbid)
    #     connectionurl  =""
    #     db_tns_info = get_db_tns(dbid)
    #     if db_tns_info:
    #         connectionurl = db_tns_info['tns']
    #     engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (schemaname, schemapwd, connectionurl),
    #                            poolclass=NullPool, echo=True)
    #     print(engine)
    #     daoManager.engine = engine
    # except Exception as e:
    #     logger.error(e, exc_info=e, stack_info=True)
    #     raise e
    #
    # try:
    #     daoManager.startTransaction()
    #     logger.info(daoManager.engine.url)
    #     spDao = daoManager.getDao(DaoKeys.DAO_SHAREPLEXMONITORDAO)
    #     if "delete" == opt:
    #         count = spDao.delete_splex_monitor_adb(schemaname, table_name, direction, trim_host_src, src_db)
    #         res = count
    #     if "insert" == opt:
    #         count = spDao.insertsplexmonitoradbdata1(schemaname, table_name, direction, trim_host_src, src_db, port,
    #                                                  logtime)
    #         res = count
    #     if "update" == opt:
    #         count = spDao.updatesplexmonitoradbdata1(schemaname, table_name, direction, trim_host_src, src_db, logtime)
    #         res = count
    #     if "select" == opt:
    #         list = spDao.getSplexMonitorAdb(schemaname, table_name, direction, trim_host_src, src_db)
    #         count = len(list)
    #         res = list
    #     logger.info("%s %s.%s from %s done ,count:%s" % (opt, schemaname, table_name, dbid, count))
    #     daoManager.commit()
    # except Exception as e:
    #     daoManager.rollback()
    #     logger.error(e, exc_info=e, stack_info=True)
    #     raise e
    # return res


# opt: select/insert/update/delete
# def opt_splex_monitor_abd(opt,dbid,port,schemaname,schemapwd, table_name,direction,src_db,trim_host_src,logtime):
#     logger.info(
#         "opt={0},dbid={1},port={2},schemaname={3},table_name={4},direction={5},src_db={6},trim_host_src={7},logtime={8}".format(
#             opt, dbid, port, schemaname, table_name, direction, src_db, trim_host_src, logtime))
#     count = None
#     res = None
#     daoManager = None
#     try:
#         daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
#         daoManager = daoManagerFactory.getDaoManager(dbid)
#         connectionurl  =""
#         db_tns_info = get_db_tns(dbid)
#         if db_tns_info:
#             connectionurl = db_tns_info['tns']
#         engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (schemaname, schemapwd, connectionurl),
#                                poolclass=NullPool, echo=True)
#         print(engine)
#         daoManager.engine = engine
#     except Exception as e:
#         logger.error(e, exc_info=e, stack_info=True)
#         raise e
#
#     try:
#         daoManager.startTransaction()
#         logger.info(daoManager.engine.url)
#         spDao = daoManager.getDao(DaoKeys.DAO_SHAREPLEXMONITORDAO)
#         if "delete" == opt:
#             count = spDao.delete_splex_monitor_adb(schemaname, table_name, direction, trim_host_src, src_db)
#             res = count
#         if "insert" == opt:
#             count = spDao.insertsplexmonitoradbdata1(schemaname, table_name, direction, trim_host_src, src_db, port,
#                                                      logtime)
#             res = count
#         if "update" == opt:
#             count = spDao.updatesplexmonitoradbdata1(schemaname, table_name, direction, trim_host_src, src_db, logtime)
#             res = count
#         if "select" == opt:
#             list = spDao.getSplexMonitorAdb(schemaname, table_name, direction, trim_host_src, src_db)
#             count = len(list)
#             res = list
#         logger.info("%s %s.%s from %s done ,count:%s" % (opt, schemaname, table_name, dbid, count))
#         daoManager.commit()
#     except Exception as e:
#         daoManager.rollback()
#         logger.error(e, exc_info=e, stack_info=True)
#         raise e
#     return res


if __name__ == "__main__":
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(now)