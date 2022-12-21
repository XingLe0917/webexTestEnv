import datetime
import logging
import uuid

from common.wbxutil import wbxutil
from dao.vo.wbxmonitoralertdetailVo import WbxmonitoralertdetailVo
from dao.wbxdaomanager import wbxdaomanagerfactory,DaoKeys

logger = logging.getLogger("DBAMONITOR")

def add_wbxmonitoralertdetail(**kwargs):
    logger.info("add_wbxmonitoralertdetail, kwargs=%s" % (kwargs))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    result = geWbxmonitoralertdetailVo(**kwargs)
    if result['status'] == "SUCCESS":
        try:
            daoManager.startTransaction()
            wbxmonitoralertdetailVo = result['data']
            dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            dao.add_alertdetail(wbxmonitoralertdetailVo)
            daoManager.commit()
        except Exception as e:
            daoManager.rollback()
            res["status"] = "FAILED"
            res["errormsg"] = str(e)
    else:
        res['status'] = result['status']
        res['errormsg'] = result['errormsg']
    return res

def geWbxmonitoralertdetailVo(**kwargs):
    res = {"status": "SUCCESS", "errormsg": "","data":None}
    if "task_type" not in kwargs:
        res['status']= 'FAILED'
        res['errormsg'] = 'Do not find task_type in parameters'
        return res
    task_type = kwargs['task_type']
    db_name = ""
    host_name = ""
    instance_name = ""
    splex_port = ""
    parameter = ""
    if "db_name" in kwargs:
        db_name = kwargs['db_name']
    if "host_name" in kwargs:
        host_name = kwargs['host_name']
        if '.webex.com' in host_name:
            host_name = str(host_name).split(".")[0]
    if "instance_name" in kwargs:
        instance_name = kwargs['instance_name']
    if "splex_port" in kwargs:
        splex_port = kwargs['splex_port']
    if "parameter" in kwargs:
        parameter = kwargs['parameter']
    if not db_name and not host_name and not instance_name and not splex_port:
        res['status'] = 'FAILED'
        res['errormsg'] = 'Do not find db_name,host_name,instance_name and splex_port'
        return res

    alert_title = task_type
    if db_name:
        alert_title += "_" + db_name
    if host_name:
        alert_title += "_" + host_name
    if instance_name:
        alert_title += "_" + instance_name
    if splex_port:
        alert_title += "_" + splex_port

    # parameter = dict(kwargs)
    # parameter_str = ''' {'''
    # index = 1
    # for key in parameter:
    #     if key not in exclude_key:
    #         parameter_str += ''' "%s": "%s" ''' % (key, parameter[key])
    #         index += 1
    #         if index <= len(parameter):
    #             parameter_str += ","
    # parameter_str += ''' }'''

    jobvo = WbxmonitoralertdetailVo(alertdetailid=uuid.uuid4().hex, alerttitle=alert_title, db_name=db_name,
                                    host_name=host_name,
                                    instance_name=instance_name, splex_port=splex_port, parameter=parameter,
                                    alert_type=task_type,
                                    job_name="")
    res['data'] = jobvo
    return res


def get_wbxmonitoralert(db_name,status,host_name,alert_type,start_date,end_date):
    logger.info("get_wbxmonitoralert")
    res = {"status": "SUCCESS", "errormsg": "","data":None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        date1 = wbxutil.convertStringToDate(end_date)
        end_date_next_day = (date1 + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        rows = dao.getWbxmonitoralert(db_name,status,host_name,alert_type,start_date,end_date_next_day)
        res['data'] = [dict(vo) for vo in rows]
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    finally:
        if daoManager is not None:
            daoManager.close()
    return res

def get_wbxmonitoralertdetail(alertid):
    logger.info("get_wbxmonitoralertdetail, alertid={0}" .format(alertid))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rows = dao.getWbxmonitoralertdetail(alertid)
        res['data'] = [dict(vo) for vo in rows]
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    finally:
        if daoManager is not None:
            daoManager.close()
    return res

def get_wbxautotask(autotaskid):
    logger.info("get_wbxautotask, autotaskid={0}".format(autotaskid))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rows = dao.getWbxautotask(autotaskid)
        list = [dict(vo) for vo in rows]
        joblist = []
        data = {}
        data['db_name'] = list[0]['db_name']
        data['host_name'] = list[0]['host_name']
        data['splex_port'] = list[0]['splex_port']
        data['parameter'] = list[0]['parameter']
        for item in list:
            job = {}
            job['jobid'] = item['jobid']
            job['processorder'] = item['processorder']
            job['job_action'] = item['job_action']
            job['status'] = item['status']
            job['execute_method'] = item['execute_method']
            joblist.append(job)
        data['joblist'] = joblist
        res['data'] = data
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    finally:
        if daoManager is not None:
            daoManager.close()
    return res

def get_wbxmonitoralert_type():
    logger.info("get_wbxmonitoralert_type ")
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rows = dao.get_wbxmonitoralert_type()
        list = [dict(vo)['alert_type'] for vo in rows]
        res['data'] = list
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    finally:
        if daoManager is not None:
            daoManager.close()
    return res


if __name__ == "__main__":
    # end_date='2021-08-05'
    # date1 = wbxutil.convertStringToDate(end_date)
    # next_day = (date1+datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    # print(next_day)

    today = datetime.datetime.now().date().strftime('%Y-%m-%d')
    week_ago = (datetime.datetime.now() - datetime.timedelta(days=7)).date().strftime('%Y-%m-%d')
    print(today)
    print(week_ago)

    a = datetime.datetime.now().strftime('%Y-%m-%d')
    print(a)
