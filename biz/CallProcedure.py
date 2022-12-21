import json
import logging
import threading
import time
import uuid
from datetime import datetime
from common.wbxtask import threadlocal
from common.wbxutil import wbxutil

from common.wbxcache import getLog, removeLog
from common.wbxexception import wbxexception
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

procedures = [
    {"procedure_name":"PKGFIXDATA.LAUNCHER","num_of_arguments":1,"exec_user":"test"},
    {"procedure_name":"PKGREPAIRMTG.ProcRepairMtgCoreTables","num_of_arguments":0,"exec_user":"splex_deny"}
]

def updateJobStatus(jobid, status,by):
    logger.info("Update job status to be %s in depotdb with jobid=%s" % (status, jobid))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daomanager = daoManagerFactory.getDefaultDaoManager()
    dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        daomanager.startTransaction()
        jobvo = dict(dao.getProcedureJobByJobid(jobid)[0])
        resultmsg = None
        if status in ('SUCCEED', 'FAILED'):
            jobvo['lastmodified_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # Do not remove below log, it will add the summarized line to the output log
            logger.info("The procedure jobid={0} status={1} " .format(jobid, status))
            resultmsg = getLog(jobid)
            threadlocal.current_jobid = None
        elif status == "RUNNING":
            jobvo['created_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            jobvo['resultmsg1'] = ''
            jobvo['resultmsg2'] = ''
            jobvo['resultmsg3'] = ''
            if jobvo['status'] in ("FAILED", "RUNNING"):
                removeLog(jobid)
            threadlocal.current_jobid = jobid

        if resultmsg is not None and resultmsg != "":
            colwidth = 3900
            resList = [resultmsg[x - colwidth:x] for x in range(colwidth, len(resultmsg) + colwidth, colwidth)]
            jobvo['resultmsg1'] = str(resList[0])
            if len(resList) > 1:
                jobvo['resultmsg2'] = str(resList[1])
            else:
                jobvo['resultmsg2'] = ''
            if len(resList) > 2:
                jobvo['resultmsg3'] = str(resList[-1])
            else:
                jobvo['resultmsg3'] = ''
        jobvo['status'] = status
        jobvo['modified_by'] = by
        dao.updateProcedureStatus(jobvo)
        daomanager.commit()
        return jobvo
    except Exception as e:
        daomanager.rollback()
        raise e
    finally:
        daomanager.close()

def selectProcedure():
    webdb = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.getAllWebDB()
        daoManager.commit()
        webdb = [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        logger.error("getAllWebDB error occurred", exc_info=e, stack_info=True)
    finally:
        daoManager.close()
    resDict = {"procedures": procedures, "webdb": webdb}
    return resDict

def get_procedureList(procedure_name):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.getProcedureList(procedure_name)
        daoManager.commit()
        return [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        logger.error("get_procedureList error occurred", exc_info=e, stack_info=True)
    finally:
        daoManager.close()

def execProcedure(jobid, db_name, exec_by,schema,sql,procedure_name):
    logger.info("start exec procedure, db_name={0},schema={1},procedure_name={2}".format(db_name,schema,procedure_name))
    daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daomanagerfactory.getDaoManager(db_name,schema)
    dao = daoManager.getDao(DaoKeys.DAO_DBAUDITDAO)
    try:
        daoManager.startTransaction()
        dao.exec_procedure(sql)
        # daoManager.commit()
        logger.info("execProcedure finish")
        if str(procedure_name).upper() == str("PKGREPAIRMTG.ProcRepairMtgCoreTables").upper():
            joblog = dao.checkwbxbackendjoblog()
            logid = dict(joblog)['logid']
            logcontent = dict(joblog)['logcontent']
            logger.info("logid:{0}" .format(logid))
            logger.info("logcontent:{0}".format(logcontent))
            logdetails = dao.get_WBXBACKENDJOBDETAILLOG(logid)
            for detail in logdetails:
                logger.info(dict(detail))
        daoManager.commit()
        updateJobStatus(jobid, "SUCCEED", exec_by)
    except Exception as e:
        daoManager.rollback()
        # logger.error("execProcedure error occurred, {0}" .format(sql))
        raise wbxexception(e)
    finally:
        daoManager.close()

def exec_job(jobid,exec_by,procedure_name):
    resDict = {"status": "SUCCEED", "resultmsg": ""}
    updateJobStatus(jobid, "RUNNING", exec_by)
    logger.info("exec job, jobid={0},procedure_name={1},exec_by={2}".format(jobid,procedure_name,exec_by))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    procedure = None
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        runnings = depotdbDao.procedureRunning()
        if len(runnings)>2:
            updateJobStatus(jobid, "FAILED", exec_by)
            resDict = {"status": "FAILED", "resultmsg": "There are already two procedures are running, please wait for execution"}
            logger.info("There are already two procedures are running")
            return resDict
        else:
            items = depotdbDao.getProcedureInfo(jobid)
            jobinfo = dict(items[0])
            procedure_name = jobinfo['procedure_name']
            schema = ""
            for procedure in procedures:
                if str(procedure_name).upper() == str(procedure['procedure_name']).upper():
                    schema = procedure['exec_user']
                    break
            logger.info("schema={0}" .format(schema))
            logger.info("procedure_name={0}".format(procedure_name))
            try:
                sql = ""
                if jobinfo['args']:
                    args = json.loads(jobinfo['args'])
                    sql = '''   begin
                                   %s(
                                   ''' % (procedure_name)
                    for key in args:
                        sql += "'" + args[key] + "'"
                    sql += "); end;"
                else:
                    sql = '''BEGIN TEST.%s; END; ''' % (procedure_name)
                logger.info(sql)
                # t = threading.Thread(target=execProcedure, args=(jobid, jobinfo['db_name'], exec_by, schema, sql))
                # t.start()
                # start exec procedure
                execProcedure(jobid, jobinfo['db_name'], exec_by, schema, sql,procedure_name)

            except Exception as e:
                logger.error("exec_procedure error occurred, e={0}" .format(str(e)))
                resDict = {"status": "FAILED",
                           "resultmsg":str(e)}
                updateJobStatus(jobid, "FAILED", exec_by)
                return resDict
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        logger.error("exec_job error occurred", exc_info=e, stack_info=True)
        resDict = {"status": "FAILED",
                   "resultmsg": str(e)}
        return resDict
    finally:
        daoManager.close()

    # try:
    #     pool = ThreadPoolExecutor(max_workers=2)
    #     all_task = []
    #     for i in range(10):
    #         all_task.append(pool.submit(exec_task, procedure['procedure_name'], procedure['db_name'], procedure['created_by'], str(i)+"_a"))
    #     for future in as_completed(all_task):
    #         data = future.result()
    #         print(data)
    # except Exception as e:
    #     resDict["status"] = "FAILED"
    #     resDict["resultmsg"] = str(e)
    #     logger.error("exec_Procedure met error %s" % (str(e)))
    return resDict

def commit_procedure(procedure_name,db_name,created_by,args):
    resDict = {"status": "SUCCEED", "resultmsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        id = uuid.uuid1()
        depotdbDao.commitprocedure(id,procedure_name,db_name,created_by,args)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        logger.error("commit_procedure error occurred", exc_info=e, stack_info=True)
    finally:
        daoManager.close()
    return resDict

def procedure_getjoblog(jobid):
    logger.info("procedure_getjoblog(jobid=%s) start" % jobid)
    resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
    logs = getLog(jobid)
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        daoManager.startTransaction()
        jobvo = dict(dao.getProcedureJobByJobid(jobid)[0])
        jobstatus = ""
        if jobvo is not None:
            if wbxutil.isNoneString(logs):
                logs = "%s%s%s" % (jobvo['resultmsg1'], jobvo['resultmsg2'], jobvo['resultmsg3'])
            jobstatus = jobvo['status']
        daoManager.commit()
    except Exception as e:
        if daoManager is not None:
            daoManager.rollback()
        resDict["status"] = "FAILED"
        resDict["resultmsg"] = str(e)
        logger.error("procedure_getjoblog(jobid=%s) met error %s" % (jobid, str(e)), exc_info=e)
    finally:
        if daoManager is not None:
            daoManager.close()

    resDict["data"] = logs
    resDict["jobstatus"] = jobstatus
    return resDict