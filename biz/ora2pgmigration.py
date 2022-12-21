from dao.wbxdaomanager import DaoKeys
from dao.wbxdaomanager import wbxdaomanagerfactory
import logging
from common.wbxexception import wbxexception
logger = logging.getLogger("DBAMONITOR")

class ora2pgmigration():

    def __init__(self):
        self.request_definition={
            "GETORA2PGTABLESBYQUERY":"getOra2PGTablesByQuery",
            "UPDBATCHORA2PGTABLESTATUS":"updBatchTableStatusByTabID"
        }

    def run(self, **kargs):
        request_type=kargs["request_type"]
        func_name=self.request_definition[request_type]
        logger.info("run func :%s , kargs: %s" %(func_name, str(kargs)))
        func = getattr(self, func_name)
        func(**kargs)

    def getOra2PGTablesByQuery(self,**kargs):
        # {"status": "FAILED", "errormsg": "empty request paras", "data": []}
        resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
        data={}
        taskid=kargs["taskid"]
        schema=kargs.get("schema",None)
        table_name=kargs.get("table_name",None)
        status=kargs.get("status",None)
        page_size=kargs.get("page_size",None)
        page_index=kargs.get("page_index",None)
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager=None
        try:
            daomanager = daomanagerfactory.getDefaultDaoManager()
            daomanager.startTransaction()

            autotaskdao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
            taskvo = autotaskdao.getAutoTaskByTaskid(taskid)
            if taskvo is None:
                raise wbxexception("Cannot find taskvo with taskid: %s ")
            data["taskid"] = taskvo.taskid
            data["schema_list"] = taskvo.parameter["schema_list"]
            data["src_db_name"] = taskvo.parameter["src_db_name"]
            data["tgt_db_name"] = taskvo.parameter["tgt_db_name"]

            ora2pgdao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            tbvolist = ora2pgdao.getOra2pgTaskTabByTabQuery(taskid, schema, table_name, status,page_size,page_index)
            data["table_list"] = [ tbvo.to_dict() for tbvo in tbvolist]
            resDict["data"] = data
        except Exception as e:
            if daomanager is not None:
                daomanager.rollback()
            resDict["status"]="FAILED"
            resDict["errormsg"] = str(e)
            # raise e
        finally:
            if daomanager is not None:
                daomanager.close()
        return resDict

    def updBatchTableStatusByTabID(self,**kargs):
        # {"status": "FAILED", "errormsg": "empty request paras", "data": []}
        resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
        taskid = kargs["taskid"]
        tableid = kargs["tableid"]
        status = kargs.get("status", None)
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = None
        try:
            daomanager = daomanagerfactory.getDefaultDaoManager()
            daomanager.startTransaction()

            ora2pgdao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            tableidlist=tableid.split(",")
            ora2pgdao.updBatchOra2pgTabStatusByTableID(taskid, status, *tableidlist)
        except Exception as e:
            if daomanager is not None:
                daomanager.rollback()
            resDict["status"] = "FAILED"
            resDict["errormsg"] = str(e)
            # raise e
        finally:
            if daomanager is not None:
                daomanager.close()
        return resDict