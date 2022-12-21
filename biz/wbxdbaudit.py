import logging
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys
from common.wbxexception import wbxexception
from biz.dbmanagement.wbxminidom import wbxminidom
import os
import glob

logger = logging.getLogger("DBAMONITOR")

def getwbxdbauditparams(**kargs):
    resDict = {"status": "SUCCEED", "resultmsg": "", "data": None}
    type=kargs["type"]
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    if type == "getparam":
        daomanager = daoManagerFactory.getDefaultDaoManager()
        try:
            dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            daomanager.startTransaction()
            rows = dao.get_wbxdbauditparam()
            if rows is None:
                raise wbxexception("wbxdbaudit encountered an exception while get parameters")
            resDict["data"]=rows
            daomanager.commit()
        except Exception as e:
            daomanager.rollback()
            resDict["status"]="FAILED"
            resDict["resultmsg"]=str(e)
        finally:
            daomanager.close()
    elif type == "getxmlpath":
        taskid = kargs["taskid"]
        auditlist = ["OS", "DB"]
        try:
            # if "LINUX" in sys.platform.upper():
            xml_dir = "/usr/local/nginx/html/wbxdbaudit/"
            # xml_dir = "C:\\Users\\wentazha\\cisco\\CCP\\WebexTestEnv\\test\\"
            if not os.path.exists(xml_dir):
                os.makedirs(xml_dir)
                os.chmod(xml_dir, 0o777)
            filepatt = xml_dir+"wbxdbaudit_*_%s.xml" %(taskid)
            # filepatt = xml_dir+"testdbaudit_*.xml"
            logger.info(filepatt)
            rows = glob.glob(filepatt)
            logger.info(rows)
            if len(rows) != 1:
                raise wbxexception("get filepath failed on current server, please double check : %s" %str(rows))

            with open(rows[0], 'r') as f:
                content=f.read()
            resDict["data"] = {"localpath":rows[0],"content":content}
        except Exception as e:
            logger.error("wbxdbaudit encountered an exception while get filepath:%s " %str(e))
            resDict["status"] = "FAILED"
            resDict["resultmsg"] = str(e)

    return resDict


if __name__ == '__main__':
    kargs={"type":"getxmlpath","taskid":"taskid"}
    resDict=getwbxdbauditparams(**kargs)
    print(resDict)