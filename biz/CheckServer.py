import logging

from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

authority_user = {
    "Le Xing":"Y2lzY29zcGFyazovL3VzL1BFT1BMRS8zMDgxOTk1OS05YWU4LTRlNWItYjhhZS0yNWUwZGZlZTdhNDI",
    "Samuel Cui":"Y2lzY29zcGFyazovL3VzL1BFT1BMRS9kMGNiN2I3MC00Y2YwLTRjNmMtYTQzNC02ZTRhNWUwZDczYzY",
    "Gates Liu":"Y2lzY29zcGFyazovL3VzL1BFT1BMRS9hNzI1ZmQ2Mi00YTcwLTQ5Y2YtYmYzNi0zNGVlMjM3MGFmYTk",
    "Hatty Hu":"Y2lzY29zcGFyazovL3VzL1BFT1BMRS8zY2VhNjU0Yy0wYjhhLTQyM2YtOTU4Ni05M2RmMmFlNjhiOGU",
    "Emily Teng":"Y2lzY29zcGFyazovL3VzL1BFT1BMRS8wOWNkY2UyMC01NDk3LTQ5NjgtOGRiMC01ZjI1ZjU5YzVjY2I",
    "Doris Ma":"Y2lzY29zcGFyazovL3VzL1JPT00vZmRiYmM1ZDAtY2ZkNC0xMWViLWIzYzAtYjM4ZjhkNDZjNDc0"
}

def CheckServer(host_name,param):
    logger.info("CheckServer host_name=%s, param=%s" % (host_name,param))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    if param is None:
        res['status'] = 'FAILED'
        res['errormsg'] = "Error occurred : Parameter is Null"
        return res
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    server = None
    try:
        server = daoManagerFactory.getServer(host_name)
        server.connect()
        if "redhat-release" == param:
            cmd = "cat /etc/redhat-release"
            result = server.exec_command(cmd)
            res['data'] = result
        elif "crond" == param:
            cmd = "/sbin/service crond status"
            result = server.exec_command(cmd)
            res['data'] = result
        else:
            res['data'] = "Sorry, I can't do it yet."
        res['status'] = 'SUCCESS'
    except Exception as e:
        res['status'] = 'FAILED'
        errormsg = "Error occurred(%s): %s" % (host_name,e)
        res['errormsg'] = errormsg
        logger.error(errormsg, exc_info=e, stack_info=True)
    return res

def OperateServer(host_name,opt,param,user):
    logger.info("OperateCrond host_name=%s, opt=%s, param=%s, user=%s " % (host_name, opt,param,user))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    flag = False
    for value in authority_user.values():
        if user == value:
            flag = True
            break
    if not flag:
        res['status'] = 'FAILED'
        res['errormsg'] = "Sorry. You don't have permission to operate it."
        return res
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    server = None
    try:
        server = daoManagerFactory.getServer(host_name)
        server.connect()
        if "crond" == param:
            if "start" == opt:
                cmd = "sudo /sbin/service crond start"
                result = server.exec_command(cmd)
                res['data'] = result
            if "stop" == opt:
                cmd = "sudo /sbin/service crond stop"
                result = server.exec_command(cmd)
                res['data'] = result
        else:
            res['data'] = "Sorry, I can't do it yet."
        res['status'] = 'SUCCESS'
    except Exception as e:
        res['status'] = 'FAILED'
        errormsg = "Error occurred(%s): %s" % (host_name,e)
        res['errormsg'] = errormsg
        logger.error(errormsg, exc_info=e, stack_info=True)
    return res
