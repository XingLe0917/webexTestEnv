import logging
from common.wbxshareplexport import wbxshareplexport
from common.wbxexception import wbxexception
from collections import OrderedDict
import threading

logger = logging.getLogger("DBAMONITOR")
lock = threading.Lock()

# If precheck failed, should not start upgrade, because many data are initialized in precheckShareplex function
def precheckShareplex(host_name, splex_port, splex_oldversion):
    logger.info("precheckShareplex(host_name=%s, splex_port=%s, splex_oldversion=%s)" % (host_name, splex_port, splex_oldversion))
    PRODDIR_NAME = "shareplex%s" % splex_oldversion.replace(".", "")
    checklist = OrderedDict([("Oracle user login",None),("Check Shareplex Port existed",None),("Shareplex Version",None),("Vardir Size",None), ("Backlog Size",None),("Splex user password in DepotDB",None)])
    step = "Oracle user login"
    sp = None
    try:
        global lock
        # In order to avoid multiple session process same port case
        try:
            if lock.acquire():
                sp = wbxshareplexport.newInstance(host_name, splex_port)
                if sp.isUpgrading():
                    raise wbxexception("This port %s is upgrading by other session" % splex_port)
                else:
                    sp.setstatus(True)
        finally:
            lock.release()
        sp.login()
        checklist[step]= True
        step = "Check Shareplex Port existed"
        sp.isPortExist()
        sp.checkEnviromentConfig()
        sp.getShareplexInstalledCount()
        checklist[step] = True
        step = "Shareplex Version"
        releasever = sp.getShareplexVersion()
        if releasever != splex_oldversion:
            raise wbxexception("Current shareplex version is %s, but should be %s" % (releasever, splex_oldversion))
        checklist[step] = True
        step = "Vardir Size"
        vardirsize = sp.getVardirSize()
        if vardirsize > 2 * 1024 * 1024 * 1024 or vardirsize == -1:
            raise wbxexception("The vardir %s size is %s which exceed limitation" % (sp.SP_SYS_VARDIR, vardirsize))
        checklist[step] = True
        step = "Backlog Size"
        res = sp.qstatus()
        lines = res.splitlines()
        for line in lines:
            line = line.strip()
            if line.find("Name:") >= 0:
                queuname = line.split()[1]
            elif line.find("Backlog (messages)") >= 0:
                backlogsize = line.split()[2]
                if int(backlogsize) > 1 * 1024 * 1024 * 1024:
                    raise wbxexception("The queue %s has %s backlog which exceed limitation 1GB" % (queuname, backlogsize))
        checklist[step] = True
        step = "Splex user password in DepotDB"
        sp.getSplexuserPassword()
        checklist[step] = True
        sp.preparecronjob()
    except Exception as e:
        logger.error(e)
        sp.setReload(True)
        checklist[step] = str(e)
    finally:
        if sp is not None:
            sp.setstatus(False)
            sp.close()
    statuslist = []
    for key, value in checklist.items():
        statuslist.append({"name":key,"status":value})
    # statuslist = [{"name":key,"status":value} for key, value in checklist.items()]

    logger.info("precheckShareplex(host_name=%s, splex_port=%s, splex_oldversion=%s) end with status=%s" % (host_name, splex_port, splex_oldversion, statuslist))

    return statuslist

def upgradeShareplex(host_name, splex_port, splex_oldversion, splex_newversion):
    global  lock
    logger.info("upgradeShareplex(host_name=%s, splex_port=%s, splex_oldversion=%s, splex_newversion=%s)" % (host_name, splex_port, splex_oldversion, splex_newversion))
    statusdict = OrderedDict([("Install Binary File",None), ("Start Blackout",None),("Stop Cronjob",None),("Stop Shareplex service",None),("Backup Vardir",None)])

    PRODDIR_NAME = "shareplex%s" % splex_oldversion.replace(".","")
    NEW_PRODDIR_NAME = "shareplex%s" % splex_newversion.replace(".","")
    issucceed = False
    try:
        sp = wbxshareplexport.newInstance(host_name, splex_port)
        for splex_sid, splex_pwd in sp.dbsiddict.items():
            key = "orasetup_for_sid_%s" % splex_sid
            statusdict[key] = None
        statusdict["Change CRS service script file"] = None
        statusdict["Start Shareplex service"] = None
        statusdict["Replace cronjob for the port"] = None
        statusdict["Set shareplex parameter"] = None
        statusdict["Start Cronjob"] = None
        statusdict["Stop Blackout"] = None

        sp.login()
        step = "Install Binary File"
        if not sp.isNewBinaryInstalled(NEW_PRODDIR_NAME):
            sp.installShareplexBinary(splex_newversion)
            statusdict[step] = True
        else:
            statusdict.pop(step)
        step = "Start Blackout"
        sp.startBlackout(8)
        statusdict[step] = True
        step = "Stop Cronjob"
        sp.server.stopService("crond")
        statusdict[step] = True
        step = "Stop Shareplex service"
        sp.stopShareplexService()
        statusdict[step] = True
        step = "Backup Vardir"
        sp.backupVardir()
        statusdict[step] = True
        for splex_sid, splex_pwd in sp.dbsiddict.items():
            step = "orasetup_for_sid_%s" % splex_sid
            sp.changeProfile(NEW_PRODDIR_NAME)
            sp.orasetup(splex_sid, splex_pwd)
            statusdict[step] = True
        step = "Change CRS service script file"
        sp.upgradeServiceConfig(NEW_PRODDIR_NAME)
        statusdict[step] = True
        issucceed = True
    except wbxexception as e:
        logger.error(e)
        statusdict[step] = str(e)
    finally:
        try:
            if issucceed:
                step = "Set shareplex parameter"
                paramdict = {"SP_OCT_TARGET_COMPATIBILITY": None,
                             "SP_SYS_TARGET_COMPATIBILITY":"7",
                             "SP_OCT_OLOG_USE_OCI":"0",
                             "SP_OCT_OLOG_NO_DATA_DELAY": "5000000",
                             "SP_OCT_DDL_UPDATE_CONFIG": "0"}
                sp.changeparameter(paramdict)
                statusdict[step] = True
            if statusdict["Stop Shareplex service"]:
                sp.startShareplexService()
                statusdict["Start Shareplex service"] = True
            if statusdict["Stop Cronjob"]:
                sp.server.startService("crond")
                statusdict["Start Cronjob"] = True
            # If failed in above step, should not change crontab
            if issucceed:
                # It block other sessions to process shareplex upgrade. But it is ok, the concurrency request is not huge;
                sp.uncommentcronjob()
                statusdict["Replace cronjob for the port"] = True
            if statusdict["Start Blackout"]:
                sp.stopBlackout()
                statusdict["Stop Blackout"] = True
        except Exception as e:
            logger.error("Error ocurred at upgradeShareplex finally step: %s" % e)
        if sp is not None:
            sp.setstatus(False)
            sp.close()
    logger.info("upgradeShareplex(host_name=%s, splex_port=%s, splex_oldversion=%s, splex_newversion=%s) end with status: %s" % (host_name, splex_port, splex_oldversion, splex_newversion, statusdict))
    statuslist = [{"name": key, "status": value} for key, value in statusdict.items()]
    return statuslist

if __name__ == "__main__":
    splex_port =19029
    host_name="txdbormt011.webex.com"
    ssh_port=22
    precheckShareplex()
    upgradeShareplex()