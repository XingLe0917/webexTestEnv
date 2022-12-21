import logging
from common.wbxshareplexport import wbxshareplexport
from common.wbxexception import wbxexception
from collections import OrderedDict
import threading

logger = logging.getLogger("DBAMONITOR")
lock = threading.Lock()

# If precheck failed, should not start upgrade, because many data are initialized in precheckShareplex function
def precheckInstallShareplex(host_name, splex_port, splex_prod_dir, splex_sid, shareplex_version, db_name):
    logger.info("precheckShareplex(host_name=%s, splex_port=%s, splex_prod_dir=%s, splex_sid=%s, shareplex_version=%s, db_name=%s)" % (
    host_name, splex_port, splex_prod_dir, splex_sid, shareplex_version, db_name))
    checklist = OrderedDict(
        [("Oracle user login", None),
         ("Get environment variables", None),
         ("Check Shareplex Port not existed", None),
         ("Check tablespace", None),
         ("Check splex user password in Oracle and DepotDB", None)])
    step = "Oracle user login"
    logger.info("Oracle user login start host_name=%s, splex_port=%s" % (host_name, splex_port))
    sp = None
    try:
        global lock
        # In order to avoid multiple session process same port case
        try:
            if lock.acquire():
                sp = wbxshareplexport.newInstallInstance(host_name, splex_port)
                if sp.isInstalling():
                    raise wbxexception(
                        "Port %s is alredy running on %s, so cannot continue with the setup. Exiting..." % (
                        splex_port, host_name))
                else:
                    sp.setinstallstatus(True)
        finally:
            lock.release()
        sp.login()
        sp.checkUserGroup()
        checklist[step] = True
        step = "Get environment variables"
        logger.info("Get environment variables start host_name=%s, splex_port=%s" % (host_name, splex_port))
        sp.checkInstallEnviromentConfig(splex_prod_dir, splex_sid, shareplex_version, db_name)
        step = "Check Shareplex Port not existed"
        logger.info("Check Shareplex Port not existed start host_name=%s, splex_port=%s" % (host_name, splex_port))
        sp.isPortNotExist()
        checklist[step] = True
        step = "Check tablespace"
        sp.checkTablespace()
        checklist[step] = True
        step = "Check splex user password in Oracle and DepotDB"
        sp.getInstallSplexuserPassword(db_name)
        checklist[step] = True
        # sp.preparecronjob()
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
        statuslist.append({"name": key, "status": value})
    # statuslist = [{"name":key,"status":value} for key, value in checklist.items()]

    logger.info(
        "precheckShareplex(host_name=%s, splex_port=%s, splex_prod_dir=%s, splex_sid=%s, shareplex_version=%s, db_name=%s) end with status=%s" % (
        host_name, splex_port, splex_prod_dir, splex_sid, shareplex_version, db_name, statuslist))

    return statuslist


def installShareplex(host_name, splex_port, splex_prod_dir, splex_sid, shareplex_version, db_name):
    global  lock
    logger.info("installShareplex(host_name=%s, splex_port=%s, splex_prod_dir=%s, splex_sid=%s, shareplex_version=%s, db_name=%s)" % (host_name, splex_port, splex_prod_dir, splex_sid, shareplex_version, db_name))
    statusdict = OrderedDict([
        ("Install Binary File",None),
        ("Start Blackout", None),
        ("Stop Cronjob", None),
        ("Stop Shareplex service",None),
        ("Create profile and vardir", None),
        ("orasetup_for_sid_%s" % splex_sid, None),
        ("Register CRS service", None),
        ("Start Shareplex service", None),
        ("Start cronjob for the port", None),
        ("Set shareplex parameter", None),
        ("Add to autorestart oraport shsetport", None),
        ("Start Cronjob", None),
        ("Stop Blackout", None)])

    issucceed = False
    try:
        sp = wbxshareplexport.newInstallInstance(host_name, splex_port)

        sp.login()
        step = "Install Binary File"
        PRODDIR_NAME = "shareplex%s" % shareplex_version.replace(".","")
        if not sp.isNewBinaryInstalled(PRODDIR_NAME):
            sp.installShareplexBinary(PRODDIR_NAME)
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
        step = "Create profile and vardir"
        sp.CreateProfileAndVardir()
        statusdict[step] = True
        step = "orasetup_for_sid_%s" % splex_sid
        sp.orasetup(splex_sid, splex_pwd)
        statusdict[step] = True
        step = "Register CRS service"
        sp.registerCRSService()
        # step = "Change CRS service script file"
        # sp.upgradeServiceConfig(NEW_PRODDIR_NAME)
        # statusdict[step] = True
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


