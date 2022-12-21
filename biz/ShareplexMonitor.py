import logging
from common.wbxshareplexport import wbxshareplexport
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from common.wbxexception import wbxexception
from collections import OrderedDict
import threading

logger = logging.getLogger("DBAMONITOR")

def monitorShareplexParameter():
    pass


def monitorCREnabled(dbid):
    logger.info("monitorCREnabled(dbid=%s)" % dbid)
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    db = daoManagerFactory.getDatabaseByDBID(dbid)
    portlist = []
    for channelid, spchannel in db.shareplexchanneldict.items():
        try:
            if spchannel.getSourceDBID() == dbid:
                continue
            # portlist is used to filter duplicated channel for one port
            if spchannel.port not in portlist:
                # if spchannel.port != 19007:
                #     continue
                portlist.append(spchannel.port)
                monitorCRByPort(db, spchannel.tgt_host, spchannel.port, spchannel.tgt_splex_sid)
        except Exception as e:
            raise wbxexception("Error occurred when monitorCR(%s) splex_port=%s with error msg %s" % (dbid, spchannel.port, e))

def monitorCRByPort(db, host_name, splex_port, splex_sid):
    logger.info("monitorCRByPort(host_name=%s, splex_port=%s)" % (host_name, splex_port))
    sp = wbxshareplexport.newInstance(host_name, splex_port)
    sp.isPortExist()
    sp.listParameters()
    spparam = sp.getParameterValue("SP_OPO_XFORM_EXCLUDE_ROWID")
    if spparam is not None:
        if spparam.value != "0":
            sp.setParameterValue(spparam, 0)
    sp.hasCRFile()
    sp.checkCRDBpatch(db.getdbid(), db.db_name, splex_port, splex_sid)


