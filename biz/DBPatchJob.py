import logging
import json
from datetime import datetime
import os
import xml.etree.ElementTree as ET
import re

from common.wbxutil import wbxutil
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from dao.vo.depotdbvo import DBPatchReleaseVO, DBPatchDeploymentVO, wbxdatabasemanager, ShareplexBaselineVO
from common.wbxexception import wbxexception
from wbxredis.wbxredis import wbxredis
from common.wbxcache import curcache

logger = logging.getLogger("DBAMONITOR")

class xmlparser:

    def __init__(self, xmlfile):
        self.xmlfile = xmlfile
        self.db_type_map={"WEBDB":"WEB","TAHOEDB":"TEL","OPDB":"OPDB","CONFIGDB":"CONFIG","GLOOKUPDB":"LOOKUP","TEODB":"TEO","MEDIATEDB":"MEDIATE","STREAMDB":"CSP"}
        self.dbtypemap = {}
        self.release_number = None
        self.release_name = None

    def savetodb(self, isincremental, dbid = None):
        logger.info("savetodb with dbid=%s" % dbid)
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        defaultDaoManager = daomanagerfactory.getDefaultDaoManager()
        defaultDaoManager.startTransaction()
        depotdbDao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)

        for xml_db_type, schemalist in self.dbtypemap.items():
            if xml_db_type == "SYSTOOLDB":
                continue

            appln_support_code = self.db_type_map[xml_db_type]
            for schematype, schema in schemalist.items():
                if schematype not in ['test', 'app', 'glookup', 'xxrpth']:
                    continue

                major_number = schema["release_major_num"]
                minor_number = schema["release_minor_num"]
                if isincremental:
                    prevreleasenumber = depotdbDao.getpreviousRelease(appln_support_code, schematype)

                dbpatchreleasevo = depotdbDao.getDBPatchRelease(self.release_number, appln_support_code, schematype)

                if dbpatchreleasevo is None:
                    dbpatchreleasevo = DBPatchReleaseVO(releasenumber=self.release_number,
                                                        appln_support_code=appln_support_code,
                                                        schematype=schematype,
                                                        major_number=major_number,
                                                        minor_number=minor_number,
                                                        description=self.release_name)
                    depotdbDao.addDBPatchRelease(dbpatchreleasevo)
                    logger.info("save object %s" % dbpatchreleasevo)
                    defaultDaoManager.commit()

                    dbList = daomanagerfactory.getDBListByAppCode(appln_support_code)
                    for ldb in dbList:
                        schemalist = ldb.getSchemaBySchemaType(appln_support_code, schematype)
                        db_type = ldb.db_type

                        for lschema in schemalist:
                            lschemname = lschema.schema

                            clustername = None
                            if appln_support_code == wbxdatabasemanager.APPLN_SUPPORT_CODE_WEBDB:
                                clustername = ldb.web_domain
                            elif appln_support_code == wbxdatabasemanager.APPLN_SUPPORT_CODE_TAHOEDB:
                                if schematype == wbxdatabasemanager.SCHEMATYPE_APP:
                                    domain = ldb.getDomainBySchemaname(lschemname)
                                    if domain is not None:
                                        clustername = domain.domainname
                                else:
                                    clustername = ldb.db_name
                            else:
                                clustername = ldb.db_name

                            if dbid is None or ldb.getdbid() != dbid:
                                deploystatus = "NOTDEPLOYED"
                            else:
                                deploystatus = "DEPLOYED"

                            dbpatchdeployvo = DBPatchDeploymentVO(releasenumber=self.release_number,
                                                                  appln_support_code=appln_support_code,
                                                                  db_type=db_type,
                                                                  trim_host=ldb.trim_host,
                                                                  db_name=ldb.db_name,
                                                                  schemaname=lschemname,
                                                                  schematype=schematype,
                                                                  cluster_name=clustername,
                                                                  deploytime=None,
                                                                  deploystatus=deploystatus,
                                                                  spdeploystatus="NOTDEPLOYED",
                                                                  major_number=None,
                                                                  minor_number=None
                                                                  )
                            try:
                                depotdbDao.addDBPatchDeployment(dbpatchdeployvo)
                            except Exception as e:
                                logger.error(e)
                            logger.info("insert process, insert vo  %s " % dbpatchdeployvo)

                splexdict = schema["splex"]
                for tgtdbtype, tgtschema in splexdict.items():
                    if tgtdbtype not in self.db_type_map:
                        continue

                    tgt_appln_support_code = self.db_type_map[tgtdbtype]
                    if not isincremental:
                        depotdbDao.deleteShareplexBaseline(self.release_number, appln_support_code, tgt_appln_support_code)
                    for tgt_schematype, tabdict in tgtschema.items():
                        for tabstatus, tablelist in tabdict.items():
                            for tabstr in tablelist:
                                specifiedkeys = None
                                columnfilter = None
                                specifiedcolumn = None
                                if wbxutil.isNoneString(tabstr):
                                    continue

                                tabarr = tabstr.split(';')
                                if tabstatus in ("add_table", "remove_table"):
                                    if len(tabarr) != 2:
                                        logger.error(
                                            "WBXERROR: the table %s in add_table/remove_table segment, but the length is not 2" % tabstr)
                                        continue

                                    src_tab_name = tabarr[0]
                                    tgt_tab_name = tabarr[1]
                                elif tabstatus == "add_tab_with_keyword":
                                    if len(tabarr) != 3:
                                        logger.error(
                                            "WBXERROR: the table %s in add_tab_with_keyword segment, but the length is not 3" % tabstr)
                                        continue
                                    src_tab_name = tabarr[0]
                                    tgt_tab_name = tabarr[2]
                                    specifiedkeys = tabarr[1]
                                elif tabstatus == "add_tab_with_partition":
                                    if len(tabarr) != 4:
                                        logger.error(
                                            "WBXERROR: the table %s in add_tab_with_partition segment, but the length is not 4" % tabstr)
                                        continue
                                    src_tab_name = tabarr[0]
                                    tgt_tab_name = tabarr[1]
                                else:
                                    continue

                                spvo = ShareplexBaselineVO(
                                    releasenumber=self.release_number,
                                    src_appln_support_code=appln_support_code,
                                    src_schematype=schematype,
                                    src_tablename=src_tab_name.upper(),
                                    tgt_appln_support_code=tgt_appln_support_code,
                                    tgt_application_type="PRI,GSB",
                                    tgt_schematype=tgt_schematype,
                                    tgt_tablename=tgt_tab_name.upper(),
                                    tablestatus=tabstatus,
                                    specifiedkey=specifiedkeys,
                                    columnfilter=columnfilter,
                                    specifiedcolumn=specifiedcolumn,
                                    changerelease=self.release_number
                                )
                                try:
                                    depotdbDao.addDBPatchSPChange(spvo)
                                    logger.info("insert shareplexbaselinevo %s " % spvo)
                                except Exception as e:
                                    logger.error(e)
                                logger.info("insert process, insert vo  %s " % spvo)
                defaultDaoManager.commit()
                if isincremental:
                    depotdbDao.mergeShareplexBaseline(prevreleasenumber, self.release_number, appln_support_code, schematype)
            defaultDaoManager.commit()


    # {"OPDB":{
    #          "APP":{
    #                 "release_major_num":1,
    #                 "release_minor_num":1,
    #                 "splex":{
    #                         "TEODB":[table_list],
    #                         "SYSTOOL":[table_list]}
    #                 },
    #          "XXRPTH":{
    #                   "release_major_num":1,
    #                    "release_minor_num":1,
    #                    "splex":{
    #                             "TEODB":[table_list]
    #                            }
    #                   }
    #         }
    # "WEBDB": {
    #           "wbxmaint":{
    #
    #                      }
    #       }
    # }
    def parseNode(self, node):
        for child in node:
            nodetag = child.tag
            if nodetag == "release_number":
                self.release_number=child.text
            elif nodetag == "release_name":
                self.release_name=child.text
            elif nodetag == "db":
                self.cur_db_type = child.attrib["dbtype"]
                # print(self.cur_db_type)
                if self.cur_db_type not in self.dbtypemap:
                    self.dbtypemap[self.cur_db_type]={}  #schemaList
            elif nodetag == "schema":
                self.schematype = child.attrib["type"]
                db = self.dbtypemap[self.cur_db_type]
                if self.schematype not in db:
                    self.dbtypemap[self.cur_db_type][self.schematype] = {}
                schema = self.dbtypemap[self.cur_db_type][self.schematype]
                schema["splex"] = {}
                schema["release_major_num"] = None
                schema["release_minor_num"] = None
            elif nodetag == "release_major_num":
                release_major_num = child.text
                schema = self.dbtypemap[self.cur_db_type][self.schematype]
                schema["release_major_num"] = release_major_num
            elif nodetag == "release_minor_num":
                release_minor_num = child.text
                schema = self.dbtypemap[self.cur_db_type][self.schematype]
                schema["release_minor_num"] = release_minor_num
            elif nodetag == "splex":
                self.targetdbtype = child.attrib["targetdb"]
                if self.targetdbtype  is None:
                    raise Exception("WBXERROR: the targetdb should not be None dbtype=%s, schematype=%s" % (self.cur_db_type, self.schematype))

                db = self.dbtypemap[self.cur_db_type]
                schema = db[self.schematype]
                splexmap = schema["splex"]
                if self.targetdbtype not in splexmap:
                    splexmap[self.targetdbtype] = {}
            elif nodetag == "target_schema":
                targetschematype = child.attrib["type"]
                db = self.dbtypemap[self.cur_db_type]
                schema = db[self.schematype]
                splexmap = schema["splex"]
                targetdbmap = splexmap[self.targetdbtype]
                if targetschematype not in targetdbmap:
                    targetdbmap[targetschematype] = {}
                self.targetschema = targetdbmap[targetschematype]
            elif nodetag == "replication":
                self.tablestatus = child.attrib["type"]
                # if self.tablestatus == "add_tab_with_keyword":
                #     print("test")
                self.targetschema[self.tablestatus] = []
            elif nodetag == "table":
                nodevalue = child.text
                self.targetschema[self.tablestatus].append(nodevalue)
            self.parseNode(child)

    def parsexml(self):
        if not os.path.isfile(self.xmlfile):
            logger.error("the %s does not exist" % self.xmlfile)
        try:
            self.src_appln_support_code = None
            root = ET.parse(self.xmlfile).getroot()
            self.parseNode(root)
            logger.info(json.dumps(self.dbtypemap, sort_keys=True, indent=4, separators=(', ', ': ')))

        except Exception as e:
            logger.error("Error occurred in parsexml with error msg: %s" % e)

def getdbpatchdeployment(dbid):
    daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()


# (releases, dbs, releasenumber) = showdbpatchdeployment(params)
def showdbpatchdeployment(ireleasenumber):
    # ireleasenumber = None
    # if "releasenumber" in params:
    #     ireleasenumber = params["releasenumber"]

    daomanagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daomanagerFactory.getDefaultDaoManager()
    try:
        depotDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        releaselist = depotDao.getLatestDBPatchRelease(10)
        deploydblist = None
        for rel in releaselist:
            releasenumber = rel.releasenumber
            # Get the first release deployment info by default. otherwise get the deployment info by inputted releasenumber
            if ireleasenumber is None:
                ireleasenumber = releasenumber

            if ireleasenumber == releasenumber:
                deploydblist = depotDao.getDBPatchDeploymentByReleaseNumber(releasenumber)
                break
            # maxdeploytime = rel.maxdeploytime
            # mindeploytime = rel.mindeploytime
            # expecteddbcount = rel.expecteddbcount
            # deployeddbcount = rel.deployedcount
            # dbpatchrelease = depotDao.getDBPatchReleaseByReleaseNumber(releasenumber,)
            # release = {"releasenumber": releasenumber,
            #            "firstdeploytime": mindeploytime,
            #            "latestdeploytime": maxdeploytime,
            #            "expecteddbcount":expecteddbcount,
            #            "deployeddbcount":deployeddbcount,
            #            "description": dbpatchrelease.description}
            # dbpatchreleaselist.append(release)
        depotDaoManager.commit()
        return (releaselist, deploydblist, ireleasenumber)
    except Exception as e:
        depotDaoManager.rollback()
    return (None, None, None)

def listshareplexmonitorresult():
    cache_key = "shareplex_baseline_result"
    sumlist = curcache.get(cache_key)
    if sumlist is not None:
        logger.info("listshareplexmonitorresult() get data from cache")
        return sumlist

    logger.info("listshareplexmonitorresult() get data from db")
    daomanagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daomanagerFactory.getDefaultDaoManager()
    try:
        depotDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        sumlist = depotDao.getShareplexMonitorSummary()
        tablist = depotDao.listShareplexMonitorDetail()
        detaildict = {}
        for o in tablist:
            # objdict = wbxutil.convertRowProxy2Dict(o)
            objdict = o
            key="%s_%s_%s_%s" % (objdict["src_splex_sid"],objdict["src_appln_support_code"],objdict["tgt_splex_sid"],objdict["tgt_appln_support_code"])
            if key not in detaildict:
                detaildict[key] = []
            detaildict[key].append(objdict)
        sumdict = []
        for o in sumlist:
            # objdict = wbxutil.convertRowProxy2Dict(o)
            objdict = o
            key = "%s_%s_%s_%s" % (objdict["src_splex_sid"], objdict["src_appln_support_code"], objdict["tgt_splex_sid"],objdict["tgt_appln_support_code"])
            if key in detaildict:
                detailist = detaildict[key]
            else:
                detailist = []
            objdict["detaildata"] = detailist
            if objdict["diffcnt"] != "0":
                sumdict.append(objdict)
        depotDaoManager.commit()
        curcache.add(cache_key, sumdict, 10 * 60)
        return sumdict
    except Exception as e:
        logger.error("Error occured executing listshareplexmonitorresult() %s " % e)
        depotDaoManager.rollback()
    return None

def listshareplexcrdeployment():
    daomanagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daomanagerFactory.getDefaultDaoManager()
    try:
        depotDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        crlist = depotDao.listShareplexCRDeploymentDetail()

        crdatadict = {}
        redis = wbxredis.getRedis()
        for crvo in crlist:
            if crvo.recentcrdate is None:
                continue
            keyname = "%s_%s_%s" % (crvo.trim_host, crvo.db_name, crvo.port)
            crdatelist = []
            crcntlist = []
            crdatalist = redis.lrange(keyname,10)
            if crdatalist is not None:
                for crdata in reversed(crdatalist):
                    crdate = crdata.split(":")[0]
                    crcnt = int(crdata.split(":")[1])
                    crdatelist.append(crdate)
                    crcntlist.append(crcnt)
            crdatadict[keyname] = [crdatelist, crcntlist]
        return (crlist, crdatadict)
        # return crlist
    except Exception as e:
        errormsg = "Error Ocurred: %s" % (e)
        logger.error(errormsg)
        depotDaoManager.rollback()
    return None

def getShareplexCRData(keyname):
    rc = wbxredis.getRedis()

    crlist =  rc.lrange(keyname, 30)
    datelist = []
    vallist = []
    for crvo in crlist:
        crdate = datetime.strptime(crvo.split(":")[0],"%Y%m%d")
        crcnt = int(crvo.split(":")[1])
        datelist.append(crdate)
        vallist.append(crcnt)
    return {"crdate":datelist,"crcount":vallist}

def listOSWMonitorDetail():
    daomanagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daomanagerFactory.getDefaultDaoManager()
    try:
        depotDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        oswMonitorList = depotDao.listOSWMonitorDetail()
        return oswMonitorList
    except Exception as e:
        depotDaoManager.rollback()
    return None

def listDBLinkMonitorDetail():
    daomanagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daomanagerFactory.getDefaultDaoManager()
    try:
        depotDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        dblinkMonitorList = depotDao.listDBLinkMonitorDetail()
        return dblinkMonitorList
    except Exception as e:
        depotDaoManager.rollback()
    return None

def listMeetingDataMonitorDetail():
    daomanagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daomanagerFactory.getDefaultDaoManager()
    try:
        depotDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        meetingDataList = depotDao.listMeetingDataMonitorDetail()
        return meetingDataList
    except Exception as e:
        depotDaoManager.rollback()
    return None

def loadshareplexbaseline(baseline_xml):
    if not os.path.isfile(baseline_xml):
        logger.error("%s is not a file." % baseline_xml)
        return
    parser = xmlparser(baseline_xml)
    parser.parsexml()
    parser.savetodb(True)

def loaddbpatchreleasexml(releasenumber, dbid):
    logger.info("Start to install dbpatch release %s with dbid=%s" % (releasenumber, dbid))
    wbxutil.installdbpatch(releasenumber)
    release_dir=os.path.join("/tmp", str(releasenumber))
    if not os.path.isdir(release_dir):
        logger.error("%s is not a dir. please check whether the dbpatch is installed successfully" % release_dir)
        return
    baseline_xml = os.path.join(release_dir, "release.xml")
    if not os.path.isfile(baseline_xml):
        logger.error("%s is not a file. Please check with DB Engineer" % baseline_xml)
        return
    #
    # baseline_xml="/Users/zhiwliu/Documents/office/oracle/Shareplex/Baseline/release_15886.xml"
    parser = xmlparser(baseline_xml)
    parser.parsexml()
    parser.savetodb(True, dbid)

if __name__ == "__main__":
    # baseline_xml = "/Users/zhiwliu/Documents/office/oracle/Shareplex/Baseline/release_15878.xml"
    baseline_xml = "/Users/zhiwliu/Documents/office/oracle/Shareplex/Baseline/release_15896.xml"
    # baseline_xml = "/Users/zhiwliu/Documents/office/oracle/Shareplex/Baseline/baseline_webdb_15878.xml"
    # loadshareplexbaseline(baseline_xml)
    parser = xmlparser(baseline_xml)
    parser.parsexml()
