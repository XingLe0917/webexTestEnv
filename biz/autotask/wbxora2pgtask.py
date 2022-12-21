import logging
from dao.wbxdaomanager import wbxdaomanagerfactory
from common.wbxexception import wbxexception
from common.wbxtask import wbxautotask,threadlocal
from dao.wbxdaomanager import DaoKeys
from datetime import datetime
import time
from common.wbxutil import wbxutil
from common.wbxssh import wbxssh

logger = logging.getLogger("DBAMONITOR")

'''
This file contains the logic used for oracle2pg, pg2pg data migration
1. All pg tables must exist in source oracle db, and the table structure is exact same. 
2. Both srcdb/tgtdb have been registered into auditdb. Get migration schema list by API and show on page; 
   DBA can edit schema list on page
3. table structure verification; write data into wbxora2pgtable; start ora2pg slave process
'''
class wbxora2pgtask(wbxautotask):
    def __init__(self,taskid = None):
        super(wbxora2pgtask,self).__init__(taskid, "DATASYNCUP_TASK")

    def initialize(self, **kwargs):
        self._src_db_name = kwargs["src_db_name"].upper()
        self._schema_list = kwargs["schema_list"].split(",")
        self._tgt_db_name = kwargs["tgt_db_name"].upper()

        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daomanagerfactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            self.srcdb = daomanagerfactory.getDatabaseByDBName(self._src_db_name)
            self.tgtdb = daomanagerfactory.getDatabaseByDBName(self._tgt_db_name)
            #1 check whether db is in factory
            if self.srcdb is None:
                raise wbxexception("source db %s does not exist in depot db" % (self._src_db_name))
            if self.tgtdb is None:
                raise wbxexception("tgtdb db %s does not exist in depot db" % (self._tgt_db_name))

            #2 check whether the dbserver can be connected
            for srcsvrname, srcdbserver in self.srcdb.getServerDict().items():
                srcdbserver.verifyConnection()

            for tgtsvrname, tgtdbserver in self.tgtdb.getServerDict().items():
                tgtdbserver.verifyConnection()

            taskvo = super(wbxora2pgtask, self).initialize(**kwargs)
            jobList = self.listTaskJobsByTaskid(taskvo.taskid)
            if len(jobList) == 0:
                self.generateJobs()
                self.initializeMigrateTableList(taskvo.taskid)
        except Exception as e:
            depotDaoManager.rollback()
            raise e
        finally:
            depotDaoManager.close()
        return taskvo

    def generateJobs(self):
        logger.info("generateJobs(taskid=%s, src_db_name=%s, tgt_db_name=%s)" % (self._taskid, self._src_db_name, self._tgt_db_name))
        self.addJob(db_name=self._src_db_name, job_action="preverify",stage="preverify", process_order=1, execute_method="SYNC",
                    isoneclick=True, src_db_name= self._src_db_name, tgt_db_name=self._tgt_db_name, schema_list = self._schema_list)

        self.addJob(db_name=self._src_db_name, job_action="checktablestructure", stage="checktablestructure", process_order=2,
                    execute_method="SYNC", isoneclick=True, src_db_name=self._src_db_name, tgt_db_name=self._tgt_db_name)

        self.addJob(db_name=self._src_db_name, job_action="migratedata", stage="migratedata", process_order=3, execute_method="SYNC",
                    isoneclick=True, src_db_name=self._src_db_name, tgt_db_name=self._tgt_db_name, schema_list=self._schema_list)

        self.addJob(db_name=self._src_db_name, job_action="postverify",process_order=4, execute_method="SYNC",
                    isoneclick=True, src_db_name=self._src_db_name, tgt_db_name=self._tgt_db_name, schema_list=self._schema_list)
        logger.info("generateCutoverStep end with successed")

    def initializeMigrateTableList(self,taskid):
        logger.info("begin to initialize WBXORA2PGTABLE table with taskid:%s ..." %taskid )
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = None
        tableList = []
        try:
            for schema_name in self._schema_list:
                tabList = self.srcdb.listTableWithSchema(schema_name)
                for tvo in tabList:
                    tvo.setTaskid(taskid)
                    tableList.append(tvo)

            daomanager = daomanagerfactory.getDefaultDaoManager()
            dao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            daomanager.startTransaction()
            dao.batchInsertOra2pgTable(*tableList)
            daomanager.commit()
            logger.info(" initialize WBXORA2PGTABLE table with taskid:%s end" % taskid)
        except Exception as e:
            if daomanager is not None:
                daomanager.rollback()
            raise e
        finally:
            if daomanager is not None:
                daomanager.close()

    def preverify(self, *args):
        # check whether ora2pgtable table_status  has pending
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("wbxora2pgtask.preverify(jobid=%s) begin..." % jobid)
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        try:
            daomanager = daomanagerfactory.getDefaultDaoManager()
            autotaskdao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
            daomanager.startTransaction()
            jobvo = autotaskdao.getAutoTaskJobByJobid(jobid)
            taskid = jobvo.taskid

            ora2pgdao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            tbvolist=ora2pgdao.getOra2pgTaskTableByTabStatus(taskid,"PENDING")
            for tbvo in tbvolist:
                logger.error("tableowner: %s, tablename: %s , tablestatus:%s" %(tbvo.table_owner,tbvo.table_name,tbvo.table_status))
            if len(tbvolist) >1:
                raise wbxexception("There are Ora2pgTaskTable still has records in pending state")
            daomanager.commit()
            logger.info(" wbxora2pgtask.preverify(jobid=%s) end." % (jobid))
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            self.updateJobStatus(jobid, "FAILED")
            if daomanager is not None:
                daomanager.rollback()
            raise e
        finally:
            if daomanager is not None:
                daomanager.close()

    def checktablestructure(self,*args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("wbxora2pgtask.checktablestructure(jobid=%s) begin..." % jobid)
        hasError = False
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        table_upd_list= []
        try:
            daomanager = daomanagerfactory.getDefaultDaoManager()
            daomanager.startTransaction()

            autotaskdao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
            jobvo = autotaskdao.getAutoTaskJobByJobid(jobid)
            taskid = jobvo.taskid

            ora2pgdao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            for schema_name in self._schema_list:
                schema_name_upper=schema_name.upper()
                tbvolist = ora2pgdao.getOra2pgTaskTabWithStatus(taskid,schema_name_upper , "READY")
                migratetable_dict= {}
                for tbvo in tbvolist:
                    if tbvo.table_name not in migratetable_dict:
                        migratetable_dict[tbvo.table_name]=[]
                    migratetable_dict[tbvo.table_name].append(tbvo)
                if not len(tbvolist):
                    logger.info("There is no READY migrate table found with %s schema(taskid:%s)" %(schema_name_upper,taskid))
                    continue
                #1 get migration tables with shema
                logger.info("Start to compare table structure under schema %s" % schema_name)
                tablename_list=migratetable_dict.keys()
                srcTableDict = self.srcdb.listTableStructureWithSchema(schema_name,*tablename_list)
                tgtTableDict = self.tgtdb.listTableStructureWithSchema(schema_name,*tablename_list)
                logger.info("There are %s tables on sourcedb and %s tables in targetdb" % (len(srcTableDict), len(tgtTableDict)))
                for table_name, srccoldict in srcTableDict.items():
                    # 1 check for difference between source and target migration table
                    if table_name not in tgtTableDict:
                        logger.error("The table %s not exist in source db" % table_name)
                        if table_name not in table_upd_list:
                            table_upd_list.append(table_name)
                        continue
                    # 2 check for difference between source and target migration table structure
                    tgtcoldict = tgtTableDict[table_name]
                    tgtcolnamelist = list(tgtcoldict.keys())
                    srccolnamelist = list(srccoldict.keys())
                    if (srccolnamelist == tgtcolnamelist):
                        continue
                    resset = set(srccolnamelist).difference(set(tgtcolnamelist))
                    if len(resset):
                        logger.error("Column %s on source db table %s not exist in target db table" % (resset, table_name))
                        if table_name not in table_upd_list:
                            table_upd_list.append(table_name)

                    resset = set(tgtcolnamelist).difference(set(srccolnamelist))
                    if len(resset):
                        logger.error("Column %s on target db table %s not exist in source db table" % (resset, table_name))
                        if table_name not in table_upd_list:
                            table_upd_list.append(table_name)
                if table_upd_list is not None :
                    tabvolist=[]
                    for table_name in table_upd_list:
                        tabvolist.extend(migratetable_dict[table_name])
                    tabidlist=[tbvo.tableid for tbvo in tabvolist]
                    ora2pgdao.updBatchOra2pgTabStatusByTableID(taskid, "ERROR", *tabidlist)
                    hasError=True
            if hasError:
                raise wbxexception("table structure are different from source and target database")
            self.updateJobStatus(jobid, "SUCCEED")
            logger.info("wbxora2pgtask.checktablestructure(jobid=%s) end with succeed." % jobid)
        except Exception as e:
            self.updateJobStatus(jobid, "FAILED")
            if daomanager is not None:
                daomanager.rollback()
            raise e
        finally:
            if daomanager is not None:
                daomanager.close()

    def postverify(self,*args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("wbxora2pgtask.postverify_migratedata(jobid=%s) start" % jobid)
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = None
        try:
            daomanager = daomanagerfactory.getDefaultDaoManager()
            autotaskdao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
            jobvo = autotaskdao.getAutoTaskJobByJobid(jobid)
            taskid = jobvo.taskid

            ora2pgdao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            migrate_schemalist={}
            for schema_name in self._schema_list:
                schema_name_upper=schema_name.upper()
                tbvolist = ora2pgdao.getOra2pgTaskTabWithStatus(taskid,schema_name_upper , "READY")
                migrate_schemalist[schema_name] = tbvolist
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
            if daomanager is not None:
                daomanager.rollback()
            raise e
        finally:
            if daomanager is not None:
                daomanager.close()
        # 1 check for difference form source and target table row numcount
        # for schema in migrate_schemalist.keys():


