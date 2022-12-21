import logging
from dao.wbxdaomanager import wbxdaomanagerfactory
from common.wbxexception import wbxexception
from common.wbxtask import wbxautotask,threadlocal
from dao.wbxdaomanager import DaoKeys
import datetime
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
            srcdb = daomanagerfactory.getDatabaseByDBName(self._src_db_name)
            tgtdb = daomanagerfactory.getDatabaseByDBName(self._tgt_db_name)
            if srcdb is None:
                raise wbxexception("source db %s does not exist in depot db" % (self._src_db_name))
            if tgtdb is None:
                raise wbxexception("source db %s does not exist in depot db" % (self._tgt_db_name))

            self.verifySchemalist()
            taskvo = super(wbxora2pgtask, self).initialize(**kwargs)
            jobList = self.listTaskJobsByTaskid(taskvo.taskid)
            if len(jobList) == 0:
                self.generateJobs()
        except Exception as e:
            depotDaoManager.rollback()
            raise e
        finally:
            depotDaoManager.close()
        return taskvo

    def verifySchemalist(self):
        self.verifySchemaListByDB(self._src_db_name)
        self.verifySchemaListByDB(self._tgt_db_name)

    def verifySchemaListByDB(self, db_name):
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = None
        try:
            db = daomanagerfactory.getDatabaseByDBName(db_name)
            daomanager = daomanagerfactory.getDaoManager(self._src_db_name)
            dao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            daomanager.startTransaction()
            for schema_name in self._schema_list:
                if not dao.isSchemaExist(schema_name, db.getDBVersion()):
                    raise wbxexception("The schema %s doesnot exist in db %s" %(schema_name, db_name))
            daomanager.commit()
        except Exception as e:
            if daomanager is not None:
                daomanager.rollback()
            raise e
        finally:
            if daomanager is not None:
                daomanager.close()

    # Just compare column exist or not, but should also check whether data type compatiability
    def verifyTableStructure(self):
        bresult = True
        for schema_name in self._schema_list:
            logger.info("Start to compare table structure under schema %s" % schema_name)
            srcTableDict = self.listTables(self._src_db_name, schema_name)
            tgtTableDict = self.listTables(self._tgt_db_name, schema_name)
            logger.info("There are %s tables on sourcedb and %s tables in targetdb" % (len(srcTableDict), len(tgtTableDict)))
            for table_name, tgtcoldict in tgtTableDict.items():
                if table_name not in srcTableDict:
                    logger.error("The table %s not exist in source db" % table_name)
                    bresult = False
                srccoldict = srcTableDict[table_name]
                srccolnamelist = list(srccoldict.keys())
                tgtcolnamelist = list(tgtcoldict.keys())
                if(srccolnamelist == tgtcolnamelist):
                    continue
                resset = set(srccolnamelist).difference(set(tgtcolnamelist))
                if len(resset):
                    logger.error("Column %s on source db table %s not exist in target db table" % (resset, table_name))
                    bresult = False

                resset = set(tgtcolnamelist).difference(set(srccolnamelist))
                if len(resset):
                    logger.error("Column %s on target db table %s not exist in source db table" % (resset, table_name))
                    bresult = False
            return  bresult

    def listTables(self, db_name, schema_name):
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = None
        try:
            db = daomanagerfactory.getDatabaseByDBName(db_name)
            daomanager = daomanagerfactory.getDaoManager(self._src_db_name)
            dao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            daomanager.startTransaction()
            tableDict = dao.listTableColumns(schema_name, db.getDBVendor())
            daomanager.commit()
            return tableDict
        except Exception as e:
            if daomanager is not None:
                daomanager.rollback()
            raise e
        finally:
            if daomanager is not None:
                daomanager.close()

    def listTableForMigration(self, db_name, schema_name):
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = None
        try:
            db = daomanagerfactory.getDatabaseByDBName(db_name)
            daomanager = daomanagerfactory.getDaoManager(self._src_db_name)
            dao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            daomanager.startTransaction()
            tableList = dao.listTableForMigration(schema_name, db.getDBVendor())
            daomanager.commit()
            return tableList
        except Exception as e:
            if daomanager is not None:
                daomanager.rollback()
            raise e
        finally:
            if daomanager is not None:
                daomanager.close()

    def listFilterTables(self, taskid, schemaname):
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = None
        tableList = []
        try:
            daomanager = daomanagerfactory.getDefaultDaoManager()
            dao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            daomanager.startTransaction()
            filterTableDict = dao.listFilterTables(taskid, schemaname)
            daomanager.commit()
            return filterTableDict
        except Exception as e:
            if daomanager is not None:
                daomanager.rollback()
            raise e
        finally:
            if daomanager is not None:
                daomanager.close()

    def initializeMigrateTableList(self):
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = None
        tableList = []
        try:
            for schema_name in self._schema_list:
                tgtTableDict = self.listTables(self._tgt_db_name, schema_name)
                tgtTablenameList = tgtTableDict.keys()
                tableList = self.listTableForMigration(self._src_db_name, schema_name)
                filterTableDict = self.listFilterTables(self.getTaskid(), schema_name)
                for tvo in tableList:
                    if tvo.table_name in tgtTablenameList:
                        #table level filter, not implement partition level filter
                        if tvo.table_name not in filterTableDict:
                            tableList.append(tvo)
            daomanager = daomanagerfactory.getDefaultDaoManager()
            dao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            daomanager.startTransaction()
            for tabvo in tableList:
                dao.insertOra2pgTable(tabvo.table_owner,tabvo.table_name, tabvo.partition_name, tabvo.num_rows, tabvo.priority)
            daomanager.commit()
        except Exception as e:
            if daomanager is not None:
                daomanager.rollback()
            raise e
        finally:
            if daomanager is not None:
                daomanager.close()

    def preverify(self, *args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("wbxora2pgtask.preverify(jobid=%s) start" % jobid)
        daomanager = None
        try:
            hasError = False
            bresult = self.verifyTableStructure()
            if not bresult:
                raise wbxexception("The table structure compare failed. Pls check log")
            self.initializeMigrateTableList()
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
            raise e
        finally:
            pass

    def migratedata(self,*args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("wbxora2pgtask.migratedata(jobid=%s) start" % jobid)
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = None
        try:
            hasError = False
            daomanager = daomanagerfactory.getDefaultDaoManager()
            dao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)
            serverList = dao.getAvailableSlaveServer()
            process_count = 0
            for svo in serverList:
                host_name = svo.host_name
                loginUsername = svo.username
                loginUserpwd = svo.pwd
                load = svo.load
                cpu_count = svo.cpu_count
                process_count = min(int(cpu_count * 2 - load),2)
                server = wbxssh(host_name, 22, loginUsername, loginUserpwd)
                for i in range(1,process_count):
                    try:
                        server.connect()
                        command = "nohup python3 /opt/ora2pg/ora2pgservice.py %s &" % self.getTaskid()
                        logger.info("start data migration on server %s with command %s" % (host_name, command))
                        server.execute_command(command)
                        process_count += 1
                    except Exception as e:
                        logger.error("failed to start data migration process on slave server %s" % host_name)
                    finally:
                        server.close()
            if process_count == 0:
                raise wbxexception("No availabe slave server. Pls check slave server status")
            logger.info("Start %s process to migrate data" % process_count)
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
            raise e

    def postverify_migratedata(self,*args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("wbxora2pgtask.postverify_migratedata(jobid=%s) start" % jobid)
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = None
        try:
            hasError = False
            daomanager = daomanagerfactory.getDefaultDaoManager()
            dao = daomanager.getDao(DaoKeys.DAO_ORA2PGDAO)

            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
            raise e

    def generateJobs(self):
        logger.info("generateJobs(taskid=%s, src_db_name=%s, tgt_db_name=%s)" % (self._taskid, self._src_db_name, self._tgt_db_name))
        process_order = 1
        self.addJob(db_name=self._src_db_name, job_action="preverify",stage="preverify",
                    process_order=1, execute_method="SYNC",isoneclick=True, src_db_name= self._src_db_name, tgt_db_name=self._tgt_db_name,
                    schema_list = self._schema_list)

        process_order += 1
        self.addJob(db_name=self._src_db_name, job_action="migratedata", stage="migratedata",
                    process_order=1, execute_method="SYNC", isoneclick=True, src_db_name=self._src_db_name,
                    tgt_db_name=self._tgt_db_name,
                    schema_list=self._schema_list)

        process_order += 1
        self.addJob(db_name=self._src_db_name, job_action="postverify_migratedata",
                    process_order=1, execute_method="SYNC", isoneclick=True, src_db_name=self._src_db_name,
                    tgt_db_name=self._tgt_db_name,
                    schema_list=self._schema_list)

