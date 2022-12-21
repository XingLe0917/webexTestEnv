import datetime
import logging

import cx_Oracle
import datetime as datetime
import paramiko
from cx_Oracle import DatabaseError
from sqlalchemy import create_engine

from biz.Adbmon import adbmoncheck_global
from biz.TaskLog import get_db_tns
from common.wbxtask import wbxautotask
from common.wbxcache import getTaskFromCache
from biz.dbmanagement.wbxdbshareplexport import wbxdbshareplexport
from common.wbxutil import wbxutil
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys
from common.wbxexception import wbxexception
import threading
from collections import OrderedDict
from biz.dbmanagement.wbxdbuser import wbxdbuser

logger = logging.getLogger("DBAMONITOR")

dblinkName="TO_BASELINE_TAHOE"
to_configdb_dblink="TO_CONFIGDBHA_AUTO"
schemaname="system"
schemapwd='sysnotallow'
directory_name = "EXPDP_DIR"
directory_path= '/staging/expdp/test'

# It must gurantee pridb/gsbdb exist in depotdb and loaded into ccp memory
class tahoebuildtask(wbxautotask):
    def __init__(self, taskid = None):
        super(tahoebuildtask,self).__init__(taskid, "TAHOEBUILD_TASK")
        self._base_host_name =None
        self._base_db_name =None
        self._pri_host_name = None
        self._pri_db_name = None
        self._pri_pool_name = None
        self._gsb_host_name = None
        self._gsb_db_name = None
        self._gsb_pool_name = None
        self._port_for_configdb = None
        self._host_name_for_config= None
        self._port_for_other = None
        self._new_tahol_schema_name=None
        self._directory_name=None
        self._directory_path= None

    def initialize(self, **kwargs):
        self._base_host_name = kwargs["base_host_name"]
        self._base_db_name = str(kwargs["base_db_name"]).upper()
        self._pri_host_name = kwargs["pri_host_name"]
        self._pri_db_name = str(kwargs["pri_db_name"]).upper()
        self._pri_pool_name = kwargs["pri_pool_name"]
        self._gsb_host_name = kwargs["gsb_host_name"]
        self._gsb_db_name = str(kwargs["gsb_db_name"]).upper()
        self._gsb_pool_name = kwargs["gsb_pool_name"]
        self._port_for_configdb = kwargs["port_for_configdb"]
        self._port_for_other = kwargs["port_for_other"]
        self._pri_schemaname = "tahoe%s" % "".join(filter(str.isnumeric, self._pri_pool_name))
        self._gsb_schemaname = "tahoe%s" % "".join(filter(str.isnumeric, self._gsb_pool_name))
        self._new_tahol_schema_name = kwargs["new_tahol_schema_name"]
        self._directory_name=directory_name
        self._directory_path=directory_path
        if self._pri_schemaname != self._gsb_schemaname:
            raise wbxexception("The primary pool name does not map to gsb pool name")

        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        baseserver = daomanagerfactory.getServer(self._base_host_name)
        basedb = daomanagerfactory.getDatabaseByDBName(self._base_db_name)
        priserver = daomanagerfactory.getServer(self._pri_host_name)
        gsbserver = daomanagerfactory.getServer(self._gsb_host_name)
        pridb = daomanagerfactory.getDatabaseByDBName(self._pri_db_name)
        gsbdb = daomanagerfactory.getDatabaseByDBName(self._gsb_db_name)

        if baseserver is None:
            raise wbxexception("Do not find the server with host_name=%s" % self._base_host_name)
        if priserver is None:
            raise wbxexception("Do not find the server with host_name=%s" % self._pri_host_name)
        if gsbserver is None:
            raise wbxexception("Do not find the server with host_name=%s" % self._gsb_host_name)
        if basedb is None:
            raise wbxexception("Do not find the db with db_name=%s" % self._base_db_name)
        if pridb is None:
            raise wbxexception("Do not find the db with db_name=%s" % self._pri_db_name)
        if gsbdb is None:
            raise wbxexception("Do not find the db with db_name=%s" % self._gsb_db_name)

        taskvo = super(tahoebuildtask, self).initialize(**kwargs)
        jobList = self.listTaskJobsByTaskid(self._taskid)
        if len(jobList) == 0:
            self.generateJobs()
        return taskvo

    def generateJobs(self):
        self.addJob(host_name=self._pri_host_name, db_name=self._pri_db_name, job_action="preverify", process_order=1, execute_method="SYNC")
        self.addJob(host_name=self._pri_host_name, db_name=self._pri_db_name, job_action="createtestschema", process_order=2,execute_method="SYNC")
        self.addJob(host_name=self._pri_host_name, db_name=self._pri_db_name, job_action="createtahoeschema", process_order=3,execute_method="SYNC")
        self.addJob(host_name=self._pri_host_name, db_name=self._pri_db_name, job_action="shareplexConfigCONFIG2TEL", process_order=4,execute_method="SYNC")
        self.addJob(host_name=self._pri_host_name, db_name=self._pri_db_name, job_action="shareplexConfigTEL2TEL", process_order=5,execute_method="SYNC")
        self.addJob(host_name=self._pri_host_name, db_name=self._pri_db_name, job_action="shareplexConfigTEL2OPDB", process_order=6,execute_method="SYNC")
        # self.addJob(host_name=self._pri_host_name, db_name=self._pri_db_name, job_action="shareplexConfigTEL2TOOLS", process_order=7,execute_method="SYNC")
        # self.addJob(host_name=self._pri_host_name, db_name=self._pri_db_name, job_action="assignPasscode",process_order=8, execute_method="SYNC")
        # self.addJob(host_name=self._pri_host_name, db_name=self._pri_db_name, job_action="addMonitor",process_order=9, execute_method="SYNC")
        # self.addJob(host_name=self._pri_host_name, db_name=self._pri_db_name, job_action="postVerification", process_order=10,execute_method="SYNC")
        self.addJob(host_name=self._pri_host_name, db_name=self._pri_db_name, job_action="addMonitor", process_order=7,execute_method="SYNC")
        self.addJob(host_name=self._pri_host_name, db_name=self._pri_db_name, job_action="postVerification",process_order=8, execute_method="SYNC")

    # STEP 1  preverify
    #
    # 1. check baseline/pri/gsb server can be connected
    # 2. find CONFIGDB/GCFGDB host name with input param <port_for_configdb>
    # 3. check port with input param <port_for_other> is exist on pri/gsb
    # 4. find OPDB host name with input param <port_for_other>
    # 5. find TOOL host name for input param <port_for_other>
    # 6. check spareplex user password in depot
    def preverify(self, *args):
        jobid = args[0]
        checkFlag = True
        try:
            logger.info("preverify for tahoe build task with pri_host_name=%s gsb_host_name=%s" % (self._pri_host_name,self._gsb_host_name))
            self.updateJobStatus(jobid, "RUNNING")
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            base_dbserver = daomanagerfactory.getServer(hostname=self._base_host_name)
            pri_dbserver = daomanagerfactory.getServer(hostname=self._pri_host_name)
            gsb_dbserver = daomanagerfactory.getServer(hostname=self._gsb_host_name)
            try:
                base_dbserver.connect()
            except Exception as e:
                raise wbxexception(
                    "cannot login the base line dbserver %s with %s" % (base_dbserver.host_name, base_dbserver.login_user))
            try:
                pri_dbserver.connect()
            except Exception as e:
                raise wbxexception("cannot login the pri server %s with %s" % (pri_dbserver.host_name,pri_dbserver.login_user))
            try:
                gsb_dbserver.connect()
            except Exception as e:
                raise wbxexception("cannot login the gsb server %s with %s" % (gsb_dbserver.host_name,gsb_dbserver.login_user))

            daoManager = daomanagerfactory.getDefaultDaoManager()
            try:
                daoManager.startTransaction()
                depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                items = depotdbDao.getRandomDBHostName()
                for item in items:
                    vo = dict(item)
                    if vo['db_name'] == 'CONFIGDB':
                        random_config_host_name = vo['host_name']
                    if vo['db_name'] == 'RACPSYT':
                        random_tool_host_name = vo['host_name']
                    if vo['db_name'] == 'RACOPDB':
                        random_opdb_host_name = vo['host_name']
                    if vo['db_name'] == 'GCFGDB':
                        random_gcfgdb_host_name = vo['host_name']
            except Exception as e:
                daoManager.rollback()
                logger.error("find config DB host name fail", exc_info=e, stack_info=True)

            try:
                logger.info("*********** find CONFIGDB host name for port_for_configdb :{0} **************" .format(self._port_for_configdb))
                config_host_name = self.findHostNameByPort(random_config_host_name,self._port_for_configdb)
                if config_host_name == "":
                    checkFlag = False
                    logger.error("Do not found host name for port:{0} on CONFIGDB" .format(self._port_for_configdb))
            except Exception as e:
                raise wbxexception(e)

            try:
                logger.info("*********** find GCFGDB host name for port_for_configdb :{0} **************" .format(self._port_for_configdb))
                gcfgdb_host_name = self.findHostNameByPort(random_gcfgdb_host_name,self._port_for_configdb)
                if gcfgdb_host_name == "":
                    checkFlag = False
                    logger.error("Do not found host name for port:{0} on GCFGDB" .format(self._port_for_configdb))
            except Exception as e:
                raise wbxexception(e)

            try:
                host_name_for_port = self.findHostNameByPort(self._pri_host_name, self._port_for_configdb)
                if host_name_for_port != self._pri_host_name:
                    checkFlag = False
                    logger.error("port:{0} not in {1}, please check it.".format(self._port_for_configdb,self._pri_host_name))
            except Exception as e:
                raise wbxexception(e)

            try:
                host_name_for_port = self.findHostNameByPort(self._gsb_host_name, self._port_for_configdb)
                if host_name_for_port != self._gsb_host_name:
                    checkFlag = False
                    logger.error("port:{0} not in {1}, please check it.".format(self._port_for_configdb, self._gsb_host_name))
            except Exception as e:
                raise wbxexception(e)

            try:
                host_name_for_port = self.findHostNameByPort(self._pri_host_name,self._port_for_other)
                if host_name_for_port != self._pri_host_name:
                    logger.error("port:{0} not in {1}, please check it. " .format(self._port_for_other,self._pri_host_name))
                    checkFlag = False
            except Exception as e:
                raise wbxexception(e)

            try:
                host_name_for_port = self.findHostNameByPort(self._gsb_host_name,self._port_for_other)
                if host_name_for_port != self._gsb_host_name:
                    logger.error("port:{0} not in {1}, please check it. " .format(self._port_for_other,self._gsb_host_name))
                    checkFlag = False
            except Exception as e:
                raise wbxexception(e)

            try:
                logger.info("*********** find OPDB host name for port_for_other :{0} **************" .format(self._port_for_other))
                opdb_host_name = self.findHostNameByPort(random_opdb_host_name,self._port_for_other)
                if opdb_host_name == "":
                    checkFlag = False
                    logger.error("Do not found host name for port:{0} on OPDB" .format(self._port_for_other))
            except Exception as e:
                raise wbxexception(e)

            # try:
            #     logger.info("*********** find TOOL host name for port_for_other :{0} **************" .format(self._port_for_other))
            #     tool_host_name = self.findHostNameByPort(random_tool_host_name,self._port_for_other)
            #     if tool_host_name == "":
            #         checkFlag = False
            #         logger.error("Do not found host name for port:{0} on TOOL" .format(self._port_for_other))
            # except Exception as e:
            #     raise wbxexception(e)

            try:
                logger.info("*********** check DB supplemental logging, db_name: {0} **************".format(self._pri_db_name))
                SupplementalLog_flag = self.checkDBSupplementalLogging(self._pri_db_name)
                if not SupplementalLog_flag:
                    checkFlag = False
                    raise wbxexception("check DB supplemental logging have issue")
            except Exception as e:
                raise wbxexception(e)

            try:
                logger.info("*********** check DB supplemental logging, db_name: {0} **************".format(self._gsb_db_name))
                SupplementalLog_flag = self.checkDBSupplementalLogging(self._gsb_db_name)
                if not SupplementalLog_flag:
                    checkFlag = False
                    raise wbxexception("check DB supplemental logging have issue")
            except Exception as e:
                raise wbxexception(e)

            pri_info = None
            gsb_info = None
            logger.info("*********** check if shareplex user password exists in depot **************")
            try:
                daoManager.startTransaction()
                depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                schema = "splex" + str(self._port_for_other)
                pri_info = depotdbDao.getPwdByUserDB(self._pri_db_name, schema)
                gsb_info = depotdbDao.getPwdByUserDB(self._gsb_db_name, schema)
                if len(pri_info)==0:
                    checkFlag = False
                    # logger.error("Do not found schema={0},db_name={1} in appln_pool_info" .format(schema,self._pri_db_name))
                    raise wbxexception("Do not found schema={0},db_name={1} in appln_pool_info" .format(schema,self._pri_db_name))
                if len(gsb_info)==0:
                    checkFlag = False
                    # logger.error("Do not found schema={0},db_name={1} in appln_pool_info" .format(schema,self._gsb_db_name))
                    raise wbxexception("Do not found schema={0},db_name={1} in appln_pool_info" .format(schema,self._gsb_db_name))
                daoManager.commit()
            except Exception as e:
                daoManager.rollback()
                logger.error("get spareplex user password in depot fail", exc_info=e, stack_info=True)
                raise wbxexception(e)

            logger.info("*********** check if shareplex user password is correct **************")
            if checkFlag:
                daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
                try:
                    daomanager = daomanagerfactory.getDaoManager(self._pri_db_name, pri_info[0]['schema'])
                    dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                    sysdate = dao.testConnect()
                    logger.info("db_name={0}, schema={1}, sysdate={2} is OK" .format(self._pri_db_name,pri_info[0]['schema'],sysdate))
                    if len(sysdate) == 0:
                        checkFlag = False
                        raise wbxexception("schema={0}, password in {1} is incorrect, please check" .format(pri_info[0]['schema']),self._pri_db_name)

                    daomanager_2 = daomanagerfactory.getDaoManager(self._gsb_db_name, gsb_info[0]['schema'])
                    dao_2 = daomanager_2.getDao(DaoKeys.DAO_DEPOTDBDAO)
                    sysdate2 = dao_2.testConnect()
                    logger.info("db_name={0}, schema={1}, sysdate={2} is OK".format(self._gsb_db_name, gsb_info[0]['schema'],sysdate2))
                    if len(sysdate) == 0:
                        checkFlag = False
                        raise wbxexception("schema={0}, password in {1} is incorrect, please check".format(gsb_info[0]['schema']), self._gsb_db_name)

                    daomanager.commit()
                except Exception as e:
                    daomanager.rollback()
                    raise wbxexception("check spareplex user password in depot fail", exc_info=e, stack_info=True)

            logger.info("********************************************************* ")
            if checkFlag:
                # save host_name_for_config/opdb_host_name/tool_host_name
                # description = "config_host_name=%s,opdb_host_name=%s,tool_host_name=%s,gcfgdb_host_name=%s" % (
                # config_host_name, opdb_host_name, tool_host_name,gcfgdb_host_name)
                description = "config_host_name=%s,opdb_host_name=%s,gcfgdb_host_name=%s" % (
                    config_host_name, opdb_host_name, gcfgdb_host_name)
                logger.info("port_for_configdb=%s,config_host_name=%s" % (self._port_for_configdb, config_host_name))
                logger.info("port_for_other=%s,opdb_host_name=%s" % (self._port_for_other, opdb_host_name))
                # logger.info("port_for_other=%s,tool_host_name=%s" % (self._port_for_other, tool_host_name))
                logger.info("port_for_other=%s,gcfgdb_host_name=%s" % (self._port_for_other, gcfgdb_host_name))
                self.updateJob_host_name_for_config(jobid, description)

                try:
                    logger.info("*********** create db link pri->baseline tahoe *********** ")
                    self.create_db_link_to_baseline_by_one(self._base_host_name, self._base_db_name, self._pri_host_name,
                                                           self._pri_db_name)
                except Exception as e:
                    raise wbxexception(e)

                try:
                    logger.info("*********** create db link gsb->baseline tahoe *********** ")
                    self.create_db_link_to_baseline_by_one(self._base_host_name, self._base_db_name, self._gsb_host_name,
                                                           self._gsb_db_name)
                except Exception as e:
                    raise wbxexception(e)
                self.updateJobStatus(jobid, "SUCCEED")
            else:
                logger.error("preverify fail, please check parameter again.")
                self.updateJobStatus(jobid, "FAILED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")


    # STEP 2 create test schema
    #
    # 1. check whether the test user and test table exist
    #   if test user exist but table not exist then return fail immediately, please drop the user and try again
    #   if both test user exist and table are exist then return success directly (do not do the next steps)
    # 2. if check step 1 ok (no exist test user)
    #   create test schema config --> pri (refer to :createtestschema_by_one)
    #   create test schema config --> gsb (refer to :createtestschema_by_one)
    def createtestschema(self,*args):
        jobid = args[0]
        logger.info("createtestschema")
        try:
            jobvo = self.updateJobStatus(jobid, "RUNNING")
            logger.info("check whether the test user and test table exist ")
            status1 = ""
            status2 = ""
            try:
                status1 = self.checkUserAndTableExist(self._pri_db_name,"TEST")
                if status1 == "01" or status1 == "02":
                    raise wbxexception("user TEST already exists in {0}, please drop the user TEST and try again." .format(self._pri_host_name))
                status2 = self.checkUserAndTableExist(self._gsb_db_name, "TEST")
                if status2 == "01" or status2 == "02":
                    raise wbxexception("user TEST already exists in {0}, please drop the user TEST and try again." .format(self._gsb_host_name))
            except Exception as e:
                raise wbxexception(e)

            if status1 == "00":
                try:
                    logger.info("************ start create tests chema by one (base line --> pri) ************")
                    self.createtestschema_by_one(self._base_db_name,self._base_host_name,self._pri_host_name,self._pri_db_name,"pri")
                except Exception as e:
                    raise wbxexception(e)

            logger.info("\n\n")
            if status2 == "00":
                try:
                    logger.info("************ start create test schema by one (base line --> gsb) ************")
                    self.createtestschema_by_one(self._base_db_name,self._base_host_name,self._gsb_host_name,self._gsb_db_name,"gsb")
                except Exception as e:
                    raise wbxexception(e)
            logger.info("createtestschema Done")
            self.updateJobStatus(jobid, "SUCCEED")

        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")


    # STEP 3 create tahoe schema
    #
    # 1. check whether the <new_tahol_schema_name> user and <new_tahol_schema_name> table exist
    #   if <new_tahol_schema_name> user exist but table not exist then return fail immediately, please drop the user <new_tahol_schema_name> and try again
    #   if both <new_tahol_schema_name> user exist and table are exist then return success directly (do not do the next steps)
    # 2. if check step 1 ok (no exist <new_tahol_schema_name> user)
    #   create tahoe schema config --> pri (refer to :createtahoeschema_by_one)
    #   create tahoe schema config --> gsb (refer to :createtahoeschema_by_one)
    def createtahoeschema(self,*args):
        jobid = args[0]
        logger.info("createtahoeschema")
        try:
            jobvo = self.updateJobStatus(jobid, "RUNNING")

            logger.info("check whether the test user and test table exist ")
            status1 = ""
            status2 = ""
            try:
                status1 = self.checkUserAndTableExist(self._pri_db_name, self._new_tahol_schema_name)
                if status1 == "01" or status1 == "02":
                    raise wbxexception("user {0} already exists in {1}, please drop the user {2} and try again.".format(self._new_tahol_schema_name,
                        self._pri_host_name,self._new_tahol_schema_name))
                if status1 == "03":
                    logger.info("{0} schema in {1} already exist, do nothing.".format(self._new_tahol_schema_name,self._pri_db_name))

                status2 = self.checkUserAndTableExist(self._gsb_db_name, self._new_tahol_schema_name)
                if status2 == "01" or status2 == "02":
                    raise wbxexception("user {0} already exists in {1}, please drop the user {2} and try again.".format(self._new_tahol_schema_name,
                        self._gsb_host_name,self._new_tahol_schema_name))
                if status2 == "03":
                    logger.info("{0} schema in {1} already exist, do nothing.".format(self._new_tahol_schema_name,self._gsb_db_name))

            except Exception as e:
                raise wbxexception(e)

            if status1 == "00":
                try:
                    logger.info("************ start create tahoe schema by one (base line --> pri) ************")
                    self.createtahoeschema_by_one(self._base_db_name,self._base_host_name,
                                                  self._pri_host_name,self._pri_db_name,"pri",self._new_tahol_schema_name,self._pri_pool_name)
                except Exception as e:
                    raise wbxexception(e)

            if status2 =="00":
                try:
                    logger.info("************ start create tahoes chema by one (base line --> gsb) ************")
                    self.createtahoeschema_by_one(self._base_db_name, self._base_host_name,
                                                  self._gsb_host_name,self._gsb_db_name,"gsb",self._new_tahol_schema_name,self._gsb_pool_name)
                except Exception as e:
                    raise wbxexception(e)

            logger.info("createtahoeschema Done")
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")

    # check user and table exist
    # check <user> is exist, if exist then random check one table with <user>
    #
    # return
    # 00: OK (no user)
    # 01: user exist but not table with user
    # 02: both user and table are exist
    # 03: user already exist in the db and tables are created and have table records
    def checkUserAndTableExist(self, db_name, username):
        username = username.upper()
        logger.info("check user:{0} and table on db_name:{1} ".format(username, db_name))
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        db = daomanagerfactory.getDatabaseByDBName(db_name)
        connUrl = None
        try:
            connUrl = db.getConnectionURL()
        except DatabaseError as e:
            logger.error("Can not getConnectionURL to db %s" % (db_name))
            raise wbxexception("Can not getConnectionURL to db %s" % (db_name))

        connect = cx_Oracle.connect("sys/sysnotallow@" + connUrl, mode=cx_Oracle.SYSDBA)
        try:
            cursor = connect.cursor()
            sql = "select * from dba_users where username ='%s'" %(username)
            cursor.execute(sql)
            result = cursor.fetchall()
            if len(result)==0:
                logger.info("The user %s does not exist in the db %s" %(username, db_name))
                return "00"
            else:
                sql = "SELECT count(1) FROM dba_tables WHERE owner='%s'" % (username)
                cursor.execute(sql)
                result2 = cursor.fetchall()
                if len(result2) == 0:
                    logger.info("The user %s already exist in the db %s, but no tables" % (username, db_name))
                    return "01"
                sql = "select num_rows from (select table_name,num_rows,last_analyzed from dba_tables where owner = '%s' order by num_rows desc) where rownum = 1" % (username)
                cursor.execute(sql)
                result2 = cursor.fetchall()
                if len(result2) == 0:
                    logger.info("The user %s already exist and tables are created in db %s, but no data" % (username, db_name))
                    return "02"
                logger.info("The user %s already exist in the db %s, tables are created and already intialize data, do nothing..." % (username, db_name))
                return "03"
        except Exception as e:
            logger.error(str(e), exc_info=e)
            raise wbxexception("grant fail, sql {0}".format(sql))
        finally:
            connect.close()

    # STEP 4
    # CONFIG2TEL
    def shareplexConfigCONFIG2TEL(self, *args):
        jobid = args[0]
        logger.info("shareplex Config Channel CONFIG2TEL ")
        try:
            jobvo = self.updateJobStatus(jobid, "RUNNING")
            config_host_name,opdb_host_name,gcfgdb_host_name = self.getJob_host_name_for_config(jobid)
            if config_host_name:
                # config -> pri
                result2 = self.shareplexConfigSrcToTgt(src_appln_support_code="CONFIG", tgt_appln_support_code="TEL",
                                                       src_db="CONFIGDB", src_host_name=config_host_name,
                                                       tgt_db=self._pri_db_name, tgt_host_name=self._pri_host_name,
                                                       port=self._port_for_configdb,
                                                       src_pool_name="",
                                                       tgt_pool_name=self._pri_pool_name,
                                                       opt_host_name=config_host_name, type="CONFIG2TEL",
                                                       new_tahol_schema_name=self._new_tahol_schema_name,
                                                       replication_to="cfg_ptahoe")

                # config -> gsb
                result = self.shareplexConfigSrcToTgt(src_appln_support_code="CONFIG", tgt_appln_support_code="TEL",
                                                      src_db="CONFIGDB", src_host_name=config_host_name,
                                                      tgt_db=self._gsb_db_name, tgt_host_name=self._gsb_host_name,
                                                      port=self._port_for_configdb,
                                                      src_pool_name="",
                                                      tgt_pool_name=self._gsb_pool_name,
                                                      opt_host_name=config_host_name,type="CONFIG2TEL",
                                                      new_tahol_schema_name=self._new_tahol_schema_name,replication_to="cfg_gtahoe")
                # gcf config  -> pri
                result4 = self.shareplexConfigSrcToTgt(src_appln_support_code="CONFIG", tgt_appln_support_code="TEL",
                                                       src_db="GCFGDB", src_host_name=gcfgdb_host_name,
                                                       tgt_db=self._pri_db_name, tgt_host_name=self._pri_host_name,
                                                       port=self._port_for_configdb,
                                                       src_pool_name="",
                                                       tgt_pool_name=self._pri_pool_name,
                                                       opt_host_name=gcfgdb_host_name, type="CONFIG2TEL",
                                                       new_tahol_schema_name=self._new_tahol_schema_name,
                                                       replication_to="gcfg_ptahoe")

                # gcf config -> gsb
                result3 = self.shareplexConfigSrcToTgt(src_appln_support_code="CONFIG", tgt_appln_support_code="TEL",
                                                      src_db="GCFGDB", src_host_name=gcfgdb_host_name,
                                                      tgt_db=self._gsb_db_name, tgt_host_name=self._gsb_host_name,
                                                      port=self._port_for_configdb,
                                                      src_pool_name="",
                                                      tgt_pool_name=self._gsb_pool_name,
                                                      opt_host_name=gcfgdb_host_name, type="CONFIG2TEL",
                                                      new_tahol_schema_name=self._new_tahol_schema_name,replication_to="gcfg_gtahoe")

            else:
                logger.error("not found config host name ")
                self.updateJobStatus(jobid, "FAILED")
            if result and result2 and result3 and result4:
                self.updateJobStatus(jobid, "SUCCEED")
            else:
                self.updateJobStatus(jobid, "FAILED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")

    # STEP 5
    # TEL2TEL
    def shareplexConfigTEL2TEL(self, *args):
        jobid = args[0]
        logger.info("shareplex Config Channel TEL2TEL")
        try:
            jobvo = self.updateJobStatus(jobid, "RUNNING")
            result = self.shareplexConfigSrcToTgt(src_appln_support_code="TEL", tgt_appln_support_code="TEL",
                                                  src_db=self._gsb_db_name, src_host_name=self._gsb_host_name,
                                                  tgt_db=self._pri_db_name, tgt_host_name=self._pri_host_name,
                                                  port=self._port_for_other,
                                                  src_pool_name=self._gsb_pool_name,
                                                  tgt_pool_name=self._pri_pool_name,
                                                  opt_host_name=self._gsb_host_name, type="TEL2TEL",
                                                  new_tahol_schema_name=self._new_tahol_schema_name,
                                                  replication_to="gtahoe_ptahoe")

            result2 = self.shareplexConfigSrcToTgt(src_appln_support_code="TEL", tgt_appln_support_code="TEL",
                                                  src_db=self._pri_db_name, src_host_name=self._pri_host_name,
                                                  tgt_db=self._gsb_db_name, tgt_host_name=self._gsb_host_name,
                                                  port=self._port_for_other,
                                                  src_pool_name=self._pri_pool_name,
                                                  tgt_pool_name=self._gsb_pool_name,
                                                  opt_host_name=self._pri_host_name, type="TEL2TEL",
                                                  new_tahol_schema_name=self._new_tahol_schema_name,
                                                    replication_to="ptahoe_gtahoe")

            if result and result2:
                self.updateJobStatus(jobid, "SUCCEED")
            else:
                self.updateJobStatus(jobid, "FAILED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")

    # STEP 6
    # TEL2OPDB
    def shareplexConfigTEL2OPDB(self,*args):
        jobid = args[0]
        logger.info("shareplex Config Channel TEL2OPEB")
        try:
            jobvo = self.updateJobStatus(jobid, "RUNNING")
            # gsb ->opdb
            config_host_name, opdb_host_name,gcfgdb_host_name = self.getJob_host_name_for_config(jobid)
            result = self.shareplexConfigSrcToTgt(src_appln_support_code="TEL", tgt_appln_support_code="OPDB",
                                                  src_db=self._gsb_db_name, src_host_name=self._gsb_host_name,
                                                  tgt_db="RACOPDB", tgt_host_name=opdb_host_name,
                                                  port=self._port_for_other,
                                                  src_pool_name=self._gsb_pool_name,tgt_pool_name="",
                                                  opt_host_name=self._gsb_host_name, type="TEL2OPDB",
                                                  new_tahol_schema_name=self._new_tahol_schema_name,replication_to="gtahoe_opdb")

            # pri ->opdb
            result2 = self.shareplexConfigSrcToTgt(src_appln_support_code="TEL", tgt_appln_support_code="OPDB",
                                                   src_db=self._pri_db_name, src_host_name=self._pri_host_name,
                                                   tgt_db="RACOPDB", tgt_host_name=opdb_host_name,
                                                   port=self._port_for_other,
                                                   src_pool_name=self._pri_pool_name,tgt_pool_name="",
                                                   opt_host_name=self._pri_host_name, type="TEL2OPDB",
                                                   new_tahol_schema_name=self._new_tahol_schema_name,replication_to="ptahoe_opdb")
            if result and result2:
                self.updateJobStatus(jobid, "SUCCEED")
            else:
                self.updateJobStatus(jobid, "FAILED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")

    # STEP 7
    # TEL2TOOLS
    # def shareplexConfigTEL2TOOLS(self,*args):
    #     jobid = args[0]
    #     logger.info("shareplexConfigTEL2TOOLS")
    #     try:
    #         jobvo = self.updateJobStatus(jobid, "RUNNING")
    #         config_host_name, opdb_host_name, gcfgdb_host_name = self.getJob_host_name_for_config(jobid)
    #         # gsb -> tools
    #         result = self.shareplexConfigSrcToTgt(src_appln_support_code="TEL", tgt_appln_support_code="TOOLS",
    #                                                src_db=self._gsb_db_name, src_host_name=self._gsb_host_name,
    #                                                tgt_db="RACPSYT", tgt_host_name=tool_host_name,
    #                                                port=self._port_for_other,
    #                                                src_pool_name=self._gsb_pool_name, tgt_pool_name="",
    #                                                opt_host_name=self._gsb_host_name, type="TEL2TOOLS",
    #                                                new_tahol_schema_name=self._new_tahol_schema_name,replication_to="gtahoe_pstool")
    #
    #         # pri -> tool
    #         result2 = self.shareplexConfigSrcToTgt(src_appln_support_code="TEL", tgt_appln_support_code="TOOLS",
    #                                                src_db=self._pri_db_name, src_host_name=self._pri_host_name,
    #                                                tgt_db="RACPSYT", tgt_host_name=tool_host_name,
    #                                                port=self._port_for_other,
    #                                                src_pool_name=self._pri_pool_name, tgt_pool_name="",
    #                                                opt_host_name=self._pri_host_name, type="TEL2TOOLS",
    #                                                new_tahol_schema_name=self._new_tahol_schema_name,replication_to="ptahoe_pstool")
    #         if result and result2:
    #             self.updateJobStatus(jobid, "SUCCEED")
    #         else:
    #             self.updateJobStatus(jobid, "FAILED")
    #     except Exception as e:
    #         logger.error(str(e), exc_info=e)
    #         self.updateJobStatus(jobid, "FAILED")

    # STEP 8
    def assignPasscode(self,*args):
        jobid = args[0]
        jobvo = self.updateJobStatus(jobid, "RUNNING")
        logger.info("start to assignPasscode from configdb")
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daomanagerfactory.getDaoManager("CONFIGDB","test")
        dao = daomanager.getDao(DaoKeys.DAO_CONFIGDBDAO)
        try:
            daomanager.startTransaction()
            logger.info("Assign passcode range for %s" % self._pri_pool_name)
            dao.assignPasscode(self._pri_pool_name)
            logger.info("Assign passcode range for %s" % self._gsb_pool_name)
            dao.assignPasscode(self._gsb_pool_name)
            daomanager.commit()
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
        finally:
            daomanager.close()

    # STEP 9
    #
    #  1 HA
    #       crsstat | grep service | grep ora.<db_name>.<ha_service_name>.svc   (ha_service_name: <pool_name>ha)
    #       if srvctl config database -d <db_name> | grep ^Services: | grep -i <pool_name>ha | wc -l =0
    #           then
    #       1.1 get <instancelist> by cmd: srvctl config database -d <db_name>
    #       1.2 srvctl add service -d <db_name> -s <service_name> -r <instancelist> -P basic -e select -m basic -z 5 -w 3
    #       1.3 srvctl start service -d <db_name> -s <service_name>
    #       1.4 check eg: crsstat | grep service | grep ora.<db_name>.<pool_name>.svc
    def addMonitor(self,*args):
        jobid = args[0]
        logger.info("addMonitor")
        try:
            jobvo = self.updateJobStatus(jobid, "RUNNING")
            logger.info("****** add HA ******")
            self.DoHAByOne(self._gsb_host_name,self._gsb_db_name,self._gsb_pool_name)
            self.DoHAByOne(self._pri_host_name, self._pri_db_name, self._pri_pool_name)
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")

    def DoHAByOne(self,host_name,db_name,pool_name):
        logger.info("add HA ,host_name:{0},db_name={1},pool_name={2}".format(host_name,db_name,pool_name))
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        try:
            server = daomanagerfactory.getServer(hostname=host_name)
            cmd = ". /home/oracle/.bash_profile;srvctl config database -d {0} | grep ^Services: | grep -i {1} | wc -l".format(db_name, pool_name)
            logger.info(cmd)
            res = server.exec_command(cmd)
            logger.info(res)
            if res and int(res)==0:
                instancelist = ""
                cmd = ". /home/oracle/.bash_profile;srvctl config database -d %s" %(db_name)
                logger.info(cmd)
                res = server.exec_command(cmd)
                logger.info(res)
                if res:
                    for line in res.split("\n"):
                        line = line.strip()
                        if "Database instances" in line:
                            instancelist = str(str(line).split(":")[1]).strip()
                else:
                    logger.error("not find on {0}" .format(db_name))
                if instancelist:
                    cmd = ". /home/oracle/.bash_profile;srvctl add service -d {0} -s {1} -r {2} -P basic -e select -m basic -z 5 -w 3" .format(db_name,pool_name+"ha",instancelist)
                    logger.info(cmd)
                    res = server.exec_command(cmd)
                    logger.info(res)

                    cmd = ". /home/oracle/.bash_profile;srvctl add service -d {0} -s {1} -r {2} -P basic -e select -m basic -z 5 -w 3".format(
                        db_name, pool_name, instancelist)
                    logger.info(cmd)
                    res = server.exec_command(cmd)
                    logger.info(res)

                    cmd = ". /home/oracle/.bash_profile;srvctl start service -d {0} -s {1}" .format(db_name,pool_name+"ha")
                    logger.info(cmd)
                    res = server.exec_command(cmd)
                    logger.info(res)

                    cmd = ". /home/oracle/.bash_profile;srvctl start service -d {0} -s {1}".format(db_name,pool_name)
                    logger.info(cmd)
                    res = server.exec_command(cmd)
                    logger.info(res)


                    cmd = ". /home/oracle/.bash_profile;crsstat | grep service | grep ora.{0}.{1}.svc".format(db_name.lower(), pool_name.lower() + "ha")
                    logger.info(cmd)
                    res = server.exec_command(cmd)
                    logger.info(res)

                    cmd = ". /home/oracle/.bash_profile;crsstat | grep service | grep ora.{0}.{1}.svc".format(
                        db_name.lower(), pool_name.lower())
                    logger.info(cmd)
                    res = server.exec_command(cmd)
                    logger.info(res)
            else:
                logger.info("HA have already done, skip it.")
                cmd = ". /home/oracle/.bash_profile;srvctl config database -d {0} | grep ^Services: | grep -i {1} ".format(db_name, pool_name)
                logger.info(cmd)
                res = server.exec_command(cmd)
                logger.info(res)

        except Exception as e:
            raise wbxexception("create directory_path:{0} fail %s" % (self._directory_path))

    #  STEP 10
    #
    #  1 check schema is exist , check one table randomly and statistics data;
    def postVerification(self,*args):
        jobid = args[0]
        logger.info("postVerification")
        try:
            logger.info("start check pri/gsb schema and table")
            jobvo = self.updateJobStatus(jobid, "RUNNING")
            flag = self.verification_by_one(self._pri_host_name,self._pri_db_name,str(self._new_tahol_schema_name).upper())
            flag2 = self.verification_by_one(self._gsb_host_name,self._gsb_db_name,str(self._new_tahol_schema_name).upper())
            if flag and flag2:
                self.updateJobStatus(jobid, "SUCCEED")
            else:
                logger.info("check schema fail, stop job")
                self.updateJobStatus(jobid, "FAILED")

        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")

    def verification_by_one(self,host_name,db_name,new_tahol_schema_name):
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        tgtdb = daomanagerfactory.getDatabaseByDBName(db_name)
        tgt_connUrl = None
        try:
            tgt_connUrl = tgtdb.getConnectionURL()
        except DatabaseError as e:
            raise wbxexception("Can not getConnectionURL to db %s" % (db_name))
        print("tgt_host_name:{0}, connUrl:{1}".format(host_name, tgt_connUrl))

        connect = cx_Oracle.connect("sys/sysnotallow@" + tgt_connUrl, mode=cx_Oracle.SYSDBA)
        cursor = connect.cursor()

        try:
            logger.info("check invalid objects")
            sql = '''
            select object_name, object_type from dba_objects where owner in ('TEST','%s') and status !='VALID'
            ''' %(new_tahol_schema_name)
            logger.info(sql)
            cursor.execute(sql)
            tables = cursor.fetchall()
            if len(tables) > 0:
                for table in tables:
                    logger.info(table)
                raise wbxexception("found invalid objects")
            else:
                logger.info("no invalid objects")
            connect.commit()
        except Exception as e:
            raise wbxexception("check invalid objects fail ,{0} {1}" .format(host_name,str(e)))

        try:
            sql = '''
                    select * from (
                    select owner,table_name,num_rows,to_char(last_analyzed,'yyyy-mm-dd hh24:mi:ss')  last_analyzed ,row_number() over(
                            partition by owner order by num_rows desc )rn
                    from dba_tables
                    where owner in( 'TEST','WBXDBA','%s')
                    )r where r.rn=1''' % (new_tahol_schema_name)
            cursor.execute(sql)
            tables = cursor.fetchall()
            schemas = {}
            for table in tables:
                owner = table[0]
                schemas[owner] = table[2]
            if 'TEST' not in schemas.keys() :
                logger.error("scheme TEST not exist, verification fail")
                return False
            if 'TEST' in schemas.keys() and schemas['TEST'] ==0:
                logger.error("scheme TEST have no table, verification fail")
                return False
            if new_tahol_schema_name not in schemas.keys() :
                logger.error("scheme %s not exist, verification fail" %(new_tahol_schema_name))
                return False
            if new_tahol_schema_name in schemas.keys() and schemas[new_tahol_schema_name] ==0:
                logger.error("scheme %s have no table, verification fail" %(new_tahol_schema_name))
                return False
            if 'WBXDBA' not in schemas.keys() :
                logger.error("scheme WBXDBA not exist, verification fail")
                return False
            if 'WBXDBA' in schemas.keys() and schemas['WBXDBA'] ==0:
                logger.error("scheme WBXDBA have no table, verification fail")
                return False
            return True
        except Exception as e:
            raise wbxexception("verification_by_one {0} fail" % (host_name))

    def create_db_link_to_baseline_by_one(self,src_host_name,src_db_name,tgt_host_name,tgt_db_name):
        logger.info("create_db_link_to_baseline, src_host_name:{0},src_db_name:{1},tgt_host_name:{2},tgt_db_name:{3}".format(src_host_name,src_db_name,tgt_host_name,tgt_db_name))
        cmd = "mkdir -p " + self._directory_path
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            tgt_server = daomanagerfactory.getServer(hostname=tgt_host_name)
            logger.info("create directory_path: {0} name:{1}".format(self._directory_path, self._directory_name))
            tgt_server.exec_command(cmd)
        except Exception as e:
            logger.error(str(e), exc_info=e)
            raise wbxexception("create directory_path:{0} fail" % (self._directory_path))

        srcdb = daomanagerfactory.getDatabaseByDBName(src_db_name)
        tgtdb = daomanagerfactory.getDatabaseByDBName(tgt_db_name)
        src_connUrl = None
        try:
            src_connUrl = srcdb.getConnectionURL()
        except DatabaseError as e:
            raise wbxexception("Can not getConnectionURL to db %s" % (src_host_name))
        print("src_host_name:{0}, connUrl:{1}".format(src_host_name, src_connUrl))

        tgt_connUrl = None
        try:
            tgt_connUrl = tgtdb.getConnectionURL()
        except DatabaseError as e:
            raise wbxexception("Can not getConnectionURL to db %s" % (tgt_host_name))
        print("tgt_host_name:{0}, connUrl:{1}".format(tgt_host_name, tgt_connUrl))

        connect = cx_Oracle.connect("sys/sysnotallow@" + tgt_connUrl, mode=cx_Oracle.SYSDBA)
        cursor = connect.cursor()

        try:
            sql = '''CREATE OR REPLACE FORCE VIEW SYS.DUAL_VIEW(ADDR,INDX,INST_ID,DUMMY) AS SELECT ADDR, INDX, INST_ID, DUMMY FROM x$dual'''
            logger.info(sql)
            cursor.execute(sql)
            connect.commit()
        except Exception as e:
            raise wbxexception(str(e))

        directorySQL = "create or replace directory " + self._directory_name + " as " + "'" + self._directory_path + "'"
        try:
            logger.info("create directory {0} on {1}".format(self._directory_name, tgt_host_name))
            cursor.execute(directorySQL)
        except Exception as e:
            logger.error("create directory {0} fail ".format(self._directory_name))
            raise wbxexception("create directory {0} fail" % (self._directory_name))

        dblinkSQL1 = "select count(1) from dba_db_links where db_link = '%s.WEBEX.COM' " % dblinkName
        dblinkSQL2 = "drop public database link %s " % dblinkName
        dblinkSQL3 = "create public database link %s connect to %s identified by %s using '%s'" % (
            dblinkName, schemaname, schemapwd, src_connUrl)
        try:
            logger.info("create database link connect to src_host_name:{0}".format(src_host_name))
            re = cursor.execute(dblinkSQL1).fetchall()
            if re[0][0] == 0:
                logger.info(dblinkSQL3)
                cursor.execute(dblinkSQL3)
            else:
                cursor.execute(dblinkSQL2)
                logger.info(dblinkSQL3)
                cursor.execute(dblinkSQL3)
        except Exception as e:
            errormsg = "error msg: %s" % e
            logger.error("Error occurred: monitordblink(%s) under schema %s to dbline %s with %s" % (
                tgt_host_name, schemaname, dblinkName, errormsg))
            raise wbxexception("Error occurred: monitordblink(%s) under schema %s to dbline %s with %s" % (
                tgt_host_name, schemaname, dblinkName, errormsg))


    # 1. create db link to config DB
    # 2. impdp test metadata from from baseline tahoe db
    # 3. impdp wbx metadata
    # 4. alter table test.wbxdatabaseversion disable all triggers;
    # 5. impdp wbxdatabase/wbxdatabaseversion data from baseline tahoe db
    # 6. update wbxdatabaseversion dbtype
    # 7. alter table test.wbxdatabaseversion enable all triggers;
    # 8. disable trigger test.TR_WBXPCNPASSCODERANGE_AUDIT
    # 9. impdp data to test schema from configdb test schema
    # 10. alter PACKAGE WBXDBA.PKG_RESIZE_TS compile BODY
    # 11. check object count
    # 12. enable trigger test.TR_WBXPCNPASSCODERANGE_AUDIT
    # 13 grant
    # 14. create user in depot
    # 15. alter PACKAGE WBXDBA.PKG_RESIZE_TS compile BODY;
    # 16. Recompile invalid objects
    # 17. gather statistics status immediately and check result
    # 18. add gather statistics status job and check result
    def createtestschema_by_one(self, src_db_name,src_host_name,tgt_host_name,tgt_db_name,db_type):
        logger.info("createtestschema for tahoe build task with src_host_name=%s,src_db_name=%s, tgt_host_name=%s, tgt_db_name=%s " % (src_host_name,src_db_name,tgt_host_name,tgt_db_name))
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        step_total = str(17) + " (" + db_type + ")"
        try:
            tgt_dbserver = daomanagerfactory.getServer(hostname=tgt_host_name)
            tgt_dbserver.connect()
            tgtdb = daomanagerfactory.getDatabaseByDBName(tgt_db_name)
            srcdb = daomanagerfactory.getDatabaseByDBName(src_db_name)
            # configdb = daomanagerfactory.getDatabaseByDBName("CONFIGDB")

            tgt_connUrl = None
            try:
                tgt_connUrl = tgtdb.getConnectionURL()
            except DatabaseError as e:
                raise wbxexception("Can not getConnectionURL to db %s" % (tgt_host_name))
            logger.info("tgt_host_name:{0} connUrl:{1}".format(tgt_host_name, tgt_connUrl))
            config_connUrl = get_db_tns('CONFIGDB')['tns']
            # try:
            #     config_connUrl = configdb.getConnectionURL()
            # except DatabaseError as e:
            #     raise wbxexception("Can not getConnectionURL to db %s" % ("CONFIGDB"))
            logger.info("{0} connUrl:{1}".format("CONFIGDB", config_connUrl))

            connect = cx_Oracle.connect("sys/sysnotallow@" + tgt_connUrl, mode=cx_Oracle.SYSDBA)
            cursor = connect.cursor()

            configdbDBlinkSQL1 = "select count(1) from dba_db_links where db_link= '%s.WEBEX.COM' " % to_configdb_dblink
            configdbDBlinkSQL2 = "drop public database link %s " % to_configdb_dblink
            configdbDBlinkSQL3 = "create public database link %s connect to %s identified by %s using '%s' " % (
                to_configdb_dblink, schemaname, schemapwd, config_connUrl)

            try:
                logger.info("[1/{0}] create database link connect to {1}".format(step_total, "CONFIGDB"))
                re = cursor.execute(configdbDBlinkSQL1).fetchall()
                if re[0][0] == 0:
                    logger.info(configdbDBlinkSQL3)
                    cursor.execute(configdbDBlinkSQL3)
                else:
                    cursor.execute(configdbDBlinkSQL2)
                    logger.info(configdbDBlinkSQL3)
                    cursor.execute(configdbDBlinkSQL3)
            except Exception as e:
                errormsg = "error msg: %s" % e
                logger.error("Error occurred: monitordblink(%s) under schema %s to dbline %s with %s" % (
                    tgt_host_name, schemaname, dblinkName, errormsg))
                raise wbxexception("Error occurred: monitordblink(%s) under schema %s to dbline %s with %s" % (
                    tgt_host_name, schemaname, dblinkName, errormsg))
            connect.commit()

            try:
                cmd2 = "ps -ef | grep smon | grep %s |grep -v grep|awk '{print $8}'| sed -n '1p'" % (
                    str(tgt_db_name).lower())
                # logger.info(cmd2)
                oracle_sid_str = tgt_dbserver.exec_command(cmd2)
                # logger.info(oracle_sid_str)
                oracle_sid = str(oracle_sid_str).split("ora_smon_")[1]
                logger.info("oracle_sid:{0}".format(oracle_sid))
            except Exception as e:
                raise wbxexception("find oracle_sid on {0} fail,e:{1}".format(tgt_host_name, str(e)))

            is_edit_tcp_window_scaling = False
            try:
                cmd = "cat /proc/sys/net/ipv4/tcp_window_scaling"
                logger.info(cmd)
                tcp_window_scaling = tgt_dbserver.exec_command(cmd)
                logger.info("tcp_window_scaling=%s" % (tcp_window_scaling))
                if tcp_window_scaling == "0":
                    is_edit_tcp_window_scaling = True
            except Exception as e:
                raise wbxexception("view tcp_window_scaling fail,e:{0}".format(str(e)))

            try:
                logtime = datetime.datetime.strftime(wbxutil.getcurrenttime(), "%Y-%m-%d_%H-%M-%S")
                logfilename = "impdpTestMetadata_%s.log" % (logtime)
                logger.info(
                    "[2/{0}] impdp test metadata from {1} to {2}, view detail log:{3}".format(step_total,
                                                                                              src_host_name,
                                                                                              tgt_host_name,
                                                                                              self._directory_path + "/" + logfilename))
                cmd2 = ". /home/oracle/.bash_profile; export ORACLE_SID={0}; impdp system/sysnotallow network_link={1} directory={2} " \
                       "schemas=test CONTENT=METADATA_ONLY EXCLUDE=STATISTICS PARALLEL=8 logfile={3}".format(
                    oracle_sid, dblinkName, self._directory_name, logfilename)
                logger.info(cmd2)
                logger.info("Please wait ...")
                tgt_dbserver.exec_command(cmd2)
            except Exception as e:
                logger.error(str(e), exc_info=e)
                logger.error(
                    "impdp test metadata {0} to {1} fail, view detail log:{2} ".format(src_host_name,
                                                                                       tgt_host_name,
                                                                                       self._directory_path + "/" + logfilename))
                raise wbxexception("impdp test metadata {0} to {1} fail" % (src_host_name, tgt_host_name))

            try:
                logtime = datetime.datetime.strftime(wbxutil.getcurrenttime(), "%Y-%m-%d_%H-%M-%S")
                logfilename = "impdpWbx_%s.log" % (logtime)
                logger.info(
                    "[3/{0}] impdp wbx metadata ,tgt_host_name:{1}, view log:{2}".format(step_total,
                                                                                         tgt_host_name,
                                                                                         self._directory_path + "/" + logfilename))
                logger.info("oracle_sid:{0}".format(oracle_sid))
                cmd = ". /home/oracle/.bash_profile; export ORACLE_SID=%s; impdp system/sysnotallow network_link=%s  directory=%s " \
                      " schemas=WBXDBA,WBXMAINT,WBXBACKUP CONTENT=METADATA_ONLY EXCLUDE=STATISTICS PARALLEL=8 logfile=%s " % (
                          oracle_sid, dblinkName, self._directory_name, logfilename)
                logger.info(cmd)
                logger.info("Please wait ...")
                re = tgt_dbserver.exec_command(cmd)
            except Exception as e:
                logger.error(
                    "impdp wbx metadata fail,view log:{0}, e :{1}".format(
                        self._directory_path + "/" + logfilename,
                        str(e)))
                raise wbxexception("impdp wbx metadata fail, e :{0}" % (e))

            try:
                logger.info("[4/{0}] alter table test.wbxdatabaseversion disable all triggers".format(step_total))
                sql = "alter table test.wbxdatabaseversion disable all triggers"
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                raise wbxexception(
                    "alter table test.wbxdatabaseversion disable all triggers fail, e :{0} ".format(str(e)))

            try:
                logtime = datetime.datetime.strftime(wbxutil.getcurrenttime(), "%Y-%m-%d_%H-%M-%S")
                logfilename = "impdpTestVersionDate_%s.log" % (logtime)
                logger.info(
                    "[5/{0}] impdp wbxdatabase/wbxdatabaseversion data from {1} to {2}, view detail log:{3}".format(
                        step_total, src_host_name, tgt_host_name,
                        self._directory_path + "/" + logfilename))
                cmd2 = ". /home/oracle/.bash_profile; export ORACLE_SID={0}; impdp system/sysnotallow network_link={1} directory={2} " \
                       "tables=test.wbxdatabase,test.wbxdatabaseversion content=DATA_ONLY logfile={3}".format(
                    oracle_sid,
                    dblinkName,
                    self._directory_name,
                    logfilename)
                logger.info(cmd2)
                logger.info("Please wait ...")
                tgt_dbserver.exec_command(cmd2)
            except Exception as e:
                raise wbxexception("impdp wbxdatabase/wbxdatabaseversion data from baseline tahoe db fail")

            try:
                logger.info("[6/{0}] update wbxdatabaseversion dbtype".format(step_total))
                dbtype = "TAHOEDB"
                if db_type == "gsb":
                    dbtype = "TAHOEDB_GSB"
                sql = "update test.wbxdatabaseversion set dbtype='%s'" % (dbtype)
                # logger.info(sql)
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                raise wbxexception(
                    "update wbxdatabaseversion dbtype fail, tgt_host_name={0}, e={1}".format(tgt_host_name, str(e)))

            try:
                logger.info("[7/{0}] alter table test.wbxdatabaseversion enable all triggers".format(step_total))
                sql = "alter table test.wbxdatabaseversion enable all triggers"
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                raise wbxexception(
                    "alter table test.wbxdatabaseversion enable all triggers fail, e :{0} ".format(str(e)))

            logger.info("[8/{0}] disable trigger test.TR_WBXPCNPASSCODERANGE_AUDIT".format(step_total))
            trigger_sql = "alter trigger test.TR_WBXPCNPASSCODERANGE_AUDIT disable "
            try:
                cursor.execute(trigger_sql)
            except Exception as e:
                logger.error("disable trigger fail, e :{0} ".format(str(e)))
            connect.commit()

            try:
                logtime = datetime.datetime.strftime(wbxutil.getcurrenttime(), "%Y-%m-%d_%H-%M-%S")
                logfilename = "impdpTestData_%s.log" % (logtime)
                logger.info(
                    "[9/{0}] impdp data to test schema from configdb test schema ,tgt_host_name:{1}, view detail log:{2}".format(
                        step_total, tgt_host_name,
                        self._directory_path + "/" + logfilename))
                table_list = self.getCreateTestSchemaTable("test")
                tables = ""
                index = 1
                for item in table_list:
                    vo = dict(item)
                    tables = "%s %s.%s" % (tables, vo['schema'], vo['tablename'])
                    if index != len(table_list):
                        tables = "%s ," % (tables)
                    index += 1
                cmd = ""
                if is_edit_tcp_window_scaling:
                    cmd = "sudo sysctl -w net.ipv4.tcp_window_scaling=1; . /home/oracle/.bash_profile; export ORACLE_SID=%s; impdp system/sysnotallow network_link=%s tables=%s " \
                          "content=DATA_ONLY cluster=N parallel=8 DIRECTORY=%s logfile=%s REMAP_SCHEMA=wbx11:test" % (
                              oracle_sid, to_configdb_dblink, tables, self._directory_name, logfilename)
                else:
                    cmd = ". /home/oracle/.bash_profile; export ORACLE_SID=%s; impdp system/sysnotallow network_link=%s tables=%s " \
                          "content=DATA_ONLY cluster=N parallel=8 DIRECTORY=%s logfile=%s REMAP_SCHEMA=wbx11:test" % (
                              oracle_sid, to_configdb_dblink, tables, self._directory_name, logfilename)
                logger.info(cmd)
                logger.info("Please wait ...")
                re = tgt_dbserver.exec_command(cmd)
                # cmd3 = "cat {0}".format(self._directory_path + "/" + logfilename)
                # res = tgt_dbserver.exec_command(cmd3)
                # logger.info(res)
            except Exception as e:
                logger.error(
                    "impdp data to test schema from configdb test schema fail,view detail log:{0}, e :{1}".format(
                        self._directory_path + "/" + logfilename, str(e)))
                raise wbxexception("impdp data to test schema from configdb test schema fail, e :{0}" % (str(e)))

            try:
                logger.info("[10/{0}] alter PACKAGE WBXDBA.PKG_RESIZE_TS compile BODY".format(step_total))
                sql = '''
                                            alter PACKAGE WBXDBA.PKG_RESIZE_TS compile BODY
                                            '''
                # logger.info(sql)
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                raise wbxexception("alter PACKAGE WBXDBA.PKG_RESIZE_TS compile BODY fail. e:{0}".format(e))

            try:
                logger.info("[11/{0}] check object count".format(step_total))
                sql = '''
                                                  select 'object_type='||ta.object_type||' object_count_in_olddb='||ta.objcnt||' object_count_in_newdb='||tb.objcnt diff_object_info
                                                   from
                                                   (select object_type, count(1) objcnt
                                                   from dba_objects@TO_BASELINE_TAHOE
                                                   where owner='TEST'
                                                   and object_type not in ('INDEX PARTITION','TABLE PARTITION')
                                                   and object_name not like 'BIN%'
                                                   and object_name not like 'SYS%'
                                                   and object_name not like 'TASK%'
                                                   group by object_type) ta,
                                                   (select object_type, count(1) objcnt
                                                   from dba_objects
                                                   where owner='TEST'
                                                   and object_type not in ('INDEX PARTITION','TABLE PARTITION')
                                                   and object_name not like 'BIN%'
                                                   and object_name not like 'SYS%'
                                                   and object_name not like 'TASK%'
                                                   group by object_type) tb
                                                   where ta.object_type=tb.object_type
                                                   and ta.objcnt !=tb.objcnt
                                                   '''
                # logger.info(sql)
                re = cursor.execute(sql).fetchall()
                if len(re) > 0:
                    for item in re:
                        logger.error(item)
                    raise wbxexception("found diff object count")
                connect.commit()
            except Exception as e:
                raise wbxexception("check object information fail.")

            logger.info("[12/{0}] enable trigger test.TR_WBXPCNPASSCODERANGE_AUDIT".format(step_total))
            trigger_sql = "alter trigger test.TR_WBXPCNPASSCODERANGE_AUDIT enable "
            try:
                cursor.execute(trigger_sql)
            except Exception as e:
                logger.error("enable trigger fail, e :{0} ".format(str(e)))
            connect.commit()

            grant_test_list = [
                "grant execute on dbms_lock to test",
                "grant execute on utl_file to test"]
            try:
                logger.info("[13/{0}] grant".format(step_total))
                for sql in grant_test_list:
                    logger.debug(sql)
                    cursor.execute(sql)
            except Exception as e:
                raise wbxexception("grant fail, sql:{0} , e:{1}".format(sql, str(e)))

            logger.info("[14/{0}] create user in depot".format(step_total))
            try:
                daoManager = daomanagerfactory.getDefaultDaoManager()
                daoManager.startTransaction()
                spDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                rows = spDao.getApplnPoolInfo(src_db_name, tgt_db_name, "test")
                src_vo = None
                tgt_vo = None
                for row in rows:
                    vo = dict(row)
                    if vo['db_name'] == src_db_name:
                        src_vo = vo
                    if vo['db_name'] == tgt_db_name:
                        tgt_vo = vo
                logger.info(src_vo)
                if src_vo and not tgt_vo:
                    spDao.createUserToApplnPoolInfo(tgtdb._trim_host, tgt_db_name, src_vo)
                else:
                    logger.info("schema test have already created,skip it")
                daoManager.commit()
            except Exception as e:
                logger.error("create user in depot fail,e: {0}".format(str(e)))
                daoManager.rollback()
                raise wbxexception("create user in depot fail")

            try:
                logger.info("[15/{0}] Recompile invalid objects".format(step_total))
                sql = '''begin
                                                    test.spvalidate;
                                                    end;'''
                # logger.info(sql)
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                errormsg = "error msg: %s" % e
                raise wbxexception("Error occurred: Recompile invalid objects, %s, sql=%s" % (errormsg, sql))
            connect.commit()

            try:
                logger.info("[16/{0}] Recompile WBXDBA.PKG_RESIZE_TS BODY".format(step_total))
                sql = '''alter PACKAGE WBXDBA.PKG_RESIZE_TS compile BODY'''
                # logger.info(sql)
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                connect.rollback()
                errormsg = "error msg: %s" % e
                raise wbxexception("Error occurred: Recompile invalid objects, %s, sql=%s" % (errormsg, sql))

            logger.info("[17/{0}] gather statistics status about TEST immediately".format(step_total))
            try:
                logger.info('execute GATHER_STATS_FIXED_OBJECTS...')
                sql = '''
                                                    begin
                                                    dbms_stats.GATHER_FIXED_OBJECTS_STATS;
                                                    end;'''
                # logger.info(sql)
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                errormsg = "error msg: %s" % e
                raise wbxexception(
                    "Error occurred: execute GATHER_STATS_FIXED_OBJECTS, %s, sql=%s" % (errormsg, sql))
            connect.commit()

            try:
                logger.info('execute GATHER_STATS_TEST...')
                sql = '''
                                                    begin
                                                    dbms_stats.gather_schema_stats(
                                                    ownname=> '"TEST"' ,
                                                    cascade=> DBMS_STATS.AUTO_CASCADE,
                                                    estimate_percent=> 10,
                                                    degree=> 8,
                                                    no_invalidate=> DBMS_STATS.AUTO_INVALIDATE,
                                                    granularity=> 'AUTO',
                                                    method_opt=> 'FOR ALL COLUMNS SIZE AUTO',
                                                    options=> 'GATHER');
                                                    end;'''
                # logger.info(sql)
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                errormsg = "error msg: %s" % e
                raise wbxexception("Error occurred: execute GATHER_STATS_TEST, %s, sql=%s" % (errormsg, sql))

            logger.info("check gather statistics status")
            try:
                sql = ''' select table_name,num_rows,last_analyzed from dba_tables where owner = 'TEST' '''
                # logger.info(sql)
                re = cursor.execute(sql).fetchall()
                if re[0][2]:
                    logger.info('check gather statistics immediately success. db_name=%s' % (tgt_db_name))
                else:
                    logger.error('check gather statistics immediately fail. db_name=%s' % (tgt_db_name))
                connect.commit()
            except Exception as e:
                errormsg = "error msg: %s" % e
                raise wbxexception(
                    "Error occurred: check gather statistics status,db_name=%s, errormsg=%s, sql=%s" % (
                        tgt_db_name, errormsg, sql))

            logger.info("[18/{0}] add gather statistics status job and check result".format(step_total))
            JOB_GATHER_STATS_FIXED_OBJECTS = False
            JOB_GATHER_STATS_TEST = False
            try:
                sql = '''select job_name from dba_scheduler_jobs where job_name in('GATHER_STATS_TEST','GATHER_STATS_FIXED_OBJECTS')'''
                re = cursor.execute(sql).fetchall()
                # logger.info(re)
                if len(re) == 2:
                    JOB_GATHER_STATS_FIXED_OBJECTS = True
                    JOB_GATHER_STATS_TEST = True
                if len(re) == 1:
                    if re[0][0] == 'GATHER_STATS_TEST':
                        JOB_GATHER_STATS_TEST = True
                    if re[0][0] == 'GATHER_STATS_FIXED_OBJECTS':
                        JOB_GATHER_STATS_FIXED_OBJECTS = True
            except Exception as e:
                errormsg = "error msg: %s" % e
                raise wbxexception(
                    "Error occurred: check GATHER_STATS_TEST/GATHER_STATS_FIXED_OBJECTS are exist, %s,host_name=%s, sql=%s" % (
                        errormsg, tgt_host_name, sql))

            site_code = ""
            daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
            try:
                daoManager = daoManagerFactory.getDefaultDaoManager()
                dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                daoManager.startTransaction()
                site_code = dict(dao.getSiteCodeByHostName(tgt_host_name)[0])['site_code']
            except Exception as e:
                daoManager.rollback()
                raise wbxexception("addGatherStatisticsScheduler,e: {0}".format(str(e)))
            logger.info("tgt_host_name={0} , site_code={1}".format(tgt_host_name, site_code))

            tomorrow = datetime.datetime.utcnow() + datetime.timedelta(days=1)
            strf_tomorrow = datetime.datetime.strftime(tomorrow, "%Y/%m/%d %H:%M:%S")
            BYHOUR_TEST = "4"
            BYMINUTE_TEST = "55"
            if str(site_code).startswith("AMS") or str(site_code).startswith("LHR"):
                BYHOUR_TEST = "22"
                BYMINUTE_TEST = "55"
            if str(site_code).startswith("SIN") or str(site_code).startswith("NRT") or str(site_code).startswith(
                    "SYD"):
                BYHOUR_TEST = "12"
                BYMINUTE_TEST = "55"
            if JOB_GATHER_STATS_FIXED_OBJECTS == False:
                try:
                    logger.info('create job GATHER_STATS_FIXED_OBJECTS....')
                    sql = '''
                                                    BEGIN
                                                      SYS.DBMS_SCHEDULER.CREATE_JOB
                                                        ( job_name          => 'GATHER_STATS_FIXED_OBJECTS'
                                                    ,start_date     => TO_TIMESTAMP('%s','yyyy/mm/dd hh24:mi:ss')
                                                    ,repeat_interval => 'FREQ=WEEKLY;INTERVAL=1'
                                                    ,end_date     => NULL
                                                    ,job_class     => 'DEFAULT_JOB_CLASS'
                                                    ,job_type     => 'PLSQL_BLOCK'
                                                    ,job_action     => 'begin
                                                       dbms_stats.GATHER_FIXED_OBJECTS_STATS;
                                                    end;'
                                                    ,comments     => 'Gather Statistics for FIXED OBJECTS a weekly'
                                                    );
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => 'GATHER_STATS_FIXED_OBJECTS'    ,attribute => 'RESTARTABLE'   ,value     => FALSE);
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => 'GATHER_STATS_FIXED_OBJECTS'    ,attribute => 'LOGGING_LEVEL'    ,value       => SYS.DBMS_SCHEDULER.LOGGING_OFF);
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL  ( name       => 'GATHER_STATS_FIXED_OBJECTS'   ,attribute => 'MAX_FAILURES');
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL  ( name       => 'GATHER_STATS_FIXED_OBJECTS'   ,attribute => 'MAX_RUNS');
                                                    BEGIN     SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => 'GATHER_STATS_FIXED_OBJECTS'    ,attribute => 'STOP_ON_WINDOW_CLOSE'  ,value     => FALSE);EXCEPTION WHEN OTHERS THEN  NULL;END;
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => 'GATHER_STATS_FIXED_OBJECTS'    ,attribute => 'JOB_PRIORITY'   ,value      => 3);
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL  ( name       => 'GATHER_STATS_FIXED_OBJECTS'   ,attribute => 'SCHEDULE_LIMIT');
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => 'GATHER_STATS_FIXED_OBJECTS'    ,attribute => 'AUTO_DROP'   ,value     => FALSE);
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => 'GATHER_STATS_FIXED_OBJECTS'    ,attribute => 'RAISE_EVENTS'   ,value      => SYS.DBMS_SCHEDULER.JOB_FAILED);
                                                    SYS.DBMS_SCHEDULER.enable(name=> 'GATHER_STATS_FIXED_OBJECTS');
                                                    END;''' % (strf_tomorrow)
                    cursor.execute(sql)
                    connect.commit()

                except Exception as e:
                    errormsg = "error msg: %s" % e
                    raise wbxexception(
                        "Error occurred: create job GATHER_STATS_FIXED_OBJECTS, %s, host_name=%s, sql=%s" % (
                            errormsg, tgt_host_name, sql))
            else:
                logger.info("GATHER_STATS_FIXED_OBJECTS exists ,skip it. ")

            if JOB_GATHER_STATS_TEST == False:
                try:
                    logger.info('create job GATHER_STATS_TEST....')
                    sql = '''
                                                    BEGIN
                                                      SYS.DBMS_SCHEDULER.CREATE_JOB
                                                        ( job_name          => 'GATHER_STATS_TEST'
                                                    ,start_date     => TO_TIMESTAMP('%s','yyyy/mm/dd hh24:mi:ss')
                                                    ,repeat_interval => 'FREQ=DAILY;BYHOUR=%s;BYMINUTE=%s;BYSECOND=0'
                                                    ,end_date     => NULL
                                                    ,job_class     => 'DEFAULT_JOB_CLASS'
                                                    ,job_type     => 'PLSQL_BLOCK'
                                                    ,job_action     => 'begin
                                                      dbms_stats.gather_schema_stats(
                                                    ownname=> ''"TEST"'' ,
                                                    cascade=> DBMS_STATS.AUTO_CASCADE,
                                                    estimate_percent=> 10,
                                                    degree=> 8,
                                                    no_invalidate=> DBMS_STATS.AUTO_INVALIDATE,
                                                    granularity=> ''AUTO'',
                                                    method_opt=> ''FOR ALL COLUMNS SIZE AUTO'',
                                                    options=> ''GATHER'');
                                                    end;'
                                                    ,comments     => 'Gather Statistics for TEST  schema every night'
                                                    );
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => 'GATHER_STATS_TEST'   ,attribute => 'RESTARTABLE'   ,value    => FALSE);
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => 'GATHER_STATS_TEST'   ,attribute => 'LOGGING_LEVEL'   ,value      => SYS.DBMS_SCHEDULER.LOGGING_OFF);
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL  ( name       => 'GATHER_STATS_TEST'   ,attribute => 'MAX_FAILURES');
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL  ( name       => 'GATHER_STATS_TEST'   ,attribute => 'MAX_RUNS');
                                                    BEGIN     SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => 'GATHER_STATS_TEST'  ,attribute => 'STOP_ON_WINDOW_CLOSE'  ,value    => FALSE);EXCEPTION WHEN OTHERS THEN  NULL;END;
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => 'GATHER_STATS_TEST'   ,attribute => 'JOB_PRIORITY'   ,value     => 3);
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL  ( name       => 'GATHER_STATS_TEST'   ,attribute => 'SCHEDULE_LIMIT');
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => 'GATHER_STATS_TEST'   ,attribute => 'AUTO_DROP'   ,value     => FALSE);
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => 'GATHER_STATS_TEST'   ,attribute => 'RAISE_EVENTS'   ,value     => SYS.DBMS_SCHEDULER.JOB_FAILED);
                                                    SYS.DBMS_SCHEDULER.enable(name=> 'GATHER_STATS_TEST');
                                                    END;''' % (strf_tomorrow, BYHOUR_TEST, BYMINUTE_TEST)
                    cursor.execute(sql)
                    connect.commit()

                except Exception as e:
                    errormsg = "error msg: %s" % e
                    raise wbxexception("Error occurred: create job GATHER_STATS_TEST, %s,host_name=%s, sql=%s" % (
                        errormsg, tgt_host_name, sql))
            else:
                logger.info("GATHER_STATS_TEST exists ,skip it. ")

            logger.info("check gather statistics status scheduler")
            try:
                sql = '''select * from dba_scheduler_jobs where job_name in( 'GATHER_STATS_TEST','GATHER_STATS_FIXED_OBJECTS')'''
                re = cursor.execute(sql).fetchall()
                logger.info(re)
                if len(re) == 2:
                    logger.info(
                        "check gather statistics status scheduler GATHER_STATS_TEST/GATHER_STATS_FIXED_OBJECTS success")
                else:
                    logger.error(
                        "check gather statistics status scheduler GATHER_STATS_TEST/GATHER_STATS_FIXED_OBJECTS fail")
            except Exception as e:
                errormsg = "error msg: %s" % e
                raise wbxexception(
                    "Error occurred: check gather statistics status scheduler, %s,host_name=%s, sql=%s" % (
                        errormsg, tgt_host_name, sql))
            except Exception as e:
                raise wbxexception(e)
        except Exception as e:
            raise wbxexception("Error occurred:createtestschema_by_one, e=%s" % (str(e)))


    # 1. impdp tahoe metadata from from baseline tahoe db
    # 2. impdp wbx metadata from baseline tahoe db
    # 3. alter table <new_tahol_schema_name>.wbxdatabaseversion disable all triggers
    # 4. impdp tahoe data from baseline tahoe db
    # 5. check object count
    # 6. initialize datacenter_status
    # 7. update wbxdatabaseversion dbtype
    # 8. alter table <new_tahol_schema_name>.wbxdatabaseversion enable all triggers
    # 9. grant
    # 10. create user in depot (appln_pool_info/appln_mapping_info)
    # 11. Recompile invalid objects
    # 12. gather statistics status immediately and check result
    # 13. add gather statistics status job and check result
    def createtahoeschema_by_one(self,src_db_name,src_host_name,tgt_host_name,tgt_db_name,db_type,new_tahol_schema_name,pool_name):
        logger.info("createtahoeschema for tahoe build task with host_name=%s" % tgt_host_name)
        flag = True
        step_num = str(13) + " (" + db_type + ")"
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            tgt_dbserver = daomanagerfactory.getServer(hostname=tgt_host_name)
            tgt_dbserver.connect()
            tgtdb = daomanagerfactory.getDatabaseByDBName(tgt_db_name)

            try:
                cmd2 = "ps -ef | grep smon | grep %s |grep -v grep| awk '{print $8}'| sed -n '1p'" %(str(tgt_db_name).lower())
                logger.info(cmd2)
                oracle_sid_str = tgt_dbserver.exec_command(cmd2)
                oracle_sid = str(oracle_sid_str).split("ora_smon_")[1]
                logger.info("oracle_sid:{0}" .format(oracle_sid))
            except Exception as e:
                raise wbxexception("find oracle_sid on {0} fail,e:{1}" .format(tgt_host_name,str(e)))

            logtime = datetime.datetime.strftime(wbxutil.getcurrenttime(), "%Y-%m-%d_%H-%M-%S")
            logfilename = "impdpTahoeMetadata_%s_%s.log" % (self._new_tahol_schema_name, logtime)
            logger.info("[1/{0}] impdp tahoe metadata from {1} to {2} , view detail log:{3}".format(step_num,
                                                                                                    src_host_name,
                                                                                                    tgt_host_name,
                                                                                                    self._directory_path + "/" + logfilename))
            try:
                cmd2 = ". /home/oracle/.bash_profile; export ORACLE_SID=%s; impdp system/sysnotallow network_link=%s " \
                       " directory=%s schemas=tahoe REMAP_SCHEMA=tahoe:%s CONTENT=METADATA_ONLY EXCLUDE=STATISTICS PARALLEL=8 transform=oid:n logfile=%s " % (
                           oracle_sid, dblinkName, self._directory_name, self._new_tahol_schema_name, logfilename)
                logger.info(cmd2)
                logger.info("Please wait ...")
                res = tgt_dbserver.exec_command(cmd2)
            except Exception as e:
                raise wbxexception(
                    "impdp tahoe metadata {0} to {1} fail ,view detail log:{2}, e: {3}".format(src_host_name,
                                                                                               tgt_host_name,
                                                                                               self._directory_path + "/" + logfilename,
                                                                                               str(e)))
            is_edit_tcp_window_scaling = False
            try:
                cmd = "cat /proc/sys/net/ipv4/tcp_window_scaling"
                logger.info(cmd)
                tcp_window_scaling = tgt_dbserver.exec_command(cmd)
                logger.info("tcp_window_scaling=%s" % (tcp_window_scaling))
                if tcp_window_scaling == "0":
                    is_edit_tcp_window_scaling = True
            except Exception as e:
                raise wbxexception("view tcp_window_scaling fail,e:{1}".format(str(e)))

            tg_connUrl = None
            try:
                tgt_connUrl = tgtdb.getConnectionURL()
            except DatabaseError as e:
                raise wbxexception("Can not getConnectionURL to db %s" % (tgt_host_name))
            logger.info("{0} connUrl:{1}".format(self._gsb_db_name, tgt_connUrl))
            connect = cx_Oracle.connect("sys/sysnotallow@" + tgt_connUrl, mode=cx_Oracle.SYSDBA)
            cursor = connect.cursor()

            try:
                logger.info("[3/{0}] alter table {1}.wbxdatabaseversion disable all triggers".format(step_num,
                                                                                                     self._new_tahol_schema_name))
                sql = "alter table %s.wbxdatabaseversion disable all triggers" % (self._new_tahol_schema_name)
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                raise wbxexception("alter table %s.wbxdatabaseversion disable all triggers fail, e :{1} ".format(
                    self._new_tahol_schema_name, str(e)))

            try:
                logtime = datetime.datetime.strftime(wbxutil.getcurrenttime(), "%Y-%m-%d_%H-%M-%S")
                logfilename = "impdptahoeData_%s_%s.log" % (self._new_tahol_schema_name, logtime)
                logger.info("[4/{0}] impdp tahoe data ,tgt_host_name:{1}, view detail log:{2}".format(step_num,
                                                                                                      tgt_host_name,
                                                                                                      self._directory_path + "/" + logfilename))
                table_list = self.getCreateTestSchemaTable("tahoe")
                tables = ""
                index = 1
                for item in table_list:
                    vo = dict(item)
                    tables = "%s %s.%s" % (tables, vo['schema'], vo['tablename'])
                    if index != len(table_list):
                        tables = "%s ," % (tables)
                    index += 1
                cmd = ""
                if is_edit_tcp_window_scaling:
                    cmd = "sudo sysctl -w net.ipv4.tcp_window_scaling=1; . /home/oracle/.bash_profile; export ORACLE_SID=%s; impdp system/sysnotallow network_link=%s " \
                          "tables=%s content=DATA_ONLY cluster=N parallel=8 DIRECTORY=%s logfile=%s REMAP_SCHEMA=tahoe:%s transform=oid:n " % (
                              oracle_sid, dblinkName, tables, self._directory_name, logfilename,
                              self._new_tahol_schema_name)
                else:
                    cmd = ". /home/oracle/.bash_profile; export ORACLE_SID=%s; impdp system/sysnotallow network_link=%s " \
                          "tables=%s content=DATA_ONLY cluster=N parallel=8 DIRECTORY=%s logfile=%s REMAP_SCHEMA=tahoe:%s transform=oid:n " % (
                              oracle_sid, dblinkName, tables, self._directory_name, logfilename,
                              self._new_tahol_schema_name)
                logger.info(cmd)
                logger.info("Please wait ...")
                re = tgt_dbserver.exec_command(cmd)

                if is_edit_tcp_window_scaling:
                    cmd = "sudo sysctl -w net.ipv4.tcp_window_scaling=0"
                    logger.info(cmd)
                    re = tgt_dbserver.exec_command(cmd)
                    logger.info(re)

            except Exception as e:
                raise wbxexception("impdp tahoe data fail,view detail log:{0}, e :{1}".format(
                    self._directory_path + "/" + logfilename, str(e)))

            try:
                logger.info("[5/{0}] check object count".format(step_num))
                sql = '''
                                           select 'object_type='||ta.object_type||' object_count_in_olddb='||ta.objcnt||' object_count_in_newdb='||tb.objcnt diff_object_info
                                            from
                                            (select object_type, count(1) objcnt
                                            from dba_objects@TO_BASELINE_TAHOE
                                            where owner='TAHOE'
                                            and object_type not in ('INDEX PARTITION','TABLE PARTITION')
                                            and object_name not like 'BIN%'
                                            and object_name not like 'SYS%'
                                            and object_name not like 'TASK%'
                                            group by object_type) ta,
                                            (select object_type, count(1) objcnt
                                            from dba_objects
                                            where owner='{0}'
                                            and object_type not in ('INDEX PARTITION','TABLE PARTITION')
                                            and object_name not like 'BIN%'
                                            and object_name not like 'SYS%'
                                            and object_name not like 'TASK%'
                                            group by object_type) tb
                                            where ta.object_type=tb.object_type
                                            and ta.objcnt !=tb.objcnt
                                            '''.format(self._new_tahol_schema_name.upper())
                logger.info(sql)
                re = cursor.execute(sql).fetchall()
                if len(re) > 0:
                    for item in re:
                        logger.error(item)
                    raise wbxexception("found diff object count")
                connect.commit()
            except Exception as e:
                raise wbxexception("check check object count fail.")

            try:
                logger.info("[6/{0}] update wbxdatabaseversion dbtype".format(step_num))
                dbtype = "TAHOEDB"
                if db_type == "gsb":
                    dbtype = "TAHOEDB_GSB"
                sql = "update %s.wbxdatabaseversion set dbtype='%s'" % (self._new_tahol_schema_name, dbtype)
                logger.info(sql)
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                raise wbxexception(
                    "update wbxdatabaseversion dbtype fail, tgt_host_name={0}, e={1}".format(tgt_host_name, str(e)))

            try:
                logger.info("[7/{0}] initialize datacenter_status".format(step_num))
                dbcenterid = 0
                if db_type == "gsb":
                    dbcenterid = 1
                sql = '''insert into {0}.datacenter_status (id,last_run_time, DB_CENTER_ID,random_begin,random_end,
                                                     db_inst_id,do_passcodeautogen,poolname) values (1,sysdate,{1},0,0,3,'Y','{2}')'''.format(
                    self._new_tahol_schema_name, dbcenterid, pool_name)
                logger.info(sql)
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                raise wbxexception("initialize datacenter_status fail, schema={0}, poo_name={1} with error ".format(
                    self._new_tahol_schema_name, pool_name, str(e)))

            try:
                logger.info("[8/{0}] alter table {1}.wbxdatabaseversion enable all triggers".format(step_num,
                                                                                                    self._new_tahol_schema_name))
                sql = "alter table %s.wbxdatabaseversion enable all triggers" % (self._new_tahol_schema_name)
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                raise wbxexception("alter table %s.wbxdatabaseversion enable all triggers fail, e :{1} ".format(
                    self._new_tahol_schema_name, str(e)))

            sqllist = self.getGrantSql(self._new_tahol_schema_name, tgt_db_name)
            try:
                logger.info("[9/{0}] grant ".format(step_num))
                for sql in sqllist:
                    logger.debug(sql)
                    cursor.execute(sql)
            except Exception as e:
                raise wbxexception("grant fail, sql:{0} , e:{1}".format(sql, str(e)))

            logger.info("[10/{0}] create user in depot".format(step_num))
            daoManager = daomanagerfactory.getDefaultDaoManager()
            try:
                daoManager.startTransaction()
                spDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                baseInfo = spDao.getApplnPoolInfoByDBName(src_db_name, "app", "")
                todoInfo = spDao.getApplnPoolInfoByDBName(tgt_db_name, "app", new_tahol_schema_name)
                src_vo = dict(baseInfo[0])
                if len(todoInfo) == 0:
                    src_vo['schema'] = new_tahol_schema_name.lower()
                    spDao.createUserToApplnPoolInfo(tgtdb._trim_host, tgt_db_name, src_vo)
                else:
                    logger.info("schema app have already created,skip it")

                mapping_info = spDao.getApplnPoolInfoByDBName(tgt_db_name, "app", new_tahol_schema_name)
                mapping_vo = dict(mapping_info[0])

                mapping = spDao.getApplnMappingInfo(tgt_db_name, mapping_vo['appln_support_code'],
                                                    pool_name.upper())
                if len(mapping) == 0:
                    spDao.insertAppln_mapping_info(tgtdb._trim_host, tgt_db_name, mapping_vo['appln_support_code'],
                                                   pool_name.upper(), new_tahol_schema_name.lower(),"")
                # alter user <schema> identified by <password>;
                # spDao.updateUserPwd(new_tahol_schema_name.lower(), src_vo['password'])
                daoManager.commit()
            except Exception as e:
                daoManager.rollback()
                raise wbxexception("create user in depot fail,e: {0}".format(str(e)))

            try:
                logger.info("[11/{0}] Recompile invalid objects".format(step_num))
                sql = '''begin
                                            %s.spvalidate;
                                            end;''' % (self._new_tahol_schema_name)
                logger.info(sql)
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                errormsg = "error msg: %s" % e
                raise wbxexception("Error occurred:  new_tahol_schema_name={0}, errormsg={1}, sql={2}".format(
                    self._new_tahol_schema_name, errormsg, sql))

            new_tahol_schema_name = str(self._new_tahol_schema_name).upper()
            logger.info("[12/{0}] gather statistics status about {1} immediately and check result".format(step_num,
                                                                                                          new_tahol_schema_name))
            try:
                logger.info('execute GATHER_STATS_TAHOE...')
                sql = '''
                                                   begin
                                                    dbms_stats.gather_schema_stats(
                                                    ownname=> '"%s"' ,
                                                    cascade=> DBMS_STATS.AUTO_CASCADE,
                                                    estimate_percent=> 10,
                                                    degree=> 8,
                                                    no_invalidate=> DBMS_STATS.AUTO_INVALIDATE,
                                                    granularity=> 'AUTO',
                                                    method_opt=> 'FOR ALL COLUMNS SIZE AUTO',
                                                    options=> 'GATHER');
                                                    end;''' % (new_tahol_schema_name)
                logger.info(sql)
                cursor.execute(sql)
                connect.commit()
            except Exception as e:
                errormsg = "error msg: %s" % e
                raise wbxexception(
                    "Error occurred: execute GATHER_STATS_TAHOE..., new_tahol_schema_name={0}, errormsg={1}, sql={2}".format(
                        new_tahol_schema_name, errormsg, sql))

            logger.info("check gather statistics status about %s" % (new_tahol_schema_name))
            try:
                sql = ''' select table_name,num_rows,last_analyzed from dba_tables where owner = '%s' ''' % (
                    new_tahol_schema_name)
                logger.info(sql)
                re = cursor.execute(sql).fetchall()
                # logger.info(re)
                if re[0][2]:
                    logger.info('check gather statistics about %s immediately success. db_name=%s' % (
                        new_tahol_schema_name, tgt_db_name))
                else:
                    logger.error('check gather statistics about %s immediately fail. db_name=%s' % (
                        new_tahol_schema_name, tgt_db_name))
                connect.commit()
            except Exception as e:
                errormsg = "error msg: %s" % e
                raise wbxexception(
                    "Error occurred: check gather statistics status new_tahol_schema_name=%s,db_name=%s, errormsg=%s, sql=%s" % (
                        new_tahol_schema_name,
                        tgt_db_name, errormsg, sql))

            logger.info("[13/{0}] add gather statistics status job and check result".format(step_num))

            site_code = ""
            daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
            try:
                daoManager = daoManagerFactory.getDefaultDaoManager()
                dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                daoManager.startTransaction()
                site_code = dict(dao.getSiteCodeByHostName(tgt_host_name)[0])['site_code']
            except Exception as e:
                daoManager.rollback()
                raise wbxexception("addGatherStatisticsScheduler,e: {0}".format(str(e)))
            logger.info("tgt_host_name={0} , site_code={1}".format(tgt_host_name, site_code))

            tomorrow = datetime.datetime.utcnow() + datetime.timedelta(days=1)
            strf_tomorrow = datetime.datetime.strftime(tomorrow, "%Y/%m/%d %H:%M:%S")
            BYHOUR_TAHOE = "5"
            BYMINUTE_TAHOE = "0"
            if str(site_code).startswith("AMS") or str(site_code).startswith("LHR"):
                BYHOUR_TAHOE = "23"
                BYMINUTE_TAHOE = "00"
            if str(site_code).startswith("SIN") or str(site_code).startswith("NRT") or str(site_code).startswith(
                    "SYD"):
                BYHOUR_TAHOE = "13"
                BYMINUTE_TAHOE = "00"

            JOB_FLAG = False
            job_name = "GATHER_STATS_%s" % (new_tahol_schema_name)
            try:
                sql = '''select * from dba_scheduler_jobs where job_name in('%s')''' % (job_name)
                re = cursor.execute(sql).fetchall()
                logger.info(re)
                if len(re) == 1:
                    JOB_FLAG = True
            except Exception as e:
                errormsg = "error msg: %s" % e
                raise wbxexception(
                    "Error occurred: check gather statistics status job, %s,host_name=%s, sql=%s" % (
                        errormsg, tgt_host_name, sql))

            if JOB_FLAG == False:
                try:
                    logger.info('create job=%s ....' % (job_name))
                    sql = '''
                                                       BEGIN
                                                      SYS.DBMS_SCHEDULER.CREATE_JOB
                                                        ( job_name          => '%s'
                                                    ,start_date     => TO_TIMESTAMP('%s','yyyy/mm/dd hh24:mi:ss')
                                                    ,repeat_interval => 'FREQ=DAILY;BYHOUR=%s;BYMINUTE=%s;BYSECOND=0'
                                                    ,end_date     => NULL
                                                    ,job_class     => 'DEFAULT_JOB_CLASS'
                                                    ,job_type     => 'PLSQL_BLOCK'
                                                    ,job_action     => 'begin
                                                      dbms_stats.gather_schema_stats(
                                                    ownname=> ''"%s"'' ,
                                                    cascade=> DBMS_STATS.AUTO_CASCADE,
                                                    estimate_percent=> 10,
                                                    degree=> 8,
                                                    no_invalidate=> DBMS_STATS.AUTO_INVALIDATE,
                                                    granularity=> ''AUTO'',
                                                    method_opt=> ''FOR ALL COLUMNS SIZE AUTO'',
                                                    options=> ''GATHER'');
                                                    end;'
                                                    ,comments     => 'Gather Statistics for %s  schema every night'
                                                    );
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => '%s'    ,attribute => 'RESTARTABLE'   ,value     => FALSE);
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => '%s'    ,attribute => 'LOGGING_LEVEL'    ,value       => SYS.DBMS_SCHEDULER.LOGGING_OFF);
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL  ( name       => '%s'   ,attribute => 'MAX_FAILURES');
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL  ( name       => '%s'   ,attribute => 'MAX_RUNS');
                                                    BEGIN     SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => '%s'    ,attribute => 'STOP_ON_WINDOW_CLOSE'  ,value     => FALSE);EXCEPTION WHEN OTHERS THEN  NULL;END;
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => '%s'    ,attribute => 'JOB_PRIORITY'   ,value      => 3);
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL  ( name       => '%s'   ,attribute => 'SCHEDULE_LIMIT');
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => '%s'    ,attribute => 'AUTO_DROP'   ,value     => FALSE);
                                                    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE  ( name      => '%s'    ,attribute => 'RAISE_EVENTS'   ,value      => SYS.DBMS_SCHEDULER.JOB_FAILED);
                                                    SYS.DBMS_SCHEDULER.enable(name=> '%s');
                                                    END;''' % (
                        job_name, strf_tomorrow, BYHOUR_TAHOE, BYMINUTE_TAHOE, new_tahol_schema_name,
                        new_tahol_schema_name, job_name, job_name, job_name, job_name, job_name, job_name, job_name,
                        job_name, job_name, job_name)
                    logger.info(sql)
                    cursor.execute(sql)
                    connect.commit()
                except Exception as e:
                    errormsg = "error msg: %s" % e
                    raise wbxexception(
                        "Error occurred: create job_name=%s, errormsg=%s, sql=%s" % (job_name, errormsg, sql))
            else:
                logger.info("{0} exist,skip it".format(job_name))

            logger.info("check gather statistics status job")
            try:
                sql = '''select * from dba_scheduler_jobs where job_name in('%s')''' % (job_name)
                re = cursor.execute(sql).fetchall()
                # logger.info(re)
                if len(re) == 1:
                    logger.info("check gather statistics status job %s success" % (job_name))
                else:
                    logger.info("check gather statistics status job %s fail" % (job_name))
            except Exception as e:
                errormsg = "error msg: %s" % e
                raise wbxexception(
                    "Error occurred: check gather statistics status job, %s,host_name=%s, sql=%s" % (
                        errormsg, tgt_host_name, sql))

        except Exception as e:
            raise wbxexception(e)

    # 1.find active file
    #   if active file exist and not configured before, then copy a new active file
    #   else create a new active file
    # 2. add new config content to file
    # 3. verify config
    # 4. active new config file
    # 5. verify if new config file is active
    # 6. insert into shareplex_info
    # 7. check Shareplex data src->tgt
    def shareplexConfigSrcToTgt(self,src_appln_support_code,tgt_appln_support_code,src_db,src_host_name,tgt_db,tgt_host_name,port,src_pool_name,tgt_pool_name,opt_host_name,type,new_tahol_schema_name,replication_to):
        logger.info("******** start shareplexConfigSrcToTgt {0}({1}) --> {2}({3}) ********" .format(src_host_name, src_db, tgt_host_name,tgt_db))
        new_tahol_schema_name = new_tahol_schema_name.upper()
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDefaultDaoManager()
        try:
            dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            daoManager.startTransaction()
            redotables = []
            logger.info(
                "getReplicationTableList, src_appln_support_code={0}, tgt_appln_support_code={1}, src_db_name={2}, tgt_db_name={3}".format(
                    src_appln_support_code, tgt_appln_support_code, src_db, tgt_db))
            tablelist = dao.getReplicationTableList(src_appln_support_code, tgt_appln_support_code, src_db, tgt_db)
            for line in tablelist:
                row = dict(line)
                if row['src_appln_support_code'].upper() != 'TEL':
                    redotables.append(row)
                elif row['src_appln_support_code'].upper()=='TEL' and row['src_schema'].upper()==new_tahol_schema_name:
                    if row['tgt_appln_support_code'].upper() != 'TEL':
                        redotables.append(row)
                    elif row['tgt_appln_support_code'].upper() == 'TEL' and row['tgt_schema'].upper() == new_tahol_schema_name:
                        redotables.append(row)
            logger.info("get tablelist count {0}" .format(len(redotables)))
            if len(redotables) == 0:
                logger.error("ReplicationTableList is null.")
                raise wbxexception("ReplicationTableList is null")
            step_num = 1
            logger.info("opt_host_name:{0}" .format(opt_host_name))
            port_server = daoManagerFactory.getServer(hostname=opt_host_name)
            port_server.connect()
            file = ""
            record = ""
            isNeedConfig = True
            pre = ""
            try:
                logger.info("[{0}] find active file".format(step_num))
                step_num += 1
                cmd = "ps -ef | grep sp_cop | grep -v grep | grep %s | awk '{print $8}' | sed -n '1p'" % port
                logger.info(cmd)
                pre_str = port_server.exec_command(cmd)
                logger.info(pre_str)
                pre = str(pre_str).split('/')[1]
                logger.info(pre)
                pre_bin = "/"+str(pre_str).split('/')[1]+"/"+str(pre_str).split('/')[2]
                cmd1 = "cat /" + pre + "/vardir_" + str(port) + "/data/statusdb | grep \"active from\" | awk -F\\\" '{print $2}'"
                logger.info(cmd1)
                file = port_server.exec_command(cmd1)
                logger.info(file)
                cmd2 = "cat /" + pre + "/vardir_" + str(port) + "/config/" + file + " |grep -i " + tgt_db + "_SPLEX |grep -v ^#"
                logger.info(cmd2)
                record = port_server.exec_command(cmd2)
            except Exception as e:
                logger.error(str(e), exc_info=e)
                raise wbxexception(str(e))
            config_filename = "/{0}/vardir_{1}/config/{2}".format(pre, port, file)
            logtime = datetime.datetime.strftime(wbxutil.getcurrenttime(), "%Y%m%d_%H%M%S")
            new_config_path = "/{0}/vardir_{1}/config/".format(pre,port)
            new_config_name = "splex.{0}.config.auto.{1}" .format(port,logtime)
            bin_path = "{0}/bin" .format(pre_bin)
            logger.info("get shareplex config tables")
            contents = self.shareplexConfigContent2(type,src_host_name,tgt_host_name,src_pool_name,tgt_pool_name,src_db,tgt_db,port,redotables,new_tahol_schema_name)
            ls = contents.split('\n')
            if file:
                if record == "":
                    logger.info("active {0} exist".format(file))
                    logger.info("[{0}] copy a new active file".format(step_num))
                    try:
                        step_num += 1
                        cmd = "cp {0} {1}".format(config_filename, new_config_path+new_config_name)
                        logger.info(cmd)
                        port_server.exec_command(cmd)

                        logger.info("[{0}] add new config content to {1}".format(step_num, new_config_path+new_config_name))
                        step_num += 1
                        for content in ls:
                            logger.info(content)
                            cmd = " echo -e \"{0}\">>{1}".format(content, new_config_path+new_config_name)
                            port_server.exec_command(cmd)
                    except Exception as e:
                        logger.error(str(e), exc_info=e)
                        raise wbxexception(str(e))
                else:
                    isNeedConfig = False
                    logger.info(
                        "[{0}] Already configured no need to configure again, port:{1}, src_db_name:{2}, tgt_db_name:{3}".format(
                            step_num, port, src_db, tgt_db))
                    step_num += 1

            if not file:
                logger.info("active shareplex config file not exist")
                logger.info("[{0}] create a config file:{1}".format(step_num, new_config_path+new_config_name))
                step_num += 1
                try:
                    # splex_sid =""
                    # if type == "CONFIG2TEL":
                    #     if src_db == "CONFIGDB":
                    #         splex_sid = "CFGDB"
                    #     if src_db == "GCFGDB":
                    #         splex_sid = "GCFGDB"
                    # else:
                    #     splex_sid = src_pool_name.upper()
                    splex_sid = src_db.upper()

                    cmd = "echo -e Datasource:o.{0}_SPLEX >>{1} ".format(src_db, new_config_path + new_config_name)
                    logger.info("Datasource:o.{0}_SPLEX" .format(splex_sid))
                    port_server.exec_command(cmd)
                    for content in ls:
                        cmd = " echo -e \"{0}\">>{1}".format(content, new_config_path+new_config_name)
                        logger.info(content)
                        port_server.exec_command(cmd)
                except Exception as e:
                    logger.error(str(e), exc_info=e)
                    raise wbxexception(str(e))

            if isNeedConfig:
                logger.info("[{0}] verify config {1}" .format(step_num,new_config_name))
                step_num += 1
                try:
                    cmd = """
                                           source %s/.profile_%s; cd %s;
                                           ./sp_ctrl << EOF
                                           verify config %s
                                           show
                                           EOF
                                           """ % (bin_path,port,bin_path,new_config_name)
                    logger.info(cmd)
                    re = port_server.exec_command(cmd)
                    logger.info(re)
                except Exception as e:
                    logger.error(str(e), exc_info=e)
                    raise wbxexception(str(e))

                activeFlag = False
                try:
                    logger.info("[{0}] active new config file".format(step_num))
                    step_num += 1
                    cmd = """              source %s/.profile_%s; cd %s;
                                           ./sp_ctrl << EOF
                                           activate config %s
                                           show
                                           EOF
                                           """ % (bin_path,port,bin_path,new_config_name)
                    logger.info(cmd)
                    port_server.exec_command(cmd)
                except Exception as e:
                    logger.error(str(e), exc_info=e)
                    raise wbxexception(str(e))

                try:
                    logger.info("[{0}] check if new config file is active".format(step_num))
                    step_num += 1
                    cmd = "cat /%s/vardir_%s/data/statusdb | grep \"active from\" | awk -F\\\" '{print $2}'" % (pre,port)
                    f = port_server.exec_command(cmd)
                    logger.info("new active file is {0}".format(f))
                    if f == new_config_name:
                        logger.info("active success")
                        logger.info("new active file path:{0}, host name:{1}" .format(new_config_path+new_config_name,opt_host_name))
                        activeFlag = True
                    else:
                        logger.info("active fail")
                except Exception as e:
                    logger.error(str(e), exc_info=e)
                    raise wbxexception(str(e))

                if activeFlag:
                    logger.info("[{0}] insert into shareplex_info" .format(step_num))
                    step_num += 1

                    # src_splex_sid = src_db.upper() + "_SPLEX"
                    # tgt_splex_sid = tgt_db.upper() + "_SPLEX"

                    splex_sid = ""
                    # if type == "CONFIG2TEL":
                    #     if src_db == "CONFIGDB":
                    #         splex_sid = "CFGDB"
                    #     if src_db == "GCFGDB":
                    #         splex_sid = "GCFGDB"
                    # else:
                    #     # splex_sid = src_pool_name.upper()
                    #     splex_sid = src_db.upper()
                    #
                    # splex_sid_tgt = ""
                    # if type == "CONFIG2TEL":
                    #     if tgt_db == "CONFIGDB":
                    #         splex_sid_tgt = "CFGDB"
                    #     if tgt_db == "GCFGDB":
                    #         splex_sid_tgt = "GCFGDB"
                    # else:
                    #     # splex_sid_tgt = tgt_pool_name.upper()
                    #     splex_sid_tgt = tgt_db.upper()

                    # logger.info("splex_sid:{0},splex_sid_tgt={1}".format(splex_sid,splex_sid_tgt))
                    # src_splex_sid = splex_sid + "_SPLEX"
                    # tgt_splex_sid = splex_sid_tgt + "_SPLEX"

                    src_splex_sid = src_db.upper() + "_SPLEX"
                    tgt_splex_sid = tgt_db.upper() + "_SPLEX"
                    logger.info("src_splex_sid:{0},tgt_splex_sid={1}".format(src_splex_sid, tgt_splex_sid))

                    src_schema = ""
                    tgt_schema = ""
                    if replication_to =="cfg_ptahoe" or replication_to =="cfg_gtahoe" or replication_to=="gcfg_ptahoe" or replication_to =="gcfg_gtahoe":
                        src_schema = "test"
                        tgt_schema = "test"
                    if replication_to == "ptahoe_gtahoe" or replication_to == "gtahoe_ptahoe":
                        src_schema = new_tahol_schema_name
                        tgt_schema = new_tahol_schema_name
                    if replication_to == "ptahoe_opdb" or replication_to =="gtahoe_opdb":
                        src_schema = new_tahol_schema_name
                        tgt_schema = "test"
                    if replication_to == "ptahoe_pstool" or replication_to =="gtahoe_pstool":
                        src_schema = new_tahol_schema_name
                        tgt_schema = "test"
                    if replication_to == "cfg_gtahoe" or replication_to =="gcfg_ptahoe":
                        src_schema = "test"
                        tgt_schema = "test"
                    try:
                        rows = dao.getShareplex_info(src_db, tgt_db, port, replication_to)
                        if len(rows) == 0:
                            logger.info(
                                "insert shareplex_info, src_host_name={0},src_db={1},port={2},replication_to={3},"
                                "tgt_host_name={4},tgt_db={5},src_splex_sid={6},tgt_splex_sid={7},src_schema={8},tgt_schema={9}".format(
                                    src_host_name, src_db, port, replication_to, tgt_host_name, tgt_db,
                                    src_splex_sid, tgt_splex_sid, src_schema, tgt_schema))
                            dao.insertShareplex_info(src_host_name, src_db, port, replication_to, tgt_host_name, tgt_db, '',
                                                     src_splex_sid, tgt_splex_sid, src_schema, tgt_schema)
                        daoManager.commit()
                    except Exception as e:
                        daoManager.rollback()
                        raise wbxexception(str(e))

                    logger.info("[{0}] check Shareplex data src->tgt".format(step_num))
                    step_num += 1
                    vo = dao.getWbxadbmon(src_db,tgt_db,port,replication_to)
                    if len(vo)>0:
                        res = adbmoncheck_global(port, src_db, src_host_name, tgt_db, tgt_host_name,replication_to)
                        if res['status'] == 'SUCCESS':
                            logger.info("check Shareplex success")
                            return True
                        else:
                            logger.error("check Shareplex fail" )
                            return False
                    else:
                        logger.error("No records in wbxadbmon ,please check. src_db={0},tgt_db={1},port={2},replication_to={3}" .format(src_db,tgt_db,port,replication_to))
                        return False
            else:
                return True

            return True

        except Exception as e:
            logger.error(str(e), exc_info=e)
            raise wbxexception(str(e))



    def shareplexConfigContent2(self,type,src_host_name,tgt_host_name,src_pool_name,tgt_pool_name,src_db,tgt_db,port,redotables,new_tahol_schema_name):
        src_db = src_db.upper()
        tgt_db = tgt_db.upper()
        src_pool_name = src_pool_name.upper()
        tgt_pool_name = tgt_pool_name.upper()
        content = "\n##Add for new %s \n" % (tgt_db)
        if type=="CONFIG2TEL":
            content += "SPLEX%s.SPLEX_MONITOR_ADB SPLEX%s.SPLEX_MONITOR_ADB %s-vip:%s2%s*%s-vip@o.%s_SPLEX\n" % (port, port, src_host_name, src_db,tgt_db, tgt_host_name, tgt_db)
            for vo in redotables:
                # table = dict(vo)
                src_schema = vo['src_schema']
                tgt_schema = vo['tgt_schema']
                src_tablename = vo['src_tablename']
                tgt_tablename = vo['tgt_tablename']
                if src_schema and tgt_schema and src_tablename and tgt_tablename:
                    content += "%s.%s %s.%s %s-vip:%s2%s*%s-vip@o.%s_SPLEX\n" % (src_schema, src_tablename, tgt_schema,
                                                                                  tgt_tablename, src_host_name, src_db,tgt_db, tgt_host_name,tgt_db)
                else:
                    logger.info("data miss {0}".format(vo))
        if type=="TEL2TEL":
            content += "SPLEX%s.SPLEX_MONITOR_ADB SPLEX%s.SPLEX_MONITOR_ADB %s-vip:%s2%s*%s-vip@o.%s_SPLEX\n" % (port, port, src_host_name, src_pool_name,tgt_pool_name, tgt_host_name, tgt_db)
            for vo in redotables:
                # table = dict(vo)
                src_schema = vo['src_schema']
                tgt_schema = vo['tgt_schema']
                src_tablename = vo['src_tablename']
                tgt_tablename = vo['tgt_tablename']
                if src_schema and tgt_schema and src_tablename and tgt_tablename:
                    content += "%s.%s %s.%s %s-vip:%s2%s*%s-vip@o.%s_SPLEX\n" % (
                    src_schema, src_tablename, tgt_schema,tgt_tablename,src_host_name,src_pool_name,tgt_pool_name,tgt_host_name,tgt_db)
                else:
                    logger.info("data miss {0}".format(vo))
        if type=="TEL2OPDB":
            content += "SPLEX%s.SPLEX_MONITOR_ADB SPLEX%s.SPLEX_MONITOR_ADB %s-vip:%s_2_OPDB*%s-vip@o.RACOPDB_SPLEX\n" % (
            port, port, src_host_name, src_pool_name, tgt_host_name)
            for vo in redotables:
                # table = dict(vo)
                src_schema = vo['src_schema']
                tgt_schema = vo['tgt_schema']
                src_tablename = vo['src_tablename']
                tgt_tablename = vo['tgt_tablename']
                if src_schema and tgt_schema and src_tablename and tgt_tablename:
                    # <new_tahol_schema_name>.<src_tablename> TEST.<tgt_tablename> <pri_host_name>-vip:<pri_pool_name>_2_OPDB*<opdb_host_name>-vip@o.<pri_db_name>_SPLEX
                    # TAHOE.CDR_PARTICIPANTS TEST.CDR_PARTICIPANTS tadbth392-vip:TTA34Nw_OPDB0*sjdbormt090-vip:TTA34Nw_OPDB0@o.RACOPDB_SPLEX
                    content += "%s.%s %s.%s %s-vip:%s_2_OPDB*%s-vip@o.RACOPDB_SPLEX\n" % (
                        src_schema, src_tablename, tgt_schema,tgt_tablename,src_host_name,src_pool_name,tgt_host_name)
                else:
                    logger.info("data miss {0}".format(vo))
        if type=="TEL2TOOLS":
            content += "SPLEX%s.SPLEX_MONITOR_ADB SPLEX%s.SPLEX_MONITOR_ADB %s-vip:%s_2_STOOL*%s-vip@o.PSYTOOL_SPLEX\n" % (
            port, port, src_host_name,  src_pool_name,tgt_host_name)
            for vo in redotables:
                # table = dict(vo)
                src_schema = vo['src_schema']
                tgt_schema = vo['tgt_schema']
                src_tablename = vo['src_tablename']
                tgt_tablename = vo['tgt_tablename']
                if src_schema and tgt_schema and src_tablename and tgt_tablename:
                    #<new_tahol_schema_name>.<src_tablename> TEST.<tgt_tablename> <pri_host_name>-vip:<pri_pool_name>_2_STOOL*<tool_host_name>-vip@o.PSYTOOL_SPLEX
                    content += "%s.%s %s.%s %s-vip:%s_2_STOOL*%s-vip@o.PSYTOOL_SPLEX\n" % (
                        src_schema, src_tablename, tgt_schema,tgt_tablename,src_host_name,src_pool_name,tgt_host_name)
        return content

    def getCreateTestSchemaTable(self,type):
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daoManagerFactory.getDefaultDaoManager()
        try:
            daoManager.startTransaction()
            depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            ls = depotdbDao.getBuildTahoeTableByType(type)
            daoManager.commit()
            return ls
        except Exception as e:
            daoManager.rollback()

    def getGrantSql(self,new_tahol_schema_name,tgt_db_name):
        sql = '''
                            grant execute on SYS.DBMS_CRYPTO to %s;
                            grant execute on SYS.DBMS_JOB to %s;
                            grant execute on SYS.DBMS_LOCK to %s;
                            grant execute on SYS.DBMS_RANDOM  to %s;
                            grant select on sys.DUAL to %s;
                            grant select on sys.V_$DATABASE to %s;
                            grant select on sys.V_$INSTANCE to %s;
                            grant select on sys.V_$SESSION to %s;
                            grant execute on TEST.PKGTAHOESEARCH to %s;
                            grant select,UPDATE,INSERT,DELETE on WBXMAINT.ACCOUNT_ACCESS_NUMBER_SWT to %s;
                            grant select,UPDATE,INSERT,DELETE on WBXMAINT.ACCOUNT_SWT to %s;
                            grant select,UPDATE,INSERT,DELETE on WBXMAINT.CONFERENCE_SWT to %s;
                            grant select,UPDATE,INSERT,DELETE on WBXMAINT.PASSCODE_SWT to %s;
                            grant select,UPDATE,INSERT,DELETE on WBXMAINT.WBXSITECONFIG_TAHOE_SWT to %s;
                    '''
        sqllist = sql.split(";")
        str = ""
        retnlist = []
        for sql in sqllist:
            sql = sql.strip()
            if sql:
                new_sql = sql.replace("%s", new_tahol_schema_name)
                retnlist.append(new_sql)
                str += new_sql + ";"

        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        db = daomanagerfactory.getDatabaseByDBName(tgt_db_name)
        connUrl = None
        try:
            connUrl = db.getConnectionURL()
        except DatabaseError as e:
            raise wbxexception("Can not getConnectionURL to db %s" % (tgt_db_name))

        connect = cx_Oracle.connect("sys/sysnotallow@" + connUrl, mode=cx_Oracle.SYSDBA)
        try:
            cursor = connect.cursor()
            sql = "select owner,table_name from dba_tables where owner='TEST' "
            cursor.execute(sql)
            tables = cursor.fetchall()
            for table in tables:
                a = "grant FLASHBACK,DEBUG,QUERY REWRITE,ON COMMIT REFRESH,REFERENCES,UPDATE,SELECT,INSERT,INDEX,DELETE,ALTER on %s.%s to %s " % (table[0],
                table[1], new_tahol_schema_name)
                retnlist.append(a)
        except Exception as e:
            raise wbxexception("select test table fail, sql {1}".format(sql))

        #
        # TAHOE -> splex<port_for_other>
        # TEST -> splex<port_for_configdb>
        try:
            cursor = connect.cursor()
            sql = "select owner,table_name from dba_tables where owner='TAHOE' "
            user = "splex" + self._port_for_other
            cursor.execute(sql)
            tables = cursor.fetchall()
            for table in tables:
                a = "grant FLASHBACK,DEBUG,QUERY REWRITE,ON COMMIT REFRESH,REFERENCES,UPDATE,SELECT,INSERT,INDEX,DELETE,ALTER on %s.%s to %s " % (table[0],
                table[1], user)
                retnlist.append(a)
        except Exception as e:
            raise wbxexception("select test table fail, sql {1}".format(sql))

        try:
            cursor = connect.cursor()
            sql = "select owner,table_name from dba_tables where owner='TEST' "
            user = "splex" + self._port_for_configdb
            cursor.execute(sql)
            tables = cursor.fetchall()
            for table in tables:
                a = "grant FLASHBACK,DEBUG,QUERY REWRITE,ON COMMIT REFRESH,REFERENCES,UPDATE,SELECT,INSERT,INDEX,DELETE,ALTER on %s.%s to %s " % (table[0],
                table[1], user)
                retnlist.append(a)
        except Exception as e:
            raise wbxexception("select test table fail, sql {1}".format(sql))

        return retnlist

    def updateJob_host_name_for_config(self,jobid, description):
        logger.info("Update job description=%s in depotdb with jobid=%s" % (description, jobid))
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            daomanager.startTransaction()
            dao.updateJobHostNameForConfig(jobid, description)
        except Exception as e:
            daomanager.rollback()

    def getJob_host_name_for_config(self,jobid):
        logger.info("get Job host name for config, jobid:%s" %(jobid))
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            daomanager.startTransaction()
            item = dao.getConfigHostNameByJobid(jobid)
            logger.info(dict(item[0])['description'])
            # config_host_name=%s,opdb_host_name=%s,tool_host_name=%s
            config_host_name = str(str(dict(item[0])['description']).split(",")[0]).split("=")[1]
            opdb_host_name = str(str(dict(item[0])['description']).split(",")[1]).split("=")[1]
            # tool_host_name = str(str(dict(item[0])['description']).split(",")[2]).split("=")[1]
            gcfgdb_host_name = str(str(dict(item[0])['description']).split(",")[2]).split("=")[1]
            return config_host_name,opdb_host_name,gcfgdb_host_name
        except Exception as e:
            daomanager.rollback()

    def findHostNameByPort(self,login_host_name,port):
        logger.info("findHostNameByPort, login_host_name:%s, port=%s" % (login_host_name,port))
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        host_name_for_port = ""
        try:
            login_server = daomanagerfactory.getServer(hostname=login_host_name)
            login_server.connect()
            cmd = ". /home/oracle/.bash_profile; crsstat | grep ^shareplex | grep %s " % (port)
            logger.info(cmd)
            res = login_server.exec_command(cmd)
            logger.info(res)
            if res:
                res = ' '.join(res.split())
                host_name_for_port = str(res).split(" ")[4]
                logger.info(
                    "found the host name:{0} with port {1}".format(host_name_for_port, port))
                return host_name_for_port
            else:
                # logger.error("Do not find host name for port {0}".format(port))
                raise wbxexception("Do not find host name for port {0}".format(port))
        except Exception as e:
            raise wbxexception("find host name for port:{0} fail".format(login_host_name))

    def checkDBSupplementalLogging(self,db_name):
        logger.info("check DB supplemental log, db_name=%s" % (db_name))
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        db = daomanagerfactory.getDatabaseByDBName(db_name)
        connUrl = None
        flag = True
        try:
            connUrl = db.getConnectionURL()
        except DatabaseError as e:
            raise wbxexception("Can not getConnectionURL to db %s" % (db_name))

        connect = cx_Oracle.connect("sys/sysnotallow@" + connUrl, mode=cx_Oracle.SYSDBA)

        sql = "select supplemental_log_data_min, supplemental_log_data_pk, supplemental_log_data_ui from v$database "
        try:
            cursor = connect.cursor()
            cursor.execute(sql)
            items = cursor.fetchall()
            count_yes = 0
            supplemental_log_data_min = items[0][0]
            supplemental_log_data_pk = items[0][1]
            supplemental_log_data_ui = items[0][2]
            if supplemental_log_data_min == "YES":
                count_yes +=1
            if supplemental_log_data_pk == "YES":
                count_yes +=1
            if supplemental_log_data_ui == "YES":
                count_yes +=1
            if count_yes < 2:
                logger.info(
                    "Check supplemental log have issue! supplemental_log_data_min={0}, supplemental_log_data_pk={1},supplemental_log_data_ui={2}, db_name={3}".format(
                        supplemental_log_data_min,supplemental_log_data_pk,supplemental_log_data_ui, db_name))
                flag = False
        except Exception as e:
            raise wbxexception("check DB supplemental log , sql:{0}, e:{1}".format(sql,str(e)))
        return flag

if __name__ == "__main__":
    # print(datetime.datetime.utcnow())
    # time = datetime.datetime.strftime(wbxutil.getcurrenttime(), "%Y/%m/%d %H:%M:%S")
    # yyyy/mm/dd hh24:mi:ss
    #2020/02/29 05:30:00
    # print(time)

    # tomorrow = datetime.datetime.utcnow()+ datetime.timedelta(days=1)
    # strf_tomorrow = datetime.datetime.strftime(tomorrow,"%Y/%m/%d %H:%M:%S")
    # print(strf_tomorrow)
    db_type="pri"
    step_total = str(12) + " (" + db_type + ")"
    a ="[1/{0}]" .format(step_total)
    print(a)
