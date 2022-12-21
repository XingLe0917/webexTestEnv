import logging
import threading
from biz.dbmanagement.wbxdb import wbxdb
from dao.wbxdaomanager import wbxdaomanagerfactory
from common.wbxexception import wbxexception
from common.wbxtask import wbxautotask,threadlocal
from dao.wbxdaomanager import DaoKeys
import time

logger = logging.getLogger("DBAMONITOR")

class wbxtahoedbcutovertask(wbxautotask):
    def __init__(self,taskid = None):
        super(wbxtahoedbcutovertask,self).__init__(taskid, "TAHOEDBCUTOVER_TASK")


    def initialize(self, **kwargs):
        self._pri_pool_name = kwargs["pri_pool_name"].upper()
        self._gsb_pool_name = kwargs["gsb_pool_name"].upper()
        self._old_pri_db_name = None
        self._old_gsb_db_name = None
        self._new_pri_db_name = kwargs["new_pri_db_name"].upper()
        self._new_gsb_db_name = None
        self._new_pri_host = []
        self._new_gsb_host = []
        self._old_pri_host = []
        self._old_gsb_host = []
        self._schema = None
        self._port_from_configdb = int(kwargs["port_from_configdb"])
        self._port_to_opdb = int(kwargs["port_to_opdb"])

        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daomanagerfactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        self._old_pri_db_name, self._old_gsb_db_name, self._schema = depotdbDao.getoldprigsbdbbypoolName(self._pri_pool_name, self._gsb_pool_name)
        self._new_gsb_db_name = depotdbDao.getgsbdbbypridbName(self._new_pri_db_name)
        self._oldpridb = daomanagerfactory.getDatabaseByDBName(self._old_pri_db_name)
        if self._oldpridb is None:
            raise wbxexception("Can not get database with db_name=%s" % self._oldpridb)
        logger.info("old db name %s exists" % self._old_pri_db_name)
        for servername, dbserver in self._oldpridb.getServerDict().items():
            self._old_pri_host.append(servername)
        self._old_pri_host.sort()
        self._oldgsbdb = daomanagerfactory.getDatabaseByDBName(self._old_gsb_db_name)
        if self._oldgsbdb is None:
            raise wbxexception("Can not get database with db_name=%s" % self._oldgsbdb)
        logger.info("old db name %s exists" % self._old_gsb_db_name)
        for servername, dbserver in self._oldgsbdb.getServerDict().items():
            self._old_gsb_host.append(servername)
        self._old_gsb_host.sort()
        self._newpridb = daomanagerfactory.getDatabaseByDBName(self._new_pri_db_name)
        if self._newpridb is None:
            raise wbxexception("Can not get database with db_name=%s" % self._newpridb)
        logger.info("new db name %s exists" % self._new_pri_db_name)
        for servername, dbserver in self._newpridb.getServerDict().items():
            self._new_pri_host.append(servername)
        self._new_pri_host.sort()
        self._newgsbdb = daomanagerfactory.getDatabaseByDBName(self._new_gsb_db_name)
        if self._newgsbdb is None:
            raise wbxexception("Can not get database with db_name=%s" % self._newgsbdb)
        logger.info("new db name %s exists" % self._new_gsb_db_name)
        for servername, dbserver in self._newgsbdb.getServerDict().items():
            self._new_gsb_host.append(servername)
        self._new_gsb_host.sort()

        logger.info("schema name : %s" % self._schema)
        # self._db_type = res["db_type"]

        taskvo = super(wbxtahoedbcutovertask, self).initialize(**kwargs)

        jobList = self.listTaskJobsByTaskid(taskvo.taskid)
        if len(jobList) == 0:
            self.generateJobs()
        return taskvo

    def executeOneStep(self, *args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("wbxdbcutover.executeOneStep(processid=%s)" % jobid)
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        try:
            daomanager.startTransaction()
            jobvo = dao.getAutoTaskJobByJobid(jobid)
            retcode = "FAILED"
            print(jobvo.parameter["stage"])
            if jobvo.parameter["stage"] == "STOPHASERVICE":
                retcode = self.stop_service(jobvo)
            elif jobvo.parameter["stage"] == "KillCONNECTION":
                retcode = self.kill_machine_connection(jobvo)
            elif jobvo.parameter["stage"] == "SETUPREPLICATION":
                retcode = self.setup_replication(jobvo)
            elif jobvo.parameter["stage"] == "STOPPORT":
                retcode = self.stop_shareplex_port(jobvo)
            elif jobvo.parameter["stage"] == "STARTHASERVICE":
                retcode = self.start_ha_service(jobvo)
            elif jobvo.parameter["stage"] == "REMOVECHANNEL":
                retcode = self.remove_channel(jobvo)
            elif jobvo.parameter["stage"] == "STOPPOOL":
                retcode = self.stop_pool(jobvo)
            elif jobvo.parameter["stage"] == "preverify":
                retcode = self.preverify(jobvo)
            if retcode == "FAILED":
                raise wbxexception("Error occurred in executeOneStep with jobid:%s" % (jobid))
            self.updateJobStatus(jobid, "SUCCEED")

        except Exception as e:
            daomanager.rollback()
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
            raise e
        finally:
            daomanager.close()
        # jobvo = self.updateJobStatus(jobid, "RUNNING")


    def generateJobs(self):
        logger.info("generateTahoeCutoverStep(taskid=%s, pri_pool_name=%s, gsb_pool_name=%s)" % (self._taskid, self._pri_pool_name, self._gsb_pool_name))
        # self.getsvrmapping()
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daomanagerfactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        ########################### DB preverify ##############
        self.addJob(host_name=self._new_pri_host[0], db_name=self._old_pri_db_name, pool_name=self._pri_pool_name,
                    job_action="executeOneStep", stage="preverify", server_type="NEW", db_type="PRI", module="SERVER",
                    command="preverify",process_order=1, execute_method="SYNC")

        ########################### Adjust SEQ_ACCOUNT and setup replication ##############
        host_name = depotdbDao.gethostNamebyport(self._port_to_opdb, self._old_gsb_db_name, self._old_gsb_host)
        self.addJob(host_name=host_name, pool_name=self._gsb_pool_name, job_action="executeOneStep", action="ADDCHANNEL",
                    new_pri_db=self._new_pri_db_name,
                    stage="SETUPREPLICATION", server_type="OLD", db_type="GSB", splex_port=self._port_to_opdb, module="SERVER",
                    command="/staging/gates/dbcutover/tahoe_cutover.sh",process_order=2, execute_method="SYNC")

        ########################### Stop PRI service ##############
        self.addJob(host_name=self._old_pri_host[0], db_name=self._old_pri_db_name, pool_name=self._pri_pool_name,
                    job_action="executeOneStep", stage="STOPHASERVICE", module="SERVER",
                    server_type="OLD", db_type="PRI", process_order=3, execute_method="SYNC")

        ########################### Kill connection on PRI db ##############
        for host_name in self._old_pri_host:
            self.addJob(host_name=host_name, db_name=self._old_pri_db_name, pri_pool_name=self._pri_pool_name,
                        gsb_pool_name=self._gsb_pool_name, job_action="executeOneStep", stage="KillCONNECTION", module="DB",
                        schema=self._schema, server_type="OLD", db_type="PRI", process_order=4, execute_method="SYNC")

        ########################### Stop new PRI service ##############
        self.addJob(host_name=self._new_pri_host[0], db_name=self._new_pri_db_name, pool_name=self._pri_pool_name,
                    job_action="executeOneStep", stage="STOPHASERVICE", module="SERVER",
                    server_type="NEW", db_type="PRI", process_order=5, execute_method="SYNC")

        ########################### Kill connection on new PRI db ##############
        for host_name in self._new_pri_host:
            self.addJob(host_name=host_name, db_name=self._new_pri_db_name, pri_pool_name=self._pri_pool_name,
                        gsb_pool_name=self._gsb_pool_name, job_action="executeOneStep", stage="KillCONNECTION", module="DB",
                        schema=self._schema, server_type="NEW", db_type="PRI", process_order=6, execute_method="SYNC")

        ########################### Failback from new GSB to new PRI ##############
        self.addJob(host_name=self._new_pri_host[0], db_name=self._new_pri_db_name, pool_name=self._pri_pool_name,
                    job_action="executeOneStep", stage="STARTHASERVICE", module="SERVER",
                    server_type="NEW", db_type="PRI", process_order=7, execute_method="SYNC")

        ########################### Optionally modify shareplex config files ##############
        # not process the port from configdb on old pri/gsb db server, because it is a little complicated to handle it
        # and it is no harm to leave it
        _pri_config_host, _gsb_config_host = depotdbDao.getconfighostbydbNameandPort(
            self._old_pri_db_name,
            self._port_from_configdb)
        self.addJob(host_name=_pri_config_host, splex_port=self._port_from_configdb, module="SHAREPLEX",
                    db_type="PRI",
                    server_type="SRC", job_action="executeOneStep", stage="REMOVECHANNEL", process_order=8,
                    pool_name=self._pri_pool_name, action="REMOVECHANNEL", execute_method="SYNC")

        self.addJob(host_name=_gsb_config_host, splex_port=self._port_from_configdb, module="SHAREPLEX",
                    db_type="GSB",
                    server_type="SRC", job_action="executeOneStep", stage="REMOVECHANNEL", process_order=8,
                    pool_name=self._pri_pool_name, action="REMOVECHANNEL", execute_method="SYNC")

        ########################### stop shareplex ports ##############
        pri_spport = self._oldpridb.getShareplexPort(self._port_to_opdb)
        pri_host_name = pri_spport.getServer().getHostname()
        # pri_host_name = depotdbDao.gethostNamebyport(self._port_to_opdb, self._oldpridb.getDBName(), self._old_pri_host)
        self.addJob(host_name=pri_host_name, db_type="PRI", module="SHAREPLEX", server_type="OLD",
                    splex_port=self._port_to_opdb, job_action="executeOneStep", stage="STOPPORT",
                    process_order=9, execute_method="SYNC")

        gsb_spport = self._oldgsbdb.getShareplexPort(self._port_to_opdb)
        gsb_host_name = gsb_spport.getServer().getHostname()
        # gsb_host_name = depotdbDao.gethostNamebyport(self._port_to_opdb, self._oldgsbdb.getDBName(), self._old_gsb_host)
        self.addJob(host_name=gsb_host_name, db_type="GSB", module="SHAREPLEX", server_type="OLD",
                    splex_port=self._port_to_opdb, job_action="executeOneStep", stage="STOPPORT",
                    process_order=9, execute_method="SYNC")
        self.addJob(host_name=self._new_pri_host[0], db_type="PRI", module="SHAREPLEX", server_type="SRC",
                    splex_port=self._port_to_opdb, job_action="executeOneStep", stage="STOPPORT",
                    process_order=9, execute_method="SYNC")

        ########################### Stop pool ##############
        pri_spport = self._oldpridb.getShareplexPort(self._port_from_configdb)
        pri_host_name = pri_spport.getServer().getHostname()
        # pri_host_name = depotdbDao.getconfighostbydbNameandPort(self._old_pri_db_name, self._port_from_configdb)
        self.addJob(host_name=pri_host_name, db_name=self._old_pri_db_name, pool_name=self._pri_pool_name,
                    new_db_name = self._new_pri_db_name, new_trim_host = self._new_pri_host[0][:-1],
                    splex_port = self._port_from_configdb, port_to_opdb=self._port_to_opdb, job_action="executeOneStep",
                stage="STOPPOOL", module="SERVER", server_type="OLD", db_type="PRI", process_order=10, execute_method="SYNC")

        gsb_spport = self._oldgsbdb.getShareplexPort(self._port_from_configdb)
        gsb_host_name = gsb_spport.getServer().getHostname()
        # gsb_host_name = depotdbDao.getconfighostbydbNameandPort(self._old_gsb_db_name, self._port_from_configdb)
        self.addJob(host_name=gsb_host_name, db_name=self._old_gsb_db_name, pool_name=self._gsb_pool_name,
                    new_db_name=self._new_gsb_db_name, new_trim_host=self._new_gsb_host[0][:-1],
                splex_port = self._port_from_configdb, port_to_opdb=self._port_to_opdb, job_action="executeOneStep",
                stage="STOPPOOL", module="SERVER", server_type="OLD", db_type="GSB", process_order=10, execute_method="SYNC")

        logger.info("generateTahoeCutoverStep end with successed")

    def preverify(self, processvo):
        gsbserver = None
        priserver = None
        cmd = None
        retcode = "SUCCEED"
        new_pri_host = self._new_pri_host[0]
        new_gsb_host = self._new_gsb_host[0]
        try:
            for servername, dbserver in self._newpridb.getServerDict().items():
                dbserver.verifyConnection()
            logger.info("new pri db servers %s ssh login verification passed" % self._new_pri_host)
            for servername, dbserver in self._newgsbdb.getServerDict().items():
                dbserver.verifyConnection()
            logger.info("new gsb db servers %s ssh login verification passed" % self._new_gsb_host)
            for servername, dbserver in self._oldpridb.getServerDict().items():
                dbserver.verifyConnection()
            logger.info("old pri db servers %s ssh login verification passed" % self._old_pri_host)
            for servername, dbserver in self._oldgsbdb.getServerDict().items():
                dbserver.verifyConnection()
            logger.info("old gsb db servers %s ssh login verification passed" % self._old_gsb_host)
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            priserver = daomanagerfactory.getServer(new_pri_host)
            priserver.connect()
            cmd = "source /home/oracle/.bash_profile; crsstat | grep %sha | wc -l" % (self._pri_pool_name.lower())
            print("%s on server %s" % (cmd, new_pri_host))
            logger.info("%s on server %s" % (cmd, new_pri_host))
            resmsg = priserver.exec_command(cmd, async_log=True)
            if resmsg.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            service_count = resmsg.split("\n")[0]
            print("pri service count : %s" % service_count)
            if int(service_count) < 1:
                raise wbxexception("please create ha service of %s on new primary server" % self._pri_pool_name.lower())

            ################
            gsbserver = daomanagerfactory.getServer(new_gsb_host)
            gsbserver.connect()
            cmd = "source /home/oracle/.bash_profile; crsstat | grep %sha | wc -l" % (self._gsb_pool_name.lower())
            logger.info("%s on server %s" % (cmd, new_gsb_host))
            resmsg = gsbserver.exec_command(cmd, async_log=True)
            if resmsg.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            service_count = resmsg.split("\n")[0]
            if int(service_count) < 1:
                raise wbxexception("please create ha service of %s on new gsb server" % self._gsb_pool_name.lower())
            cmd = "source /home/oracle/.bash_profile; crsstat | grep shareplex%s | wc -l" % (self._port_to_opdb)
            logger.info("%s on server %s" % (cmd, new_pri_host))
            resmsg = priserver.exec_command(cmd, async_log=True)
            if resmsg.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            port_count = resmsg.split("\n")[0]
            logger.info("port count : %s" % port_count)
            if int(port_count) < 1:
                raise wbxexception("please create shareplex port %s on new primary server" % self._port_to_opdb)
        except Exception as e:
            retcode = "FAILED"
            logger.error(e)
        finally:
            if gsbserver:
                gsbserver.close()
            if priserver:
                priserver.close()
        return retcode

    def stop_service(self, processvo):
        server = None
        cmd = None
        retcode = "SUCCEED"
        host_name = processvo.parameter["host_name"].lower()
        db_name = processvo.parameter["db_name"].upper()
        pool_name = processvo.parameter["pool_name"].upper()
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)
            server.connect()
            cmd = "source /home/oracle/.bash_profile; srvctl stop service -d %s -s %sha" % (db_name, pool_name)
            logger.info("%s on server %s" % (cmd, host_name))
            resmsg = server.exec_command(cmd, async_log=True)
            if resmsg.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            cmd = "source /home/oracle/.bash_profile; crsstat | grep '%sha' | awk '{print $3$4}'" % pool_name
            logger.info("%s on server %s" % (cmd, host_name))
            targetstate = server.exec_command(cmd, async_log=True)
            if targetstate.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            for line in targetstate.split("\n"):
                # if "OFFLINEOFFLINE" not in line:
                #     raise wbxexception("Stop service on %s failed" % host_name)
                logger.info(line)
        except Exception as e:
            retcode = "FAILED"
            logger.error(e)
        finally:
            if server:
                server.close()
        return retcode

    def kill_machine_connection(self, processvo):
        cmd = None
        retcode = "SUCCEED"
        db_name = processvo.parameter["db_name"].upper()
        pri_pool_name = processvo.parameter["pri_pool_name"].upper()
        gsb_pool_name = processvo.parameter["gsb_pool_name"].upper()
        schema = processvo.parameter["schema"].upper()
        host_name = processvo.parameter["host_name"].lower()
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daomanagerfactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        ha_name = depotdbDao.getinstanceNamebydbNameandhost(db_name, host_name)
        server = None
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)
            server.connect()

            cmd = """
                    . /home/oracle/.bash_profile
                    db
                    export ORACLE_SID=%s
                    sqlplus / as sysdba << EOF 
                    SET pagesize 0 linesize 1000 feedback off heading off echo off serveroutput on;
                    select 'alter system kill session ''' ||sid || ',' || serial# || ''' immediate;' from %s where regexp_like(machine,'^%s[[:alpha:]]')
                    union 
                    select 'alter system kill session ''' ||sid || ',' || serial# || ''' immediate;' from %s where regexp_like(machine,'^%s[[:alpha:]]');
                    exit;
                    EOF
                    """ % (ha_name, "gv\$session", pri_pool_name.lower(), "gv\$session", gsb_pool_name.lower())
            logger.info(cmd)
            cmd_list = server.exec_command(cmd)
            if cmd_list.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            exec_cmd_list = self.address_cmd_list(cmd_list)
            cmd_str = ""
            for cmd in exec_cmd_list:
                cmd_str = cmd_str + cmd + "\n"
            cmd_str += "exit;"
            cmd = """
                    . /home/oracle/.bash_profile
                    db
                    export ORACLE_SID=%s
                    sqlplus / as sysdba << EOF 
                    %s
                    EOF
                    """ % (ha_name, cmd_str)
            logger.info(cmd)
            kill_conn = server.exec_command(cmd)
            if kill_conn.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
        except Exception as e:
            retcode = "FAILED"
            logger.error(e)
        finally:
            if server:
                server.close()
        return retcode

    def setup_replication(self, processvo):
        server = None
        cmd = None
        retcode = "SUCCEED"
        command = processvo.parameter["command"]
        pool_name = processvo.parameter["pool_name"].upper()
        host_name = processvo.parameter["host_name"].lower()
        port = processvo.parameter["splex_port"]
        action = processvo.parameter["action"]
        new_pri_db = processvo.parameter["new_pri_db"]
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)
            server.connect()
            cmd = "source /home/oracle/.bash_profile; sh %s %s %s %s %s" % (command, pool_name, port, action, new_pri_db)
            logger.info("%s on server %s" % (cmd, host_name))
            resmsg = server.exec_command(cmd, async_log=True)
            if resmsg.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            cmd = "crsstat | grep '%sha' | awk '{print $3$4}'" % pool_name.lower()
            logger.info("%s on server %s" % (cmd, host_name))
            targetstate = server.exec_command(cmd, async_log=True)
            if targetstate.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            for line in targetstate.split("\n"):
                # if "OFFLINEOFFLINE" not in line:
                #     raise wbxexception("Stop service on %s failed" % host_name)
                logger.info(line)
        except Exception as e:
            retcode = "FAILED"
            logger.error(e)
        finally:
            if server:
                server.close()
        return retcode

    def address_cmd_list(self, cmd_list):
        cmds = cmd_list.split("\n")
        rst_list = []
        for cmd_item in cmds:
            if "alter system kill session" in cmd_item:
                rst_list.append("alter system kill session" + cmd_item.split("alter system kill session")[-1])
        rst_list = list(set(rst_list))
        return rst_list

    def stop_shareplex_port(self, processvo):
        server = None
        cmd = None
        retcode = "SUCCEED"
        host_name = processvo.parameter["host_name"].lower()
        port = processvo.parameter["splex_port"]
        server_type = processvo.parameter["server_type"]
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            defaultDaoManager = daomanagerfactory.getDefaultDaoManager()
            depotDao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            if server_type != "SRC":
                isShared = depotDao.isportshared(host_name, port)
                if isShared:
                    raise wbxexception("This port is shared by multiple src_splex_sid based on depotdb data")
            server = daomanagerfactory.getServer(host_name)
            server.connect()
            cmd = "source /home/oracle/.bash_profile; crsctl stop resource shareplex%s -f" % port
            logger.info("%s on server %s" % (cmd, host_name))
            resmsg = server.exec_command(cmd, async_log=True)
            if resmsg.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            time.sleep(10)
            cmd = "ps aux | grep sp_cop | grep %s | grep -v grep | wc -l" % port
            logger.info("%s on server %s" % (cmd, host_name))
            sp_cop_count = server.exec_command(cmd, async_log=True)
            if sp_cop_count.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            sp_cop_count = sp_cop_count.split("\n")[0]
            if int(sp_cop_count) > 0:
                raise wbxexception("stop shareplex port %s failed on host %s" % (port, host_name))
            cmd = "source /home/oracle/.bash_profile; crsctl delete resource shareplex%s" % port
            logger.info("%s on server %s" % (cmd, host_name))
            resmsg = server.exec_command(cmd, async_log=True)
            if resmsg.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            cmd = "crsstat | grep shareplex%s | wc -l" % port
            crs_port_count = server.exec_command(cmd, async_log=True)
            if crs_port_count.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            crs_port_count = crs_port_count.split("\n")[0]
            if int(crs_port_count) > 0:
                raise wbxexception("delete shareplex port %s failed on host %s" % (port, host_name))
            else:
                logger.info("Shareplex port is stopped and removed from CRS")
        except Exception as e:
            retcode = "FAILED"
            logger.error(e, exc_info = e)
        finally:
            if server:
                server.close()
        return retcode

    def start_ha_service(self, processvo):
        server = None
        cmd = None
        retcode = "SUCCEED"
        host_name = processvo.parameter["host_name"].lower()
        db_name = processvo.parameter["db_name"].upper()
        pool_name = processvo.parameter["pool_name"].upper()
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)
            server.connect()
            cmd = "source /home/oracle/.bash_profile; srvctl start service -d %s -s %sha" % (db_name, pool_name)
            logger.info("%s on server %s" % (cmd, host_name))
            resmsg = server.exec_command(cmd, async_log=True)
            if resmsg.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            cmd = "source /home/oracle/.bash_profile; crsstat | grep '%sha' | awk '{print $3$4}'" % pool_name
            logger.info("%s on server %s" % (cmd, host_name))
            targetstate = server.exec_command(cmd, async_log=True)
            if targetstate.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
            for line in targetstate.split("\n"):
                # if "ONLINEONLINE" not in line:
                #     raise wbxexception("Stop service on %s failed" % host_name)
                logger.info(line)
        except Exception as e:
            retcode = "FAILED"
            logger.error(e)
        finally:
            if server:
                server.close()
        return retcode

    def remove_channel(self, jobvo):
        server = None
        cmd = None
        retcode = "SUCCEED"
        host_name = jobvo.parameter["host_name"].lower()
        splex_port = jobvo.parameter["splex_port"]
        pool_name = jobvo.parameter["pool_name"]
        try:
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(host_name)
            server.connect()
            cmd = "source /home/oracle/.bash_profile; sh /staging/gates/dbcutover/tahoe_cutover.sh %s %s REMOVECHANNEL" % (pool_name, splex_port)
            logger.info("Execute %s on server %s" % (cmd, host_name))
            resmsg = server.exec_command(cmd)
            logger.info(resmsg)
            if resmsg.find("WBXERROR") >= 0:
                raise wbxexception("Error occurred with command %s" % (cmd))
        except Exception as e:
            retcode = "FAILED"
            logger.error(e)
        finally:
            if server:
                server.close()
        return retcode

    def stop_pool(self, jobvo):
        server = None
        cmd = None
        retcode = "SUCCEED"
        host_name = jobvo.parameter["host_name"].lower()
        pool_name = jobvo.parameter["pool_name"].upper()
        db_name = jobvo.parameter["db_name"].upper()
        new_db_name = jobvo.parameter["new_db_name"].upper()
        new_trim_host = jobvo.parameter["new_trim_host"].lower()
        port_from_configdb = jobvo.parameter["splex_port"]
        port_to_opdb = jobvo.parameter["port_to_opdb"]
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daoManager = daomanagerfactory.getDaoManager(db_name)
        sessionCount = 0
        try:
            dao = daoManager.getDao(DaoKeys.DAO_DBCUTOVERDAO)
            daoManager.startTransaction()
            sessionCount = dao.getUserSessionCount()
            daoManager.commit()
        except Exception as e:
            daoManager.rollback()
        finally:
            daoManager.close()

        defaultDaoManager = daomanagerfactory.getDefaultDaoManager()
        try:
            server = daomanagerfactory.getServer(host_name)
            server.connect()
            ha_service_name = "%sha" % pool_name
            cmd = "source /home/oracle/.bash_profile; crsstat | grep -i %s\.svc | grep -c ONLINE" % ha_service_name.lower()
            vCount = server.exec_command(cmd)
            if int(vCount) > 0:
                cmd = "source /home/oracle/.bash_profile; srvctl stop service -d %s -s %s" % (db_name, ha_service_name.lower())
                logger.info("The HA service %s is still online, stop it with cmd: %s" % (ha_service_name.lower(), cmd))
                server.exec_command(cmd)

            cmd = "source /home/oracle/.bash_profile; crsstat | grep -i  %s\.svc | wc -l" % ha_service_name.lower()
            vCount = server.exec_command(cmd)
            if int(vCount) > 0:
                cmd = "source /home/oracle/.bash_profile; srvctl remove service -d %s -s %s" % (db_name, ha_service_name.lower())
                logger.info("Remove service %s from CRS with cmd: %s" % (ha_service_name.lower(), cmd))
                server.exec_command(cmd)

            cmd = "source /home/oracle/.bash_profile; crsstat | grep -i %s\.svc | grep -c ONLINE" % pool_name.lower()
            vCount = server.exec_command(cmd)
            if int(vCount) > 0:
                cmd = "source /home/oracle/.bash_profile; srvctl stop service -d %s -s %s" % (db_name, pool_name.lower())
                logger.info("The HA service %s is still online, stop it with cmd: %s" % (pool_name.lower(), cmd))
                server.exec_command(cmd)

            cmd = "source /home/oracle/.bash_profile; crsstat | grep -i %s\.svc | wc -l" % pool_name.lower()
            vCount = server.exec_command(cmd)
            if int(vCount) > 0:
                cmd = "source /home/oracle/.bash_profile; srvctl remove service -d %s -s %s" % (db_name, pool_name.lower())
                logger.info("Remove service %s from CRS with cmd: %s" % (pool_name.lower(), cmd))
                server.exec_command(cmd)

            defaultDaoManager.startTransaction()
            dao = defaultDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            vPoolCount = dao.getTahoePoolCountInDB(db_name, pool_name)
            vTgtDBCount = dao.getTgtdbCountByPort(host_name, port_from_configdb)
            isdbdecomm = False
            if sessionCount > 0:
                logger.info("There are %s user session in this db, so can not stop this db" % sessionCount)
            elif  vPoolCount > 0:
                logger.info("There are other pool in this db based on depotdb data. so can not stop this db")
            else:
                if vTgtDBCount == 1:
                    self.stop_shareplex_port(jobvo)
                isdbdecomm = True

                cmd = "source /home/oracle/.bash_profile; srvctl stop database -d %s" % db_name
                logger.info("Stop database with cmd: %s" % cmd)
                server.exec_command(cmd)

                cmd = "source /home/oracle/.bash_profile; srvctl stop database -d %s" % db_name
                logger.info("Remove database from CRS with cmd: %s" % cmd)
                server.exec_command(cmd)
            dao.updateDepotDBForTahoeCutover(db_name, pool_name, port_from_configdb, port_to_opdb,
                                            new_trim_host, new_db_name, isdbdecomm, False if vPoolCount > 0 or sessionCount > 0 else True)
            defaultDaoManager.commit()

        except Exception as e:
            defaultDaoManager.rollback()
            retcode = "FAILED"
            logger.error(e)
        finally:
            defaultDaoManager.close()
            if server:
                server.close()
        return retcode

def get_tahoedb_status(db_name, pri_pool_name, gsb_pool_name):
    retcode = "SUCCEED"
    connection_status_log = ""
    daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daomanagerfactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    schema = depotdbDao.getschemabydbNameandpoolName(pri_pool_name, db_name)

    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daomanager = daoManagerFactory.getDaoManager(db_name)
    dao = daomanager.getDao(DaoKeys.DAO_DBAUDITDAO)
    try:
        daomanager.startTransaction()
        _machine_connection_list = dao.gettahoemachineconnection(schema, pri_pool_name, gsb_pool_name)
        connection_status_log = "\n".join(_machine_connection_list)
        daomanager.commit()
    except Exception as e:
        retcode = "FAILED"
        logger.error(e)
    finally:
        daomanager.close()
    return {
        "status": retcode,
        "connection_status_log": connection_status_log}




