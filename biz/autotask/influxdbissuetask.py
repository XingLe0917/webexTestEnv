import logging
import time

from common.Config import Config
from common.wbxexception import wbxexception
from common.wbxtask import wbxautotask
from dao.wbxdaomanager import wbxdaomanagerfactory

logger = logging.getLogger("DBAMONITOR")

class influxdbtask(wbxautotask):
    def __init__(self, taskid = None):
        super(influxdbtask,self).__init__(taskid, "INFLUXDB_ISSUE_TASK")
        self._host_name =None
        self._db_name =None
        self._self_heal=None
        self._instance_name = None
        self._influxdb_client = Config().getInfluxDB_SJC_client()

    def initialize(self, **kwargs):
        self._host_name = kwargs["host_name"]
        self._db_name = kwargs["db_name"].upper()
        self._self_heal = kwargs["self_heal"].upper()
        self._instance_name = kwargs["instance_name"]

        taskvo = super(influxdbtask, self).initialize(**kwargs)
        jobList = self.listTaskJobsByTaskid(self._taskid)
        if len(jobList) == 0:
            self.generateJobs()
        return taskvo

    def generateJobs(self):
        self.addJob(host_name=self._host_name, db_name=self._db_name, job_action="fix", process_order=1,
                    execute_method="SYNC")

    def fix(self, *args):
        jobid = args[0]
        logger.info("fix influxdb issue host_name=%s db_name=%s" % (self._host_name, self._db_name))
        fix_flag = False
        try:
            self.updateJobStatus(jobid, "RUNNING")
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            server = daomanagerfactory.getServer(hostname=self._host_name)
            try:
                server.connect()
            except Exception as e:
                raise wbxexception("cannot login the server %s with %s" % (server.host_name,server.login_user))
            try:
                logger.info("************* check instance *************")
                cmd = ". /home/oracle/.bash_profile;ps aux | grep lgwr | grep -v grep | grep -vi ASM | awk '{print $NF}' | awk -F_ '{print $NF}'"
                nodes = server.exec_command(cmd)
                nodes_list = nodes.split('\n')
                if self._instance_name not in nodes_list:
                    logger.info("instance_name={0} not on the {1}, please check info." .format(self._instance_name,self._host_name))
                    # todo send to chatbot room
                    self.updateJobStatus(jobid, "UNSOLVE")
                else:
                    logger.info("************* check dbmonitor log path and file *************")
                    cmd = "ls -l /var/log/dbmonitor/data/ | grep %s" %(self._db_name)
                    logger.info(cmd)
                    res_log = server.exec_command(cmd)
                    # log_file_list = str(res_log).split("\n")
                    logger.info(res_log)
                    if "cannot access" in res_log or len(str(res_log).split("\n"))<2:
                        logger.info("Try to reinstall relegraf...")
                        install_telegraf_file = "/staging/gates/telegraf/install_telegraf.sh"
                        cmd = "ls %s" % (install_telegraf_file)
                        res = server.exec_command(cmd)
                        if "install_telegraf.sh" not in res:
                            error_msg = "No found %s on %s" % (install_telegraf_file, self._host_name)
                            raise wbxexception(error_msg)
                        cmd = "sh %s" % (install_telegraf_file)
                        logger.info(cmd)
                        res = server.exec_command(cmd)
                        logger.info(res)
                        logger.info("check influxdb data, db_name={0}, instance_name={1}".format(self._db_name,
                                                                                                 self._instance_name))
                        logger.info("Please wait a moment.")
                        flag = self.check_influxdb_data()
                        logger.info("check influxdb data result=%s" %(flag))
                        if flag:
                            fix_flag = True
                            self.updateJobStatus(jobid, "SUCCEED")

                if not fix_flag:
                    logger.info("************* check dba_scheduler_jobs ************* ")
                    rest_list = ""
                    owner = ""
                    try:
                        sql = "select owner,job_name, last_start_date,ENABLED||',isflag' from dba_scheduler_jobs where job_name like " + "'%MONITOR%'"
                        cmd1 = """
                                                                             . /home/oracle/.bash_profile
                                                                                    db
                                                                                    export ORACLE_SID=%s
                                                                                    sqlplus / as sysdba << EOF 
                                                                                    set linesize 1000;
                                                                                    %s;
                                                                                    exit;
                                                                                    EOFF
                                                                            """ % (self._instance_name, sql)
                        logger.debug(cmd1)
                        rest_list = server.exec_command(cmd1)
                        logger.info(rest_list)
                    except Exception as e:
                        raise wbxexception(
                            "Error occurred: view dba_scheduler_jobs, instance_name={0},host_name={1}, e={2}".format(
                                self._instance_name, self._host_name, str(e)))
                    for res_info in str(rest_list).split("\n"):
                        if "isflag" in res_info:
                            if owner == "":
                                owner = str(res_info.split()[0]).strip()
                            _str = res_info.split(" ")[-1]
                            ENABLED = _str.split(",")[0]
                            if ENABLED == "FALSE":
                                job_name = ""
                                for s in res_info.split("\t"):
                                    s = s.strip()
                                    if "MONITOR" in s:
                                        job_name = s
                                sql2 = "exec dbms_scheduler.enable(name=>'%s');" % (job_name)
                                cmd2 = """
                                                                                                             . /home/oracle/.bash_profile
                                                                                                                    db
                                                                                                                    %s
                                                                                                                    exit;
                                                                                                                    EOF
                                                                                                            """ % (sql2)
                                logger.info(cmd2)
                                try:
                                    rest_list = server.exec_command(cmd2)
                                    logger.info(rest_list)
                                except Exception as e:
                                    raise wbxexception(
                                        "Error occurred: exec dbms_scheduler.enable, sql={0}, e={1}".format(
                                            sql2, str(e)))
                    logger.info("************* check wbxproclog ************* ")
                    sql = '''
                                                select p1 from %s.wbxproclog where procname 
                                                in ('CollectODMMonitorDataSecondly','CollectODMMonitorDataMinutely',
                                                'OutputODMMonitorData','CollectOSSMonitorData','OutputOSSMonitorData',
                                                'CollectOWIMonitorData','OutputOWIMonitorData','CollectSQLServiceNameMapData',
                                                'CollectSQLMonitorData','OutputSQLMonitorData','CollectSessionMonitorData',
                                                'OutputSessionMonitorData','CollectArchivedLog','OutputArchivedLog','CollectRmanLog',
                                                'OutputRmanLog','CollectOracleJobMonitorData','OutputOracleJobMonitorData',
                                                'CollectTableSpaceUsage','OutputTableSpaceUsage','CollectOSSTATMonitorData',
                                                'OutputOSSTATMonitorData') and logtime > sysdate - 10/60/24 ;
                                                ''' % (owner)
                    cmd = """
                                                . /home/oracle/.bash_profile
                                                db
                                                export ORACLE_SID=%s
                                                sqlplus / as sysdba << EOF 
                                                set linesize 1000;
                                                %s;
                                                exit;
                                                EOFF
                                                 """ % (self._instance_name, sql)
                    logger.info(cmd)
                    try:
                        rest_list = server.exec_command(cmd)
                        logger.log(rest_list)
                        if "no rows selected" not in rest_list:
                            sql = "	grant execute on dbms_lock to %s" % (owner)
                            cmd = """
                                                                                    . /home/oracle/.bash_profile
                                                                                    db
                                                                                    export ORACLE_SID=%s
                                                                                    sqlplus / as sysdba << EOF 
                                                                                    set linesize 1000;
                                                                                    %s;
                                                                                    exit;
                                                                                    EOFF
                                                                                     """ % (self._instance_name, sql)
                            try:
                                logger.info(cmd)
                                res = server.exec_command(cmd)
                                logger.info(res)
                            except Exception as e:
                                raise wbxexception(
                                    "Error occurred: sql={0}, e={1}".format(sql, str(e)))

                            sql = "	GRANT CREATE ANY DIRECTORY TO %s" % (owner)
                            cmd = """
                                                             . /home/oracle/.bash_profile
                                                             db
                                                             export ORACLE_SID=%s
                                                             sqlplus / as sysdba << EOF 
                                                             set linesize 1000;
                                                             %s;
                                                             exit;
                                                             EOFF
                                                             """ % (
                                self._instance_name, sql)
                            try:
                                logger.info(cmd)
                                res = server.exec_command(cmd)
                                logger.info(res)
                            except Exception as e:
                                raise wbxexception(
                                    "Error occurred: sql={0}, e={1}".format(sql, str(e)))

                            sql = "	GRANT SELECT ANY DICTIONARY TO %s" % (owner)
                            cmd = """
                                                            . /home/oracle/.bash_profile
                                                            db
                                                            export ORACLE_SID=%s
                                                            sqlplus / as sysdba << EOF 
                                                            set linesize 1000;
                                                            %s;
                                                            exit;
                                                            EOFF
                                                            """ % (
                                self._instance_name, sql)
                            try:
                                logger.info(cmd)
                                res = server.exec_command(cmd)
                                logger.info(res)
                            except Exception as e:
                                raise wbxexception(
                                    "Error occurred: sql={0}, e={1}".format(sql, str(e)))

                            sql = "	GRANT EXECUTE ON DBMS_LOCK to %s" % (owner)
                            cmd = """
                                                             . /home/oracle/.bash_profile
                                                             db
                                                             export ORACLE_SID=%s
                                                             sqlplus / as sysdba << EOF 
                                                             set linesize 1000;
                                                             %s;
                                                             exit;
                                                            EOFF
                                                            """ % (
                                self._instance_name, sql)
                            try:
                                logger.info(cmd)
                                res = server.exec_command(cmd)
                                logger.info(res)
                            except Exception as e:
                                raise wbxexception(
                                    "Error occurred: sql={0}, e={1}".format(sql, str(e)))

                            sql = "	select count(1) from dba_objects where object_name='UTL_FILE' and object_type='PACKAGE' and owner='SYS'"
                            cmd = """
                                                                                                 . /home/oracle/.bash_profile
                                                                                                 db
                                                                                                 export ORACLE_SID=%s
                                                                                                 sqlplus / as sysdba << EOF 
                                                                                                 set linesize 1000;
                                                                                                 %s;
                                                                                                 exit;
                                                                                                EOFF
                                                                                                """ % (
                                self._instance_name, sql)
                            try:
                                logger.info(cmd)
                                res = server.exec_command(cmd)
                                logger.info(res)
                            except Exception as e:
                                raise wbxexception(
                                    "Error occurred: sql={0}, e={1}".format(sql, str(e)))

                        logger.info("************* telegraf restart ************* ")
                        cmd = "sudo service telegraf restart"
                        try:
                            logger.info(cmd)
                            res = server.exec_command(cmd)
                            logger.info(res)
                        except Exception as e:
                            raise wbxexception("Error occurred: e={1}".format(str(e)))

                        flag = self.check_influxdb_data()
                        if flag:
                            fix_flag = True
                            self.updateJobStatus(jobid, "SUCCEED")
                        else:
                            logger.info("The issue has not been fixed.")
                            self.updateJobStatus(jobid, "FAILED")

                    except Exception as e:
                        raise wbxexception("Error occurred: e={0}".format(str(e)))
            except Exception as e:
                self.updateJobStatus(jobid, "FAILED")
                raise wbxexception("Error occurred: e:{0}".format(str(e)))

        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")

    def check_influxdb_data(self):
        time.sleep(20)
        flag = self.exec_check()
        num1 = 0
        while not flag and num1 < 3:
            time.sleep(5)
            flag = self.exec_check()
            num1 += 1
        return flag

    def exec_check(self):
        sql = "select * from wbxdb_monitor_odm where time > now() - 5m and and db_name = '%s' and db_inst_name='%s'  " % (self._db_name, self._instance_name)
        results = self._influxdb_client.query(sql)
        points = results.get_points()
        flag = False
        data_count = 0
        for data in points:
            vo = dict(data)
            data_count += 1
            if data_count > 0:
                flag = True
                break
        return flag




if __name__ == "__main__":
    sql = "select owner,job_name, last_start_date,ENABLED||\"isflag\" from dba_scheduler_jobs where job_name like " +"'%MONITOR%'"
    print(sql)