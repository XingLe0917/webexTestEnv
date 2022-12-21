import logging

from common.Config import Config
from common.wbxexception import wbxexception
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
import time
from datetime import datetime
from common.wbxutil import wbxutil
from common.wbxtask import wbxautotask,threadlocal
from common.wbxssh import wbxssh

logger = logging.getLogger("DBAMONITOR")

#Author :wentazha@cisco.com
#Desc   :Create DataBase
class dbcabuildtask(wbxautotask):
    def __init__(self, taskid = None):
        super(dbcabuildtask,self).__init__(taskid, "DBCABUILD_TASK")

    def initialize(self, **kwargs):
        self._host_name = kwargs["host_name"]
        self._db_name = kwargs["dbname"].lower()
        self._sgasize = kwargs["sgasize"]
        self._pgasize = kwargs["pgasize"]
        self._charset = kwargs["charset"]
        self._dbversion = kwargs["dbversion"]
        self._compatible = kwargs["compatible"]
        self._base_db_name = kwargs["base_db_name"].upper()
        self._db_env = kwargs["db_type"]
        self._splex_sid = kwargs["splex_sid"]

        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        dbserver=daoManagerFactory.getServer(self._host_name)
        if not dbserver:
            raise wbxexception("can not find the server with host_name=%s in depot db" % self._host_name)
        self._racdict=dbserver.getRacNodeDict()
        for server in self._racdict.values():
            server.verifyConnection()
        self.wbxvo = wbxssh(dbserver.host_name,dbserver.ssh_port,dbserver.login_user,dbserver.login_pwd)
        taskvo = super(dbcabuildtask, self).initialize(**kwargs)
        jobList = self.listTaskJobsByTaskid(self._taskid)
        if len(jobList) == 0:
            self.generateJobs()
        return taskvo

    def generateJobs(self):
        logger.info("generateDBCAStep(taskid=%s, db_name=%s)" % (self._taskid, self._db_name))
        self.addJob(host_name=self._host_name, db_name=self._db_name, job_action="preverify", process_order=1, execute_method="SYNC",
                    isoneclick=True,description="1:check sga&hugpage size;2:check host whether is first node;3:check whether db installed;4:check base db tablespace size")
        self.addJob(host_name=self._host_name, db_name=self._db_name, job_action="createresponse", process_order=2,execute_method="SYNC",
                    isoneclick=True,description="generate dbca response file")
        self.addJob(host_name=self._host_name, db_name=self._db_name, job_action="createtemplete", process_order=3,execute_method="SYNC",
                    isoneclick=True,description="gnenerate dbca templete file")
        self.addJob(host_name=self._host_name, db_name=self._db_name, job_action="dbcainstall", process_order=4,execute_method="SYNC",
                    isoneclick=True,description="run dbca to create database")
        for dbserver in self._racdict.values():
            self.addJob(host_name=dbserver.host_name, db_name=self._db_name, job_action="dbaudit", process_order=5,
                        execute_method="SYNC",isoneclick=True,
                        description="run shell script for 1:add tns info;2:check db parameters;3:create database link;4:Register HA service;"
                                    "5:create tablespace;6:check user&undo tablespace;7:create WBXBACKUP,SPLEX_DENY schema;8:enable scheduled jobs")
        self.addJob(host_name=self._host_name, db_name=self._db_name, job_action="postVerification",process_order=6
                    ,execute_method="SYNC",isoneclick=True,description="1:check whether crsstat script is exists;2:check whether db status is online")

    # STEP 1  preverify
    #
    # 1. check hugpage
    # 2. check if server is the first node
    # 3. check db is installed
    def preverify(self, *args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("preverify for dbca build task with host_name:%s jobid:%s" % (self._host_name,jobid))
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        hasError = False
        dblist=[]
        try:
            logger.info("check sga & hugpage size ...")
            cmd = "source /home/oracle/.bash_profile 2>/dev/null;srvctl config database"
            rows = self.wbxvo.exec_command(cmd)
            totsga = 0
            if not wbxutil.isNoneString(rows):
                dblist = rows.splitlines()
                for item in dblist:
                    logger.info("check total sga size under db:%s" % (item))
                    cmd = "select value/1024/1024 from v\$parameter where name = 'sga_target' order by name;"
                    cmd = self.getOraCmdStr(item, cmd)
                    row = self.wbxvo.exec_command(cmd)
                    logger.info("%s sga size is %s M" % (item, row.strip()))
                    totsga += int(row.strip())
            totsga = totsga + int(self._sgasize)*1024
            logger.info("if db:%s installed ,total db sga siz is:%s M " %(self._db_name,str(totsga)))

            for dbserver in self._racdict.values():
                try:
                    cmd="cat /proc/meminfo | grep -i HugePages_Total|awk '{print $NF}'|sed 's/[[:space:]]//g'"
                    rows = dbserver.exec_command(cmd)
                    if not wbxutil.isNoneString(rows):
                        hugpage=int(rows.strip())*2
                        logger.info("hugpage size on host:%s is %s M" % (dbserver.host_name, str(hugpage)))
                        if hugpage<totsga:
                            hasError=True
                            logger.error("if db %s installed,total sga size is %s M,but current hugpagesize is %s M on host:%s" %(
                                self._db_name,str(totsga),rows,dbserver.host_name))
                    else:
                        hasError=True
                        logger.error("if db %s install,total sga size is %s ,but current hugpagesize is %s on host")
                finally:
                    dbserver.close()

            logger.info("check whether host %s is first node" %(self._host_name))
            dbserver=self._racdict["node1"]
            if dbserver.host_name != self._host_name:
                hasError = True
                logger.error("host: %s does not first node in the rac." %(self._host_name))

            logger.info("check whether db:%s is or not installed in rac" %self._db_name)
            if self._db_name in dblist:
                hasError = True
                logger.error("db :%s has been installed in rac" %(self._db_name))

            basedb=daoManagerFactory.getDatabaseByDBName(self._base_db_name)
            if basedb :
                logger.info("check db %s tablespace size" %(self._base_db_name))
                rows=basedb.getDBTabspacesSize()
                filecnt=0
                for key in rows.keys():
                    # logger.info("%s tablespace datafile number:%s" %(key,rows[key]))
                    filecnt+=rows[key]
                filesize=filecnt*20*1024 # Threshold:90% initial:20G
                logger.info("basedb:%s has %s datafiles,will cost %s M space" % (self._base_db_name, str(filecnt),str(filesize)))
                Threshold=round(filesize/0.9,2)

                asm_fsize=0
                cmd="""source /home/oracle/.bash_profile 2>/dev/null; 
                asmcmd lsdg | grep -Ei "DG_DATA"|awk '{print $NF":"$(NF-3)}'|awk '{sub(/\//,""); print $0}'|sed 's/[[:space:]]//g'"""
                rows=self.wbxvo.exec_command(cmd)
                if not wbxutil.isNoneString(rows):
                    for item in rows.splitlines():
                        logger.info(item)
                        asm_fsize+=int(item.split(":")[1])
                    logger.info("on this rac has %s Usable_file_MB" %str(asm_fsize))
                    if asm_fsize < filesize:
                        hasError=True
                        logger.error("this action will use space:%s,if you donot want alert, Usable_file must uper to %s,but Usable_file_MB is:%s" % (str(filesize),str(Threshold),rows))
                else:
                    hasError=True
                    logger.error("can not get DG_DATA Usable_file_MB on host:%s " %(self._host_name))

            if hasError :
                raise wbxexception("preverify jobid :%s end with error" %jobid)
            logger.info("preverify jobid :%s end with SUCCEED" %jobid)

            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e))
            self.updateJobStatus(jobid, "FAILED")
            raise e
        finally:
            self.wbxvo.close()

    def createresponse(self,*args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("createresponse for dbca build task on host:%s with jobid:%s" % (self._host_name, jobid))
        resp_file="/home/oracle/dbca_{0}.rsp".format(self._db_name)
        logger.info("generate DBCA response file on host %s ,file path : %s" %(self._host_name,resp_file))

        config = Config.getConfig()
        dbca_rsp = config.getdbcaRspConfigFile()
        f = open(dbca_rsp)
        dbca = f.read()
        f.close()
        try:
            cmd="CSSD_DIR=`dirname $( ps -eo args | grep ocssd.bin | grep -v grep | awk '{print $1}')`;${CSSD_DIR}/olsnodes"
            msg = self.wbxvo.exec_command(cmd)
            nodelist=",".join(msg.split())
            templatefile="General_Purpose_{0}".format(self._db_name)
            dbca_db = dbca.replace('{ORACLE_SID}', self._db_name).replace('{generalPurpose}', templatefile)
            dbca_db = dbca_db.replace('{nodelist}', nodelist).replace('{characterSet}', self._charset)
            logger.info(dbca_db)
            cmd = "echo '{0}' >{1} && chmod 777 {2}".format(dbca_db,resp_file,resp_file)
            self.wbxvo.exec_command(cmd,async_log = True)

            logger.info("begin to Postverify... ")
            row = self.chkFileisexits(self.wbxvo, resp_file)
            if not row["isexists"]:
                raise wbxexception("%s file does not exists on host : %s" %(resp_file,self._host_name))
            logger.info("Postverify end with SUCCEED")
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e))
            self.updateJobStatus(jobid, "FAILED")
        finally:
            self.wbxvo.close()

    def createtemplete(self,*args):
        jobid = args[0]
        threadlocal.current_jobid = jobid

        logger.info("createtemplete for dbca build task on host:%s with jobid:%s" % (self._host_name, jobid))
        templetefile="/home/oracle/General_Purpose_{0}.dbt".format(self._db_name)
        logger.info("generate DBCA templete file on host %s ,file path : %s" %(self._host_name,templetefile))

        tagplacelist = ["<UndoDatafileAttr id=\"UNDOTBS\"> </UndoDatafileAttr>",
                        "<UndoTablespaceAttr id=\"UNDOTBS\"></UndoTablespaceAttr>",
                        "<RedoLogGroupAttr id=\"RedoLogGroup\"></RedoLogGroupAttr>"
                        ]
        undoDatafileAttr = '''
        <DatafileAttributes id="+DG_DATA/{ORACLE_SID}/undotbs{CNT}.dbf">
            <tablespace>UNDOTBS{CNT}</tablespace>
            <temporary>false</temporary>
            <online>true</online>
            <status>0</status>
            <size unit="MB">30720</size>
            <reuse>true</reuse>
            <autoExtend>true</autoExtend>
            <increment unit="MB">1024</increment>
            <maxSize unit="MB">32767</maxSize>
        </DatafileAttributes>
'''

        undoTablespaceAttr = '''
        <TablespaceAttributes id="UNDOTBS{CNT}">
            <online>true</online>
            <offlineMode>1</offlineMode>
            <readOnly>false</readOnly>
            <temporary>false</temporary>
            <defaultTemp>false</defaultTemp>
            <undo>true</undo>
            <local>true</local>
            <blockSize>8192</blockSize>
            <allocation>1</allocation>
            <uniAllocSize unit="KB">-1</uniAllocSize>
            <initSize unit="KB">64</initSize>
            <increment unit="MB">1024</increment>
            <incrementPercent>0</incrementPercent>
            <minExtends>1</minExtends>
            <maxExtends>2147483645</maxExtends>
            <minExtendsSize unit="KB">64</minExtendsSize>
            <logging>true</logging>
            <recoverable>false</recoverable>
            <maxFreeSpace>0</maxFreeSpace>
            <autoSegmentMgmt>false</autoSegmentMgmt>
            <bigfile>false</bigfile>
            <datafilesList>
                <TablespaceDatafileAttributes id="+DG_DATA/{ORACLE_SID}/undotbs{CNT}.dbf">
                    <id>-1</id>
                </TablespaceDatafileAttributes>
            </datafilesList>
        </TablespaceAttributes>
'''

        RedoLogGroupAttr = '''
        <RedoLogGroupAttributes id="{CNT}">
            <reuse>false</reuse>
            <fileSize unit="KB">1048576</fileSize>
            <Thread>{PARCNT}</Thread>
            <member ordinal="0" memberName="redo{CNT}.log" filepath="+DG_REDO/{ORACLE_SID}/"/>
        </RedoLogGroupAttributes>
'''

        auto_conents = {"UndoDatafileAttr": undoDatafileAttr,
                        "UndoTablespaceAttr": undoTablespaceAttr,
                        "RedoLogGroupAttr": RedoLogGroupAttr}
        try:
            cmd = "CSSD_DIR=`dirname $( ps -eo args | grep ocssd.bin | grep -v grep | awk '{print $1}')`;${CSSD_DIR}/olsnodes"
            msg = self.wbxvo.exec_command(cmd,async_log = True)
            nodelist = msg.split()

            config = Config.getConfig()
            dbca_dbt = config.getdbcaDbtConfigFile()
            retmsg = ""
            with open(dbca_dbt, 'r') as f:
                for line in f.readlines():
                    if line.strip() in tagplacelist:
                        if line.strip().find("UndoDatafileAttr") > 0:
                            arg = "UndoDatafileAttr"
                        if line.strip().find("UndoTablespaceAttr") > 0:
                            arg = "UndoTablespaceAttr"
                        if line.strip().find("RedoLogGroupAttr") > 0:
                            arg = "RedoLogGroupAttr"
                        msg = self.generatedbt(arg,*nodelist,**auto_conents)
                        retmsg += msg
                    else:
                        retmsg += line

            dbca_dbt = retmsg.replace('{ORACLE_SID}', self._db_name).replace('{characterSet}', self._charset)
            dbca_dbt = dbca_dbt.replace('{sgatarget}', str(self._sgasize)).replace('{pgatarget}', str(self._pgasize))
            logger.info(dbca_dbt)
            cmd = "echo '{0}' >{1} && chmod 777 {2}".format(dbca_dbt,templetefile,templetefile)
            self.wbxvo.exec_command(cmd)

            logger.info("begin to Postverify... ")
            row = self.chkFileisexits(self.wbxvo, templetefile)
            if not row["isexists"]:
                raise wbxexception("%s file does not exists on host : %s" % (templetefile, self._host_name))
            logger.info("Postverify end with SUCCEED")

            self.updateJobStatus(jobid, "SUCCEED")

        except Exception as e:
            logger.error(str(e))
            self.updateJobStatus(jobid, "FAILED")
            raise e
        finally:
            self.wbxvo.close()

    def generatedbt(self,arg,*args,**kargs):
        nodelist=args
        retmsg=""
        if arg in ["UndoDatafileAttr", "UndoTablespaceAttr"]:
            for i in range(1, len(nodelist) + 1):
                cnt = str(i).zfill(2)
                retmsg += kargs[arg].replace("{CNT}", cnt)
        else:
            cnt = 1
            for i in range(1, len(nodelist) + 1):
                for j in range(1, 5):
                    retmsg += kargs[arg].replace("{CNT}", str(cnt).zfill(2)).replace("{PARCNT}", str(i))
                    cnt += 1
        return retmsg

    def dbcainstall(self, *args):
        jobid = args[0]
        threadlocal.current_jobid = jobid

        logger.info("create database on host:%s jobid:%s" % (self._host_name, jobid))
        try:
            currtime = datetime.now().strftime('%Y%m%d%H%M%S')
            dbca_logfile = "/home/oracle/dbca_{0}_{1}.log".format(self._db_name, currtime)
            logger.info("dbca log file on host {0} {1}".format(self._host_name, dbca_logfile))

            self.wbxrundbca(dbca_logfile)

            self.checkdbcaprocess(dbca_logfile)

            logger.info("build database on host_name=%s dbname=%s end with successed")

            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e))
            self.updateJobStatus(jobid, "FAILED")
            raise e

    def wbxrundbca(self, dbca_logfile):
        try:
            cmd="source /home/oracle/.bash_profile 2>/dev/null;asmcmd ls DG_FRA | grep -i {0}".format(self._db_name)
            logger.info("execute command %s on host:%s" %(cmd,self._host_name))
            rows=self.wbxvo.exec_command(cmd)
            logger.info(rows)
            if wbxutil.isNoneString(rows):
                cmd="source /home/oracle/.bash_profile;asmcmd mkdir DG_FRA/{0}".format(self._db_name)
                logger.info(cmd)
                self.wbxvo.exec_command(cmd)
            cmd = "source /home/oracle/.bash_profile 2>/dev/null && nohup dbca -silent -createDatabase -responseFile /home/oracle/dbca_{0}.rsp >{1} &".format(
                self._db_name, dbca_logfile)
            logger.info("execute command %s on host:%s" %(cmd,self._host_name))
            logger.info("dbcainstall is running ,please wait...")
            self.wbxvo.exec_command(cmd)
        except Exception as e:
            raise e
        finally:
            self.wbxvo.close()

    def checkdbcaprocess(self, dbca_logfile):
        logger.info("check dbca process...")
        step = 0
        try:
            cmd = "ps -ef|grep dbca | grep -v grep | grep '/home/oracle/dbca_{0}.rsp'".format(self._db_name)
            while True:
                step += 1
                rows = self.wbxvo.exec_command(cmd)
                if wbxutil.isNoneString(rows):
                    break
                time.sleep(5)
                if step % 24 == 0:
                    logger.info("dbcainstall is running ,please wait...")

            row = self.chkFileisexits(self.wbxvo, dbca_logfile)
            if not row["isexists"]:
                raise wbxexception("dbca logfile :%s  does not exists on host:%s" %(dbca_logfile,self._host_name))

            logger.info("check dbca logfile")
            cmd = "echo && cat %s" % dbca_logfile
            res = self.wbxvo.exec_command(cmd, async_log=True)
            if res.find("FATAL") >= 0:
                raise wbxexception("wbxrundbca run failed.")
        except Exception as e:
            raise e
        finally:
            self.wbxvo.close()

    def dbaudit(self, *args):
        jobid = args[0]
        threadlocal.current_jobid = jobid

        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)

        scripts="/staging/gates/dbconsolidation/dbconsolidation_installdb_19c.sh"
        try:
            daomanager.startTransaction()
            jobvo = dao.getAutoTaskJobByJobid(jobid)
            host_name=jobvo.host_name
            dbserver=daoManagerFactory.getServer(host_name)
            logger.info("dbaudit on host:%s jobid:%s" % (host_name, jobid))

            wbxvo = wbxssh(dbserver.host_name, dbserver.ssh_port,dbserver.login_user,dbserver.login_pwd)
            row = self.chkFileisexits(wbxvo, scripts)
            if not row["isexists"]:
                raise wbxexception("%s file does not exists on host : %s" % (scripts, dbserver.host_name))

            cmd = '''sh {0} -d {1} -t {2} -b {3} -s {4}'''.format(scripts,self._db_name, self._db_env, self._base_db_name,self._splex_sid)
            logger.info("execute command %s on host:%s" %(cmd,host_name))

            wbxvo.connect()
            wbxvo.send(cmd)
            kargs = {}
            time.sleep(1)
            rows=""
            while True:
                buff = wbxvo.recvs(**kargs)
                logger.info(buff)
                if buff:
                    rows += buff
                    if buff.strip().endswith(('$')):
                        if rows.find("WBXERROR") >= 0:
                            raise wbxexception("db audit has been failed jobid :%s".format(jobid))
                        break
            # rows=wbxvo.exec_command(cmd, async_log=True)
            # if rows.find("WBXERROR") > 0:
            #     raise wbxexception("db audit has been failed jobid :%s".format(jobid))
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e))
            self.updateJobStatus(jobid, "FAILED")
        finally:
            wbxvo.close()
            daomanager.close()

    def postVerification(self, *args):
        jobid = args[0]
        threadlocal.current_jobid = jobid

        logger.info("postVerification for dbca build task with host_name=%s dbname=%s" % (self._host_name, self._db_name))
        try:
            logger.info("check if crsstat exists ")
            cmd = ". /home/oracle/.bash_profile 2>/dev/null && which crsstat|wc -l"
            row = self.wbxvo.exec_command(cmd)
            if int(row) == 0:
                raise wbxexception("crsstat does not exists ")

            logger.info("check db [%s] resource status" % self._db_name)
            cmd = "crsstat|grep -i {0}|awk '{{print $4}}'|uniq|wc -l".format(self._db_name)
            row = self.wbxvo.exec_command(cmd)
            if int(row) != 1:
                raise wbxexception("The db crs resource has {0} different status.".format(str(row)))
            cmd = "crsstat|grep -i {0}|awk '{{print $4}}'|uniq|sed 's/[[:space:]]//g'".format(self._db_name)
            row = self.wbxvo.exec_command(cmd,async_log = True)
            row = row.strip()
            if row != "ONLINE":
                raise wbxexception("The db crs resource is in a {0} status.".format(row))
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e))
            self.updateJobStatus(jobid, "FAILED")
        finally:
            self.wbxvo.close()

    def getOraCmdStr(self,db_name,cmdstr):
        cmd = """
localsid=`ps -ef|grep ora_smon|grep -i {0}|grep -v grep |awk '{{print $NF}}'|awk -F '_' '{{print $NF}}'`
source /home/oracle/.bash_profile 2>/dev/null
export ORACLE_SID=$localsid
sqlplus -S / as sysdba << EOF
SET pagesize 0 linesize 1000 feedback off heading off echo off serveroutput on;
{1}
exit;
EOF""".format(db_name, cmdstr)
        return cmd

    def chkFileisexits(self,server,*args):
        res={"isexists":True}
        for item in args:
            cmd ="if [ -f %s ];then echo True;else echo False ;fi; " %(item)
            logger.info("check file %s whether exists" %(item))
            row=server.exec_command(cmd,async_log=True)
            if row.strip() == "False":
                logger.error("%s file does not exists on server host %s" %(item,server.host_name))
                res["isexists"] = False
        return res