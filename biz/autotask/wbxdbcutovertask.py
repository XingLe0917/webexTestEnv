import logging
from biz.dbmanagement.wbxdb import wbxdb
from dao.wbxdaomanager import wbxdaomanagerfactory
from common.wbxexception import wbxexception
from common.wbxtask import wbxautotask,threadlocal
from dao.wbxdaomanager import DaoKeys
from common.wbxutil import wbxutil
import time

logger = logging.getLogger("DBAMONITOR")

#Author :wentazha@cisco.com
#Desc   :Database Cutover
class wbxdbcutovertask(wbxautotask):
    def __init__(self,taskid = None):
        super(wbxdbcutovertask,self).__init__(taskid, "DBCUTOVER_TASK")
        self.vermap={"11":"11g","19":"19c"}
        self._old_host_name = None
        self._new_host_name = None
        self._db_name = None

    def initialize(self, **kwargs):
        self._old_host_name = kwargs["old_host_name"]
        self._new_host_name = kwargs["new_host_name"]
        self._db_name = kwargs["db_name"].upper()
        if self._db_name == "RACPSYT":
            self._db_splex_sid = "PSYTOOL_SPLEX"
        else:
            self._db_splex_sid="%s_SPLEX" % self._db_name
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daomanagerfactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        try:
            self._olddb = daomanagerfactory.getDatabaseByDBName(self._db_name)
            if self._olddb is None:
                raise wbxexception("Can not get database with db_name=%s" % self._db_name)
            logger.info("old db name %s exists" % self._db_name)
            for servername, dbserver in self._olddb.getServerDict().items():
                dbserver.verifyConnection()
            logger.info("old db servers ssh login verification passed")
            self._newdb = wbxdb(self._db_name, self._db_splex_sid)
            self._newdb.initFromServer(self._new_host_name)
            logger.info("new db servers ssh login verification passed")
            self._newserver = daomanagerfactory.getServer(self._new_host_name)
            self._oldserver = daomanagerfactory.getServer(self._old_host_name)
            self._spportMapping,self._oldsplexdict = self._olddb.getSplxPortbydb()
            res = depotdbDao.getdbtype(old_host_name=self._old_host_name, new_host_name=self._new_host_name,db_name=self._db_name)
            self._db_type = res["db_type"].upper()
            self._appln_code=res["appln_support_code"].upper()

            taskvo = super(wbxdbcutovertask, self).initialize(**kwargs)

            jobList = self.listTaskJobsByTaskid(taskvo.taskid)
            if len(jobList) == 0:
                self.generateJobs()
        except Exception as e:
            depotDaoManager.rollback()
            raise e
        finally:
            depotDaoManager.close()
        return taskvo

    def getsvrmapping(self):
        cutoversvrmapping={}
        newspportDict,newspportstatDict = self._newserver.getShareplexPortListFromCRS()
        # newspportDict={19002:"sjdbormt0156"}
        for key in self._oldsplexdict.keys():
            if key in newspportDict.keys():
                cutoversvrmapping[key]=[self._oldsplexdict[key],newspportDict[key]]
            else:
                raise wbxexception("port %s on %s does not find in new server,please double check!" % (key,self._oldsplexdict[key]))
        return cutoversvrmapping

    #check shareplex port from srcdb and tgtdb
    def preverify(self, *args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
        try:
            logger.info("DBCUTOVER PreVerify Begin...")
            daomanager.startTransaction()
            jobvo = dao.getAutoTaskJobByJobid(jobid)
            hasError=False
            #1 check wbxdba/wbxmaint/SPLEX_DENY/app schema on new host db
            logger.info("Check wbxdba/wbxmaint/app schema on new db %s" %(self._db_name))
            conditsql="'WBXDBA','WBXMAINT','SPLEX_DENY'"
            row=self._olddb.getAppSchemas()
            if row:
                schemalist=row[0].split(',')
                conditsql=conditsql+",'"+"','".join(schemalist)+"'"
            cmd="select listagg(username,',') within group(order by username) from dba_users where username in (%s);" %(conditsql.upper())
            logger.info(cmd)
            cmd = self.getOraCmdStr(cmd)
            res=self._newserver.exec_command(cmd)
            logger.info(res)
            if not wbxutil.isNoneString(res):
                res=res.strip()
                if "SPLEX_DENY" not in res.split(",") or len(res.split(","))!=1:
                    hasError=True
                    logger.error("only SPLEX_DENY schemas can exists in the new db,but now:%s" %(res.strip()))
            else:
                hasError = True
                logger.error("SPLEX_DENY does not exist in new db [%s]" %(self._db_name))

            # 2 check gather_stats job number on new db
            logger.info("Check gather_stats job on new DB %s" % (self._db_name))
            cmd = "select listagg(job_name,',') within group(order by job_name) from dba_scheduler_jobs where job_name like '%GATHER_STATS%';"
            cmd = self.getOraCmdStr(cmd)
            res = self._newserver.exec_command(cmd)
            logger.info(res)
            if not wbxutil.isNoneString(res):
                res=res.strip()
                if len(res.split(","))<2:
                    hasError=True
                    logger.error("gather_stats job is less than 2 on new db %s : %s" % (self._new_host_name,str(res)))
            else:
                hasError = True
                logger.error("new db %s does not exists any gather_stats job ,pls double check" % (self._db_name))

            # 3 check sharePlex port on new host server
            logger.info("Check sharePlex port status on new host %s" % (self._new_host_name))
            newspportDict, newspportstatDict = self._newserver.getShareplexPortListFromCRS()
            for key in self._oldsplexdict.keys():
                if newspportstatDict[key] != "ONLINE" or key not in newspportstatDict.keys():
                    hasError=True
                    logger.error("port:%s status under %s on new host server %s" %(str(key),newspportstatDict[key],self._new_host_name))

            # 4 check db profile on old host server
            cmd="""
                CSSD_DIR=`dirname $( ps -eo args | grep ocssd.bin | grep -v grep | awk '{print $1}')`;
                ${CSSD_DIR}/crsctl query crs softwareversion |awk '{print $NF}'|awk -F '.' '{print substr( $1,2)}'
            """
            row=self._oldserver.exec_command(cmd)
            vertype=self.vermap[row]
            logger.info(vertype)
            db_profile=".%s_db" %vertype
            grid_profile=".%s_grid" %vertype
            args = ["/home/oracle/"+db_profile,"/home/oracle/"+grid_profile,"/home/oracle/.bash_profile"]
            for servername, dbserver in self._olddb.getServerDict().items():
                logger.info("Check %s_db %s_grid .bash_profile on old server host %s" % (db_profile, grid_profile,dbserver.host_name))
                row = self.chkFileisexits(dbserver, *args)
                if not row["isexists"]:
                    hasError = True

            # 5 check TEST schema referenced on old db
            if self._appln_code in ['WEB','CONFIG','OPDB','TEL']:
                logger.info("check TEST schema referenced on old db %s" % (self._db_name))
                cmd = "select listagg(name,',') within group(order by name) from dba_dependencies where owner='TEST' and referenced_owner='WBXBACKUP';"
                logger.info(cmd)
                cmd=self.getOraCmdStr(cmd)
                res = self._oldserver.exec_command(cmd)
                logger.info(res)
                if not wbxutil.isNoneString(res):
                    logger.error("TEST schema referenced on old db %s : %s" % (self._db_name, str(res)))

            if hasError:
                raise wbxexception("DBCUTOVER PreVerify jobid : %s end with Error" %(jobvo.jobid))

            daomanager.commit()
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(e)
            daomanager.rollback()
            self.updateJobStatus(jobid, "FAILED")
            raise e
        finally:
            self._newserver.close()
            self._oldserver.close()
            daomanager.close()

    def executeOneStep(self, *args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("wbxdbcutover.executeOneStep(processid=%s)" % jobid)
        try:
            daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
            daomanager = daoManagerFactory.getDefaultDaoManager()
            try:
                dao = daomanager.getDao(DaoKeys.DAO_AUTOTASKDAO)
                daomanager.startTransaction()
                jobvo = dao.getAutoTaskJobByJobid(jobid)
                processorder=jobvo.processorder
                pre_order = int(processorder) - 1

                job_timedelta=jobvo.parameter["timedelta"]
                db_timedelta = dao.getJobTimedelta(jobvo.taskid,pre_order)
                db_timedelta=db_timedelta[0]
                logger.info("job_timedelta:%s , db_timedelta:%s " %(job_timedelta,db_timedelta))
                daomanager.commit()
            except Exception as e:
                daomanager.rollback()
                raise e
            finally:
                daomanager.close()

            if job_timedelta > db_timedelta:
                raise wbxexception("this job must be run %s minutes after the completion of job processorder:%s"%(job_timedelta,pre_order))

            if jobvo.parameter["server_type"] == "NEW":
                retcode = self._newdb.executeCutover(jobvo)
            elif jobvo.parameter["server_type"] == "OLD":
                retcode = self._olddb.executeCutover(jobvo)
            elif jobvo.parameter["server_type"] == "SRC":
                db = wbxdb(jobvo.parameter["src_db"])
                retcode = db.executeCutover(jobvo)

            if retcode == "FAILED":
                raise wbxexception("executeOneStep jobid:%s end with FAILED" % (jobid))
            logger.info("executeOneStep jobid:%s end with SUCCEED" % (jobid))
            #Postverify
            res=self.postVerify(jobvo)
            if res["status"]=="FAILED":
                raise wbxexception("Post Verify jobid : %s end with FAILED" % (jobid))
            logger.info("Post Verify jobid : %s end with SUCCEED" % (jobid))
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
            raise e

    def generateJobs(self):
        logger.info("generateCutoverStep(taskid=%s, db_name=%s)" % (self._taskid, self._db_name))
        cutoversvrmapping=self.getsvrmapping()
        ########################### DB preverify ##############
        self.addJob(host_name=self._old_host_name, db_name=self._db_name,job_action="preverify",stage="PREVERIFY",server_type="OLD",module="SERVER",
                    command="preverify",process_order=1, execute_method="SYNC",timedelta=0,isoneclick=True)

        ########################### SETENVIROMENT ##############
        self.addJob(host_name=self._old_host_name, db_name=self._db_name, job_action="executeOneStep",stage="SETENVIROMENT", server_type="OLD",
                    module="DB", opaction="START",command="/staging/gates/dbcutover/dbcutover_setenviroment.sh",process_order=7, execute_method="SYNC"
                    ,timedelta=0,isoneclick=True)

        ########################### Precutover ##############
        # db on old server
        self.addJob(host_name=self._old_host_name, db_name=self._db_name,job_action="executeOneStep",stage="PRECUTOVER",server_type="OLD",
                    module="DB",command="/staging/gates/dbcutover/dbcutover_oldserver.sh",process_order=2, execute_method="SYNC",timedelta=0,isoneclick=True)

        # shareplex on old server
        for item in self._oldsplexdict.keys():
            port=item
            old_host=self._oldsplexdict[port]
            new_host=cutoversvrmapping[port][1]
            self.addJob(host_name=old_host, db_name=self._db_name, splex_port=port, job_action="executeOneStep",
                        stage="PRECUTOVER", server_type="OLD", module="SHAREPLEX",old_host=old_host, new_host=new_host,
                        process_order=3, command="/staging/gates/dbcutover/dbcutover_oldserver.sh",execute_method="SYNC",timedelta=0,isoneclick=True)
        # db on new server
        self.addJob(host_name=self._new_host_name, db_name=self._db_name, job_action="executeOneStep",
                    stage="PRECUTOVER", server_type="NEW", module="DB",process_order=4,command="/staging/gates/dbcutover/dbcutover_newserver.sh",
                    execute_method="SYNC",timedelta=0,isoneclick=True)

        # shareplex on new server
        for item in self._oldsplexdict.keys():
            port=item
            old_host=self._oldsplexdict[port]
            new_host=cutoversvrmapping[port][1]
            self.addJob(host_name=new_host, db_name=self._db_name, splex_port=port, job_action="executeOneStep",stage="PRECUTOVER",
                        server_type="NEW", module="SHAREPLEX",old_host=old_host, new_host=new_host,command="/staging/gates/dbcutover/dbcutover_newserver.sh",
                        process_order=5, execute_method="SYNC",timedelta=0,isoneclick=True)

        # Src server
        for item in self._spportMapping["tgt"]:
            src_host=item["src_host"]
            src_db=item["src_db"]
            old_host=item["tgt_host"]
            port=item["port"]
            new_host=cutoversvrmapping[port][1]
            self.addJob(host_name=src_host, db_name=self._db_name, splex_port=port,stage="PRECUTOVER", server_type="SRC", module="SHAREPLEX",
                        job_action="executeOneStep",old_host=old_host, new_host=new_host, src_db=src_db,command="/staging/gates/dbcutover/dbcutover_srcserver.sh",
                        process_order=6, execute_method="SYNC",timedelta=0,isoneclick=True)

            self.addJob(host_name=src_host, db_name=self._db_name, splex_port=port,stage="PREPARE1", server_type="SRC", module="SHAREPLEX",
                        job_action="executeOneStep",old_host=old_host, new_host=new_host, src_db=src_db,command="/staging/gates/dbcutover/dbcutover_srcserver.sh",
                        process_order=8 ,execute_method="SYNC",timedelta=0,isoneclick=True)

            self.addJob(host_name=src_host, db_name=self._db_name, splex_port=port, stage="PREPARE", server_type="SRC",module="SHAREPLEX",
                        job_action="executeOneStep", old_host=old_host, new_host=new_host, src_db=src_db,command="/staging/gates/dbcutover/dbcutover_srcserver.sh",
                        process_order=9, execute_method="SYNC",timedelta=0,isoneclick=True)

        ########################### PREPARE ##############
        # shareplex on old server
        for item in self._oldsplexdict.keys():
            port=item
            old_host=self._oldsplexdict[port]
            new_host=cutoversvrmapping[port][1]
            self.addJob(host_name=new_host, db_name=self._db_name, splex_port=port, job_action="executeOneStep",stage="PREPARE",
                        server_type="NEW", module="SHAREPLEX", old_host=old_host, new_host=new_host,command="/staging/gates/dbcutover/dbcutover_newserver.sh",
                        process_order=10, execute_method="SYNC",timedelta=0, isoneclick=True)

            self.addJob(host_name=old_host, db_name=self._db_name, splex_port=port, job_action="executeOneStep",stage="PREPARE",
                        server_type="OLD", module="SHAREPLEX",old_host=old_host, new_host=new_host,command="/staging/gates/dbcutover/dbcutover_oldserver.sh",
                        process_order=11, execute_method="SYNC",timedelta=0,isoneclick=True)

        self.addJob(host_name=self._old_host_name, db_name=self._db_name, job_action="executeOneStep",stage="PREPARE", server_type="OLD",
                    module="DB",command="/staging/gates/dbcutover/dbcutover_oldserver.sh",process_order=12, execute_method="SYNC",timedelta=0,isoneclick=True)

        #################################### START ############################
        self.addJob(host_name=self._old_host_name, db_name=self._db_name, job_action="executeOneStep",stage="START", server_type="OLD",
                    module="DB",command="/staging/gates/dbcutover/dbcutover_oldserver.sh",process_order=13, execute_method="SYNC",timedelta=0,isoneclick=True)
        self.addJob(host_name=self._new_host_name, db_name=self._db_name, job_action="executeOneStep",stage="START", server_type="NEW",
                    module="DB",command="/staging/gates/dbcutover/dbcutover_newserver.sh",process_order=14, execute_method="SYNC",timedelta=0,isoneclick=True)

        #################################### POST ############################
        self.addJob(host_name=self._old_host_name, db_name=self._db_name, job_action="executeOneStep",stage="POST", server_type="OLD",
                    module="DB",command="/staging/gates/dbcutover/dbcutover_oldserver.sh",process_order=15, execute_method="SYNC",timedelta=0,isoneclick=True)
        self.addJob(host_name=self._new_host_name, db_name=self._db_name, job_action="executeOneStep",stage="POST", server_type="NEW",
                    module="DB",command="/staging/gates/dbcutover/dbcutover_newserver.sh",process_order=16, execute_method="SYNC",timedelta=0,isoneclick=True)

        # shareplex on new server
        for item in self._oldsplexdict.keys():
            port = item
            old_host = self._oldsplexdict[port]
            new_host = cutoversvrmapping[port][1]
            self.addJob(host_name=new_host, db_name=self._db_name, splex_port=port,job_action="executeOneStep",stage="POST",
                        server_type="NEW", module="SHAREPLEX", old_host=old_host, new_host=new_host,command="/staging/gates/dbcutover/dbcutover_newserver.sh",
                        process_order=17, execute_method="SYNC",timedelta=0,isoneclick=True)

        #################################### REGISTER ############################
        self.addJob(host_name=self._new_host_name, db_name=self._db_name, job_action="executeOneStep",stage="REGISTER", server_type="NEW",
                    module="DB", application_type=self._olddb.getApplicationType(),appln_support_code=self._olddb.getApplnSupportCode(),
                    db_type=self._db_type,command="/staging/gates/dbcutover/dbcutover_registerdb.sh",process_order=18, execute_method="SYNC"
                    ,timedelta=0,isoneclick=True)

        #################################### addcr ############################
        tgt_port = []
        for item in self._spportMapping["tgt"]:
            port = item["port"]
            new_host = cutoversvrmapping[port][1]
            if port in tgt_port:
                continue
            self.addJob(host_name=new_host, db_name=self._db_name, job_action="executeOneStep", stage="ADDCR",server_type="NEW",
                        splex_sid=self._db_splex_sid, splex_port=port, module="SHAREPLEX",command="/staging/gates/addcr_for_shareplex.sh",
                        process_order=19,execute_method="SYNC",timedelta=0,isoneclick=False)
            tgt_port.append(port)

        #################################### install_jobmanager ############################
        newsvrlist = self._newserver.getRacServerList(self._db_name)
        for host in newsvrlist:
            self.addJob(host_name=host, job_action="executeOneStep", stage="INSTALL_JOBMANAGER", command="/staging/gates/install_jobmanager.sh",
                        module="SERVER",server_type="NEW",process_order=20,execute_method="SYNC",timedelta=0,isoneclick=False)

        # Src server
        for item in self._spportMapping["tgt"]:
            src_host = item["src_host"]
            src_db = item["src_db"]
            old_host = item["tgt_host"]
            port = item["port"]
            new_host = cutoversvrmapping[port][1]
            self.addJob(host_name=src_host, db_name=self._db_name, splex_port=port, stage="ACTIVATEPOSTCONFIGFILE",server_type="SRC", module="SHAREPLEX",
                        job_action="executeOneStep", old_host=old_host, new_host=new_host, src_db=src_db,
                        command="/staging/gates/dbcutover/dbcutover_srcserver.sh",
                        process_order=21, execute_method="SYNC", timedelta=360,isoneclick=False)

        logger.info("generateCutoverStep end with successed")

    def chkFileisexits(self,server,*args):
        res={"isexists":True}
        for item in args:
            cmd ="if [ -f %s ];then echo True;else echo False ;fi; " %(item)
            logger.info("check file %s whether exists" %(item))
            row=server.exec_command(cmd)
            logger.info(row)
            if row.strip() == "False":
                logger.error("%s file does not exists on server host %s" %(item,server.host_name))
                res["isexists"] = False
        return res

    def getSplexProdDir(self,server,port):
        splx_proddir=""
        splx_vardir=""
        cmd="""
        ps -ef | grep sp_cop | egrep -v "grep|sp_ctrl|splex_action" |grep '%s$'|awk '{print $8}' | awk -F"/" '{print "/"$2"/"$3}'|sed 's/[[:space:]]//g'
        """%(port)
        splx_proddir=server.exec_command(cmd)
        logger.info(splx_proddir)
        if not wbxutil.isNoneString(splx_proddir):
            splx_proddir=splx_proddir.strip()
            splx_vardir=splx_proddir[:splx_proddir.rfind("/")]+"/vardir_"+str(port)
            res={"splx_proddir":splx_proddir,"splx_vardir":splx_vardir}
        else:
            raise wbxexception("con not get shareplex production directory on host:%s,pls double check!" %(server.host_name))
        return res

    def postVerify(self,jobvo):
        server=None
        logger.info("beging to Post Verify( jobid : %s )" %(jobvo.jobid))
        res = {"status": "SUCCEED","data": None}
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        depotDaoManager = daomanagerfactory.getDefaultDaoManager()
        depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        server = daomanagerfactory.getServer(jobvo.host_name)
        hasError=False
        l_db_name = self._db_name.lower()
        host_name=jobvo.host_name
        port=jobvo.splex_port
        try:
            if jobvo.processorder==2: #1 check expdp_dbconsolidation.par & 2 EXPDP_DIR
                logger.info("check whether /staging/expdp/%s/expdp_dbconsolidation.par exist on old db" %(l_db_name))
                args="/staging/expdp/%s/expdp_dbconsolidation.par" %(l_db_name)
                self._oldserver.connect()
                row = self.chkFileisexits(self._oldserver, args)
                if not row["isexists"]:
                    hasError = True
                # if not hasError:
                logger.info("check whether data directory EXPDP_DIR exist or not on old db")
                cmd="select directory_path from dba_directories where directory_name='EXPDP_DIR';"
                cmd = self.getOraCmdStr(cmd)
                row = self._oldserver.exec_command(cmd)
                logger.info(row)
                if wbxutil.isNoneString(row):
                    hasError = True
                    logger.error("the EXPDP_DIR %s : %s" % (self._db_name, str(row)))
            elif jobvo.processorder == 3:
                args="/staging/gates/dbconsolidation/%s/SPLEX%s_cr_tabs.sql" %(self._db_name,port)
                logger.info("check whether %s file exist or not on old host %s" %(args,host_name))
                server.connect()
                row = self.chkFileisexits(server, args)
                if not row["isexists"]:
                    hasError = True
            elif jobvo.processorder == 4:
                #1 check impdp_dbconsolidation.par on new host
                args="/staging/expdp/%s/impdp_dbconsolidation.par"%(l_db_name)
                logger.info("check whether %s file exist or not on new host %s" %(args,self._new_host_name))
                self._newserver.connect()
                row = self.chkFileisexits(self._newserver, args)
                if not row["isexists"]:
                    hasError = True
                # if not hasError:
                #2 check new db mode
                logger.info("check whether new db in non archivelog mode")
                cmd = "select log_mode from v\$database; "
                cmd = self.getOraCmdStr(cmd)
                row = self._newserver.exec_command(cmd)
                logger.info(row)
                if not wbxutil.isNoneString(row):
                    row = row.strip()
                    if row != "NOARCHIVELOG":
                        hasError = True
                        logger.error("current new db %s log_mode should be [NOARCHIVELOG] ,but now [%s]." % (self._db_name, str(row)))
                else:
                    hasError = True
                    logger.error("can not get db log_mod on new db with host:%s" %(host_name))
            elif jobvo.processorder == 5:
                #1 check splex.<port>.config.<DB_NAME>.old config file
                args="/staging/gates/dbconsolidation/%s/splex.%s.config.%s.old" %(self._db_name,str(port),self._db_name)
                logger.info("check whether %s file exist or not on new host %s" % (args, host_name))
                self._newserver.connect()
                server.connect()
                row = self.chkFileisexits(self._newserver, args)
                if row["isexists"]:
                    row = self.getSplexProdDir(server, port)
                    args="%s/config/splex.%s.config.cutover.post" %(row["splx_vardir"],port)
                    row = self.chkFileisexits(server, args)
                    if not row["isexists"]:
                        hasError = True
                # if not hasError:
                #2 check whether SPLEX_MONITOR_ADB table exists in new db
                logger.info("check whether SPLEX%s.SPLEX_MONITOR_ADB table exist in new DB" %(str(port)))
                cmd="select table_name from dba_tables where owner='SPLEX%s' and table_name='SPLEX_MONITOR_ADB';" %(str(port))
                cmd = self.getOraCmdStr(cmd)
                row = self._newserver.exec_command(cmd)
                logger.info(row)
                if wbxutil.isNoneString(row):
                    hasError = True
                    logger.error("SPLEX%s.SPLEX_MONITOR_ADB table does not exists in new db" %(str(port)))
            elif jobvo.processorder == 6:
                logger.info("check 3 config file exist (splex.%s.config.cutover.prepare, splex.%s.config.cutover.pre, "
                            "splex.%s.config.cutover.post) on src host:%s " %(str(port),str(port),str(port),jobvo.host_name))
                server=daomanagerfactory.getServer(jobvo.host_name)
                server.connect()
                row = self.getSplexProdDir(server, port)
                splex_cnfdir=row["splx_vardir"]+"/config"
                args = ["{0}/splex.{1}.config.cutover.prepare".format(splex_cnfdir,str(port))
                      , "{0}/splex.{1}.config.cutover.pre".format(splex_cnfdir,str(port))
                      , "{0}/splex.{1}.config.cutover.post".format(splex_cnfdir,str(port))]
                row = self.chkFileisexits(server, *args)
                if not row["isexists"]:
                    hasError = True
            elif jobvo.processorder==8:
                logger.info("check whether shareplex channel exist from srcdb on new db")
                cutoversvrmapping = self.getsvrmapping()
                tgt_host_name=cutoversvrmapping[port][1]
                server=daomanagerfactory.getServer(tgt_host_name)
                server.connect()
                row = self.getSplexProdDir(server, port)
                splx_bindir = row["splx_proddir"] + "/bin"
                cmd = """source %s/.profile_u%s; echo "show import" | %s/sp_ctrl 2>&1| grep -i %s-vip""" % (splx_bindir, port, splx_bindir, host_name)
                logger.info("execute %s on new host %s" % (cmd, tgt_host_name))
                row = server.exec_command(cmd)
                logger.info(row)
                if wbxutil.isNoneString(row):
                    hasError = True
                    logger.error("%s-vip shareplex channel does not exist on new db,pls double check" %host_name)
            elif jobvo.processorder==9:
                logger.info("check whether activate config file is splex.%s.config.cutover.pre on src host:%s " %(str(port),host_name))
                server.connect()
                row = self.getSplexProdDir(server, port)
                cmd=""" cat %s/data/statusdb | grep "active from" | grep "active from"|awk -F \\\" '{print $2}' """%(row["splx_vardir"])
                logger.info("execute %s on host:%s" %(cmd,host_name))
                row = server.exec_command(cmd)
                logger.info(row)
                if not wbxutil.isNoneString(row):
                    splx_activefile="splex.{0}.config.cutover.pre".format(str(port))
                    if row.strip()!=splx_activefile:
                        hasError=True
                        logger.error("activate config file should be %s but not now is %s on src host:%s" %(splx_activefile,row.strip(),host_name))
                else:
                    hasError = True
                    logger.error("does not find activate config file on src host:%s" %(host_name))
            elif jobvo.processorder == 12:
                logger.info("check whether old db is in restricted mode")
                cmd = "select logins from v\$instance;"
                cmd = self.getOraCmdStr(cmd)
                self._oldserver.connect()
                row = self._oldserver.exec_command(cmd)
                logger.info(row)
                row = row.strip()
                if row != "RESTRICTED":
                    hasError = True
                    logger.error("current old db %s mode shoud be [RESTRICTED],but now [%s]." % (self._db_name, str(row)))
            elif jobvo.processorder == 13:
                args = "/staging/expdp/%s/expdp_dbconsolidation_%s.log" % (l_db_name, self._db_name)
                logger.info("check %s file whether exists" % (args))
                self._oldserver.connect()
                row = self.chkFileisexits(self._oldserver, args)
                if not row["isexists"]:
                    hasError = True
                if not hasError:
                    cmd="""cat %s | grep -i "successfully completed" """ % (args)
                    row = self._oldserver.exec_command(cmd)
                    logger.info(row)
                    if wbxutil.isNoneString(row):
                        hasError = True
                        logger.error("%s file does not include 'successfully completed' character,pls double check" %(args))
            elif jobvo.processorder == 14:  # jobvo.processorder==14
                args = "/staging/expdp/%s/impdp_dbconsolidation_%s.log" % (l_db_name, self._db_name)
                logger.info("check %s file whether exists" % (args))
                self._newserver.connect()
                row = self.chkFileisexits(self._newserver, args)
                if not row["isexists"]:
                    hasError = True
                # if not hasError:
                logger.info("Check WBXDBA on new db :%s" % (self._db_name))
                cmd = "select listagg(username,',') within group(order by username) from dba_users where username in ('WBXDBA');"
                cmd = self.getOraCmdStr(cmd)
                row = self._newserver.exec_command(cmd)
                logger.info(row)
                if not wbxutil.isNoneString(row):
                    row = row.strip()
                    if len(row.split(",")) < 1:
                        hasError = True
                        logger.error("current wbxdba schema should be on new db %s,but not now:%s" % (self._db_name, str(row)))
                else:
                    hasError = True
                    logger.error("wbxdba schema does not exists on new db [%s]" % (self._db_name))
            elif jobvo.processorder==16:#jobvo.processorder==16
                logger.info("check whether new db in archivelog mode")
                cmd="select log_mode from v\$database; "
                cmd = self.getOraCmdStr(cmd)
                self._newserver.connect()
                row = self._newserver.exec_command(cmd)
                logger.info(row)
                row = row.strip()
                if row!="ARCHIVELOG":
                    hasError = True
                    logger.error("new db %s log_mode is current on [%s],pls double check" %(self._db_name,str(row)))
                appschems = self._newdb.getAppSchemas()
                # check appschems objects whether vaild/analyzed
                if appschems:
                    schemalist = appschems[0].split(',')
                    for item in schemalist:
                        logger.info("check whether all objects are vaild under [%s] schema on new db" %item)
                        cmd="select listagg(object_name,',') within group(order by object_name) from dba_objects where owner='%s' and status='INVALID';"%item
                        cmd = self.getOraCmdStr(cmd)
                        row = self._newserver.exec_command(cmd)
                        logger.info(row)
                        if not wbxutil.isNoneString(row):
                            hasError = True
                            logger.error("below object are INVALID on new db %s : %s" % (self._db_name, str(row.strip())))

                        logger.info("check whether tables has not been analyzed under [%s] schema on new db" % item)
                        cmd = "select listagg(table_name,',') within group(order by table_name) from dba_tables where owner='%s' and last_analyzed is null and temporary !='Y';" % item
                        logger.info("execute command %s on new db %s " % (cmd, self._db_name))
                        cmd = self.getOraCmdStr(cmd)
                        row = self._newserver.exec_command(cmd)
                        logger.info(row)
                        if not wbxutil.isNoneString(row):
                            hasError = True
                            logger.error("below object are not analyzed on new db %s : %s" % (self._db_name, str(row.strip())))
            elif jobvo.processorder == 17:
                logger.info("check whether all shareplex port post process are RUNNING on new db")
                time.sleep(30)
                server.connect()
                row = self.getSplexProdDir(server, port)
                splx_bindir=row["splx_proddir"]+"/bin"
                cmd = """source %s/.profile_u%s; echo "show" | %s/sp_ctrl 2>&1| grep -i post |awk '{print $2","$3","$4}'""" % (splx_bindir,port, splx_bindir)
                logger.info("execute %s on new db server:%s " %(cmd,host_name))
                row = server.exec_command(cmd)
                logger.info(row)
                row = row.strip()
                if not wbxutil.isNoneString(row):
                    for line in row.splitlines():
                        channel=line.split(",")
                        if channel[2].upper() !="RUNNING":
                            hasError = True
                            logger.error("shareplex post process under %s on new db host" %(line))
            elif jobvo.processorder == 18:
                logger.info("Check whether depotdb data is right for instance_info and shareplex_info")
                cmd="ps -ef|grep ora_smon|grep -i {0}|grep -v grep |awk '{{print $NF}}'|awk -F '_' '{{print $NF}}'".format(self._db_name)
                server.connect()
                instance_name = server.exec_command(cmd)
                logger.info(instance_name)
                if not wbxutil.isNoneString(instance_name):
                    rows=depotdbDao.isExistDeport(instance_name,host_name)
                    logger.info(rows)
                    if len(rows)==0:
                        hasError = True
                        logger.error("can not get instance info from depot db with host:%s and instance_name:%s" % (host_name, instance_name))
                else:
                    hasError = True
                    logger.error("can not get instance_name from host:%s" %(host_name))
            elif jobvo.processorder == 20:
                logger.info("check wbxjobmanagerinstance status")
                time.sleep(15)
                rows = depotdbDao.getWbxJobMgStatusByHost(host_name)
                logger.info(rows)
                if rows:
                    row=rows[0]
                    logger.info(row)
                    if wbxutil.isNoneString(row):
                        hasError = True
                        logger.error("does not get wbxjobmanagerinstance status with host_name:%s" % (host_name))
                else:
                    hasError = True
                    logger.error("does not get wbxjobmanagerinstance status with host_name:%s" %(host_name))
            elif jobvo.processorder == 21:
                logger.info("check whether all shareplex port post process are RUNNING on new db")
                postconfigfilename = "splex.%s.config.cutover.post" %(port)
                server.connect()
                row = self.getSplexProdDir(server, port)
                splx_bindir=row["splx_proddir"]+"/bin"
                cmd = """source %s/.profile_u%s; echo "list config" | %s/sp_ctrl 2>&1| grep -iw "Active" | awk '{print $1","$2","$3}'""" % (splx_bindir,port, splx_bindir)
                logger.info("execute %s on new db server:%s " %(cmd,host_name))
                row = server.exec_command(cmd)
                logger.info(row)
                row = row.strip()
                if not wbxutil.isNoneString(row):
                    channel = row.split(",")
                    if not channel[0].startswith(postconfigfilename):
                        hasError = True
                        logger.error("The config filename on the current %s server %s port should be start with %s, but now it is %s"
                                     %(host_name,port,postconfigfilename,channel[0]))

            depotDaoManager.commit()
        except Exception as e:
            hasError = True
            logger.error(e)
            depotDaoManager.rollback()
        finally:
            try:
                depotDaoManager.close()
                if not self._newserver:
                    self._newserver.close()
                if not self._oldserver:
                    self._oldserver.close()
                server.close()
            except Exception as e :
                logger.info("db server closed with error %s" %str(e))

        if hasError:
            res["status"] = "FAILED"
        return res

    def getOraCmdStr(self, cmdstr,db_name=None):
        # logger.info(cmdstr)
        if db_name is None:
            db_name=self._db_name
        cmd = """
localsid=`ps -ef|grep ora_smon|grep -i {0}|grep -wv grep |awk '{{print $NF}}'|awk -F '_' '{{print $NF}}'`
source /home/oracle/.bash_profile
export ORACLE_SID=$localsid
sqlplus -S / as sysdba << EOF
SET pagesize 0 linesize 1000 feedback off heading off echo off serveroutput on;
{1}
exit;
EOF""".format(db_name, cmdstr)
        return cmd
