import logging
from common.Config import Config
from biz.dbmanagement.wbxminidom import wbxminidom
from dao.wbxdaomanager import wbxdaomanagerfactory
from common.wbxssh import wbxssh
import prettytable as pt
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from common.wbxtask import wbxautotask,threadlocal
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import as_completed
from dao.wbxdaomanager import DaoKeys
from datetime import datetime
import re
logger = logging.getLogger("DBAMONITOR")

class wbxdbaudittask(wbxautotask):
    def __init__(self,taskid = None):
        super(wbxdbaudittask, self).__init__(taskid, "DBAUDIT_TASK")
        self._configfile="dbaudit.xml"
        self._auditlist=["OS","DB"]
        self._nodelist=["title","command","actualval","compareval"]
        self.nodecache ={}
        self._db_name = None
        self._host_name=None
        self._auditdom = None
        self._basedom = None

    def initialize(self,**kwargs):
        self._host_name = kwargs["host_name"]
        self._db_name = kwargs["db_name"].upper()
        self._modelid = kwargs["modelid"]
        self._auditType= kwargs["auditType"].upper()

        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        server = daoManagerFactory.getServer(self._host_name)

        if wbxutil.isNoneString(self._host_name):
            raise wbxexception("host_name is required ,db_name is optional.")

        try:
            if server is None:
                raise wbxexception("Can not get this server with host_name=%s" % self._host_name)
            logger.info("server %s exists" % self._host_name)
            svrdict = server.getRacNodeDict()
            for nodetype,servervo in svrdict.items():
                servervo.verifyConnection()
            logger.info("servers ssh login verification passed")

            if not wbxutil.isNoneString(self._db_name):
                cmd="ps aux | grep ora_smon| grep -wv grep | grep -i %s|awk '{{print $NF}}'" %(self._db_name)
                logger.info("exec command %s on host :%s" %(cmd,self._host_name))
                server.connect()
                rows=server.exec_command(cmd,timeout=10)
                logger.info(rows)
                if wbxutil.isNoneString(rows):
                    raise wbxexception("Can not find db with db_name :%s on host:%s" % (self._db_name,self._host_name))

            config = Config.getConfig()
            auditconf = config.getdbAuditConfigFile()
            self._basedom = wbxminidom(auditconf)
            for item in self._auditlist:
                self.nodecache[item] = {}
                parentNodeList = self._basedom.getNodesByTagname(item, self._basedom.dom)
                if len(parentNodeList) > 0:
                    nodes = parentNodeList[0].getElementsByTagName("templete")
                    for node in nodes:
                        templeteid = node.getAttribute("id")
                        self.nodecache[item][templeteid] = node

            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            kwargs["timestamp"]=timestamp
            taskvo = super(wbxdbaudittask, self).initialize(**kwargs)
            jobList = self.listTaskJobsByTaskid(taskvo.taskid)
            # self._taskid = taskvo.taskid
            if len(jobList) == 0:
                self.generateJobs()
        except Exception as e:
            raise e
        finally:
            server.close()
        return taskvo

    def generateJobs(self):
        dbmsg = ""
        kargs={"host_name":self._host_name,"db_name":self._db_name,"modelid":self._modelid,"auditType":self._auditType,
               "job_action":"executeOneStep","process_order":1,"execute_method":"SYNC","isoneclick":True}
        if not wbxutil.isNoneString(self._db_name):
            dbmsg = ",db_name=%s" %self._db_name

        logger.info("generateAuditStep(taskid=%s, [host_name=%s %s])" % (self._taskid, self._host_name,dbmsg))
        self.addJob(**kargs)
        logger.info("generateAuditStep end with successed")

    def executeOneStep(self, *args):
        jobid = args[0]
        threadlocal.current_jobid = jobid
        logger.info("wbxdbaudit.executeOneStep(processid=%s)" % jobid)
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daoManagerFactory.getDefaultDaoManager()
        try:
            try:
                dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
                daomanager.startTransaction()
                rows=dao.get_wbxdbauditlist(self._modelid)
                if rows is None:
                    raise wbxexception("can not get templelist with modelid: %s" %self._modelid)
                daomanager.commit()
            except Exception as e:
                daomanager.rollback()
                raise e
            finally:
                daomanager.close()

            if wbxutil.isNoneString(self._db_name):
                self._auditType="OS"

            if self._auditType == "ALL":
                kargs = rows
            else:
                kargs = {self._auditType:rows[self._auditType]}
            self.start(**kargs)
            self.updateJobStatus(jobid, "SUCCEED")
        except Exception as e:
            logger.error(str(e), exc_info=e)
            self.updateJobStatus(jobid, "FAILED")
            raise e

    def start(self,**kargs):
        self._auditdom = wbxminidom()
        args={"host_name":self._host_name}
        if not wbxutil.isNoneString(self._db_name):
            args["db_name"]=self._db_name
        nodeRoot=self._auditdom.createElement("DatabaseAudit",**args)
        daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        server = daoManagerFactory.getServer(self._host_name)
        svrdict = server.getRacNodeDict()

        xmlpath = "/usr/local/nginx/html/wbxdbaudit/"
        filenamestr="wbxdbaudit_{}_{}.xml".format(self._host_name,self._taskid)
        if not wbxutil.isNoneString(self._db_name):
            filenamestr="wbxdbaudit_{}_{}_{}.xml".format(self._db_name,self._host_name,self._taskid)
        filename="%s" %(filenamestr)

        # Audit (OS/DB)
        if "OS" in kargs.keys():
            for key in sorted(svrdict.keys()):
                logger.info("wbxOSAudit on server:%s" %(svrdict[key].host_name))
                node = self.wbxAudit(svrdict[key],"OS", *kargs["OS"])
                nodeRoot.appendChild(node)
        if "DB" in kargs.keys():
            logger.info("wbxDBAudit on db:%s" % (self._db_name))
            node = self.wbxAudit(server,"DB",*kargs["DB"])
            nodeRoot.appendChild(node)

        self._auditdom.dom.appendChild(nodeRoot)

        self._auditdom.wbxWriteXML(xmlpath+filename)

    def wbxAudit(self,server,auditType,*args):
        hasErr=False

        if auditType=="OS":
            kargs = {"host_name": server.host_name}
        elif auditType=="DB":
            kargs = {"db_name": self._db_name}
        nodeFirstlev = self._auditdom.createElement(auditType, **kargs)
        joblist=[]
        for templeteid in args:
            basenode = self.nodecache[auditType][templeteid]
            command = self._auditdom.getCDATANodetext(basenode, "command")
            if auditType=="DB":
                command = self.getOraCmdStr(command)

            kargs = {"command": command}
            kargs["templeteid"] = templeteid
            kargs["basenode"] = basenode
            kargs["operator"] = server
            joblist.append(kargs)
        try:
            executor = ThreadPoolExecutor(2)
            fs = [executor.submit(self.templeteparse, **kargs) for kargs in joblist]
            for f in as_completed(fs):
                row = f.result()
                if row["status"]=="SUCCEED":
                    nodeFirstlev.appendChild(row["node"])
                else:
                    hasErr=True
                    logger.error(row["msg"])
            if hasErr:
                raise wbxexception("issue was occurd on wbxAudit")
        finally:
            server.close()
        return nodeFirstlev

    def templeteparse(self,**kargs):
        pattstr = re.compile('{\s+}')
        templeteid = kargs["templeteid"]
        basenode=kargs["basenode"]
        command=kargs["command"]
        server=kargs["operator"]
        wbxvo = wbxssh(server.host_name, server.ssh_port, server.login_user, server.login_pwd)
        res={"status":"SUCCEED","msg":""}

        logger.info("begin to parse templeid : %s" %templeteid)
        try:
            args = {"id": templeteid}
            nodeTwolev = self._auditdom.createElement("templete", **args)
            scopetext=self._auditdom.getCDATANodetext(basenode, "scope")
            thresholdtext=self._auditdom.getCDATANodetext(basenode, "threshold")
            resultsetText =self._auditdom.getCDATANodetext(basenode, "resultset")
            thresholdtext=pattstr.sub('',thresholdtext)
            scopetext=pattstr.sub('',scopetext)
            resultsetText=pattstr.sub('',resultsetText)
            args = {}
            wbxvo.connect()
            actualval = wbxvo.exec_command(command,timeout=10)
            pre_scope = {"actualval": actualval}
            exec(scopetext, pre_scope)
            scope = pre_scope["scope"]
            exec(thresholdtext, scope)
            compareval = scope["status"]
            res_scope = {"actualval": actualval,"status":compareval}
            exec(resultsetText, res_scope)
            resultset = res_scope["compareres"]

            for nodename in self._nodelist:
                # handle text node content
                if nodename in ["actualval"]:
                    text=" "
                    actHeader=self._auditdom.getNodetext(basenode, "header")

                    if not wbxutil.isNoneString(resultset):
                        if not wbxutil.isNoneString(actHeader):
                            tb = pt.PrettyTable()
                            tb.field_names =actHeader.split('|+|')
                        else:
                            tb = pt.PrettyTable(header=False)
                        for item in resultset.splitlines():
                            tb.add_row(item.split('|+|'))
                        text = tb.get_string()
                elif nodename in ["compareval"]:
                    text=compareval
                else:
                    text = self._auditdom.getCDATANodetext(basenode, nodename)

                # handle node
                if nodename in ["command", "actualval"] and not wbxutil.isNoneString(text):
                    nodetext = self._auditdom.createCDATATextElement(text)
                else:
                    nodetext = self._auditdom.createTextElement(text)

                node = self._auditdom.createElement(nodename, **args)
                node.appendChild(nodetext)
                nodeTwolev.appendChild(node)
            res["node"]=nodeTwolev
        except Exception as e:
            res["status"] = "FAILED"
            res["msg"] = "issue was occurd templeteid: %s,error:%s" %(templeteid,str(e))
            # logger.error("issue was occurd templeteid: %s,error:%s" %(templeteid,str(e)))
        finally:
            wbxvo.close()
        logger.info("parse templeid : %s end" %templeteid)

        return res

    def getOraCmdStr(self, cmdstr, db_name=None):
        if db_name is None:
            db_name = self._db_name
        cmd = """
localsid=`ps -ef|grep ora_smon|grep -i {0}|grep -wv grep |awk '{{print $NF}}'|awk -F '_' '{{print $NF}}' | head -1`
source /home/oracle/.bash_profile
export ORACLE_SID=$localsid
sqlplus -S / as sysdba << EOF
SET pagesize 0 linesize 1000 feedback off heading off echo off serveroutput on;
{1}
exit;
EOF""".format(db_name, cmdstr)
        return cmd
