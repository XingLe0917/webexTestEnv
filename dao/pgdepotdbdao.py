import json
import logging

from dao.vo.alertvo import AlertVo
from dao.wbxdaomanager import wbxdao
from dao.vo.depotdbvo import wbxdatabasemanager, wbxdatabase, wbxshareplexchannel, wbxschema, wbxserver, wbxmappinginfo, \
    wbxadbmon, wbxinstance, DBPatchDeploymentVO, DBPatchReleaseVO, ShareplexBaselineVO, ShareplexCRDeploymentVO, \
    MeetingDataMonitorVO, \
    DBLinkBaselineVO, DBLinkMonitorResultVO, WebDomainDataMonitorVO, wbxloginuser, wbxdbwaiteventvo
from common.wbxexception import wbxexception
from sqlalchemy import text, or_, func, and_, literal_column
from sqlalchemy.exc import DBAPIError
from common.wbxutil import wbxutil
from biz.dbmanagement.wbxdb import db_vendor
# from dao.vo.pgdepotdbvo import wbxpguser

class PGDepotDBDao(wbxdao):
    def listDatabases(self):
        session = self.getLocalSession()
        SQL = ''' SELECT db_name,host_name,cluster_name,db_vendor,db_version,db_type,application_type,
                         appln_support_code,db_home,listener_port,monitored,wbx_cluster,web_domain
                  FROM database_info
                  WHERE db_vendor='POSTGRESQL'
              '''
        dbList = session.execute(SQL).fetchall()
        return dbList

    def listServers(self):
        session = self.getLocalSession()
        SQL = '''SELECT cname,host_name,domain,site_code,region_name,public_ip,private_ip,os_type_code,processor,
                        kernel_release,hardware_platform,physical_cpu,cores,cpu_model,flag_node_virtual,install_date,
                       comments,ssh_port
                 FROM host_info'''
        serverList = session.execute(SQL).fetchall()
        return serverList

    def listdbUsers(self):
        session = self.getLocalSession()
        SQL = '''SELECT upper(db_name) db_name,appln_support_code,schemaname,password,password_vault_path,schematype FROM appln_pool_info '''
        userList = session.execute(SQL).fetchall()
        res = [dict(zip(user.keys(), user)) for user in userList]
        # userList = session.query(wbxpguser).from_statement(text(SQL)).all()
        return res

    def listosUsers(self):
        session = self.getLocalSession()
        userList = session.query(wbxloginuser).from_statement(text(
            " select HOST_NAME, USERNAME, pwd "
            " from host_user_info WHERE lower(username)='postgres'")).all()
        return userList