import logging
import os
from base64 import b64encode

import requests

from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

def getWbxCassClusterName(casscluster):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        casscluster_list = dao.get_wbxCassClusterName(casscluster)
        daoManager.commit()
        res['data'] = [dict(vo) for vo in casscluster_list]
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def getWbxCassClusterInfo(casscluster,localdc):
    logger.info("getWbxCassClusterInfo,casscluster=%s,localdc=%s" %(casscluster,localdc))
    res = {"status": "SUCCESS", "errormsg": "","data":None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    if not casscluster and not localdc:
        res['status'] = "FAILED"
        res['errormsg'] = "param is null"
        return res
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        cassClusterInfo = dao.get_WbxCassClusterInfo(casscluster,localdc)
        userInfo = dao.get_wbxCassUser(casscluster)
        keyspacesInfo = dao.get_wbxCassAppKeyspaceConnInfo("",casscluster,localdc)
        daoManager.commit()
        data = {}
        data['cassClusterInfo'] = [dict(vo) for vo in cassClusterInfo]
        data['userInfo'] = [dict(vo) for vo in userInfo]
        data["keyspacesInfo"] = [dict(vo) for vo in keyspacesInfo]
        data["cassclustername"] = casscluster
        res['data'] = data
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def addCassAppService(serviceid,servicename,servicelevel):
    logger.info("addCassAppService, servicename=%s servicelevel=%s" % (servicename,servicelevel))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        if serviceid:
            dao.update_wbxCassAppService(serviceid,servicename,servicelevel)
        else:
            dao.add_wbxCassAppService(servicename,servicelevel)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def getWbxCasscluster():
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = dao.get_wbxCasscluster()
        daoManager.commit()
        res['data'] = [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def getWbxCassKeyspaceid(casscluster,localdc,keyspacename):
    logger.info("getWbxCassKeyspaceid,casscluster=%s,localdc=%s,keyspacename%s" %(casscluster,localdc,keyspacename))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        if casscluster and not localdc and not keyspacename:
            list = dao.get_wbxCassKeyspaceidBycasscluster(casscluster)
        elif casscluster and localdc and not keyspacename:
            list = dao.get_wbxCassKeyspaceidBycassclusterDC(casscluster,localdc)
        else:
            list = dao.get_wbxCassKeyspaceid(casscluster,localdc,keyspacename)
        daoManager.commit()
        res['data'] = [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def addWbxCassKeyspaceEnvServiceMap(keyspaceid,serviceid,envtype,applocation,webdomain):
    res = {"status": "SUCCESS", "errormsg": "","data":None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        dao.add_wbxCassEnvAppServiceMap(serviceid, envtype, applocation, webdomain)
        envappservice = dao.get_wbxCassEnvAppServiceMap(serviceid, applocation, envtype)
        envid = dict(envappservice[0])['envid']
        dao.add_wbxCassKeyspaceEnvServiceMap(keyspaceid, envid, serviceid)
        daoManager.commit()
        data = {}
        data['envid'] = envid
        res['data'] = data
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def updateWbxCassKeyspaceEnvServiceMap(keyspaceid, serviceid, envid):
    logger.info("editWbxCassKeyspaceEnvServiceMap,keyspaceid=%s,serviceid=%s,envid=%s")
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        dao.update_wbxCassKeyspaceEnvServiceMap(keyspaceid, envid,serviceid)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def deleteWbxCassKeyspaceEnvServiceMap(keyspaceid, serviceid, envid):
    logger.info("deleteWbxCassKeyspaceEnvServiceMap,keyspaceid=%s,serviceid=%s,envid=%s")
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        dao.delete_wbxCassEnvAppServiceMap(envid, serviceid)
        dao.delete_wbxCassKeyspaceEnvServiceMap(keyspaceid, envid, serviceid)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res


def addWbxCassUser(userid,username,userrole,password,casscluster):
    logger.info("addCassAppService")
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        if userid:
            dao.update_wbxCassUser(userid,username, userrole, password, casscluster)
        else:
            dao.add_wbxCassUser(username,userrole,password,casscluster)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def deleteWbxCassUser(userid):
    logger.info("deleteWbxCassUser,userid=%s" %(userid))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        userInfo = dao.get_wbxCassAppKeyspaceConnInfo_by_userid(userid)
        if userInfo:
            res['status'] = "FAILED"
            res['errormsg'] = "This casscluster user is not allowed to be deleted."
            return res
        else:
            dao.delete_wbxCassUser(userid)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res


def addwbxCassClusterInfo(clusterid,localdc,contactpoints,port,cassclustername):
    logger.info("addwbxCassClusterinfo")
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        if clusterid:
            dao.update_wbxCassClusterinfo(clusterid,localdc, contactpoints, port, cassclustername)
        else:
            dao.add_wbxCassClusterinfo(localdc,contactpoints,port,cassclustername)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def deletewbxCassClusterInfo(clusterid):
    logger.info("deletewbxCassClusterInfo,clusterid=%s"%(clusterid))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        wbxCassClusterInfo = dao.get_WbxCassClusterInfoByClusterid(clusterid)
        cassclustername = dict(wbxCassClusterInfo[0])['cassclustername']
        localdc = dict(wbxCassClusterInfo[0])['localdc']
        port = dict(wbxCassClusterInfo[0])['port']
        contactpoints = dict(wbxCassClusterInfo[0])['contactpoints']
        keyspacesInfo = dao.get_wbxCassAppKeyspaceConnInfo("",cassclustername, localdc)
        if keyspacesInfo:
            res['status'] = "FAILED"
            res['errormsg'] = "This casscluster info is not allowed to be deleted."
            return res
        dao.delete_wbxCassClusterinfo(clusterid)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def getCassUserByCasscluster(casscluster):
    logger.info("getCassuserByCasscluster, casscluster=%s" %(casscluster))
    res = {"status": "SUCCESS", "errormsg": "","data":None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = dao.get_cassUserByCasscluster(casscluster)
        daoManager.commit()
        cassUsers = [dict(vo) for vo in list]
        res['data'] = cassUsers
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def getCassLocaldcByCasscluster(casscluster):
    logger.info("getCassLocaldcByCasscluster, casscluster=%s" % (casscluster))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = dao.get_cassLocaldcByCasscluster(casscluster)
        daoManager.commit()
        cassUsers = [dict(vo) for vo in list]
        res['data'] = cassUsers
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def addWbxCassAppKeyspaceConnInfo(keyspaceid,keyspacename,localdc,contactpoints,port,userid,casscluster):
    logger.info("addWbxCassAppKeyspaceConnInfo, keyspaceid=%s,keyspacename=%s,localdc=%s,contactpoints=%s,port=%s,userid=%s,casscluster=%s" % (
    keyspaceid,keyspacename, localdc, contactpoints, port, userid, casscluster))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        if keyspaceid:
            dao.update_wbxCassAppKeyspaceConnInfo(keyspaceid,keyspacename,localdc,contactpoints,port,userid,casscluster)
        else:
            dao.add_wbxCassAppKeyspaceConnInfo(keyspacename,localdc,contactpoints,port,userid,casscluster)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def deleteWbxCassAppKeyspaceConnInfo(keyspaceid):
    logger.info(
        "deleteWbxCassAppKeyspaceConnInfo, keyspaceid=%s" % (keyspaceid))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        vo = dao.get_wbxCassAppKeyspaceConnInfo(keyspaceid,"","")
        if vo:
            serviceid = dict(vo[0])['serviceid']
            if serviceid:
                res['status'] = "FAILED"
                res['errormsg'] = "This WbxCassAppKeyspaceConnInfo is not allowed to be deleted."
                return res
            else:
                dao.delete_wbxCassAppKeyspaceConnInfo(keyspaceid)
                daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res


def getCassServiceName(servicename):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        casscluster_list = dao.get_wbxCassAppService(servicename)
        daoManager.commit()
        res['data'] = [dict(vo) for vo in casscluster_list]
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def getWbxCassAppService(servicename):
    logger.info("getWbxCassAppService, servicename=%s" %(servicename))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = dao.get_WbxCassAppService(servicename)
        daoManager.commit()
        res['data'] = [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def getWbxCassEnvAppServiceMap(serviceid):
    logger.info("getWbxCassEnvAppServiceMap,serviceid=%s"%(serviceid))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        service_info = dao.get_wbxCassAppServiceByServiceid(serviceid)
        servicename = dict(service_info[0])['servicename']
        servicelevel = dict(service_info[0])['servicelevel']
        res = getWbxCassAppServiceByCMDB(servicename, servicelevel)
        daoManager.commit()
        if res['status'] == 'SUCCESS':
            datalist = []
            for vo in res['data']:
                item = dict(vo)
                applocation = item['applocation']
                envtype = item['envtype']
                envinfo = dao.get_wbxCassEnvAppServiceMap(serviceid,applocation,envtype)
                if envinfo:
                    serviceid = dict(envinfo[0])['serviceid']
                    item['serviceid'] = serviceid
                    item['envid'] = dict(envinfo[0])['envid']
                else:
                    item['serviceid'] = None
                    item['envid'] = None
                datalist.append(item)
            res['data'] = datalist
        else:
            res['status'] = "FAILED"
            res['errormsg'] = res['errormsg']
            return res
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def getWbxCassEndpoints(envid):
    logger.info("getWbxCassEndpoints, envid=%s" % (envid))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = dao.get_wbxCassEndpoints(envid)
        daoManager.commit()
        res['data'] = [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def addWbxCassEnvAppServiceMap(envid,serviceid,envtype, applocation, webdomain):
    logger.info("addWbxCassEnvAppServiceMap, envid=%s,serviceid=%s,envtype=%s,applocation=%s,webdomain=%s" % (
    envid, serviceid, envtype, applocation, webdomain))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        if envid:
            dao.update_wbxCassEnvAppServiceMap(envid,serviceid,envtype, applocation, webdomain)
        else:
            dao.add_wbxCassEnvAppServiceMap(serviceid,envtype, applocation, webdomain)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

# def getGraphqlData():
#     url = "https://csgssot.webex.com/v1/graphql/etl/v1/graphql"
#     headers = {'content-type': 'application/json; charset=UTF-8', 'Authorization': "Bearer " + token}
#     r = requests.get(url=url, headers=headers)
#     return r

def get_access_token_for_CMDB():
    url = "https://idbroker.webex.com/idb/oauth2/v1/access_token?grant_type=client_credentials&scope=Identity:Organization"
    ClientId = "C70980c9c8ff13184b0b58343f9ba0405e3c8a2385e0ca0c473e612f47c694591"
    Secret = "78c47f568803681b05c1f8613a261a2a7a288488ec0e110c366558d9489b3e88"
    token = b64encode((ClientId + ':' + Secret).encode('utf-8')).decode('utf-8')
    headers = {'content-type': 'application/x-www-form-urlencoded', 'Authorization': "Basic " + token}
    response = requests.post(url=url, headers=headers)
    return response.json()

def getWbxCassAppServiceByCMDB(servicename,servicelevel):
    logger.info("getWbxCassAppServiceByCMDB,servicename=%s,servicelevel=%s" %(servicename,servicelevel))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    access_token_json = get_access_token_for_CMDB()
    if "access_token" not in access_token_json:
        res['status'] = "FAILED"
        res['errormsg'] = "get_access_token_for_CMDB error"
        return res
    else:
        access_token = access_token_json['access_token']
        url = "https://csgssot.webex.com/v1/graphql"
        headers = {'content-type': 'application/json', 'Authorization': "Bearer " + access_token}

        body='''
        {
          "query": "{ cmdb_ci_webex_pool(where:{u_component  : \\"%s\\"},sort: [\\"u_data_center\\"])  @global { u_component u_cluster u_data_center u_environment u_short_name} }"
        }
        ''' %(servicename)
        print(body)
        response = requests.post(url=url, headers=headers, data=body)
        cmdb_ci_webex_pool_list = response.json()
        data_list = []
        for vo in cmdb_ci_webex_pool_list['data']['cmdb_ci_webex_pool']:
            u_environment = vo['u_environment']
            if u_environment in ['Production','BTS']:
                data = {}
                applocation = vo['u_data_center']
                webdomain = vo['u_cluster']
                u_short_name = vo['u_short_name']
                if webdomain:
                    if len(webdomain) == 1:
                        webdomain = "d"+webdomain+"wd"
                    else:
                        webdomain = webdomain+"wd"
                if "domain" not in str(servicelevel).lower():
                    webdomain = ""
                envtype = u_environment
                if "Production" == u_environment:
                    envtype = "PROD"
                if "domain" in str(servicelevel).lower() and u_short_name:
                    envtype = "%s.%s" % (envtype, str(u_short_name).upper())

                data['servicename'] = servicename
                data["envtype"] = envtype
                data["applocation"] = applocation
                data["webdomain"] = webdomain
                if data not in data_list:
                    data_list.append(data)
                else:
                    logger.info("skip:%s" %(vo))
                res['data'] = data_list
        return res