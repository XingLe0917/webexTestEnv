import logging
from base64 import b64encode

import requests

from common.envConfig import mapping
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys


logger = logging.getLogger("DBAMONITOR")

def getTokenByCode(code,env):
    config=mapping[env]()
    # headers = {'content-type': 'application/x-www-form-urlencoded',
    #            "Authorization": "Basic Q2M1OTkyZGI4MmUxZjU2MDBmYjIyMWYwZmY5ZTRlZjk0ZDVhNTAzOGNhNTRhYzczZDk2YWIxODFhOWQ2NmI0MmM6OTc2MTEzNzUyNTA3MTQzMjEwYjRiYWZiNGU2YzBhMjU4MTMxNmFhNGIyMjYzMTg1NjlmNTY4NzA2NTdmMjk0Ng=="}
    # url = "https://idbroker.webex.com/idb/oauth2/v1/access_token?grant_type=authorization_code&redirect_uri=http%3A%2F%2Fsjgrcabt102.webex.com%3A9000%2FloginRedirection&code="+code
    headers = {'content-type': 'application/x-www-form-urlencoded',
               "Authorization": config.authorization}
    url = "https://idbroker.webex.com/idb/oauth2/v1/access_token?grant_type=authorization_code&redirect_uri="+config.redirect_uri+"&code=" + code
    # print(config.authorization)
    # print(url)
    response = requests.post(url=url,headers=headers)
    return response.json()

def getUserByToken(token):
    url = "https://identity.webex.com/identity/scim/v1/Users/me"
    headers = {'content-type': 'application/json; charset=UTF-8','Authorization':"Bearer "+token}
    r = requests.get(url=url, headers=headers)
    return r

def checkUserInPCCP(cec):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        daoManager.startTransaction()
        rows = depotdbDao.getCCPUserRole(cec)
        daoManager.commit()
        if len(rows)>0:
            return True
        else:
            return False
    except Exception as e:
        daoManager.rollback()
        raise e
    finally:
        daoManager.close()

def getTokeninfo(access_token):
    config = mapping["prod"]()
    headers = {'content-type': 'application/x-www-form-urlencoded',
               "Authorization": config.token_info_authorization}
    url = "https://idbroker.webex.com/idb/oauth2/v1/tokeninfo"
    body = "access_token=" + str(access_token)
    response = requests.post(url=url, headers=headers,data=body)
    return response.json()

def getOtherUser(username):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        daoManager.startTransaction()
        user = depotdbDao.get_OtherUser(username)
        daoManager.commit()
        if len(user)>0:
            return dict(user[0])
        else:
            return None
    except Exception as e:
        daoManager.rollback()
        raise e
    finally:
        daoManager.close()

def getOAuthTokenForMA():
    logger.info("getOAuthTokenForMA")
    orgid = "6078fba4-49d9-4291-9f7b-80116aab6974"
    url = "https://idbroker.webex.com/idb/token/%s/v2/actions/GetBearerToken/invoke" %(orgid)
    headers = {'content-type': 'application/json; charset=UTF-8'}
    data = {
        "name":"pg_pccp_ua",
        "password":"EVKO.bfik.49.SJZL.fykp.13.BUYO.chmn.1267"
    }

    response = requests.post(url=url,headers=headers,json=data)
    return response.json()

def getAccessTokenForMA():
    logger.info("getAccessTokenForMA")
    config = mapping["prod"]()
    res = getOAuthTokenForMA()
    if "BearerToken" in res:
        bearerToken = res['BearerToken']
        headers = {'content-type': 'application/x-www-form-urlencoded',
                   "Authorization": config.authorization}
        url = "https://idbroker.webex.com/idb/oauth2/v1/access_token?grant_type=urn:ietf:params:oauth:grant-type:saml2-bearer&assertion=" + bearerToken + "&scope=identity:myprofile_read"
        response = requests.post(url=url,headers=headers)
        return response.json()['access_token']
    else:
        logger.error("Error get BearerToken")

def getAuthorizationForMA():
    res = {"status": "SUCCESS", "errormsg": "", "authorization": None}
    logger.info("getAuthorizationForMA")
    username = "pg_pccp_ua"
    access_token = getAccessTokenForMA()
    authorization = "Basic " + b64encode((username + ':' + access_token).encode('utf-8')).decode('utf-8')
    res['authorization'] = authorization
    return res

if __name__ == "__main__":
    # env="prod"
    # config = mapping[env]()
    # print(config.client_id)
    # print(config.redirect_uri)
    # print(config.authorization)

    # scopes = ["identity:myprofile_read", "sms_identity|sms_api", "other_identity|other_api", "front_identity|front_api"]
    # a = ["identity:myprofile_read"]
    # if a[0] in scopes:
    #     print("yes")
    # else:
    #     print("no")
    # access_token="MGE2YjQxMDEtZDlmOC00ZmM2LWExNmEtNTQ3NjA5Y2Y5NTQ0Y2NjMzI5MGMtZDU0_PF84"
    # a=getTokeninfo(access_token)
    # print(a)

    # code ="NmIwMTM3ODUtZWZlYy00MGYyLWE0N2EtZTcwYWU2Y2I5Nzg5NjlkMDU3NmYtYzg0_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"
    # env ="china"
    # resulst= getTokenByCode(code,env)
    # print(resulst)

    access_token="YzY4OGUyMTYtZDFkNy00YmU2LThlNGEtN2I5NjJmN2RkN2RmNzMwODU1MTAtOTAy_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"
    env="prod"
    a = getTokeninfo(access_token,env)
    print(a)
