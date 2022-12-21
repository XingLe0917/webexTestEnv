import logging
from base64 import b64encode
from flask import Flask, request, Response, redirect, current_app
from flasgger import Swagger
from common.envConfig import mapping
from flask_session import Session
from biz.UserUtil import checkUserInPCCP, getOtherUser, getTokeninfo
from views import main

logger = logging.getLogger("DBAMONITOR")

scopes=["identity:myprofile_read","sms_identity|sms_api", "other_identity|other_api", "front_identity|front_api"]
whitelist=[
    "/api/kafka_monitor_alert",
    "/api/sentAlertToBot", 
    "/api/get_jira_issue_list",
    "/api/add_wbxmonitoralertdetail",
    "/api/getAuthorizationForMA"
]
compatibilitylist= ["/api/get_db_metadata"]


def myredirect():
    if request.path.startswith('/api') and request.path not in whitelist:
        auth = request.authorization
        if auth:
            if request.path in compatibilitylist:
                logger.debug("path in compatibility, path={0}" .format(request.path))
                logger.debug(auth)
                tok = b64encode((auth.username + ':' + auth.password).encode('utf-8')).decode('utf-8')
                if tok == "YXBwOlczNkpNJUYwdlR1WQ==":
                    logger.info("Try to call old check_auth (fixed token), path=%s" %(request.path))
                    if not check_auth(auth.username, auth.password, request.path):
                        logger.info("no auth !!! path={0}".format(request.path))
                        return not_authenticated()
                else:
                    logger.info("call use CI authorization")
                    flag = check_auth_2(auth.username, auth.password, request.path)
                    if not flag:
                        logger.info("check_auth_2 fail !!! auth={0},path={1}".format(auth, request.path))
                        return not_authenticated()
            else:
                # api authentication with CI
                try:
                    tok = b64encode((auth.username + ':' + auth.password).encode('utf-8')).decode('utf-8')
                    if tok == "Y2NwX3Rlc3Q6Tjd3amgyJVlP":
                        logger.info("call api from chatbot, path=%s" %(request.path))
                    else:
                        flag = check_auth_2(auth.username, auth.password, request.path)
                        if not flag:
                            # logger.info("Try to call old check_auth (check_auth_2 failure), username=%s, path=%s" %(auth.username,request.path))
                            # pccp_user_flag = check_auth(auth.username, auth.password, request.path)
                            # if not pccp_user_flag:
                            #     logger.info("check_auth fail !!! auth={0},path={1}".format(auth, request.path))
                            #     return not_authenticated()
                            # logger.info("Try to call old check_auth (check_auth_2 failure), username=%s, path=%s" %(auth.username,request.path))
                            # return not_authenticated()
                            env = current_app.config.get("ENV")
                            config = mapping[env]()
                            PCCP_ERROR_URL = config.PCCP_ERROR_URL
                            return redirect(PCCP_ERROR_URL)

                except Exception as e:
                    logger.error("old check_auth_2 error,auth={0}".format(auth))

        else:
            logger.error("no authorization")
            return not_authenticated()

def check_auth(username,pwd,path):

    """This function is called to check if a username /
       password combination is valid.
       """
    logger.info("call(checkUserInPCCP) old check_auth, username={0}, path={1}" .format(username,path))
    if "app" == username and pwd == "W36JM%F0vTuY":
        logger.info("call from %s,continue " %(username))
        return True
    # resDict = getTokeninfo(pwd)
    # logger.info("resDict={0}".format(resDict))
    # if 'user_type' in resDict and resDict['user_type'] in ['machine', 'service']:
    #     logger.info("user_type=%s" % (resDict['user_type']))
    #     return True
    flag = checkUserInPCCP(username)
    if flag:
        return True
    else:
        user = getOtherUser(username)
        if user:
            password = user['password']
            if pwd == password:
                return True
    return False

def check_auth_2(username,password,path):
    logger.info("check_auth_2, username={0}, path={1}".format(username,path))
    resDict = getTokeninfo(password)
    logger.info("resDict={0}" .format(resDict))
    if 'scope' in resDict:
        scope=resDict['scope'][0]
        if scope in scopes:
            if 'user_type' in resDict and resDict['user_type'] in ['machine','service']:
                logger.info("user_type=%s" %(resDict['user_type']))
                return True
            if 'user_id' in resDict:
                user_id = resDict['user_id']
                logger.info("user_id={0}, scope={1}, path={2}".format(user_id, scope, path))
                user_name = str(user_id).split("@")[0]
                logger.info("call(checkUserInPCCP), user_name=%s" %(username))
                flag = checkUserInPCCP(user_name)
                if flag:
                    return True
                else:
                    logger.info("{0} is not PCCP user, please contact DBA team." .format(user_name))
                    return False
            else:
                logger.info("call api from other")
                return True
        else:
            return False
    else:
        logger.info("no scope.")
        return False

def not_authenticated():
    return Response(
        "Could not verify your access level for that URL.\nYou have to login with proper credentials", 
        401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'}
    )


def create_app():
    app = Flask(__name__)
    app.secret_key = 'my unobvious secret key'

    SESSION_TYPE = 'filesystem'
    app.config.from_pyfile("config.cfg")
    # app.before_request(myredirect)
    app.register_blueprint(main)
    swagger = Swagger(app)
    Session(app)

    return app

