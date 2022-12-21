import datetime
import uuid
from base64 import b64encode

from flask import redirect, request, url_for, session, Blueprint, jsonify, current_app

from biz.Adbmon import getadbmonlist, adbmondetail, getadbmonlistAlert, getAdbmonlistByDCName, \
    adbmon_check_new
from biz.AlterUserPassword import generaterPwd, updateUserPassword
from biz.Archivelog import getArchivelog
from biz.Bot import sentAlertToBot
from biz.CallProcedure import exec_job, commit_procedure, selectProcedure, get_procedureList, procedure_getjoblog
from biz.CheckServer import CheckServer, OperateServer
# from biz.DBBaseline import generateDBBaseline
from biz.PGAlertRule import setpgalertrule, deletepgalertrule, getpgalertrule, getpgalertruletypes
from biz.cassandraManagement import getWbxCassClusterName, getWbxCassClusterInfo, addCassAppService, addWbxCassUser, \
    deleteWbxCassUser, addwbxCassClusterInfo, deletewbxCassClusterInfo, getCassUserByCasscluster, \
    getCassLocaldcByCasscluster, addWbxCassAppKeyspaceConnInfo, getCassServiceName, getWbxCassAppService, \
    getWbxCassEnvAppServiceMap, getWbxCassEndpoints, addWbxCassKeyspaceEnvServiceMap, addWbxCassEnvAppServiceMap, \
    getWbxCassKeyspaceid, deleteWbxCassAppKeyspaceConnInfo, getWbxCasscluster, getWbxCassAppServiceByCMDB, \
    updateWbxCassKeyspaceEnvServiceMap, deleteWbxCassKeyspaceEnvServiceMap
from biz.getSplexParams import getSplexParams,addMonitorSplexParam,checkSplexParams,getSplexParamsServerHostname
from biz.DBLinkMonitor import getDBlinkmonitordetail, checkDBlink
from biz.DBSchemaDiff import getDiffFileNameByDate, dbSchemaDiff
from biz.DepotdbManagement import get_depot_manage_info, get_depot_manage_user_info, \
    get_depot_manage_splexplex_info, depot_manage_reload, depot_manage_add_rac, depot_manage_add_DB, \
    depot_manage_add_user, depot_manage_reload_shareplex, get_depot_manage_instance, get_depot_manage_pool_info, \
    add_depot_manage_pool_info, get_DB_connection_endpoint, get_pgdb_info
from biz.KafkaMonitor import kafka_monitor, update_kafka_alert, \
    get_kafka_alert_threshold, delete_alert_threshold, getCheckCronStatus, checkOneCronStatus
from biz.Report import getOneReportHtml,getReport
from biz.SelfHealing import add_wbxmonitoralertdetail, get_wbxmonitoralert, get_wbxmonitoralertdetail, get_wbxautotask, \
    get_wbxmonitoralert_type
from biz.TaskLog import addTaskLog, get_db_tns, get_SQLResultByDB

from biz.UndoTablespace import getUndoTablespace, getundoTablespaceByDBName
from biz.UserUtil import getTokenByCode, getUserByToken, checkUserInPCCP, getOtherUser, getTokeninfo, \
    getAuthorizationForMA
# from biz.getmetadata import get_metadata
from common.envConfig import mapping
from dao.vo.alertvo import AlertVo
import json
import logging

from common.wbxutil import wbxutil

from biz.DBPatchJob import listshareplexmonitorresult, listshareplexcrdeployment, \
    listMeetingDataMonitorDetail
from biz.ShareplexUpgrade import precheckShareplex, upgradeShareplex
from biz.CronjobManagement import listJobTemplate, addJobTemplate, deleteJobTemplate, listJobManagerInstance, \
    shutdownJobmanagerInstance, updateJobInstance
from biz.CronjobManagement import startJobmanagerInstance, deleteJobmanagerInstance, listJobInstance, deleteJobInstance, \
    addJobInstance, pauseJobInstance, resumeJobInstance
from biz.DepotDBResource import addcronjoblog, getDBNameList, loadDepotDBInfo
from biz.PermissionManagement import get_user_role_dict_from_depot, assign_role_to_user_to_depot, \
    delete_user_from_role_to_depot, get_role_page_dict_from_depot, add_role_to_page_to_depot, \
    change_role_to_page_to_depot,identify_user_access_from_depot
from biz.ESDataReporter import get_osw_data_from_es, getDBListForDBHealth
from biz.DBAuditJob import getConfigDBShareplexDelay,getShareplexDelaySRCDBData, getShareplexDelay, generateAWRReport, listDBInstanceName, generateASHReport, getMeetingDataReplicationDelay
from biz.autotask.wbxautotaskmanager import biz_autotask_listJobsByTaskid, biz_autotask_listtasks, \
    biz_autotask_initialize, biz_autotask_executejob, biz_autotask_getjoblog, biz_autotask_tahoebuild, \
    biz_autotask_preexecutejob, biz_autotask_exeoneclick, getSelfHealingJobList,biz_autotask_updjobstatus,get_DBMemoSize
from biz.PermissionManagement import check_login_user_from_depot, get_role_list_from_depot, get_access_dir_from_depot, \
    get_favourit_page_by_username, add_favourite_page, delete_favourite_page, get_health

# from biz.ScriptDistribution import compare_distributing_script_preview, distribute_script_by_jenkins, \
#     get_distribute_log_from_jenkins

from biz.StorageManagement import add_nfs_client, mount_nfs, create_nfs_volume, resize_nfs_volume
from biz.Shareplexinfluxdbmonitor import get_splex_performance_monitor, get_detailed_metric_monitor
from biz.ggperformancemonitor import get_gg_comparision_performance_monitor, get_splex_comparision_performance_monitor
from biz.Shareplexencryption import precheck_shareplex_encryption, excute_shareplex_encryption
from biz.meetingDBUsed import get_meetingDB_baseInfo, get_meetingDB_dbName_by_hostName, get_meetingDB_tableList
from biz.ShareplexReplicationTableList import get_channel_tablelist, get_replication_tablelist
from biz.telegrafmonitor import get_telegraf_monitor_list, telegraf_action
from biz.getserveroraclepwd import get_db_metadata
from biz.waiteventmonitor import listwaitevent
from biz.jobsMonitor import listjobs,jobmonitor
from biz.wbxmydbpurgelog import wbxmydbpurgelog,getmydbdellog
from biz.getmetadata import get_metadata, get_all_dc_name, get_all_server_info
from biz.failedJob import get_failed_job_tablelist,execute_failed_job
from biz.Tahoedbfailover import stop_ha_service_and_kill_gsb_connection, check_tahoe_db_status, kill_primary_application
# from biz.showdbconfigmonitor import show_db_config_monitor
from biz.ShareplexPortInstall import get_shareplex_port_install_history_list, get_shareplex_port_install_detail, preverify_shareplex_port_install, setup_shareplex_port

# from biz.wbxdbcutover import listCutoveredDBs, dbcutover_preverify, generateCutoverStep, listDBCutoverStep, saveCutoverStep, executeCutoverStep
from biz.notifyChannel import get_notify_tablelist,add_notify_channel,update_notify_channel
from biz.metricSeeting import get_metric_tablelist,add_metric_setting, update_metric_setting
#from biz.DBParameter import get_db_parameter_list,get_parameter_in_db_list,get_failed_parameter_list
from biz.getjiraissuelist import get_jira_issue_list
from biz.getdroppartitionstatus import get_drop_partition_new_status, get_db_drop_partition_detail_status
from biz.dblicences import getdblicences,getdbliclabelinfo
from biz.getserveroraclepwd import getpoolinfobydbname
# from biz.getosanddbinfo import get_os_and_db_info_by_host_name, get_os_and_db_info_by_db_name
from biz.RmanBackupStatus import get_rman_backup_status_list,check_rman_backup_status
from biz.splexDenyUser import get_splex_deny_user_list
from biz.Teodbfailoverfailback import failover, failback, check_teodb_status, get_teodb_failover_list, get_teodb_failover_detail
from biz.ShareplexCRMonitor import checkCRConfigStatus,getCRConfigStatus,getCRLogCount,getSingleCRconfigStatus,fixFailedCR,getCRLogCountHistory
from biz.GatherStatsMonitor import getGatherStatus
from biz.SqlExecutionPlanMonitor import getSqlExecutionPlan,getSqlExecutionPlanDetail
from biz.Homepage import get_homepage_db_version_count, get_homepage_db_count, get_homepage_db_type_count, get_shareplex_count, get_rencent_alert_info, get_top_active_session_db_count
from biz.wbxdbaudit import getwbxdbauditparams
from biz.Handover import list_oncall_handover, send_oncall_handoff_mail, add_oncall_handoff, get_oncall_cec
from biz.Tahoedbfailoverfailback import tahoe_db_failover_first, tahoe_db_failover_second, tahoe_failback, \
    check_tahoe_pool_status, get_tahoedb_name_by_pool
from biz.ora2pgmigration import ora2pgmigration
from common.wbxchatbot import wbxchatbot

route = Blueprint("origin", __name__)

logger = logging.getLogger("DBAMONITOR")


@route.route("/api/getAuthorizationForMA", methods=['POST'])
def authorizationForMA():
    if request.method == 'POST':
        return jsonify(getAuthorizationForMA())

# Currently not being used
@route.route('/login', methods=['GET', 'POST'])
def login():
    # newusername = None
    if request.method == 'POST':
        # session['username'] = request.form['username']
        # passwd = request.form['password']
        # newusername = session['username'] + ' you are great'

        username = request.form.get("username")
        next_url = request.form.get("next")
        session["username"] = username
        session["next_url"] = next_url

        res = session.get('redirect')

        redirectUrl = 'https://idbroker.webex.com/idb/oauth2/v1/authorize?response_type=code&client_id=C2940cd3dd7f2b74dc4f5fec11e7734263a38f03c4dae9be060704e09ae19c79c&redirect_uri=http%3A%2F%2Ftagrcabt101.webex.com%3A9000%2FloginRedirection&scope=identity%3Amyprofile_read&realm=1eb65fdf-9643-417f-9974-ad72cae0e10f&state=this-should-be-a-random-string-for-security-purpose'
        return redirect(url_for(redirectUrl))
    # return render_template('login.html')


@route.route("/loginRedirection", methods=['GET', 'POST'])
def loginRedirection():
    logger.info("loginRedirection")
    logger.info(request.args)
    code = request.args.get("code")
    env = current_app.config.get("ENV")
    logger.info(env)
    config = mapping[env]()
    PCCP_INDEX_URL=config.PCCP_INDEX_URL
    PCCP_ERROR_URL=config.PCCP_ERROR_URL
    if code:
        response = getTokenByCode(code,env)
        logger.info(response)
        access_token = response['access_token']
        r = getUserByToken(access_token)
        if r.status_code == 200:
            user = r.json()
            userName = user['userName']
            logger.info(userName)
            cec = str(userName).split("@")[0]
            fullName = str(user['name']['givenName']) +" "+str(user['name']['familyName'])
            logger.info(fullName)
            logger.info("cec=%s" % (cec))
            flag = checkUserInPCCP(cec)
            if flag:
                logger.info("check userName={0} is OK." .format(userName))
                # ccp frontend call api
                # ccpuser = getOtherUser('ccp')
                # logger.info(ccpuser)
                # token = 'Basic ' + b64encode((cec + ':' + ccpuser['password']).encode('utf-8')).decode('utf-8')
                # return redirect(PCCP_INDEX_URL+"?user="+cec+"&token="+token+"&fullName="+fullName)

                # api authentication with CI
                token = 'Basic ' + b64encode((cec + ':' + access_token).encode('utf-8')).decode('utf-8')
                logger.info("Authorization:{0}" .format(token))
                redirect_url = PCCP_INDEX_URL + "?user=" + cec + "&token=" + token + "&fullName=" + fullName
                return redirect(redirect_url)

            else:
                message = "The userName={0} not in PCCP user role" .format(userName)
                logger.info(message)
                return redirect(PCCP_ERROR_URL+"?user="+cec)
        else:
            logger.error("Error: get user by access_token")
            return redirect(PCCP_ERROR_URL)
    else:
        logger.error("Do not get code.")
        return redirect(PCCP_ERROR_URL)

@route.route('/api/refreshmetadata', methods=['GET', 'POST'])
def refreshmetadata():
    loadDepotDBInfo()
    return {"result": "SUCCESS", "msg": None}

# @route.route('/oswatcher', methods=['GET', 'POST'])
# def oswatcher():
#     global path
#     return render_template('oswatcher.html',tree=make_tree(path))

@route.route('/api/meetingdatamonitor', methods=['GET', 'POST'])
def meetingdatamonitor():
    # dictres = wbxutil.converttodict(listMeetingDataMonitorDetail())
    meetinglist = listMeetingDataMonitorDetail()
    meetingdictlist = [meetingvo.to_dict() for meetingvo in meetinglist]
    response = jsonify(meetingdictlist)
    return response


@route.route("/api/shareplexmonitor", methods=['GET', 'POST'])
def shareplexmonitor():
    sumlist = listshareplexmonitorresult()
    logger.info(sumlist)
    return jsonify(sumlist)


@route.route("/api/shareplexcrdeployment", methods=['GET', 'POST'])
def shareplexcrdeployment():
    (crlist, crdatadict) = listshareplexcrdeployment()
    return jsonify(wbxutil.converttodict(crlist))


# response = make_response(render_template("shareplexcrdeployment.html", crlist=crlist, crdatadict=crdatadict))
# return response

@route.route("/api/shareplexupgrade_precheck", methods=['POST'])
def shareplexupgrade_precheck():
    if request.method == 'POST':
        # p = request.args.get('p')
        host_name = request.json["host_name"].strip()
        splex_port = request.json["splex_port"].strip()
        return json.dumps(precheckShareplex(host_name, splex_port, '8.6.3'))


@route.route("/api/shareplexupgrade_upgrade", methods=['POST'])
def shareplexpugrade_upgrade():
    if request.method == 'POST':
        host_name = request.json["host_name"].strip()
        splex_port = request.json["splex_port"].strip()
        return json.dumps(upgradeShareplex(host_name, splex_port, '8.6.3', '9.2.1'))


@route.route('/api/listjobtemplate', methods=['POST'])
def view_listJobTemplate():
    job_name = request.json["jobname"]
    templatedict = listJobTemplate(job_name)
    return jsonify(templatedict)


@route.route('/api/addjobtemplate', methods=['POST'])
def view_addJobTemplate():
    try:
        templatedata = request.json["data"]
        addJobTemplate(templatedata)
        return {"result": "SUCCESS", "msg": None}
    except Exception as e:
        return {"result": "FAILED", "msg": str(e)}


@route.route('/api/deletejobtemplate', methods=['POST'])
def view_deleteJobTemplate():
    try:
        templateid = request.json["templateid"]
        deleteJobTemplate(templateid)
        return {"result": "SUCCESS", "msg": None}
    except Exception as e:
        return {"result": "FAILED", "msg": str(e)}


@route.route('/api/listjobmanagerinstance', methods=['POST'])
def view_listJobManagerInstance():
    host_name = request.json["host_name"]
    jobManagerList = listJobManagerInstance(host_name)
    return jsonify(jobManagerList)


@route.route('/api/shutdownjobmanagerinstance', methods=['POST'])
def view_shutdownJobmanagerInstance():
    try:
        host_name = request.json["host_name"]
        shutdownJobmanagerInstance(host_name)
        return {"result": "SUCCESS", "msg": None}
    except Exception as e:
        return {"result": "FAILED", "msg": str(e)}


@route.route('/api/startjobmanagerinstance', methods=['POST'])
def view_startJobmanagerInstance():
    try:
        host_name = request.json["host_name"]
        startJobmanagerInstance(host_name)
        return {"result": "SUCCESS", "msg": None}
    except Exception as e:
        return {"result": "FAILED", "msg": str(e)}


@route.route('/api/deletejobmanagerinstance', methods=['POST'])
def view_deleteJobmanagerInstance():
    try:
        host_name = request.json["host_name"]
        deleteJobmanagerInstance(host_name)
        return {"result": "SUCCESS", "msg": None}
    except Exception as e:
        return {"result": "FAILED", "msg": str(e)}


@route.route('/api/listjobinstance', methods=['POST'])
def view_listJobInstance():
    host_name = request.json["host_name"]
    jobInstanceList = listJobInstance(host_name)
    return jsonify(jobInstanceList)


@route.route('/api/deletejobinstance', methods=['POST'])
def view_deleteJobInstance():
    try:
        jobid = request.json["jobid"]
        deleteJobInstance(jobid)
        return {"result": "SUCCESS", "msg": None}
    except Exception as e:
        return {"result": "FAILED", "msg": str(e)}


@route.route('/api/addjobinstance', methods=['POST'])
def view_addJobInstance():
    try:
        jsondata = request.json["data"]
        addJobInstance(jsondata)
        return {"result": "SUCCESS", "msg": None}
    except Exception as e:
        return {"result": "FAILED", "msg": str(e)}


@route.route('/api/updatejobinstance', methods=['POST'])
def view_updateJobInstance():
    try:
        jsondata = request.json["data"]
        updateJobInstance(jsondata)
        return {"result": "SUCCESS", "msg": None}
    except Exception as e:
        return {"result": "FAILED", "msg": str(e)}


@route.route('/api/pausejobinstance', methods=['POST'])
def view_pauseJobInstance():
    try:
        jobid = request.json["jobid"]
        pauseJobInstance(jobid)
        return {"result": "SUCCESS", "msg": None}
    except Exception as e:
        return {"result": "FAILED", "msg": str(e)}


@route.route('/api/resumejobinstance', methods=['POST'])
def view_resumeJobInstance():
    try:
        jobid = request.json["jobid"]
        resumeJobInstance(jobid)
        return {"result": "SUCCESS", "msg": None}
    except Exception as e:
        return {"result": "FAILED", "msg": str(e)}


@route.route('/api/addcronjoblog', methods=['POST'])
def view_addcronjoblog():
    try:
        jsondata = request.get_json()
        logger.info(type(jsondata))
        addcronjoblog(jsondata)
        return {"result": "OKOKOK"}
    except Exception as e:
        logger.error("error occurred", exc_info=e, stack_info=True)
        return {"result": "FAILED", "msg": str(e)}


@route.route("/api/db_schema_diff", methods=['POST'])
def db_schema_diff():
    if request.method == 'POST':
        diff_date = request.json["diff_date"].strip()
        return json.dumps(getDiffFileNameByDate(diff_date))


@route.route("/api/generaterPwd", methods=['POST'])
def alter_user_password():
    if request.method == 'POST':
        trim_host = request.json["trim_host"].strip()
        db_name = request.json["db_name"].strip()
        user_name = request.json["user_name"].strip()
        return json.dumps(generaterPwd(trim_host, db_name, user_name))


@route.route("/api/alterUserPassword", methods=['POST'])
def alterUserPassword():
    if request.method == 'POST':
        trim_host = request.json["trim_host"].strip()
        db_name = request.json["db_name"].strip()
        user_name = request.json["user_name"].strip()
        password = request.json["password"].strip()
        return json.dumps(updateUserPassword(password, trim_host, db_name, user_name))


@route.route("/api/add_task_log", methods=['POST'])
def add_task_log():
    if request.method == 'POST':
        job = request.json["job"].strip()
        host_name = request.json["host_name"].strip()
        port = request.json["port"].strip()
        parameters = request.json["parameters"].strip()
        status = request.json["status"].strip()
        return json.dumps(addTaskLog(job, host_name, port, parameters, status))


@route.route("/api/db_schema_diff_detail", methods=['POST'])
def db_schema_diff_detail():
    if request.method == 'POST':
        target_db = request.json["target_db"].strip()
        diff_date = request.json["diff_date"].strip()
        return json.dumps(dbSchemaDiff(target_db, diff_date))


@route.route("/api/kafka_monitor_alert", methods=['POST'])
def view_kafka_monitor():
    if request.method == 'POST':
        top = request.json["top"]
        return jsonify(kafka_monitor(top))


@route.route("/api/get_osw_data/<metric_type>", methods=['POST'])
def get_osw_iostat_util(metric_type):
    if request.method == 'POST':
        start_time = request.json["startTime"].strip()
        end_time = request.json["endTime"].strip()
        host_name = request.json["hostName"].strip()
        db_name = request.json["dbName"].strip()
        return json.dumps(get_osw_data_from_es(metric_type, host_name, db_name, start_time, end_time))

@route.route("/api/get_osw_data/dblist", methods=['POST'])
def getDatabaseListForDBHealth():
    dbDict = getDBListForDBHealth()
    returnval = json.dumps(dbDict)
    return returnval

@route.route("/api/get_user_role_dict", methods=['GET', 'POST'])
def get_user_role_dict():
    return json.dumps(get_user_role_dict_from_depot())


@route.route("/api/get_role_page_dict", methods=['POST'])
def get_role_page_dict():
    if request.method == 'POST':
        url_list = request.json["urlArray"]
    return json.dumps(get_role_page_dict_from_depot(url_list))


@route.route("/api/identify_user_access", methods=['POST'])
def identify_user_access():
    if request.method == 'POST':
        username = request.json["username"].strip()
    return json.dumps(identify_user_access_from_depot(username))


@route.route("/api/change_role_to_page", methods=['POST'])
def change_role_to_page():
    if request.method == 'POST':
        route_list = request.json["routeArray"]
    return json.dumps(change_role_to_page_to_depot(route_list))


@route.route("/api/add_role_to_page", methods=['POST'])
def add_role_to_page():
    if request.method == 'POST':
        role_name = request.json["roleName"].strip()
    return json.dumps(add_role_to_page_to_depot(role_name))


@route.route("/api/assign_role_to_user", methods=['POST'])
def assign_role_to_user():
    if request.method == 'POST':
        username = request.json["userName"].strip()
        role_name = request.json["roleName"].strip()
        return json.dumps(assign_role_to_user_to_depot(username, role_name))


@route.route("/api/delete_user_from_role", methods=['POST'])
def delete_user_from_role():
    if request.method == 'POST':
        username = request.json["userName"].strip()
        role_name = request.json["roleName"].strip()
        return json.dumps(delete_user_from_role_to_depot(username, role_name))


@route.route("/api/get_kafka_alert_threshold", methods=['POST'])
def kafka_alert_threshold():
    if request.method == 'POST':
        metric_type = request.json["metric_type"]
        id = request.json["id"]
        return jsonify(get_kafka_alert_threshold(metric_type, id))


@route.route("/api/delete_alert_threshold", methods=['POST'])
def delete_kafka_alert_threshold():
    if request.method == 'POST':
        id = request.json["id"]
        return jsonify(delete_alert_threshold(id))


@route.route("/api/add_update_kafka_alert", methods=['POST'])
def add_kafka_alert_metric():
    if request.method == 'POST':
        alertVo = AlertVo()
        alertVo.metric_type = request.json["metric_type"]
        alertVo.metric_name = request.json["metric_name"]
        alertVo.metric_operator = request.json["metric_operator"]
        alertVo.threshold_value = request.json["threshold_value"]
        alertVo.severity = request.json["severity"]
        alertVo.db_host = request.json["db_host"]
        alertVo.db_name = request.json["db_name"]
        alertVo.shareplex_port = request.json["shareplex_port"]
        alertVo.threshold_times = request.json["threshold_times"]
        opt = ''
        id = request.json["id"]
        if id == '':
            opt = 'add'
            alertVo.id = str(uuid.uuid1())
        else:
            opt = 'update'
            alertVo.id = str(id)
        return json.dumps(update_kafka_alert(alertVo, opt))


@route.route("/api/getShareplexReplicationTime", methods=['POST'])
def getShareplexReplicationTime():
    if request.method == 'POST':
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]
        resList = getConfigDBShareplexDelay(start_time, end_time)
        return json.dumps(resList)

@route.route("/api/getShareplexDelaySRCDB", methods=['POST','GET'])
def getShareplexDelaySRCDB():
    resList = getShareplexDelaySRCDBData()
    return jsonify(resList)

@route.route("/api/getShareplexDelay", methods=['POST'])
def getShareplexDelayData():
    if request.method == 'POST':
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]
        src_db_name = request.json["src_db_name"]
        resList = getShareplexDelay(start_time, end_time, src_db_name)
        return jsonify(resList)

@route.route("/api/getShareplexMonitorData", methods=['POST'])
def view_getShareplexMonitorData():
    if request.method == 'POST':
        cluster_name = request.json["cluster_name"]
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]
        resList = getMeetingDataReplicationDelay(cluster_name, start_time, end_time)
        return jsonify(resList)

# @route.route("/api/listCutoveredDBs", methods=['GET', 'POST'])
# def list_Cutovered_DBs():
#     return jsonify(listCutoveredDBs())


# @route.route("/api/dbcutover_preverify", methods=['POST'])
# def db_cutover_preverify():
#     if request.method == 'POST':
#         dbName = request.json["dbName"].strip()
#         newHostName = request.json["newHostName"].strip()
#         oldHostName = request.json["oldHostName"].strip()
#         dbSplexSid = request.json["dbSplexSid"].strip()
#         return json.dumps(dbcutover_preverify(oldHostName, newHostName, dbName, dbSplexSid))
#
#
# @route.route("/api/generateCutoverStep", methods=['POST'])
# def view_generateCutoverStep():
#     if request.method == 'POST':
#         cutoverid = request.json["cutoverid"].strip()
#         return json.dumps(generateCutoverStep(cutoverid))
#
#
# @route.route("/api/saveCutoverStep", methods=['POST'])
# def view_saveCutoverStep():
#     if request.method == 'POST':
#         cutoverid = request.json["cutoverid"].strip()
#         return json.dumps(saveCutoverStep(cutoverid))
#
#
# @route.route("/api/listDBCutoverStep", methods=['POST'])
# def list_DBCutover_step():
#     if request.method == 'POST':
#         cutoverid = request.json["cutoverid"].strip()
#         return json.dumps(listDBCutoverStep(cutoverid))

# @route.route("/api/executeCutoverStep", methods=['POST'])
# def view_executeCutoverStep():
#     if request.method == 'POST':
#         processid = request.json["processid"].strip()
#         cutoverid = request.json["cutoverid"].strip()
#         return json.dumps(executeCutoverStep(processid, cutoverid))

# @route.route("/api/compare_distributing_script", methods=['POST'])
# def compare_distributing_script():
#     if request.method == 'POST':
#         source_file = request.json["sourceFile"].strip()
#         target_dir = request.json["targetDir"].strip()
#         return json.dumps(compare_distributing_script_preview(source_file, target_dir))

# @route.route("/api/distribute_script", methods=['POST'])
# def distribute_script():
#     if request.method == 'POST':
#         server_list_file = request.json["serverListFile"]
#         source_file = request.json["sourceFile"]
#         target_dir = request.json["targetDir"]
#         return json.dumps(distribute_script_by_jenkins(server_list_file, source_file, target_dir))


# @route.route("/api/get_distribute_log", methods=['GET', 'POST'])
# def get_distribute_log():
#     return json.dumps(get_distribute_log_from_jenkins())


@route.route("/api/checkCronStatus", methods=['POST'])
def checkCronStatus():
    if request.method == 'POST':
        return jsonify(getCheckCronStatus())

@route.route("/api/checkOneCronStatus", methods=['POST'])
def CheckOneCronStatus():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        opt = request.json["opt"]
        return jsonify(checkOneCronStatus(host_name,opt))

@route.route("/api/getReport", methods=['POST'])
def report():
    if request.method == 'POST':
        type = request.json["type"]
        env = current_app.config.get("ENV")
        return json.dumps(getReport(type,env))

@route.route("/api/getReportHtml", methods=['POST'])
def getReportHtml():
    if request.method == 'POST':
        filename = request.json["filename"]
        type = request.json["type"]
        env = current_app.config.get("ENV")
        return json.dumps(getOneReportHtml(filename,type,env))

@route.route("/api/check_login_user", methods=['POST'])
def check_login_user():
    if request.method == 'POST':
        username = request.json["username"]
        return jsonify(check_login_user_from_depot(username))


@route.route("/api/get_role_list", methods=['GET', 'POST'])
def get_role_list():
    return jsonify(get_role_list_from_depot())


@route.route("/api/get_access_dir", methods=['POST'])
def get_access_dir():
    if request.method == 'POST':
        username = request.json["username"]
    return jsonify(get_access_dir_from_depot(username))

@route.route("/api/listdbinstancename", methods=['POST'])
def view_listdbinstancename():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        return jsonify(listDBInstanceName(db_name))

@route.route("/api/generateAWRReport", methods=['POST'])
def view_generateAWRReport():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        str_start_time = request.json["start_time"]
        str_end_time = request.json["end_time"]
        instance_number = request.json["instance_number"]
        env = current_app.config.get("ENV")
        return jsonify(generateAWRReport(db_name, str_start_time, str_end_time, instance_number,env))

@route.route("/api/listDBName", methods=['POST'])
def listDBName():
    return jsonify(getDBNameList())

@route.route("/api/generateASHReport", methods=['POST'])
def view_generateASHReport():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        str_start_time = request.json["start_time"]
        str_end_time = request.json["end_time"]
        instance_number = request.json["instance_number"]
        env = current_app.config.get("ENV")
        return jsonify(generateASHReport(db_name, str_start_time, str_end_time, instance_number,env))

@route.route("/api/getadbmonlist", methods=['POST'])
def get_adbmonlist():
    if request.method == 'POST':
        src_db = request.json["src_db"]
        tgt_db = request.json["tgt_db"]
        port = request.json["port"]
        db_type = request.json["db_type"]
        return jsonify(getadbmonlist(src_db,tgt_db,port,db_type))

@route.route("/api/getadbmonlistAlert", methods=['POST'])
def get_adbmonlistAlert():
    if request.method == 'POST':
        src_db = request.json["src_db"]
        tgt_db = request.json["tgt_db"]
        port = request.json["port"]
        db_type = request.json["db_type"]
        return jsonify(getadbmonlistAlert(src_db,tgt_db,port,db_type))

@route.route("/api/adbmoncheck", methods=['POST'])
def adbmon_check():
    if request.method == 'POST':
        env = current_app.config.get("ENV")
        port = request.json["port"]
        src_db = request.json["src_db"]
        src_host = request.json["src_host"]
        tgt_db = request.json["tgt_db"]
        tgt_host = request.json["tgt_host"]
        replication_to = request.json["replication_to"]
        return adbmon_check_new(port,src_db,src_host,tgt_db,tgt_host,replication_to,env)

@route.route("/api/autotask_listjobsbytaskid", methods=['POST'])
def view_autotask_listJobsByTaskid():
    if request.method == 'POST':
        taskid = request.json["taskid"]
        return jsonify(biz_autotask_listJobsByTaskid(taskid))

@route.route("/api/autotask_listtasks", methods=['POST'])
def view_autotask_listtasks():
    if request.method == 'POST':
        task_type = request.json["task_type"]
        return jsonify(biz_autotask_listtasks(task_type))

@route.route("/api/autotask_initialize", methods=['POST'])
def view_autotask_initialize():
    if request.method == 'POST':
        try:
            createby = request.authorization["username"]
        except:
            createby="AutomationTool"
        kwargs = request.json
        kwargs["createby"]=createby
        if "self_heal" in kwargs and kwargs['self_heal'] == "1":
            now = wbxutil().gettimestr()
            kwargs['call_time'] = now
        return jsonify(biz_autotask_initialize(**kwargs))

@route.route("/api/autotask_preexecutejob", methods=['POST'])
def view_autotask_preexecutejob():
    if request.method == 'POST':
        taskType = request.json["taskType"]
        taskid = request.json["taskid"]
        jobid = request.json["jobid"]
        return jsonify(biz_autotask_preexecutejob(taskType,taskid,jobid))

@route.route("/api/autotask_executejob", methods=['POST'])
def view_autotask_executejob():
    if request.method == 'POST':
        taskid = request.json["taskid"]
        jobid = request.json["jobid"]
        return jsonify(biz_autotask_executejob(taskid,jobid))

@route.route("/api/autotask_getlog", methods=['POST'])
def view_autotask_getjoblog():
    if request.method == 'POST':
        jobid = request.json["jobid"]
        return jsonify(biz_autotask_getjoblog(jobid))

@route.route("/api/autotask_tahoebuild", methods=['POST'])
def view_autotask_tahoebuild():
    if request.method == 'POST':
        taskid = request.json["taskid"]
        return jsonify(biz_autotask_tahoebuild(taskid))

@route.route("/api/autotask_exeonclick", methods=['POST'])
def view_autotask_exeoneclick():
    if request.method == 'POST':
        taskid = request.json["taskid"]
        return jsonify(biz_autotask_exeoneclick(taskid))

@route.route("/api/autotask_updjobstatus", methods=['POST'])
def view_autotask_updjobstatus():
    if request.method == 'POST':
        taskid=request.json["taskid"]
        jobid = request.json["jobid"]
        status= request.json["status"]
        return jsonify(biz_autotask_updjobstatus(taskid,jobid,status))

@route.route("/api/add_nfs_client", methods=['POST'])
def addnfsclient():
    if request.method == 'POST':
        datacenter = request.json["datacenter"]
        mount_ip = request.json["mount_ip"]
        vol_name = request.json["vol_name"]
        client_list = request.json["client_list"]
        notification_user = request.json["notification_user"]
        vol_type = request.json["vol_type" \
                                ""]
        return jsonify(add_nfs_client(datacenter, mount_ip, vol_name, client_list, notification_user, vol_type))


@route.route("/api/mount_nfs", methods=['POST'])
def mountnfs():
    if request.method == 'POST':
        mount_ip = request.json["mount_ip"]
        client_list = request.json["client_list"]
        vol_name = request.json["vol_name"]
        mount_dir = request.json["mount_dir"]
        return jsonify(mount_nfs(mount_ip, client_list, vol_name, mount_dir))


@route.route("/api/create_nfs_volume", methods=['POST'])
def createnfsvolume():
    if request.method == 'POST':
        client_list = request.json["client_list"]
        datacenter = request.json["datacenter"]
        vol_name = request.json["vol_name"]
        mount_dir = request.json["mount_dir"]
        size_gb = request.json["size_gb"]
        notification_user = request.json["notification_user"]
        return jsonify(create_nfs_volume(datacenter, size_gb, vol_name, client_list, notification_user, mount_dir))


@route.route("/api/resize_nfs_volume", methods=['POST'])
def resizenfsvolume():
    if request.method == 'POST':
        additional_size = request.json["additional_size"]
        datacenter = request.json["datacenter"]
        vol_name = request.json["vol_name"]
        mount_ip = request.json["mount_ip"]
        notification_user = request.json["notification_user"]
        return jsonify(
            resize_nfs_volume(datacenter, additional_size, mount_ip, vol_name, notification_user))

@route.route("/api/adbmondetail", methods=['POST'])
def adbmon_detail():
    if request.method == 'POST':
        port = request.json["port"]
        src_db = request.json["src_db"]
        src_host = request.json["src_host"]
        tgt_db = request.json["tgt_db"]
        tgt_host = request.json["tgt_host"]
        replication_to = request.json["replication_to"]
        return jsonify(adbmondetail(port,src_db,src_host,tgt_db,tgt_host,replication_to))

@route.route("/api/sentAlertToBot", methods=['POST'])
def sent_AlertToBot():
    """
    :param to_person     : person cec or oncall
    :param optional_flag : AT or TO
    """
    if request.method == 'POST':
        content = request.json["content"]
        room_id = request.json.get("roomId", None)
        to_person = request.json.get("toPersonCec", None)
        optional_flag = request.json.get("oncallFlag", None)
        return jsonify(sentAlertToBot(content, room_id, to_person, optional_flag))


@route.route("/api/get_splex_performance_monitor", methods=['POST'])
def getggperformancemonitor():
    if request.method == 'POST':
        src_db = request.json["src_db"]
        tgt_db = request.json["tgt_db"]
        src_host = request.json["src_host"]
        tgt_host = request.json["tgt_host"]
        replication_to = request.json["replication_to"]
        return jsonify(get_splex_performance_monitor(src_db, tgt_db, src_host, tgt_host, replication_to))


@route.route("/api/get_detailed_metric_monitor", methods=['POST'])
def getdetailedmetricmonitor():
    if request.method == 'POST':
        src_db = request.json["src_db"]
        tgt_db = request.json["tgt_db"]
        src_host = request.json["src_host"]
        tgt_host = request.json["tgt_host"]
        queue_name = request.json["queue_name"]
        process_type = request.json["process_type"]
        metric_type = request.json["metric_type"]
        return jsonify(
            get_detailed_metric_monitor(src_db, tgt_db, src_host, tgt_host, queue_name, process_type, metric_type))

@route.route("/api/getarchivelog", methods=['POST'])
def get_archivelog():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]
        return jsonify(getArchivelog(db_name,start_time,end_time))

@route.route("/api/get_gg_comparision_performance_monitor", methods=['POST'])
def getggcomparisionperformancemonitor():
    if request.method == 'POST':
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]
        return jsonify(get_gg_comparision_performance_monitor(start_time, end_time))

@route.route("/api/get_splex_comparision_performance_monitor", methods=['POST'])
def getsplexcomparisionperformancemonitor():
    if request.method == 'POST':
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]
        return jsonify(get_splex_comparision_performance_monitor(start_time, end_time))

@route.route("/api/getundoTablespace", methods=['GET', 'POST'])
def get_undoTablespace():
    return jsonify(getUndoTablespace())

@route.route("/api/getundoTablespaceByDBName", methods=['POST'])
def get_undoTablespaceByDBName():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        start_date = request.json["start_date"]
        end_date = request.json["end_date"]
        return jsonify(getundoTablespaceByDBName(db_name,start_date,end_date))

@route.route("/api/getDBTns", methods=['POST'])
def getDBTns():
    db_name = request.json["db_name"]
    return get_db_tns(db_name)

@route.route("/api/precheck_shareplex_encryption", methods=['POST'])
def precheckshareplexencryption():
    if request.method == 'POST':
        src_host = request.json["src_host"]
        port = request.json["port"]
        return jsonify(precheck_shareplex_encryption(src_host, port))

@route.route("/api/excute_shareplex_encryption", methods=['POST'])
def excuteshareplexencryption():
    if request.method == 'POST':
        src_host = request.json["src_host"]
        port = request.json["port"]
        return jsonify(excute_shareplex_encryption(src_host, port))


@route.route("/api/get_meetingDB_baseInfo", methods=['GET', 'POST'])
def getmeetingDBbaseInfo():
    return jsonify(get_meetingDB_baseInfo())

@route.route("/api/get_meetingDB_dbName_by_hostName", methods=['POST'])
def getmeetingDBdbNamebyhostName():
    host_name = request.json["host_name"]
    return get_meetingDB_dbName_by_hostName(host_name)

@route.route("/api/get_meetingDB_tableList", methods=['POST'])
def getmeetingDBtableList():
    db_name = request.json["db_name"]
    host_name = request.json["host_name"]
    return get_meetingDB_tableList(db_name, host_name)

@route.route("/api/get_channel_tableList", methods=['POST'])
def getchanneltableList():
    src_appln_support_code = request.json["src_appln_support_code"]
    tgt_appln_support_code = request.json["tgt_appln_support_code"]
    return get_channel_tablelist(src_appln_support_code, tgt_appln_support_code)

@route.route("/api/get_replication_tableList", methods=['POST'])
def getreplicationtableList():
    table_name = request.json["table_name"]
    return get_replication_tablelist(table_name)

@route.route("/api/get_telegraf_monitor_list", methods=['GET', 'POST'])
def gettelegrafmonitorlist():
    return jsonify(get_telegraf_monitor_list())


@route.route("/api/telegraf_action", methods=['POST'])
def telegrafaction():
    host_name = request.json["host_name"]
    action_type = request.json["action_type"]
    return telegraf_action(host_name, action_type)


@route.route("/api/get_db_metadata", methods=['POST'])
def getoracleuserpwdbyhostname():
    data_type = request.json["data_type"]
    data_value = request.json["data_value"]
    logger.info("get_db_metadata,data_type=%s,data_value=%s" % (data_type, data_value))
    res = get_db_metadata(data_type, data_value)
    logger.info(res)
    return res

@route.route("/api/listwaitevent", methods=['POST'])
def view_listwaitevent():
    search_type = request.json["search_type"]
    db_name = request.json["db_name"]
    return jsonify(listwaitevent(search_type, db_name))

@route.route("/api/listjobs", methods=['GET','POST'])
def view_listjobs():
    search_type = request.json.get("search_type", None)
    job_name = request.json.get("job_name", None)
    curpage = request.json.get("curpage", "1")
    pagesize = request.json.get("pagesize", "10")
    return jsonify(listjobs(search_type,job_name,curpage,pagesize))

@route.route("/api/jobmonitor", methods=['GET','POST'])
def view_jobmonitor():
    job_name = request.json.get("job_name", None)
    datadate = request.json.get("datadate", datetime.datetime.now().strftime('%Y-%m-%d'))
    return jsonify(jobmonitor(job_name,datadate))


@route.route("/api/allProcedure", methods=['GET','POST'])
def get_procedure_list():
    return jsonify(selectProcedure())

@route.route("/api/getProcedureList", methods=['GET','POST'])
def get_ProcedureList():
    procedure_name = request.json["procedure_name"]
    return jsonify(get_procedureList(procedure_name))

@route.route("/api/commit_procedure", methods=['POST'])
def commitProcedure():
    procedure_name = request.json["procedure_name"]
    db_name = request.json["db_name"]
    created_by = request.authorization["username"]
    args = ""
    if request.json["args"]:
        args = json.dumps(request.json["args"])
    return commit_procedure(procedure_name,db_name,created_by,args)

@route.route("/api/exec_procedure", methods=['POST'])
def exec_procedure():
    jobid = request.json["jobid"]
    procedure_name = request.json["procedure_name"]
    exec_by = request.authorization["username"]
    resDict = exec_job(jobid,exec_by,procedure_name)
    return resDict

@route.route("/api/getFailedJobList", methods=['GET','POST'])
def failed_job_tablelist():
    return jsonify(get_failed_job_tablelist())

@route.route("/api/executeFailedJob", methods=['POST'])
def execute_failed_job_action():
    jsondata = request.json["data"]
    return execute_failed_job(jsondata)


@route.route("/api/get_metadata", methods=['POST'])
def getmetadata():
    data_type = request.json["data_type"]
    data_value = request.json["data_value"]
    return jsonify(get_metadata(data_type, data_value))

@route.route("/api/tokeninfo", methods=['POST'])
def get_tokeninfo():
    access_token = request.json["access_token"]
    resDict = getTokeninfo(access_token)
    logger.info(resDict)
    return resDict

@route.route("/api/procedure_getjoblog", methods=['POST'])
def view_procedure_getjoblog():
    if request.method == 'POST':
        jobid = request.json["jobid"]
        return jsonify(procedure_getjoblog(jobid))

@route.route("/api/delmydblog", methods=['GET','POST'])
def delmydbmeetinglog():
    mydbname = request.json.get("mydbname")
    mydbschema = request.json.get("mydbschema")
    return jsonify(wbxmydbpurgelog(mydbname,mydbschema))

@route.route("/api/getmydbpurgelog", methods=['GET','POST'])
def getmydbpurgelog():
    mydbname = request.json.get("mydbname", None)
    mydbschema = request.json.get("mydbschema", None)
    return jsonify(getmydbdellog(mydbname,mydbschema))

@route.route("/api/getNotifyTablelist", methods=['GET','POST'])
def notify_tablelist():
    return jsonify(get_notify_tablelist())

# @route.route("/api/addNotifyCannel", methods=['POST'])
# def addNotifyCannel():
#     if request.method == 'POST':
#         channel_name = request.json["channel_name"]
#         channel_type = request.json["channel_type"]
#         emails = request.json["emails"]
#         teams = request.json["teams"]
#         return jsonify(add_notify_channel(channel_name, channel_type, emails, teams))

# @route.route("/api/updateNotifyChannel", methods=['POST'])
# def updateNotifyChannel():
#     if request.method == 'POST':
#         channel_id = request.json['channel_id']
#         channel_name = request.json["channel_name"]
#         channel_type = request.json["channel_type"]
#         emails = request.json["emails"]
#         teams = request.json["teams"]
#         return jsonify(update_notify_channel(channel_name, channel_type, emails, teams,channel_id))

# @route.route("/api/show_db_config_monitor", methods=['POST'])
# def showdbconfigmonitor():
#     if request.method == 'POST':
#         db_name = request.json["db_name"]
#         return jsonify(show_db_config_monitor(db_name))

@route.route("/api/getMetricTablelist", methods=['GET','POST'])
def metric_tablelist():
    return jsonify(get_metric_tablelist())

# @route.route("/api/addMetricSetting", methods=['POST'])
# def addMetricSetting():
#     if request.method == 'POST':
#         metric_name = request.json["metric_name"]
#         job_name = request.json["job_name"]
#         warning_value = request.json["warning_value"]
#         warning_channels = request.json["warning_channels"]
#         critical_value = request.json["critical_value"]
#         critical_channels = request.json["critical_channels"]
#         operator = request.json["operator"]
#         alerttype = request.json["alerttype"]
#         db_name = request.json["db_name"]
#         instance_name = request.json["instance_name"]
#         db_type = request.json["db_type"]
#         host_name = request.json["host_name"]
#         return jsonify(add_metric_setting(metric_name,job_name,warning_value,warning_channels,critical_value,critical_channels,operator,alerttype,db_name,instance_name,host_name,db_type))

# @route.route("/api/updateMetricSetting", methods=['POST'])
# def updateMetricSetting():
#     if request.method == 'POST':
#         metric_name = request.json["metric_name"]
#         job_name = request.json["job_name"]
#         warning_value = request.json["warning_value"]
#         warning_channels = request.json["warning_channels"]
#         critical_value = request.json["critical_value"]
#         critical_channels = request.json["critical_channels"]
#         operator = request.json["operator"]
#         alerttype = request.json["alerttype"]
#         db_name = request.json["db_name"]
#         instance_name = request.json["instance_name"]
#         db_type = request.json["db_type"]
#         host_name = request.json["host_name"]
#         thresholdid = request.json["thresholdid"]
#         return jsonify(update_metric_setting(metric_name,job_name,warning_value,warning_channels,critical_value,critical_channels,operator,alerttype,db_name,instance_name,host_name,db_type, thresholdid))


@route.route("/api/get_shareplex_port_install_history_list", methods=['GET', 'POST'])
def get_shareplexportinstallhistorylist():
    return jsonify(get_shareplex_port_install_history_list())

@route.route("/api/get_shareplex_port_install_detail", methods=['POST'])
def get_shareplexportinstalldetail():
    if request.method == 'POST':
        taskid = request.json["taskid"]
        return jsonify(get_shareplex_port_install_detail(taskid))


@route.route("/api/preverify_shareplex_port_install", methods=['POST'])
def preverify_shareplexportinstall():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        port = request.json["port"]
        datasource = request.json["datasource"]
        return jsonify(preverify_shareplex_port_install(host_name, port, datasource))

# @route.route("/api/execute_shareplex_port_install", methods=['POST'])
# def execute_shareplexportinstall():
#     if request.method == 'POST':
#         host_name = request.json["host_name"]
#         port = request.json["port"]
#         datasource = request.json["datasource"]
#         return jsonify(execute_shareplex_port_install(host_name, port, datasource))

# @route.route("/api/get_db_parameter_tablelist", methods=['POST'])
# def getDBParameterList():
#     if request.method == 'POST':
#         db_name = request.json["db_name"]
#         return jsonify(get_db_parameter_list(db_name))

# @route.route("/api/get_parameter_in_db_tablelist", methods=['POST'])
# def getParameterInDBList():
#     if request.method == 'POST':
#         type = request.json["type"]
#         name = request.json["name"]
#         return jsonify(get_parameter_in_db_list(type, name))

# @route.route("/api/get_failed_parameter_tablelist", methods=['GET', 'POST'])
# def getFailedParameterList():
#     return jsonify(get_failed_parameter_list())

@route.route("/api/setup_shareplex_port", methods=['POST'])
def setupshareplexport():
    if request.method == 'POST':
        try:
            createby = request.authorization["username"]
        except:
            createby="AutomationTool"
        host_name = request.json["host_name"]
        port = request.json["port"]
        datasource = request.json["datasource"]
        return jsonify(setup_shareplex_port(host_name, port, datasource, createby))

@route.route("/api/get_drop_partition_new_status", methods=['GET', 'POST'])
def getdroppartitionnewstatus():
    env = current_app.config.get("ENV")
    return jsonify(get_drop_partition_new_status(env))

@route.route("/api/get_db_drop_partition_detail_status", methods=['POST'])
def getdbdroppartitiondetailstatus():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        return jsonify(get_db_drop_partition_detail_status(db_name))

@route.route("/api/get_jira_issue_list", methods=['GET'])
def get_jiraissuelist():
    return jsonify(get_jira_issue_list())

@route.route("/api/get_depot_manage_info", methods=['POST'])
def getDepot_manage_info():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        host_name = request.json["host_name"]
        return jsonify(get_depot_manage_info(db_name,host_name))

@route.route("/api/get_depot_manage_user_info", methods=['POST'])
def getDepot_manage_user_info():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        trim_host = request.json["trim_host"]
        return jsonify(get_depot_manage_user_info(db_name,trim_host))

@route.route("/api/get_depot_manage_splexplex_info", methods=['POST'])
def getDepot_manage_splexplex_info():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        return jsonify(get_depot_manage_splexplex_info(db_name))

@route.route("/api/depot_manage_reload", methods=['POST'])
def depotManage_reload():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        return jsonify(depot_manage_reload(host_name,"reload"))

@route.route("/api/depot_manage_add_rac", methods=['POST'])
def depotManage_add_rac():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        return depot_manage_add_rac(host_name)

@route.route("/api/depot_manage_add_DB", methods=['POST'])
def depotManage_add_DB():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        db_name = request.json["db_name"]
        db_type = request.json["db_type"]
        application_type = request.json["application_type"]
        appln_support_code = request.json["appln_support_code"]
        web_domain = request.json["web_domain"]
        ismonitor = request.json["ismonitor"]
        wbx_cluster = request.json["wbx_cluster"]
        return jsonify(depot_manage_add_DB(host_name,db_name,db_type,application_type,appln_support_code,web_domain,ismonitor,wbx_cluster))

@route.route("/api/depot_manage_reload_shareplex", methods=['POST'])
def depotManage_reload_shareplex():
    if request.method == 'POST':
        src_host_name = request.json["src_host_name"]
        port = request.json["port"]
        return jsonify(depot_manage_reload_shareplex(src_host_name,port))

@route.route("/api/depot_manage_add_user", methods=['POST'])
def depotManage_add_user():
    if request.method == 'POST':
        password = request.json["password"]
        trim_host = request.json["trim_host"]
        db_name = request.json["db_name"]
        user_name = request.json["user_name"]
        schematype = request.json["schematype"]
        appln_support_code = request.json["appln_support_code"]
        return jsonify(depot_manage_add_user(password,db_name,trim_host,user_name,appln_support_code,schematype))

@route.route("/api/getdblicences", methods=['POST'])
def get_dblicences():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        dc_name = request.json["dc_name"]
        db_type = request.json["db_type"]
    return jsonify(getdblicences(host_name=host_name,dc_name=dc_name,db_type=db_type))

@route.route("/api/getdbliclabelinfo", methods=['POST'])
def get_dbliclabelinfo():
    return jsonify(getdbliclabelinfo())

@route.route("/api/getAdbmonlistByDCName", methods=['POST'])
def get_adbmonlistByDCName():
    if request.method == 'POST':
        dc_name = request.json["dc_name"]
        delay_min = 10
        if "delay_min" in request.json:
            delay_min = request.json["delay_min"]
        return jsonify(getAdbmonlistByDCName(dc_name,delay_min))

@route.route("/api/getpoolinfobydbname", methods=['POST'])
def getpoolinfo_bydbname():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        return jsonify(getpoolinfobydbname(db_name))

@route.route("/api/getSelfHealingJobList", methods=['POST','GET'])
def get_self_heal_job_list():
    if request.method == 'POST':
        return jsonify(getSelfHealingJobList())

@route.route("/api/get_depot_manage_instance", methods=['POST','GET'])
def getDeport_manage_instance():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        return jsonify(get_depot_manage_instance(db_name))

@route.route("/api/get_depot_manage_pool_info", methods=['POST'])
def getDepot_manage_pool_info():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        return jsonify(get_depot_manage_pool_info(db_name))

@route.route("/api/add_depot_manage_pool_info", methods=['POST'])
def depot_manage_add_pool_info():
    if request.method == 'POST':
        trim_host = request.json["trim_host"]
        db_name = request.json["db_name"]
        appln_support_code = request.json["appln_support_code"]
        pool_name = request.json["pool_name"]
        schema = request.json["schema"]
        service_name = request.json["service_name"]
        return jsonify(add_depot_manage_pool_info(trim_host,db_name,appln_support_code,pool_name,schema,service_name))


# @route.route("/api/get_os_and_db_info_by_host_name", methods=['POST'])
# def get_os_and_db_info_by_hostname():
#     host_name = request.json["host_name"]
#     return jsonify(get_os_and_db_info_by_host_name(host_name))
#
#
# @route.route("/api/get_os_and_db_info_by_db_name", methods=['POST'])
# def get_os_and_db_info_by_dbname():
#     db_name = request.json["db_name"]
#     return jsonify(get_os_and_db_info_by_db_name(db_name))

@route.route("/api/get_rman_backup_status_list", methods=['POST','GET'])
def rman_backup_statuslistjobs_list():
    return jsonify(get_rman_backup_status_list())

@route.route("/api/get_splex_deny_user_list", methods=['POST'])
def splex_deny_user_list():
    curpage = request.json.get("curpage", "1")
    pagesize = request.json.get("pagesize", "10")
    return jsonify(get_splex_deny_user_list(curpage, pagesize))

@route.route("/api/getDBMemoSize", methods=['POST','GET'])
def getdbmemosize():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        return jsonify(get_DBMemoSize(db_name))


@route.route("/api/teodb-failover-failback", methods=['POST'])
def teodbfailover():
    try:
        createby = request.authorization["username"]
    except:
        createby = "AutomationTool"
    # pri_db_name = request.json["pri_db_name"]
    # gsb_db_name = request.json["gsb_db_name"]
    action_type = request.json["actionType"]
    kwargs = request.json
    kwargs["createby"] = createby
    kwargs["task_type"] = "TEODB_FAILOVER_TASK"
    if action_type == 0:
        return jsonify(failover(**kwargs))
    if action_type == 2:
        return jsonify(failback(**kwargs))


@route.route("/api/get-teodb-failover-list", methods=['GET', 'POST'])
def getteodbfailoverlist():
    return jsonify(get_teodb_failover_list())


@route.route("/api/get-teodb-failover-detail", methods=['POST'])
def getteodbfailoverdetail():
    if request.method == 'POST':
        taskid = request.json["taskid"]
        jobid = request.json["jobid"]
        return jsonify(get_teodb_failover_detail(taskid, jobid))


@route.route("/api/check-teodb-status/<db_name>", methods=['POST', 'GET'])
def checkteodbstatus(db_name):
    return jsonify(check_teodb_status(db_name.upper()))


@route.route("/api/get_all_dc_name", methods=['POST','GET'])
def get_all_dcname():
    return jsonify(get_all_dc_name())

@route.route("/api/check_rman_backup_status", methods=['POST'])
def checkRmanBackupStatus():
    host_name = request.json["host_name"]
    db_name = request.json["db_name"]
    return jsonify(check_rman_backup_status(host_name, db_name))

@route.route("/api/add_wbxmonitoralertdetail", methods=['POST'])
def addWbxmonitoralertdetail():
    if request.method == 'POST':
        kwargs = request.json
        return jsonify(add_wbxmonitoralertdetail(**kwargs))

@route.route("/api/get_wbxmonitoralert", methods=['POST', 'GET'])
def getWbxmonitoralert():
    if request.method == 'POST':
        status = ""
        if "status" in request.json and request.json["status"] != "ALL":
            status = request.json["status"]
        db_name = request.json["db_name"]
        host_name = request.json["host_name"]
        alert_type = request.json["alert_type"]
        start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).date().strftime('%Y-%m-%d')
        end_date = datetime.datetime.now().date().strftime('%Y-%m-%d')
        if "start_date" in request.json and "end_date" in request.json:
            start_date = request.json["start_date"]
            end_date = request.json["end_date"]
        return jsonify(get_wbxmonitoralert(db_name,status,host_name,alert_type,start_date,end_date))

@route.route("/api/get_wbxmonitoralertdetail", methods=['POST'])
def getWbxmonitoralertdetail():
    if request.method == 'POST':
        alertid = request.json["alertid"]
        return jsonify(get_wbxmonitoralertdetail(alertid))

@route.route("/api/get_wbxautotask", methods=['POST'])
def getWbxautotask():
    if request.method == 'POST':
        autotaskid = request.json["autotaskid"]
        return jsonify(get_wbxautotask(autotaskid))

@route.route("/api/checkServer", methods=['POST'])
def checkServer():
    if request.method == 'POST':
        param = request.json['param']
        host_name = request.json["host_name"]
        return jsonify(CheckServer(host_name,param))

@route.route("/api/OperateServer", methods=['POST'])
def operateCronStatus():
    if request.method == 'POST':
        param = request.json['param']
        host_name = request.json["host_name"]
        opt = request.json["opt"]
        user = request.json["user"]
        return jsonify(OperateServer(host_name,opt,param,user))

@route.route("/api/getDBlinkmonitordetail", methods=['POST'])
def get_DBlinkmonitordetail():
    if request.method == 'POST':
        trim_host = request.json["trim_host"]
        db_name = request.json["db_name"]
        status = request.json["status"]
        return jsonify(getDBlinkmonitordetail(trim_host,db_name,status))

@route.route("/api/get_splex_params", methods=['POST'])
def getServerSplexParams():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        port_number = request.json['port_number']
        param_name = request.json['param_name']
        if "ismodified" in request.json:
            ismodified = request.json["ismodified"]
        else:
            ismodified = ""
        curpage = request.json.get("curpage","1")
        pagesize = request.json.get("pagesize","10")
        return jsonify(getSplexParams(host_name,port_number,param_name,ismodified,curpage,pagesize))

@route.route("/api/checkDBlink", methods=['POST'])
def check_DBlinkmonitor():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        try:
            createby = request.authorization["username"]
        except:
            createby = ""
        return jsonify(checkDBlink(db_name,createby))

@route.route("/api/add_monitor_splex_params", methods=['POST'])
def add_monitor_splex_params():
    if request.method == 'POST':
        param_name = request.json["paramName"]
        return jsonify(addMonitorSplexParam(param_name))


@route.route("/api/check_splex_params", methods=['POST'])
def check_Splexparams():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        if "port_number" in request.json:
            port_number = request.json["port_number"]
        else:
            port_number = ""
        return jsonify(checkSplexParams(host_name,port_number))


@route.route("/api/get_splex_server_hostname", methods=['POST'])
def get_SplexServerHostname():
    return jsonify(getSplexParamsServerHostname())


@route.route("/api/check_splexcr_status", methods=['POST'])
def check_crstatus():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        if "port_number" in request.json:
            port_number = request.json["port_number"]
        else:
            port_number = ""
        return jsonify(checkCRConfigStatus(host_name,port_number))

@route.route("/api/get_singlesplexcr_status", methods=['POST'])
def check_singlecrstatus():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        if "port_number" in request.json:
            port_number = request.json["port_number"]
        else:
            port_number = ""
        return jsonify(getSingleCRconfigStatus(host_name,port_number))

@route.route("/api/fix_splexcr_failed", methods=['POST'])
def fix_splexcr_failed():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        if "port_number" in request.json:
            port_number = request.json["port_number"]
        else:
            port_number = ""
        return jsonify(fixFailedCR(host_name,port_number))

@route.route("/api/get_splexcr_status", methods=['POST'])
def get_splexcrstatus():
        return jsonify(getCRConfigStatus())

@route.route("/api/get_splexcrlog_count", methods=['POST'])
def get_splexcrlogcount():
        return jsonify(getCRLogCount())

@route.route("/api/get_crlog_count_history", methods=['POST'])
def get_crlogcounthistory():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        db_name = request.json['db_name']
        splex_port = request.json['splex_port']
        return jsonify(getCRLogCountHistory(host_name,db_name,splex_port))

@route.route("/api/get_gather_stats", methods=['POST'])
def get_gatherstats():
    if request.method == 'POST':
        trim_host = request.json["trim_host"]
        db_name = request.json['db_name']
        schema_name = request.json['schema_name']
        curpage = request.json.get("curpage", "1")
        pagesize = request.json.get("pagesize", "10")
        return jsonify(getGatherStatus(trim_host,db_name,schema_name,curpage, pagesize))

# get_homepage_server_count, get_homepage_db_count, get_homepage_db_type_count, get_shareplex_count, get_rencent_alert_info, get_top_active_session_db_count
@route.route("/api/get-homepage-db-version-count", methods=['POST', 'GET'])
def gethomepagedbversioncount():
    return jsonify(get_homepage_db_version_count())


@route.route("/api/get-homepage-db-count", methods=['POST', 'GET'])
def gethomepagedbcount():
    return jsonify(get_homepage_db_count())


@route.route("/api/get-homepage-db-type-count", methods=['POST', 'GET'])
def gethomepagedbtypecount():
    return jsonify(get_homepage_db_type_count())


@route.route("/api/get-shareplex-count", methods=['POST', 'GET'])
def getshareplexcount():
    return jsonify(get_shareplex_count())


@route.route("/api/get-rencent-alert-info", methods=['POST', 'GET'])
def getrencentalertinfo():
    return jsonify(get_rencent_alert_info())


@route.route("/api/get-top-active-session-db-count", methods=['POST', 'GET'])
def gettopactivesessiondbcount():
    return jsonify(get_top_active_session_db_count())

@route.route("/api/get_wbxmonitoralert_type", methods=['POST', 'GET'])
def getWbxmonitoralerttype():
    return jsonify(get_wbxmonitoralert_type())

@route.route("/api/getwbxdbauditparams", methods=['POST'])
def get_wbxdbauditparams():
    if request.method == 'POST':
        kwargs = request.json
        return jsonify(getwbxdbauditparams(**kwargs))


@route.route("/api/list-oncall-handover", methods=['POST'])
def listoncallhandover():
    if request.method == 'POST':
        start_time = request.json["startTime"].strip()
        end_time = request.json["endTime"].strip()
        return jsonify(list_oncall_handover(start_time, end_time))


@route.route("/api/send-oncall-handoff-mail", methods=['POST'])
def sendoncallhandoffmail():
    if request.method == 'POST':
        start_time = request.json["startTime"].strip()
        end_time = request.json["endTime"].strip()
        return jsonify(send_oncall_handoff_mail(start_time, end_time))


@route.route("/api/add-oncall-handoff", methods=['POST'])
def addoncallhandoff():
    if request.method == 'POST':
        created_by = request.json["created_by"].strip()
        from_shift = request.json["from_shift"].strip()
        to_shift = request.json["to_shift"].strip()
        classification_severity = request.json["classification_severity"].strip()
        status = request.json["status"].strip()
        description = request.json["description"].strip()
        category = request.json["category"].strip()
        cluster = request.json["cluster"].strip()
        return jsonify(add_oncall_handoff(created_by, from_shift, to_shift, classification_severity, status, description, category, cluster))


@route.route("/api/get-oncall-cec", methods=['POST', 'GET'])
def getoncallcec():
    return jsonify(get_oncall_cec())

@route.route("/api/get_DB_connection_endpoint", methods=['POST'])
def getDBConnectionEndpoint():
    if request.method == 'POST':
        db_type = request.json['db_type']
        appln_support_code = request.json["appln_support_code"]
        web_domain = request.json["web_domain"]
        schema = request.json["schema"]
        db_name = request.json["db_name"]
        return jsonify(get_DB_connection_endpoint(db_type,appln_support_code,web_domain,schema,db_name))


@route.route("/api/tahoe_db_failover_first", methods=['GET','POST'])
def tahoe_db_failoverfirst():
    pool_name = request.json["pool_name"]
    return jsonify(stop_ha_service_and_kill_gsb_connection(pool_name))


@route.route("/api/tahoe_db_failover_second", methods=['POST'])
def tahoe_db_failoversecond():
    pool_name = request.json["pool_name"]
    return jsonify(kill_primary_application(pool_name))


@route.route("/api/check_tahoe_db_status", methods=['POST'])
def check_tahoedbstatus():
    pool_name = request.json["pool_name"]
    return jsonify(check_tahoe_db_status(pool_name))


@route.route("/api/get_favourit_page_by_username", methods=['POST'])
def getfavouritpagebyusername():
    username = request.json["username"]
    return jsonify(get_favourit_page_by_username(username))


@route.route("/api/add_favourite_page", methods=['POST'])
def addfavouritepage():
    username = request.json["username"]
    page_name = request.json["page_name"]
    url = request.json["url"]
    return jsonify(add_favourite_page(username, page_name, url))


@route.route("/api/delete_favourite_page", methods=['POST'])
def deletefavouritepage():
    username = request.json["username"]
    page_name = request.json["page_name"]
    url = request.json["url"]
    return jsonify(delete_favourite_page(username, page_name, url))


@route.route("/api/health", methods=['POST', 'GET'])
def gethealth():
    return get_health()

# @route.route("/api/createDBBaseline", methods=['POST'])
# def createDBBaseline():
#     if request.method == 'POST':
#         dbtype = request.json["dbtype"]
#         schema = request.json["schema"]
#         upToReleaseNumber = request.json["upToReleaseNumber"]
#         return json.dumps(generateDBBaseline(dbtype,schema,upToReleaseNumber))


@route.route("/api/get-all-server-info", methods=['POST', 'GET'])
def getallserverinfo():
    return jsonify(get_all_server_info())


@route.route("/api/tahoe-db-failover-first", methods=['POST'])
def tahoedbfailoverfirst():
    try:
        createby = request.authorization["username"]
    except:
        createby = "AutomationTool"
    pool_name_list = request.json["pool_name_list"]
    db_name = request.json["db_name"]
    kwargs = request.json
    kwargs["createby"] = createby
    kwargs["pool_name_list"] = pool_name_list
    kwargs["db_name"] = db_name
    return jsonify(tahoe_db_failover_first(**kwargs))


@route.route("/api/tahoe-db-failover-second", methods=['POST'])
def tahoedbfailoversecond():
    try:
        createby = request.authorization["username"]
    except:
        createby = "AutomationTool"
    pool_name_list = request.json["pool_name_list"]
    db_name = request.json["db_name"]
    kwargs = request.json
    kwargs["createby"] = createby
    kwargs["pool_name_list"] = pool_name_list
    kwargs["db_name"] = db_name
    return jsonify(tahoe_db_failover_second(**kwargs))


@route.route("/api/tahoe-failback", methods=['POST'])
def tahoefailback():
    try:
        createby = request.authorization["username"]
    except:
        createby = "AutomationTool"
    pool_name_list = request.json["pool_name_list"]
    db_name = request.json["db_name"]
    kwargs = request.json
    kwargs["createby"] = createby
    kwargs["pool_name_list"] = pool_name_list
    kwargs["db_name"] = db_name
    return jsonify(tahoe_failback(**kwargs))


@route.route("/api/check-tahoe-db-status", methods=['POST'])
def checktahoedbstatus():
    pool_name_list = request.json["pool_name_list"]
    db_name = request.json["db_name"]
    kwargs = request.json
    kwargs["pool_name_list"] = pool_name_list
    kwargs["db_name"] = db_name
    return jsonify(check_tahoe_pool_status(**kwargs))


@route.route("/api/get-tahoedb-name-by-pool", methods=['POST'])
def gettahoedbnamebypool():
    pool_name_list = request.json["pool_name_list"]
    kwargs = request.json
    kwargs["pool_name_list"] = pool_name_list
    return jsonify(get_tahoedb_name_by_pool(**kwargs))

@route.route("/api/get_sql_execution_plan", methods=['POST'])
def get_sqlexecutionplan():
    if request.method == 'POST':
        trim_host = request.json["trim_host"]
        db_name = request.json['db_name']
        sql_id = request.json['sql_id']
        curpage = request.json.get("curpage", "1")
        pagesize = request.json.get("pagesize", "10")
        return jsonify(getSqlExecutionPlan(trim_host,db_name,sql_id,curpage, pagesize))

@route.route("/api/get_sql_execution_plan_detail", methods=['POST'])
def get_sqlexecutionplan_detail():
    if request.method == 'POST':
        trim_host = request.json["trim_host"]
        db_name = request.json['db_name']
        sql_id = request.json['sql_id']
        return jsonify(getSqlExecutionPlanDetail(trim_host,db_name,sql_id))

@route.route("/api/getSQLResultByDB", methods=['POST'])
def getSQLResultByDB():
    db_name = request.json["db_name"]
    sql = request.json["sql"]
    return get_SQLResultByDB(db_name,sql)

@route.route("/api/getWbxCassClusterName", methods=['POST', 'GET'])
def get_wbxCassClusterName():
    casscluster = request.json['casscluster']
    return getWbxCassClusterName(casscluster)

@route.route("/api/getWbxCassClusterInfo", methods=['POST', 'GET'])
def get_wbxCassClusterInfo():
    casscluster = request.json['casscluster']
    localdc = request.json['localdc']
    return getWbxCassClusterInfo(casscluster, localdc)

@route.route("/api/addwbxCassUser", methods=['POST'])
def add_wbxCassUser():
    userid = request.json["userid"]
    username = request.json["username"]
    userrole = request.json["userrole"]
    password = request.json['password']
    casscluster = request.json['casscluster']
    return addWbxCassUser(userid,username,userrole,password,casscluster)

@route.route("/api/deleteWbxCassUser", methods=['POST'])
def delete_cass_app_service():
    userid = request.json["userid"]
    return deleteWbxCassUser(userid)

@route.route("/api/addWbxCassClusterInfo", methods=['POST'])
def add_wbxCassClusterInfo():
    clusterid = request.json["clusterid"]
    localdc = request.json["localdc"]
    contactpoints = request.json["contactpoints"]
    port = request.json['port']
    cassclustername = request.json['cassclustername']
    return addwbxCassClusterInfo(clusterid,localdc,contactpoints,port,cassclustername)

@route.route("/api/deleteWbxCassClusterInfo", methods=['POST'])
def delete_wbxCassClusterInfo():
    clusterid = request.json["clusterid"]
    return deletewbxCassClusterInfo(clusterid)

@route.route("/api/getCassUserByCasscluster", methods=['POST'])
def get_cass_user_by_casscluster():
    casscluster = request.json['casscluster']
    return getCassUserByCasscluster(casscluster)

@route.route("/api/getCassLocaldcByCasscluster", methods=['POST'])
def get_cass_localdc_by_casscluster():
    casscluster = request.json['casscluster']
    return getCassLocaldcByCasscluster(casscluster)

@route.route("/api/addWbxCassAppKeyspaceConnInfo", methods=['POST'])
def add_wbxCassAppKeyspaceConnInfo():
    keyspaceid = request.json['keyspaceid']
    keyspacename = request.json['keyspacename']
    localdc = request.json['localdc']
    contactpoints = request.json['contactpoints']
    port = request.json['port']
    userid = request.json['userid']
    casscluster = request.json['casscluster']
    return addWbxCassAppKeyspaceConnInfo(keyspaceid,keyspacename,localdc,contactpoints,port,userid,casscluster)

@route.route("/api/deleteWbxCassAppKeyspaceConnInfo", methods=['POST'])
def delete_wbxCassAppKeyspaceConnInfo():
    keyspaceid = request.json['keyspaceid']
    return deleteWbxCassAppKeyspaceConnInfo(keyspaceid)

@route.route("/api/getCassServiceName", methods=['POST'])
def get_cass_servicename():
    servicename = request.json['servicename']
    return getCassServiceName(servicename)

@route.route("/api/getWbxCassAppService", methods=['POST'])
def get_wbxCassAppService():
    servicename = request.json['servicename']
    return getWbxCassAppService(servicename)

@route.route("/api/getWbxCassEnvAppServiceMap", methods=['POST'])
def get_wbxCassEnvAppServiceMap():
    serviceid = request.json['serviceid']
    return getWbxCassEnvAppServiceMap(serviceid)

@route.route("/api/getWbxCassEndpoints", methods=['POST'])
def get_wbxCassEndpoints():
    envid = request.json['envid']
    return getWbxCassEndpoints(envid)

@route.route("/api/addwbxCassAppService", methods=['POST'])
def add_cass_app_service():
    serviceid = request.json["serviceid"]
    servicename = request.json["servicename"]
    servicelevel = request.json["servicelevel"]
    return addCassAppService(serviceid,servicename,servicelevel)

@route.route("/api/addWbxCassEnvAppServiceMap", methods=['POST'])
def add_wbxCassEnvAppServiceMap():
    envid = request.json["envid"]
    serviceid = request.json["serviceid"]
    envtype = request.json["envtype"]
    applocation = request.json['applocation']
    webdomain = request.json['webdomain']
    return addWbxCassEnvAppServiceMap(envid,serviceid,envtype, applocation, webdomain)

@route.route("/api/getWbxCasscluster", methods=['POST'])
def get_WbxCasscluster():
    return getWbxCasscluster()

@route.route("/api/getWbxCassKeyspaceid", methods=['POST'])
def get_wbxCassKeyspaceid():
    casscluster = request.json["casscluster"]
    localdc = request.json["localdc"]
    keyspacename = request.json["keyspacename"]
    return getWbxCassKeyspaceid(casscluster,localdc,keyspacename)

@route.route("/api/addWbxCassKeyspaceEnvServiceMap", methods=['POST'])
def add_wbxCassKeyspaceEnvServiceMap():
    keyspaceid = request.json["keyspaceid"]
    serviceid = request.json["serviceid"]
    envtype = request.json["envtype"]
    applocation = request.json["applocation"]
    webdomain = request.json["webdomain"]
    return addWbxCassKeyspaceEnvServiceMap(keyspaceid,serviceid,envtype,applocation,webdomain)

@route.route("/api/updateWbxCassKeyspaceEnvServiceMap", methods=['POST'])
def update_WbxCassKeyspaceEnvServiceMap():
    keyspaceid = request.json["keyspaceid"]
    serviceid = request.json["serviceid"]
    envid = request.json["envid"]
    return updateWbxCassKeyspaceEnvServiceMap(keyspaceid, serviceid, envid)

@route.route("/api/deleteWbxCassKeyspaceEnvServiceMap", methods=['POST'])
def delete_WbxCassKeyspaceEnvServiceMap():
    keyspaceid = request.json["keyspaceid"]
    serviceid = request.json["serviceid"]
    envid = request.json["envid"]
    return deleteWbxCassKeyspaceEnvServiceMap(keyspaceid, serviceid, envid)

@route.route("/api/getWbxCassAppServiceByCMDB", methods=['POST'])
def get_WbxCassAppServiceByCMDB():
    servicename = request.json["servicename"]
    servicelevel = request.json["servicelevel"]
    return getWbxCassAppServiceByCMDB(servicename,servicelevel)

@route.route("/api/ora2pgdatamigration", methods=['POST'])
def ora2pgdatamigration():
    kwargs = request.json
    obj = ora2pgmigration()
    res = obj.run(**kwargs)
    return jsonify(res)


@route.route("/api/get-roomid-by-roomname/<room_name>", methods=['POST', 'GET'])
def getroomidbyroomname(room_name):
    return jsonify(wbxchatbot().get_roomid_by_roomname(room_name))


@route.route("/api/setpgalertrule", methods=['POST'])
def setpgalertrules():
    kwargs = request.json
    res = setpgalertrule(**kwargs)
    return jsonify(res)

@route.route("/api/deletepgalertrule", methods=['POST'])
def deletepgalertrules():
    alert_id = request.json["alert_id"]
    res = deletepgalertrule(alert_id)
    return jsonify(res)

@route.route("/api/getpgalertrule", methods=['POST'])
def getpgalertrulelist():
    alert_type = request.json["alert_type"]
    res = getpgalertrule(alert_type)
    return jsonify(res)

@route.route("/api/getpgalerttype", methods=['POST'])
def getpgalerttype():
    res = getpgalertruletypes()
    return jsonify(res)

@route.route("/api/getschemaregistryforlma", methods=['POST'])
def get_schema_registry_for_lma():
    res = getpgalertruletypes()
    return jsonify(res)

@route.route("/api/pgdb_info", methods=['POST'])
def pgdb_info():
    if request.method == 'POST':
        kwargs = request.json
        return jsonify(get_pgdb_info(**kwargs))