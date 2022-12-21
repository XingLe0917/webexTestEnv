import logging
import json
import base64
import threading
import uuid
from datetime import datetime
from requests.auth import HTTPBasicAuth
from email.mime.text import MIMEText
from common.wbxmail import wbxemailmessage, wbxemailtype, sendemail
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from sqlalchemy.exc import  DBAPIError, DatabaseError
from common.wbxexception import wbxexception
from common.wbxinfluxdb import wbxinfluxdb
from common.wbxtask import wbxautotask
from common.wbxtask import threadlocal
from datetime import datetime
from collections import OrderedDict
from cacheout import LRUCache
from common.wbxutil import wbxutil
from dao.vo.autotaskvo import wbxautotaskvo, wbxautotaskjobvo
from common.wbxchatbot import wbxchatbot


curcache = LRUCache(maxsize=1024)
logger = logging.getLogger("DBAMONITOR")


def list_oncall_handover(start_time, end_time):
    start_time = wbxutil.convertStringtoDateTime(start_time)
    end_time = wbxutil.convertStringtoDateTime(end_time)
    status = "SUCCEED"
    errormsg = ""
    data = []
    logger.info("Starting to list_oncall_handover...")
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data = depotdbDao.get_oncall_handover(start_time, end_time)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to list_oncall_handover by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "data": data,
                "errormsg": errormsg}


def send_oncall_handoff_mail(start_time, end_time):
    start_time = wbxutil.convertStringtoDateTime(start_time)
    end_time = wbxutil.convertStringtoDateTime(end_time)
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to send_oncall_handoff_mail...")
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        oncall_handover_list = depotdbDao.get_oncall_handover(start_time, end_time)
        send_mail(oncall_handover_list)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to send_oncall_handoff_mail by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "errormsg": errormsg}


def send_mail(oncall_handover_list):
    logger.info("Starting to send_oncall_handoff_mail...")

    if not oncall_handover_list:
        return True

    updated_oncall_data = oncall_handover_list[0]
    created_by = updated_oncall_data["created_by"]
    oncall_date = updated_oncall_data["oncall_date"]
    from_shift = updated_oncall_data["from_to"].split("/")[0]
    to_shift = updated_oncall_data["from_to"].split("/")[-1]
    timestramp = "%A, %B %d %Y"
    table_timestramp = "%d-%b-%Y"
    oncall_date = wbxutil.convertStringtoDateTime(oncall_date)
    updated_oncall_date = oncall_date.strftime(timestramp)

    emailcontent = """
<br />
<p style="font-family:Monospace;">Hi,</p>
<p style="font-family:Monospace;">Below are the on-call handoff for {0} from {1} to {2}</p>
<p style="font-family:Monospace;">~ {3} \n</p>
<br />
    """.format(updated_oncall_date, from_shift, to_shift, created_by)
    html = """
    <head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <body>
    <div id="container">
      <div id="content">
       <table border="0" cellspacing="2" style="font-size: 13px;">
      <tr style="background-color:#e4e4e4;font-family: Microsoft YaHei;font-size: 12px;">
        <th style="width: 100px;">On-Call Date</th>
        <th>Classification/Severity</th>
        <th style="width: 100px;">From/To</th>
        <th style="width: 100px;">Created By</th>
        <th style="width: 100px;">Category</th>
        <th style="width: 100px;">Cluster</th>
        <th style="width: 100px;">Status</th>
        <th>Description</th>
      </tr>
      """
    is_odd = False
    for oncall_item in oncall_handover_list:
        if is_odd:
            html += """
        <tr style="background-color:#e4e4e4;font-family:Monospace;">"""
            is_odd = False
        else:
            html += """
        <tr style="background-color:#FFF;font-family:Monospace;">"""
            is_odd = True
        html += """
            <td>{0}</td>
            <td>{1}</td>
            <td>{2}</td>
            <td>{3}</td>
            <td>{4}</td>
            <td>{5}</td>
            <td>{6}</td>
            <td>{7}</td>
          </tr>""".format(wbxutil.convertStringtoDateTime(oncall_item["oncall_date"]).strftime(table_timestramp), oncall_item["classification_severity"], oncall_item["from_to"], oncall_item["created_by"], oncall_item["category"], oncall_item["cluster"], oncall_item["status"], oncall_item["description"])
    html += """
    </table>
      </div>
    </div>
    </body>"""

    emailtitle = "On-call handoff - {0} - {1}".format(
        updated_oncall_date, updated_oncall_data["from_to"])
    emailmsg = wbxemailmessage(emailtitle, emailcontent + html, receiver="cwopsdba@cisco.com", emailformat="html")
    # emailmsg = wbxemailmessage(emailtitle, emailcontent + html, receiver="yejfeng@cisco.com", emailformat="html")
    emailmsg.sender = "%s@cisco.com" % created_by.lower()
    sendemail(emailmsg)
    logger.info("send_oncall_handoff_mail end.")


def add_oncall_handoff(created_by, from_shift, to_shift, classification_severity, oncall_status, description, category, cluster):
    status = "SUCCEED"
    errormsg = ""
    data = []
    classification = classification_severity.split("/")[0]
    severity = classification_severity.split("/")[-1]
    logger.info("Starting to add_oncall_handoff...")
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data = depotdbDao.insert_oncall_handover(created_by, from_shift, to_shift, classification, severity,
                                                 oncall_status, description, category, cluster)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to list_oncall_handover by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "data": data,
                "errormsg": errormsg}


def get_oncall_cec():
    oncall_cec_cache_key = "CURRENT_ONCALL_CEC"
    if curcache.has(oncall_cec_cache_key):
        oncall_user_email = curcache.get(oncall_cec_cache_key)["oncall_mail"]
        return oncall_user_email
    from pdpyras import APISession
    api_token = "u+EKr6hAANd3NQFExEYg"
    oncall_team_name = "CEO-cwopsdba-Primary"
    session = APISession(api_token)
    oncall_user_list = []
    for item in session.iter_all('oncalls'):
        if item["schedule"] and item["schedule"]["summary"] == oncall_team_name:
            if item["user"] and item["user"]["summary"]:
                oncall_user_list.append(item["user"]["summary"])
    oncall_user_list = list(set(oncall_user_list))
    if len(oncall_user_list) != 1:
        wbxchatbot().alert_msg_to_person("the oncall list from pagerduty is %s which is abnormal!!" % oncall_user_list,
                                         "yejfeng@cisco.com")
    oncall_user_name = oncall_user_list[0]
    oncall_user_email = session.find('users', oncall_user_name, attribute="name").get("email", None)
    logger.info("oncall : %s" % oncall_user_email)
    curcache.add(oncall_cec_cache_key, {"oncall_mail": oncall_user_email}, ttl=3 * 60)
    return oncall_user_email
