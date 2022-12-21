import sys
import smtplib
import http.client

from email.mime.text import MIMEText
from email.header import Header
import logging

emailthreadpool = None
logger = logging.getLogger("DBAMONITOR")

class wbxemailtype:
    EMAILTYPE_VERIFY_SCHEMA="Schema login failed"
    EMAILTYPE_ACCOUNT_LOGIN_FAILED="Account Login Failed"
    EMAILTYPE_CHECK_DB_PARAMETER = "The db parameter is changed"
    EMAILTYPE_DBHEALTH_EXAM= "DB Health Examination daily"
    EMAILTYPE_SESSION_BLOCKED = "Alert: Found blocked Session"
    EMAILTYPE_DB_DATA_EXAMINATION = "Alert: DBDataExamination Job Failed"
    EMAILTYPE_DB_STAT_EXAMINATION = "Alert: DB has performance issue"
    EMAILTYPE_SHAREPLEX_MONITOR = "Alert: There are shareplex backlog"
    EMAILTYPE_CRONJOB_INSTALLATION = "Alert: standard cronjob is not installed"
    EMAILTYPE_CRONJOB_EXECUTION_FAIL = "Alert: standard cronjob executed failed"
    EMAILTYPE_ALTER_USER_PSW = "User password has been changed"

    EMAILTYPE_ARCHIVELOG_LOGMNR = "Archivelog Logminr end"
    EMAILTYPE_WEBDOMAIN_DATA_MONITOR = "PCN job status"

    EMAIL_FORMAT_PLAIN="plain"
    EMAIL_FORMAT_HTML = "html"

class wbxemailmessage:
    def __init__(self, emailtopic, emailcontent, priority = 10, receiver="zhiwliu@cisco.com", issendtospark="N", emailformat="plain"):
        self.priority = priority
        self.sender = "dbamonitortool@cisco.com"
        self.receiver = receiver
        self.emailtopic = emailtopic
        self.emailcontent = emailcontent
        self.emailformat = emailformat
        self.issendtospark = issendtospark

    def __str__(self):
        return self.emailcontent

#sendmail(self, from_addr, to_addrs, msg, mail_options=[],rcpt_options=[]):
def sendemail(emailmsg):
    if isinstance(emailmsg, wbxemailmessage):
        try:
            mailto = emailmsg.receiver.split(",")
            message = MIMEText(emailmsg.emailcontent,_subtype = emailmsg.emailformat, _charset = 'utf-8')
            message['From'] = Header(emailmsg.sender)
            message['To'] = Header(",".join(mailto))
            subject = emailmsg.emailtopic
            message['Subject'] = Header(subject)

            smtpObj = smtplib.SMTP(host='mda.webex.com:25')
            senderrs= smtpObj.sendmail(emailmsg.sender, mailto, message.as_string())
            if len(senderrs) > 0:
                logger.error("Unexpected error:{0}".format(senderrs))
            smtpObj.quit()
        except smtplib.SMTPException:
            logger.error("Unexpected error:", sys.exc_info()[0])


def sendtospark(message):
    try:
        conn = http.client.HTTPSConnection("stap.webex.com")
        message = message.replace("\n", "<br>")
        payload = "{\"message\":\"%s\"}" % message
        print(payload)
        headers = {
            'authorization': "Basic c3RhcDpFTkc=",
            'content-type': "application/json",
            'cache-control': "no-cache"
        }
        conn.request("POST", "/sparkAPI/client/sendMsg/Test%20INC", payload, headers)
        res = conn.getresponse()
        data = res.read()
    except Exception as e:
        logger.error("Unexpected error:", sys.exc_info()[0])

def sendalert(emailmsg):
    if emailmsg.receiver is not None:
        sendemail(emailmsg)
    if emailmsg.issendtospark == 'Y':
        sendtospark(emailmsg.emailcontent)


if __name__ == "__main__":
    volist = [{"username":'tahoe', "userhost":"sjdbbtsmct1.webex.com", "failedcount":1},
              {"username": 'tahoe', "userhost": "sjdbgridcontrol1.webex.com", "failedcount": 10}]

    emailcontent = "%-30s%-30s%-10s\n-----------------------------------------------------------\n" % ("username", "userhost", "failedtimes")

    for vo in volist:
        emailcontent = "%s%-50s%-50s%-30s\n" % (emailcontent, vo["username"], vo["userhost"], vo["failedcount"])

    msg1 = wbxemailmessage(wbxemailtype.EMAILTYPE_ACCOUNT_LOGIN_FAILED, emailcontent)
    sendemail(msg1)
    # msg2 = wbxemailmessage(wbxemailtype.EMAILTYPE_VERIFY_SCHEMA, "wbxmaint schema can not login database")
    # sendemailasync(msg2)
    # time.sleep(10)
    #
    # msg3 = wbxemailmessage(wbxemailtype.EMAILTYPE_VERIFY_SCHEMA, "splex19063 schema can not login database")
    # sendemailasync(msg3)
    # msg4 = wbxemailmessage(wbxemailtype.EMAILTYPE_VERIFY_SCHEMA, "stap_ro schema can not login database")
    # sendemailasync(msg4)
    # time.sleep(50)