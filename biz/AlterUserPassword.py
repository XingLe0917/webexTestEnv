import logging
import random
import string

from common.wbxmail import wbxemailmessage, sendemail, wbxemailtype
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")
def generaterPwd(trim_host,db_name,user_name):
    logger.info("generaterPwd, trim_host=%s db_name=%s user_name=%s" % (trim_host, db_name,user_name))
    flg = checkInfo(trim_host, db_name)
    result = {}
    if not flg:
        result['code'] = '0001'
        result['message'] = 'No records found, please check the information'
        return result
    result['code'] = '0000'
    passwords = create_password(1)
    result['password'] = list(passwords)[0]
    result['message'] = 'success'
    logging.info(result)
    return result

def updateUserPasswordInfo(password,trim_host,db_name,user_name,appln_support_code):
    logger.info("updateUserPassword, trim_host=%s db_name=%s user_name=%s password=%s appln_support_code=%s" % (trim_host, db_name, user_name,password,appln_support_code))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        applist = depotdbDao.getApplnList(trim_host, db_name,user_name)
        if len(applist)==0:
            depotdbDao.insertUserPwdInfo(password,trim_host,db_name,user_name,appln_support_code,"wbxmaint")
        else:
            depotdbDao.updateUserPwdInfo(password,trim_host,db_name,user_name)
        daoManager.commit()
        return True
    except Exception as e:
        daoManager.rollback()
        logger.error("updateUserPassword error occurred", exc_info=e, stack_info=True)
    return None

def alterUserPassword(daoManager,password,trim_host,db_name,user_name):
    logger.info("alterUserPassword, trim_host=%s db_name=%s user_name=%s password=%s" % (
    trim_host, db_name, user_name, password))
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        depotdbDao.updateUserPwd(user_name,password)
        daoManager.commit()
        return True
    except Exception as e:
        daoManager.rollback()
        logger.error("alterUserPassword error occurred", exc_info=e, stack_info=True)
    return None

'''
random password generator and length limit 8-12
generate num password at a time
'''
def create_password(num):
    passwords = set()
    punc = ['#',"$"]
    upper = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','0','P','Q','R','S','T','U','V','W','X','Y','Z']
    lower = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
             'v', 'w', 'x', 'y', 'z']
    digit = ['0','1','2','3','4','5','6','7','8','9']
    while len(passwords) != num:
        length = random.randint(8,12)
        password1 = random.sample(upper+lower+digit+punc,length)
        # 1. must have uppercase,
        # 2. must have lowercase,
        # 3. must have digit,
        # 4. must have punctuation,
        # 5. first must be letter
        if set(password1) & set(string.ascii_uppercase) and set(password1) & set(string.ascii_lowercase) and \
                set(password1) & set(string.digits) and set(password1) & set(punc) and password1[0] in string.ascii_letters:
            password = ''.join(password1)
            passwords.add(password)
    return passwords

'''
check database info
'''
def checkInfo(trim_host,db_name):
    logger.info("checkInfo, trim_host=%s db_name=%s " % (trim_host, db_name))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        applist = depotdbDao.checkInfo(trim_host, db_name)
        if len(applist)>0:
            return applist[0]
    except Exception as e:
        daoManager.rollback()
        logger.error("checkInfo error occurred", exc_info=e, stack_info=True)
    return None

def getApplnMappingInfo(trim_host,db_name):
    logger.info("getApplnMappingInfo, trim_host=%s db_name=%s " % (trim_host, db_name))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        applist = depotdbDao.applnMappingInfo(trim_host, db_name)
        if len(applist) > 0:
            return applist
    except Exception as e:
        daoManager.rollback()
        logger.error("getApplnMappingInfo error occurred", exc_info=e, stack_info=True)
    return None


def updateUserPassword_ori(password,trim_host,db_name,user_name):
    logger.info("updateUserPassword, trim_host=%s db_name=%s user_name=%s password=%s " % (trim_host, db_name,user_name,password))
    trim_host = trim_host.lower()
    db_name = db_name.upper()
    result = {}
    appinfo = checkInfo(trim_host,db_name)
    logger.info(appinfo)

    if not appinfo:
        result['code'] = '0001'
        result['message'] = 'No records found, please check the information'
        return result
    alter_flg = alterUserPassword(password,trim_host,db_name,user_name)

    if alter_flg:
        update_flg = updateUserPasswordInfo(password,trim_host,db_name,user_name,appinfo[2].lower())
        if update_flg:
            result['code'] = '0000'
            result['message'] = 'success'
            return result
        else:
            result['code'] = '0003'
            result['message'] = 'alter user password info fail'
            return result
    else:
        result['code'] = '0002'
        result['message'] = 'alter user password fail'
        return result

# 1. check input valid
# 2. if right in step 1 ,then do the following things:
#    1.alter user password
#    2.insert or update user password info in table appln_pool_info
#    3.send email ,if webdb send to csg-meetingops, else tahoe send to csg-avops
def updateUserPassword(password,trim_host,db_name,user_name):
    logger.info("updateUserPassword, trim_host=%s db_name=%s user_name=%s password=%s " % (trim_host, db_name,user_name,password))
    trim_host = trim_host.lower()
    db_name = db_name.upper()
    result = {}
    # check input valid
    appinfo = checkInfo(trim_host,db_name)
    logger.info(appinfo)

    if not appinfo:
        result['code'] = '0001'
        result['message'] = 'No records found, please check the information'
        return result
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    dbid = trim_host + "_" + db_name
    logger.info("bdid:" + dbid)
    daoManager = daoManagerFactory.getDaoManager(dbid)
    try:
        # alter user password
        alter_flg = alterUserPassword(daoManager,password,trim_host,db_name,user_name)
        if alter_flg:
            # insert or update user password info in table appln_pool_info
            appln_support_code = appinfo[2]
            update_flg = updateUserPasswordInfo(password,trim_host,db_name,user_name,appln_support_code.lower())
            if update_flg:
                result['code'] = '0000'
                result['message'] = 'success'
                # send email
                content = "The user password has been changed. Please note that.\n\n"
                content += "db name: " + db_name + "\n\n"
                applnMappingList = getApplnMappingInfo(trim_host,db_name)
                if applnMappingList:
                    for item in applnMappingList:
                        mapping_name = item[1]
                        schema = item[2]
                        content += "mapping name: " + mapping_name + "\n"
                        content += "schema: " + schema + "\n\n"
                receiver = ""
                logger.debug(appln_support_code.upper())
                send_mail = False
                if "TEL" == appln_support_code.upper():
                    receiver = "csg-avops@cisco.com"
                    send_mail = True
                if "WEB" == appln_support_code.upper():
                    receiver = "csg-meetingops@cisco.com"
                    send_mail = True
                if send_mail:
                    logger.debug("send to email: {0}, appln_support_code:{1}".format(receiver,appln_support_code))
                    msg1 = wbxemailmessage(emailtopic=wbxemailtype.EMAILTYPE_ALTER_USER_PSW, emailcontent=content, receiver=receiver)
                    sendemail(msg1)
                return result
            else:
                result['code'] = '0003'
                result['message'] = 'alter user password info fail'
                return result
        else:
            result['code'] = '0002'
            result['message'] = 'alter user password fail'
            return result
    except Exception as e:
        daoManager.rollback()
        logger.error("alterUserPassword error occurred", exc_info=e, stack_info=True)
    return None

if __name__ == "__main__":
    # passwords = create_password(1)
    # print(list(passwords)[0])
    # updateUserPassword("china2018","sjdbwbf","RACFWEB","wbxdba")


    trim_host="sjdbormt02"
    db_name="RACUWEB"
    receiver="lexing@cisco.com"
    content = "The user password has been changed. Please note that.\n\n"
    content = content + "db name: " + db_name + "\n"

    msg1 = wbxemailmessage(emailtopic=wbxemailtype.EMAILTYPE_ALTER_USER_PSW, emailcontent=content, receiver=receiver)
    sendemail(msg1)
