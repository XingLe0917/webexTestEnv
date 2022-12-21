import logging
import datetime

import cx_Oracle
from pandas import np
from prettytable import PrettyTable

from common.wbxchatbot import wbxchatbot
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")


def getServerItemsForBot(rep,dates,percentile_80,percentile_90):
    if len(rep) == 0: return ""
    date_list = sorted(dates)
    x = PrettyTable()
    title = ["#", "count", "00", "01", "02", "03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"]
    for data in date_list:
        num_00 = "--"
        num_01 = "--"
        num_02 = "--"
        num_03 = "--"
        num_04 = "--"
        num_05 = "--"
        num_06 = "--"
        num_07 = "--"
        num_08 = "--"
        num_09 = "--"
        num_10 = "--"
        num_11 = "--"
        num_12 = "--"
        num_13 = "--"
        num_14 = "--"
        num_15 = "--"
        num_16 = "--"
        num_17 = "--"
        num_18 = "--"
        num_19 = "--"
        num_20 = "--"
        num_21 = "--"
        num_22 = "--"
        num_23 = "--"
        if str(data) +" 00" in rep:
            num_00 = rep[str(data) +" 00"]
            num_00 = "[" + str(num_00) + "]" if num_00 > percentile_80 else str(num_00)
        if str(data) + " 01" in rep:
            num_01 = rep[str(data) + " 01"]
            num_01 = "[" + str(num_01) + "]" if num_01 > percentile_80 else str(num_01)
        if str(data) + " 02" in rep:
            num_02 = rep[str(data) + " 02"]
            num_02 = "[" + str(num_02) + "]" if num_02 > percentile_80 else str(num_02)
        if str(data) + " 03" in rep:
            num_03 = rep[str(data) + " 03"]
            num_02 = "[" + str(num_03) + "]" if num_03 > percentile_80 else str(num_03)
        if str(data) +" 04" in rep:
            num_04 = rep[str(data) +" 04"]
            num_04 = "[" + str(num_04) + "]" if num_04 > percentile_80 else str(num_04)
        if str(data) + " 05" in rep:
            num_05 = rep[str(data) + " 05"]
            num_05 = "[" + str(num_05) + "]" if num_05 > percentile_80 else str(num_05)
        if str(data) + " 06" in rep:
            num_06 = rep[str(data) + " 06"]
            num_06 = "[" + str(num_06) + "]" if num_06 > percentile_80 else str(num_06)
        if str(data) + " 07" in rep:
            num_07 = rep[str(data) + " 07"]
            num_07 = "[" + str(num_07) + "]" if num_07 > percentile_80 else str(num_07)
        if str(data) + " 08" in rep:
            num_08 = rep[str(data) + " 08"]
            num_08 = "[" + str(num_08) + "]" if num_08 > percentile_80 else str(num_08)
        if str(data) + " 09" in rep:
            num_09 = rep[str(data) + " 09"]
            num_09 = "[" + str(num_09) + "]" if num_09 > percentile_80 else str(num_09)
        if str(data) + " 10" in rep:
            num_10 = rep[str(data) + " 10"]
            num_10 = "[" + str(num_10) + "]" if num_10 > percentile_80 else str(num_10)
        if str(data) + " 11" in rep:
            num_11 = rep[str(data) + " 11"]
            num_11 = "[" + str(num_11) + "]" if num_11 > percentile_80 else str(num_11)
        if str(data) + " 12" in rep:
            num_12 = rep[str(data) + " 12"]
            num_12 = "[" + str(num_12) + "]" if num_12 > percentile_80 else str(num_12)
        if str(data) + " 13" in rep:
            num_13 = rep[str(data) + " 13"]
            num_13 = "[" + str(num_13) + "]" if num_13 > percentile_80 else str(num_13)
        if str(data) + " 14" in rep:
            num_14 = rep[str(data) + " 14"]
            num_14 = "[" + str(num_14) + "]" if num_14 > percentile_80 else str(num_14)
        if str(data) + " 15" in rep:
            num_15 = rep[str(data) + " 15"]
            num_15 = "[" + str(num_15) + "]" if num_15 > percentile_80 else str(num_15)
        if str(data) + " 16" in rep:
            num_16 = rep[str(data) + " 16"]
            num_16 = "[" + str(num_16) + "]" if num_16 > percentile_80 else str(num_16)
        if str(data) + " 17" in rep:
            num_17 = rep[str(data) + " 17"]
            num_17 = "[" + str(num_17) + "]" if num_17 > percentile_80 else str(num_17)
        if str(data) + " 18" in rep:
            num_18 = rep[str(data) + " 18"]
            num_18 = "[" + str(num_18) + "]" if num_18 > percentile_80 else str(num_18)
        if str(data) + " 19" in rep:
            num_19 = rep[str(data) + " 19"]
            num_19 = "[" + str(num_19) + "]" if num_19 > percentile_80 else str(num_19)
        if str(data) + " 20" in rep:
            num_20 = rep[str(data) + " 20"]
            num_20 = "[" + str(num_20) + "]" if num_20 > percentile_80 else str(num_20)
        if str(data) + " 21" in rep:
            num_21 = rep[str(data) + " 21"]
            num_21 = "[" + str(num_21) + "]" if num_21 > percentile_80 else str(num_21)
        if str(data) + " 22" in rep:
            num_22 = rep[str(data) + " 22"]
            num_22 = "[" + str(num_22) + "]" if num_22 > percentile_80 else str(num_22)
        if str(data) + " 23" in rep:
            num_23 = rep[str(data) + " 23"]
            num_23 = "[" + str(num_23) + "]" if num_23 > percentile_80 else str(num_23)
        line_content = [data, dates[data], num_00, num_01, num_02, num_03, num_04, num_05, num_06,
                        num_07, num_08, num_09, num_10, num_11, num_12, num_13, num_14, num_15, num_16,
                        num_17, num_18, num_19, num_20, num_21, num_22, num_23]
        x.add_row(line_content)
    x.field_names = title
    return str(x)

def sendAlertToChatBot(content, alert_title, roomId):
    msg = "### %s \n" % (alert_title)
    msg += "```\n {0} \n```".format(content)
    logger.info(msg)
    wbxchatbot().alert_msg_to_dbabot_by_roomId(msg, roomId)

def getArchivelog(db_name,start_time,end_time):
    logger.info("getArchivelog db_name:{0},start_time:{1},end_time:{2}".format(db_name,start_time,end_time))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    retn = {}
    try:
        daoManager.startTransaction()
        spDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rows = spDao.getDBTnsInfo(db_name)
        daoManager.commit()
        if len(rows)==0:
            retn['code'] = 'fail'
            retn['message']=" %s is invalid due to no tns info" %(db_name)
            return retn
        item = rows[0]
        trim_host = item['trim_host']
        listener_port = item['listener_port']
        service_name = "%s.webex.com" % item['service_name']
        value = '(DESCRIPTION ='
        if item['scan_ip1']:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                value, item['scan_ip1'], listener_port)
        if item['scan_ip2']:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                value, item['scan_ip2'], listener_port)
        if item['scan_ip3']:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                value, item['scan_ip3'], listener_port)
        value = '%s (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' % (
            value, service_name)

        if start_time and end_time :
            try:
                datetime.datetime.strptime(start_time, '%Y-%m-%d')
                datetime.datetime.strptime(end_time, '%Y-%m-%d')
            except Exception as e:
                logging.error(str(e))
                retn['code'] = 'fail'
                retn['message'] = "Incorrect data format, should be YYYY-MM-DD"
                return retn

        list = []
        try:
            srcconn = cx_Oracle.connect("system/sysnotallow@"+value)
            cursor = srcconn.cursor()
            if start_time and end_time:
                cursor.prepare('''
                select to_char(trunc(first_time, 'hh'),'YYYY-MM-DD hh24') first_time,count(1) ARC
                from v$archived_log
               where first_time between to_date(:start_time,'YYYY-MM-DD') and to_date(:end_time,'YYYY-MM-DD')
                group by to_char(trunc(first_time, 'hh'),'YYYY-MM-DD hh24')
                order by 1''')
                cursor.execute(None,{'start_time': start_time,'end_time':end_time})
            else:
                sql = '''
                     select to_char(trunc(first_time, 'hh'),'YYYY-MM-DD hh24') first_time,count(1) ARC
                    from v$archived_log
                    where first_time between sysdate -7 and sysdate
                    group by to_char(trunc(first_time, 'hh'),'YYYY-MM-DD hh24')
                     order by 1
                     '''
                cursor.execute(sql)
            result = cursor.fetchall()
            col_name = cursor.description
            for row in result:
                dict = {}
                for col in range(len(col_name)):
                    key = col_name[col][0]
                    value = row[col]
                    dict[key] = value
                list.append(dict)
        except Exception as e:
            logging.error(str(e))
            retn['code'] = 'fail'
            retn['message'] = str(e)
            return retn
        # tt = spDao.getarchivelog(start_time,end_time)

        rep = {}
        date = {}
        quantile = []
        for vo in list:
            # item=dict(vo)
            first_time = vo['FIRST_TIME']
            first_date = first_time.split(" ")[0]
            if first_date not in date:
                date[first_date]=vo['ARC']
            else:
                count= date[first_date]
                date[first_date]=count+vo['ARC']
            arc_mb = vo['ARC']
            rep[first_time]=arc_mb
            quantile.append(arc_mb)
        print(len(quantile))
        retn['data']=rep
        retn['date']=date
        retn['percentile_80'] = round(np.percentile(quantile, 80))
        retn['percentile_90'] = round(np.percentile(quantile, 90))
        retn['code']='success'
        content = getServerItemsForBot(rep,date,retn['percentile_80'],retn['percentile_90'])
        msg = "### archivelog %s %s %s\n" % (db_name,start_time,end_time)
        msg += "```\n {} \n```".format(content)
        retn['msg'] = msg
        logger.info(msg)
        return retn
    except Exception as e:
        daoManager.rollback()
        logger.error(e)
        logger.error("getArchivelog error occurred", exc_info=e, stack_info=True)
        retn['code'] = 'fail'
        retn['message'] = str(e)
        return retn




