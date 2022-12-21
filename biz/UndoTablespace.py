
import logging

from common.Config import Config

logger = logging.getLogger("DBAMONITOR")

def getUndoTablespace():
    logger.info("getUndoTablespace start")
    config = Config.getConfig()
    influxDBclient = config.getInfluxDB_SJC_client()
    sql = "select * from db_undostat where time > now() - 10m order by time desc "
    results = influxDBclient.query(sql)
    points = results.get_points()
    num = 0
    alert_list = {}
    list = []
    try:
        for data in points:
            num += 1
            vo = dict(data)
            t = str(vo['time']).split("T")
            vo['time']= t[0]+" "+str(t[1])[0:8]
            if "." in str(vo['host_name']):
                vo['host_name'] = str(vo['host_name']).split(".")[0]
            # used_ratio = vo['used_ratio']
            db_name = vo['db_name']
            tablespace_name = vo['tablespace_name']
            key = db_name+"_"+tablespace_name
            if key not in alert_list:
                alert_list[db_name] = vo
                list.append(vo)
        logger.info("getUndoTablespace num:{0}".format(len(list)))
        list.sort(key=lambda k: (k.get('used_ratio', 0)), reverse=True)
        return list
    except Exception as e:
        logger.error("getUndoTablespace error occurred", exc_info=e, stack_info=True)

def getundoTablespaceByDBName(db_name,start_date,end_date):
    logger.info("getundoTablespaceByDBName db_name:{0},start_date:{1},end_date:{2}".format(db_name,start_date,end_date))
    config = Config.getConfig()
    influxDBclient = config.getInfluxDB_SJC_client()
    sql = "select * from db_undostat where db_name = '%s' and time > '%s' and time < '%s' order by time " %(db_name,start_date,end_date)
    print(sql)
    results = influxDBclient.query(sql)
    points = results.get_points()
    num = 0
    alert_list = {}
    try:
        for data in points:
            print(data)
            num += 1
            vo = dict(data)
            t = str(vo['time']).split("T")
            vo['time']= t[0]+" "+str(t[1])[0:8]
            if "." in str(vo['host_name']):
                vo['host_name'] =str(vo['host_name']).split(".")[0]
            # used_ratio = vo['used_ratio']
            db_name = vo['db_name']
            if db_name not in alert_list:
                alert_list[db_name]=[]
                alert_list[db_name].append(vo)
            else:
                alert_list[db_name].append(vo)
        # logger.info("getundoTablespaceByDBName db_name:{0},db_num:{1},start_date:{2},end_date:{3}".format(db_name,len(alert_list[db_name]),start_date,end_date))
        return alert_list
    except Exception as e:
        logger.error("getUndoTablespace error occurred", exc_info=e, stack_info=True)