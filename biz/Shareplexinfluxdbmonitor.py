import logging
from common.wbxinfluxdb import wbxinfluxdb
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from common.wbxexception import wbxexception
from collections import OrderedDict
import threading

logger = logging.getLogger("DBAMONITOR")


def get_detailed_metric_monitor(src_db, tgt_db, src_host, tgt_host, queue_name, process_type, metric_type):
    status = "SUCCEED"
    rstDict = {}
    result_data = None
    influx_db_obj = wbxinfluxdb()
    if process_type == "import":
        result_data = influx_db_obj.get_import_data(src_host, tgt_host, queue_name)
        if not result_data:
            rstDict = "Cannot get import info by src_host:%s, tgt_host:%s queuename:%s in influxdb" % (
            src_host, tgt_host, queue_name)
            status = "FAIL"
    elif process_type == "export":
        result_data = influx_db_obj.get_export_data(src_host, tgt_host, queue_name)
        if not result_data:
            rstDict = "Cannot get export info by src_host:%s, tgt_host:%s, tgt_queuename:%s in influxdb" % (src_host, tgt_host, queue_name)
            status = "FAIL"
    elif process_type == "post":
        result_data = influx_db_obj.get_post_data(src_db, tgt_db, queue_name)
        if not result_data:
            rstDict = "Cannot get post info by src_db:%s, tgt_db:%s queuename:%s in influxdb" % (
            src_db, tgt_db, queue_name)
            status = "FAIL"
    elif process_type == "read":
        result_data = influx_db_obj.get_read_data(src_host, src_db)
        if not result_data:
            rstDict = "Cannot get read info by src_host:%s, src_db:%s in influxdb" % (src_host, src_db)
            status = "FAIL"
    elif process_type == "capture":
        result_data = influx_db_obj.get_capture_data(src_host, src_db)
        if not result_data:
            rstDict = "Cannot get capture info by src_host:%s, src_db:%s in influxdb" % (src_host, src_db)
            status = "FAIL"
    if not result_data:
        return {
            "status": status,
            "rstDict": rstDict
        }

    if metric_type == "opertaion":
        rstDict = influx_db_obj.get_opteration_item(result_data)
    elif metric_type == "backlog":
        rstDict = influx_db_obj.get_backlog_item(result_data)
    elif metric_type == "delaytime":
        rstDict = influx_db_obj.get_delaytime_item(result_data)
    return {
        "status": status,
        "rstDict": rstDict
    }


def get_splex_performance_monitor(src_db, tgt_db, src_host, tgt_host, replication_to):
    status = "SUCCEED"
    rstDict = OrderedDict()
    rstDict["capture"] = {}
    rstDict["read"] = {}
    rstDict["export"] ={}
    rstDict["import"] = {}
    rstDict["post"] = {}
    error_msg = {}
    influx_db_obj = wbxinfluxdb()
    # get export data
    export_data = influx_db_obj.get_latest_export_data(src_host, tgt_host, replication_to)
    if not export_data:
        error_msg["export"] = "Cannot get export info by src_host:%s, tgt_host:%s, replication_to:%s in influxdb" % (src_host, tgt_host, replication_to)
        error_msg["import"] = "Cannot get export queue info by src_host:%s, tgt_host:%s, replication_to:%s in influxdb" % (
        src_host, tgt_host, replication_to)
        error_msg["post"] = "Cannot get export queue info by src_host:%s, tgt_host:%s, replication_to:%s in influxdb" % (
        src_host, tgt_host, replication_to)
    else:
        rstDict["export"] = export_data
    # get read data
    read_data = influx_db_obj.get_latest_read_data(src_host, src_db)
    if not read_data:
        error_msg["read"] = "Cannot get read info by src_host:%s, src_db:%s in influxdb" % (src_host, src_db)
    else:
        rstDict["read"] = read_data
    # get capture data
    capture_data = influx_db_obj.get_latest_capture_data(src_host, src_db)
    if not capture_data:
        error_msg["capture"] = "Cannot get capture info by src_host:%s, src_db:%s in influxdb" % (src_host, src_db)
    else:
        rstDict["capture"] = capture_data
    # get import and post data
    if export_data:
        queue_name = export_data["queuename"]
        import_data = influx_db_obj.get_latest_import_data(src_host, tgt_host, queue_name)
        if not import_data:
            error_msg["import"] = "Cannot get import info by src_host:%s, tgt_host:%s queuename:%s in influxdb" % (src_host, tgt_host, queue_name)
        else:
            rstDict["import"] = import_data

        post_data = influx_db_obj.get_latest_post_data(src_db, tgt_db, queue_name)
        if not post_data:
            error_msg["post"] = "Cannot get post info by src_db:%s, tgt_db:%s queuename:%s in influxdb" % (
            src_db, tgt_db, queue_name)
        else:
            rstDict["post"] = post_data
    if error_msg:
        status = "FAIL"
    return {
        "status": status,
        "rstDict": list(rstDict.values()),
        "error_msg": error_msg
    }


if __name__ == '__main__':
    a = {"src_db": "RACTARPT", "src_host": "tadbrpt2", "tgt_db": "RACGDIAG", "tgt_host": "rsdboradiag002",
     "replication_to": "NONE"}
    print(get_gg_performance_monitor(a["src_db"], a["tgt_db"], a["src_host"], a["tgt_host"], a["replication_to"]))


