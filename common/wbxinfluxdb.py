from influxdb import InfluxDBClient
import math
import logging
import base64
from common.singleton import Singleton
from common.wbxutil import wbxutil
from common.Config import Config
import datetime
logger = logging.getLogger("DBAMONITOR")


@Singleton
class wbxinfluxdb:
    def __init__(self):

        self._username = "admin"  # "yejfeng"#
        self._pwd = b'aW5mbHV4QE5paGFv'  # b"WUp4bjM5NSM="
        self._client = Config().getInfluxDBclient()

    def utctime(self, time):
        timeformat = "%Y-%m-%d %H:%M:%S"
        aaa_strptime = datetime.datetime.strptime(time, timeformat)
        dateformat = "%Y-%m-%dT%H:%M:%SZ"
        return datetime.datetime.strftime(aaa_strptime, dateformat)

    def utctostr(self, time):
        timeformat = "%Y-%m-%dT%H:%M:%SZ"
        aaa_strptime = datetime.datetime.strptime(time, timeformat)
        dateformat = "%Y-%m-%d %H:%M:%S"
        return datetime.datetime.strftime(aaa_strptime, dateformat)

    def get_export_data(self, src_host, tgt_host, queue_name):
        # {'time': '2020-06-28T00:57:03Z', 'backlog': 0.0, 'db_name': None, 'delaytime': None, 'host': 'rsdboradiag002.webex.com', 'host_name': 'rsdboradiag002', 'operation': 600008.0, 'port': '19010', 'process_type': 'export', 'queuename': None, 'replication_to': 'NONE', 'src_db': None, 'src_host': 'rsdboradiag002-vip', 'src_queuename': 'Gnew_CFG2NewCV', 'status': 1.0, 'tgt_db': None, 'tgt_host': 'sjdbormt011-vip', 'tgt_queuename': 'Gnew_CFG2NewCV', 'tgt_sid': 'RACCVWEB_SPLEX'}
        sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'export' and " \
              "src_host = '%s-vip' and tgt_host = '%s-vip' and tgt_queuename = '%s' order by time desc" \
              % (src_host, tgt_host, queue_name)
        result = self._client.query(sql)
        return result

    def get_read_data(self, src_host, src_db):
        sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'read' and src_host = '%s-vip' and src_db = '%s' order by time desc" % (
        src_host, src_db)
        result = self._client.query(sql)
        return result

    def get_os_info_for_chatbot(self):
        rst_dict = {}
        sql = "select total,available,huge_pages_total,host from mem where time > now() - 1d group by host order by time desc limit 1"
        result = self._client.query(sql)
        if not result:
            return rst_dict
        points = result.get_points()
        for item in points:
            rst_dict[item["host"].split(".")[0]] = {
                "mem_total": address_string_number(item["total"]),
                "mem_available": address_string_number(item["available"]),
                "huge_page_size": address_string_number(item["huge_pages_total"])
            }
        return rst_dict

    def get_capture_data(self, src_host, src_db):
        sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'capture' and src_host = '%s-vip' and src_db = '%s' order by time desc" % (
            src_host, src_db)
        result = self._client.query(sql)
        return result

    def get_import_data(self, src_host, tgt_host, queue_name):
        sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'import' and src_host = '%s-vip' and tgt_host = '%s-vip' and queuename = '%s' order by time desc" % (
            src_host, tgt_host, queue_name)
        result = self._client.query(sql)
        return result

    def get_post_data(self, src_db, tgt_db, queue_name):
        sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'post' and src_db = '%s' and tgt_db = '%s' and queuename = '%s' order by time desc" % (
            src_db, tgt_db, queue_name)
        result = self._client.query(sql)
        return result

    def get_latest_export_data(self, src_host, tgt_host, replication_to):
        sql = "select * from shareplex_process where time > now() - 1d  and process_type = 'export' and " \
              "src_host = '%s-vip' and tgt_host = '%s-vip' and replication_to = '%s' order by time desc limit 1" \
              % (src_host, tgt_host, replication_to)
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        rst_dict = {}
        for item in points:
            rst_dict["queuename"] = item["tgt_queuename"]
            rst_dict["operation"] = item["operation"]
            rst_dict["status"] = item["status"]
            rst_dict["delaytime"] = item["delaytime"]
            rst_dict["backlog"] = item["backlog"]
            rst_dict["process_type"] = item["process_type"]
        return rst_dict

    def get_latest_read_data(self, src_host, src_db):
        sql = "select * from shareplex_process where time > now() - 1d  and process_type = 'read' " \
              "and src_host = '%s-vip' and src_db = '%s' order by time desc limit 1" % (
        src_host, src_db)
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        rst_dict = {}
        for item in points:
            rst_dict["queuename"] = item["queuename"]
            rst_dict["operation"] = item["operation"]
            rst_dict["status"] = item["status"]
            rst_dict["delaytime"] = item["delaytime"]
            rst_dict["backlog"] = item["backlog"]
            rst_dict["process_type"] = item["process_type"]
        return rst_dict

    def get_latest_capture_data(self, src_host, src_db):
        sql = "select * from shareplex_process where time > now() - 1d  and process_type = 'capture' " \
              "and src_host = '%s-vip' and src_db = '%s' order by time desc limit 1" % (
            src_host, src_db)
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        rst_dict = {}
        for item in points:
            rst_dict["queuename"] = item["queuename"]
            rst_dict["operation"] = item["operation"]
            rst_dict["status"] = item["status"]
            rst_dict["delaytime"] = item["delaytime"]
            rst_dict["backlog"] = item["backlog"]
            rst_dict["process_type"] = item["process_type"]
        return rst_dict

    def get_latest_import_data(self, src_host, tgt_host, queue_name):
        sql = "select * from shareplex_process where time > now() - 1d  and process_type = 'import' " \
                    "and src_host = '%s-vip' and tgt_host = '%s-vip' and queuename = '%s' order by time desc limit 1" % (
            src_host, tgt_host, queue_name)
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        rst_dict = {}
        for item in points:
            rst_dict["queuename"] = item["queuename"]
            rst_dict["operation"] = item["operation"]
            rst_dict["status"] = item["status"]
            rst_dict["delaytime"] = item["delaytime"]
            rst_dict["backlog"] = item["backlog"]
            rst_dict["process_type"] = item["process_type"]
        return rst_dict

    def get_latest_post_data(self, src_db, tgt_db, queue_name):
        sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'post' " \
              "and src_db = '%s' and tgt_db = '%s' and queuename = '%s' order by time desc limit 1" % (
            src_db, tgt_db, queue_name)
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        rst_dict = {}
        for item in points:
            rst_dict["queuename"] = item["queuename"]
            rst_dict["operation"] = item["operation"]
            rst_dict["status"] = item["status"]
            rst_dict["delaytime"] = item["delaytime"]
            rst_dict["backlog"] = item["backlog"]
            rst_dict["process_type"] = item["process_type"]
        return rst_dict

    def get_src_db_data(self):
        sql = "show tag values from splex_monitor_adb_real_delay with key=src_db_name; "
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        src_db = []
        print(points)
        for item in points:
            rst_dict = {"label": item["value"],"value": item["value"]}
            src_db.append(rst_dict)
            print(rst_dict, src_db)
        return src_db

    def get_tgt_db_data_by_src_db_name(self,src_db_name):
        sql = "select  delayinsecond,tgt_db_name from splex_monitor_adb_real_delay where src_db_name='%s'; " % src_db_name
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        tgt_db = []
        tgt_result = []
        for item in points:
            tgt_db.append(item["tgt_db_name"])
        tgt_result = list(set(tgt_db))
        # print(tgt_result)
        return tgt_result

    def get_delay_by_src_db_name_and_time(self, start_time, end_time, src_db_name, tgt_db_name):
        sql = "select time, delayinsecond from splex_monitor_adb_real_delay where splex_port > '0' and src_db_name = '%s' and tgt_db_name = '%s' and time >= '%s' and time < '%s'" % (src_db_name, tgt_db_name, self.utctime(start_time), self.utctime(end_time) )
        result = self._client.query(sql)
        # print(sql)
        if not result:
            return {}
        points = result.get_points()
        delay = []
        for item in points:
            # print(item)
            delay.append({"time": self.utctostr(item["time"]),"delayinsecond": item["delayinsecond"]})
        return delay

    def test(self):
        # {'time': '2020-06-28T07:15:01Z', 'backlog': None, 'db_name': None, 'delaytime': None,
        #  'host': 'lndbormt027.webex.com', 'host_name': 'lndbormt027', 'operation': 600042.0, 'port': '19007',
        #  'process_type': 'import', 'queuename': 'nGCFG2NEWI', 'replication_to': None, 'src_db': None,
        #  'src_host': 'rsdboradiag002-vip', 'src_queuename': None, 'status': 1.0, 'tgt_db': None,
        #  'tgt_host': 'lndbormt027-vip', 'tgt_queuename': None, 'tgt_sid': None}
        # sql = "select * from shareplex_process where time > now() - 1d and port = '17003' order by time asc limit 30"
        # sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'export' and tgt_queuename='nGCFG2NEWI'"
        # sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'export' and src_host = 'rsdboradiag002-vip' and tgt_host = 'sjdbormt011-vip'"
        #sql = "show tag values from splex_monitor_adb_real_delay with key=src_db_name;"
        #delayinsecond
        sql = "select * from splex_monitor_adb_real_delay where splex_port > '0' limit 10"
        # sql = "select time, delayinsecond from splex_monitor_adb_real_delay where src_db_name = 'IDPRDAM' and tgt_db_name = 'IDPRDFAR' and time >= '2022-03-11T00:00:00Z' and time < '2022-03-22T18:02:00Z'"
        # sql = "select delayinsecond from splex_monitor_adb_real_delay where src_db_name = 'CONFIGDB' and time >= "+"'"+self.utctime('2022-3-18 00:00:00')+"'"+" and time < "+"'"+self.utctime('2022-3-18 01:00:00')+"'"
        # sql = "show field keys from splex_monitor_adb_real_delay; "
        print(sql)
        result = self._client.query(sql)
        return result


    def get_opteration_item(self, result):
        points = result.get_points()
        rst_dict = {}
        tem = 0
        for item in points:
            currtime = wbxutil.convertTZtimeToDatetime(item["time"])
            currtime = wbxutil.convertDatetimeToString(currtime)
            if item["operation"] - tem >= 0:
                rst_dict[currtime] = item["operation"] - tem
            tem = item["operation"]
        return rst_dict

    def get_backlog_item(self, result):
        points = result.get_points()
        rst_dict = {}
        for item in points:
            currtime = wbxutil.convertTZtimeToDatetime(item["time"])
            currtime = wbxutil.convertDatetimeToString(currtime)
            if item["backlog"] >= 0:
                rst_dict[currtime] = item["backlog"]
        return rst_dict

    def get_delaytime_item(self, result):
        points = result.get_points()
        rst_dict = {}
        for item in points:
            currtime = wbxutil.convertTZtimeToDatetime(item["time"])
            currtime = wbxutil.convertDatetimeToString(currtime)
            if item["delaytime"] >= 0:
                rst_dict[currtime] = item["delaytime"]
        return rst_dict

    def get_queuename_item(self, x_result):
        points = x_result.get_points()
        rst_dict = []
        for item in points:
            rst_dict.append(item["tgt_queuename"])
        rst_dict = list(set(rst_dict))
        return rst_dict

    def get_active_session_count(self):
        sql = """
        SELECT db_inst_name, db_name, last("active_session_count") as "active_session_count" FROM (SELECT max("db_session_stat_value") as "active_session_count" FROM "wbxdb_monitor_session" WHERE ("db_session_status" = 'ACTIVE' AND "db_service_name" != 'Backend') AND time > now() - 6h GROUP BY time(1m), "db_name", "db_inst_name" ) GROUP BY  "db_name", "db_inst_name"
        """
        # sql = "select * from wbxdb_monitor_session order by time desc limit 1"
        points = self._client.query(sql)
        result = []
        for item in points:
            result.append(item[0])
        return sorted(result, key=lambda dbitem: dbitem["active_session_count"], reverse=True)


def address_string_number(item):
    if not item:
        return item
    if isinstance(item, str) and not item.isdigit():
        return item
    else:
        item = int(item)
    if item / 1024 / 1024 / 1024 > 1:
        return "%sG" % math.ceil(item / 1024 / 1024/ 1024)
    if item / 1024 / 1024 > 1:
        return "%sM" % math.ceil(item / 1024 / 1024)
    if item / 1024 > 1:
        return "%sK" % math.ceil(item / 1024)
    return "%sB" % item


if __name__ == '__main__':
    influx_db_obj = wbxinfluxdb()
    a = {"port":"24504","src_db":"RACINFRG","src_host":"tadborbf06","tgt_db":"RACINFRA","tgt_host":"sjdborbf06","replication_to":"gsb2p"}
    # export_data = influx_db_obj.get_export_data(a["src_host"], a["tgt_host"], a["replication_to"])
    data = influx_db_obj.test()
    # data = influx_db_obj.get_delay_by_src_db_name_and_time('2022-3-11 00:00:00','2022-3-22 18:02:00','CONFIGDB','RACAIWEB')
    # print(data)
    points = data.get_points()
    for item in points:
        print(item)

# capture
# {'time': '2020-06-23T01:57:04Z', 'backlog': None, 'db_name': 'GCFGDB_SPLEX', 'delaytime': 0.0, 'host': 'rsdboradiag002.webex.com', 'host_name': 'rsdboradiag002', 'operation': 58836.0, 'port': '19086', 'process_type': 'capture', 'queuename': None, 'src_db': None, 'src_host': None, 'status': 1.0, 'tgt_db': None, 'tgt_host': None}
# export
# {'time': '2020-06-25T13:39:04Z', 'backlog': 0.0, 'db_name': None, 'delaytime': None, 'host': 'rsdboradiag002.webex.com', 'host_name': 'rsdboradiag002', 'operation': 31646.0, 'port': '17005', 'process_type': 'export', 'queuename': None, 'replication_to': 'NONE', 'src_db': None, 'src_host': 'rsdboradiag002-vip', 'src_queuename': 'GCFG2TTACOMB2', 'status': 1.0, 'tgt_db': None, 'tgt_host': 'tadbth441-vip', 'tgt_queuename': 'GCFG2TTACOMB2', 'tgt_sid': 'TTACOMB2_SPLEX'}
# post
# {'time': '2020-06-23T02:00:02Z', 'backlog': 0.0, 'db_name': None, 'delaytime': 0.0, 'host': 'tadbrpt1.webex.com', 'host_name': 'tadbrpt1', 'operation': 8399185.0, 'port': '21010', 'process_type': 'post', 'queuename': 'reftrptBign', 'src_db': 'RACOPDB_SPLEX', 'src_host': None, 'status': 1.0, 'tgt_db': 'RACTARPT_SPLEX', 'tgt_host': None}
# read
# {'time': '2020-06-23T02:00:01Z', 'backlog': 0.0, 'db_name': 'RACAMCSP_SPLEX', 'delaytime': None, 'host': 'amdbormt011', 'host_name': 'amdbormt011', 'operation': 135684.0, 'port': '19024', 'process_type': 'read', 'queuename': None, 'src_db': None, 'src_host': None, 'status': 1.0, 'tgt_db': None, 'tgt_host': None}
# import
# {'time': '2020-06-25T13:27:04Z', 'backlog': None, 'db_name': None, 'delaytime': None, 'host': 'rsdboradiag002.webex.com', 'host_name': 'rsdboradiag002', 'operation': 440.0, 'port': '24511', 'process_type': 'import', 'queuename': 'RACPTN_Nn', 'replication_to': None, 'src_db': None, 'src_host': 'sjdbormt062-vip', 'src_queuename': None, 'status': 1.0, 'tgt_db': None, 'tgt_host': 'rsdboradiag002-vip', 'tgt_queuename': None, 'tgt_sid': None}
