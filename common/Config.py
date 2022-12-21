import base64
import os
import json

from influxdb import InfluxDBClient

from common.wbxexception import wbxexception

class Config:

    def __init__(self):
        self.CONFIGFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), "conf")
        self.loadDBAMonitorConfigFile()
        self.DEPOT_URL="depotdb_url"
        self.DEPOT_DBNAME="depotdb_dbname"
        self.DEPOT_USERNAME="depotdb_username"
        self.DEPORT_PASSWORD="depotdb_password"
        self.PGDEPOT_URL = "pg_depotdb_url"
        self.PGDEPOT_PORT = "pg_depotdb_port"
        self.PGDEPOT_DBNAME = "pg_depotdb_dbname"
        self.PGDEPOT_USERNAME = "pg_depotdb_username"
        self.PGDEPORT_PASSWORD = "pg_depotdb_password"
        self.PGDEPORT_SSLCERT = "pg_sslcert_file"
        self.PGDEPORT_SSLKEY = "pg_sslkey_file"
        self.PGDEPORT_SSLROOTCERT = "pg_sslrootcert_file"
        self.REDIS_SERVERS="redis-servers"
        self.ENABLE_SHAREPLEXCONFIGFILE_MONITOR = "enable_shareplexconfigfile_monitor"
        self.ENABLE_SHAREPLEXREPLICATION_MONITOR = "enable_shareplexreplication_monitor"
        self.InfluxDB_ip = "influxDB_ip",
        self.InfluxDB_port = "influxDB_port",
        self.InfluxDB_user = "influxDB_user",
        self.InfluxDB_pwd = "influxDB_pwd"

    @staticmethod
    def getConfig():
        return Config()

    def getDBAMonitorConfigFile(self):
        dbamonitor_config_file = os.path.join(self.CONFIGFILE_DIR, "dbamonitortool_config.json")
        # dbamonitor_config_file = os.path.join(self.CONFIGFILE_DIR, "bjdbamonitortool_config.json")
        if not os.path.isfile(dbamonitor_config_file):
            raise wbxexception("%s does not exist" % dbamonitor_config_file)
        return dbamonitor_config_file

    def getLoggerConfigFile(self):
        logger_config_file = os.path.join(self.CONFIGFILE_DIR, "logger.conf")
        if not os.path.isfile(logger_config_file):
            raise wbxexception("%s does not exist" % logger_config_file);
        return logger_config_file

    def getJobManagerLoggerConfigFile(self):
        logger_config_file = os.path.join(self.CONFIGFILE_DIR, "jobmanager_logger.conf")
        if not os.path.isfile(logger_config_file):
            raise wbxexception("%s does not exist" % logger_config_file);
        return logger_config_file

    def getdbcaRspConfigFile(self):
        dbca_rsp = os.path.join(self.CONFIGFILE_DIR, "dbca.rsp")
        if not os.path.isfile(dbca_rsp):
            raise wbxexception("%s does not exist" % dbca_rsp);
        return dbca_rsp

    def getdbAuditConfigFile(self):
        dbaudit_conf = os.path.join(self.CONFIGFILE_DIR, "dbaudit.xml")
        if not os.path.isfile(dbaudit_conf):
            raise wbxexception("%s does not exist" % dbaudit_conf);
        return dbaudit_conf

    def getdbcaDbtConfigFile(self):
        dbca_dbt = os.path.join(self.CONFIGFILE_DIR, "General_Purpose.dbt")
        if not os.path.isfile(dbca_dbt):
            raise wbxexception("%s does not exist" % dbca_dbt);
        return dbca_dbt

    def loadDBAMonitorConfigFile(self):
        dbamonitor_config_file= self.getDBAMonitorConfigFile()
        f = open(dbamonitor_config_file, "r")
        self.dbconnectionDict = json.load(f)
        f.close()

    def getDepotConnectionurl(self):
        return (self.dbconnectionDict[self.DEPOT_USERNAME],self.dbconnectionDict[self.DEPORT_PASSWORD],self.dbconnectionDict[self.DEPOT_URL])

    def getPGDepotConnectionurl(self):
        DBConnectionUrl="%s:%s/%s" % (self.dbconnectionDict[self.PGDEPOT_URL], self.dbconnectionDict[self.PGDEPOT_PORT], self.dbconnectionDict[self.PGDEPOT_DBNAME].lower())
        return (self.dbconnectionDict[self.PGDEPOT_USERNAME],self.dbconnectionDict[self.PGDEPORT_PASSWORD],DBConnectionUrl)

    def getDepotdbname(self):
        return (self.dbconnectionDict[self.DEPOT_DBNAME])

    def getPGDepotdbname(self):
        return (self.dbconnectionDict[self.PGDEPOT_DBNAME])

    def getPGDepotLoginUser(self):
        return (self.dbconnectionDict[self.PGDEPOT_USERNAME])

    def getSSLCert(self):
        return (self.dbconnectionDict[self.PGDEPORT_SSLCERT])

    def getSSLKey(self):
        return (self.dbconnectionDict[self.PGDEPORT_SSLKEY])

    def getSSLRootCert(self):
        return (self.dbconnectionDict[self.PGDEPORT_SSLROOTCERT])

    def enableShareplexConfigFileMonitor(self):
        if self.dbconnectionDict[self.ENABLE_SHAREPLEXCONFIGFILE_MONITOR] == "true":
            return True
        else:
            return False

    def enableShareplexReplicationMonitor(self):
        if self.dbconnectionDict[self.ENABLE_SHAREPLEXREPLICATION_MONITOR] == "true":
            return True
        else:
            return False

    def getRedisClusterConnectionInfo(self):
        return self.dbconnectionDict[self.REDIS_SERVERS]

    def getDBType(self):
        return self.dbconnectionDict["db_type"]

    def getInfluxDBclient(self):
        database = "oraclemetric"
        return InfluxDBClient(self.dbconnectionDict['influxDB_ip'], int(self.dbconnectionDict['influxDB_port']), self.dbconnectionDict['influxDB_user'],
                              base64.b64decode(self.dbconnectionDict['influxDB_pwd']).decode("utf-8"), database)

    def getInfluxDB_SJC_client(self):
        # database = "telegraf"
        database = "oraclemetric"
        return InfluxDBClient(self.dbconnectionDict['influxDB_ip_SJC'], int(self.dbconnectionDict['influxDB_port']), self.dbconnectionDict['influxDB_user'],
                              base64.b64decode(self.dbconnectionDict['influxDB_pwd']).decode("utf-8"), database)

    def getInfluxDB_DFW_client(self):
        # database = "telegraf"
        database = "oraclemetric"
        return InfluxDBClient(self.dbconnectionDict['influxDB_ip_DFW'], int(self.dbconnectionDict['influxDB_port']), self.dbconnectionDict['influxDB_user'],
                              base64.b64decode(self.dbconnectionDict['influxDB_pwd']).decode("utf-8"), database)

    def getChinaInfluxDBclient(self):
        return InfluxDBClient(self.dbconnectionDict['influxDB_china_ip'], int(self.dbconnectionDict['influxDB_port']), self.dbconnectionDict['influxDB_user'],
                              base64.b64decode(self.dbconnectionDict['influxDB_pwd']).decode("utf-8"), 'telegraf')


if __name__ == "__main__":
    config = Config.getConfig()
    print(config.getDBAMonitorConfigFile())
    print(config.getLoggerConfigFile())





