import logging
from common.wbxelasticsearch import wbxelasticsearchfactory
from common.singleton import threadlocal

logger = logging.getLogger("DBAMONITOR")

def getActiveTablespaceUsage(db_name):
    query_string = {"message.DB_NAME:%s AND message.DB_METRICS_TYPE.keyword:TABLESPACE" % (db_name)}
    agg_field_name = "message.DB_TABLESPACE_NAME.keyword"
    query_field_list = ["DB_TABLESPACE_TOTALSIZE", "DB_TABLESPACE_USEDSIZE"]
    return getAggActiveDBMetric(query_string, agg_field_name, query_field_list)

def getActiveASMUsage(db_name):
    query_string = {"message.DB_NAME:%s AND message.DB_METRICS_TYPE.keyword:ASMDISKUSAGE" % (db_name)}
    agg_field_name = "message.DB_DISKGROUP_NAME.keyword"
    query_field_list = ["DB_DISKGROUP_TOTAL_MB", "DB_DISKGROUP_FREE_MB"]
    return getAggActiveDBMetric(query_string, agg_field_name, query_field_list)

def getActiveDiskUsage(host_name):
    query_string={"message.DB_HOST:%s AND message.DB_METRICS_TYPE.keyword:OS-DISKUSAGE" % (host_name)}
    agg_field_name="message.DB_DISK_MOUNT_POINT.keyword"
    query_field_list = ["DB_DISK_TOTAL_SIZE","DB_DISK_USED_SIZE","DB_INODE_TOTAL_SIZE","DB_INODE_USED_SIZE"]
    dgdict = getAggActiveDBMetric(query_string, agg_field_name, query_field_list)
    return dgdict

def getActiveUserSessionUsage(db_name):
    query_string = "message.DB_NAME:%s AND message.DB_METRICS_TYPE.keyword:DB-USERSESSION" % (db_name)
    query_field_list = ["DB_DISKGROUP_TOTAL_MB", "DB_DISKGROUP_FREE_MB"]
    return getActiveDBMetric(query_string, query_field_list)

def getActiveDBMetric(query_string, fieldList):
    esfactory = wbxelasticsearchfactory()
    es = esfactory.getElasticSearch(threadlocal.site_code)
    queyrbody = {
        "size": 1,
        "query": {
            "bool": {
                "must": [
                    {"query_string": {"query": query_string}},
                    {"range": {
                        "message.DB_SAMPLE_DATE": {
                            "gte": "now-1d/d",
                            "lte": "now/d"
                        }
                    }}
                ]
            }
        },
        "sort": [{"message.DB_SAMPLE_DATE": {"order": "desc"}}],
    }
    response = es.search(queyrbody)
    fielddict = {}
    rowList = response["hits"]["hits"]
    if len(rowList) > 0:
        row = rowList[0]
        msg = row["_source"]["message"]
        return  msg
    else:
        return None

def getAggActiveDBMetric(query_string, aggFieldname, fieldList):
    esfactory = wbxelasticsearchfactory()
    es = esfactory.getElasticSearch(threadlocal.site_code)
    queyrbody = {
        "size": 0,
        "query": {
                "bool": {
                    "must": [
                        {"query_string": {"query": query_string}},
                        {"range": {
                            "message.DB_SAMPLE_DATE": {
                                "gte": "now-1d/d",
                                "lte": "now/d"
                            }
                        }}
                    ]
                }
        },
        "sort": [{"message.DB_SAMPLE_DATE": {"order": "desc"}}],
        "aggs": {
            "aggfield_name": {
                "terms": {
                    "field": aggFieldname,
                    "size": 50
                }, "aggs": {
                    "rated": {
                        "top_hits": {
                            "sort": [{"message.DB_SAMPLE_DATE": {"order": "desc"}}],
                            "size": 1
                        }
                    }
                }
            }
        }
    }
    response = es.search(queyrbody)
    fielddict = {}
    rowList = response["aggregations"]["aggfield_name"]["buckets"]
    for row in rowList:
        aggname = row["key"]
        if aggname not in fielddict:
            fielddict[aggname] = {}
        msg = row["rated"]["hits"]["hits"][0]["_source"]["message"]
        for fieldname in fieldList:
            fielddict[aggname][fieldname] = msg[fieldname]
        fielddict[aggname]["DB_SAMPLE_DATE"] = msg["DB_SAMPLE_DATE"]
    return fielddict