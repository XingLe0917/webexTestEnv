import logging
import datetime
from common.wbxelasticsearch import wbxelasticsearchfactory
from common.singleton import threadlocal
from common.wbxexception import wbxexception
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys
from common.wbxutil import wbxutil

logger = logging.getLogger("DBAMONITOR")

# os metric aggregation granularity is >= 1 minute
def getGranularity(starttime, endtime):
    currentTime = datetime.datetime.now()
    if endtime > currentTime:
        endtime = datetime.datetime.now()
    if endtime < starttime:
        raise wbxexception("Starttime %s should not larger than endtime %s" % (starttime, endtime))
    d = endtime - starttime
    if d.total_seconds() < 5 * 60 * 60:
        granularity = None
    elif d.total_seconds() >= 5 * 60 * 60 and d.days < 1:
        granularity = "1m"
    elif d.days >= 1 and d.days < 7:
        granularity = "10m"
    elif d.days >= 7 and d.days < 30:
        granularity = "30m"
    elif d.days >= 30 and d.days < 90:
        granularity = "2h"
    else:
        granularity = "1d"
    return granularity

def getDiskMetric(host_name, start_time, end_time, fieldList):
    esfactory = wbxelasticsearchfactory()
    es = esfactory.getElasticSearch(threadlocal.site_code)
    sourceList = ["message.DB_SAMPLE_DATE", "message.DB_DEVICE:"]
    sourceList.extend(fieldList)
    granularity = getGranularity(start_time, end_time)
    querybody = {
        "size":10000,
        "_source":sourceList,
        "sort":"message.DB_SAMPLE_DATE",
        "query":{
            "bool":{
                "filter":[{"term":{"message.DB_HOST":host_name}},
                        {"term":{"message.DB_METRICS_TYPE.keyword":"OS-IOSTAT"}},
                        {"range":{"message.DB_SAMPLE_DATE": {
                                "gte": start_time,
                                "lte": end_time
                            }}
                        }
                        ]
            }
        }
    }
    aggdict = {}
    if granularity is not None:
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            aggdict[aggname] = {
                "terms": {
                    "field": "message.DB_DEVICE:.keyword",
                    "size": 10,
                    "order": {"_key": "desc"}
                },
                    "aggs": {
                        "sample_date_bucket": {
                            "date_histogram": {
                                "field": "message.DB_SAMPLE_DATE",
                                "interval": granularity
                            },
                            "aggs": {
                                "field_value": {"avg": {"field": fieldname}}
                            }
                        }
                    }
            }
        querybody["aggs"] = aggdict
        querybody["size"] = 0

    response = es.search(querybody)
    fieldDict = {}
    if granularity is None:
        for row in response["hits"]["hits"]:
            msg = row["_source"]["message"]
            for fieldname in fieldList:
                fname = fieldname.split('.')[-1]
                if fname not in fieldDict:
                    fieldDict[fname] = {}
                fieldValDict = fieldDict[fname]
                db_device = msg["DB_DEVICE:"]
                if db_device not in fieldValDict:
                    fieldValDict[db_device] = []
                fieldDict[fname][db_device].append({msg["DB_SAMPLE_DATE"]: round(msg[fname], 1)})
    else:
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            fname = fieldname.split('.')[-1]
            if fname not in fieldDict:
                fieldDict[fname] = {}
            keyDict = fieldDict[fname]
            rowList = response["aggregations"][aggname]["buckets"]
            for row in rowList:
                key = row["key"]
                # valList = [{databucket["key_as_string"]: round(databucket["field_value"]["value"])} for databucket in row["sample_date_bucket"]["buckets"]]
                for databucket in row["sample_date_bucket"]["buckets"]:
                    if databucket["field_value"]["value"] is None:
                        databucket["field_value"]["value"] = 0
                valList = [{databucket["key_as_string"]: round(databucket["field_value"]["value"])} for databucket in
                           row["sample_date_bucket"]["buckets"]]
                if key not in keyDict:
                    keyDict[key] = valList
                else:
                    keyDict[key].extend(valList)
    return fieldDict

# The smallest histogram granuliarity is 1m
def getNetworkInterfaceMetric(host_name, start_time, end_time, fieldList):
    esfactory = wbxelasticsearchfactory()
    es = esfactory.getElasticSearch(threadlocal.site_code)
    sourceList = ["message.DB_SAMPLE_DATE","message.DB_NETWORK_INTERFACE"]
    sourceList.extend(fieldList)
    granularity = getGranularity(start_time, end_time)
    if granularity is None:
        granularity = "1m"
    querybody = {
        "size": 0,
        "sort": "message.DB_SAMPLE_DATE",
        "_source": sourceList,
        "query": {
            "bool": {
                "filter": [{"term": {"message.DB_HOST": host_name}},
                           {"term": {"message.DB_METRICS_TYPE.keyword": "OS-IFCONFIG"}},
                           {"range": {"message.DB_SAMPLE_DATE": {
                               "gte": start_time,
                               "lte": end_time
                           }}
                           }
                           ]
            }
        }
    }

    aggdict = {}
    for fieldname in fieldList:
        aggname = "agg_%s" % fieldname.split('.')[-1]
        aggdict[aggname] = {"terms": {
                "field": "message.DB_NETWORK_INTERFACE.keyword",
                "size": 30
            },
                "aggs": {
                    "sample_date_bucket": {
                        "date_histogram": {
                            "field": "message.DB_SAMPLE_DATE",
                            "interval": granularity
                        },
                        "aggs": {
                            "field_value": {"max": {"field": fieldname}},
                            "derivative_value": {"derivative": {"buckets_path": "field_value"}}
                        }
                    }
                }
            }
    querybody["aggs"] = aggdict
    querybody["size"] = 0
    response = es.search(querybody)
    fieldDict = {}
    for fieldname in fieldList:
        aggname = "agg_%s" % fieldname.split('.')[-1]
        fname = fieldname.split('.')[-1]
        if fname not in fieldDict:
            fieldDict[fname] = {}
        keyDict = fieldDict[fname]
        rowList = response["aggregations"][aggname]["buckets"]
        for row in rowList:
            key = row["key"]
            valList = [{databucket["key_as_string"]: databucket["derivative_value"]["value"]} for databucket in row["sample_date_bucket"]["buckets"] if "derivative_value" in databucket ]
            if key not in keyDict:
                keyDict[key] = valList
            else:
                keyDict[key].extend(valList)
    return fieldDict

def getMemoryMetric(host_name, start_time, end_time, fieldList):
    esfactory = wbxelasticsearchfactory()
    es = esfactory.getElasticSearch(threadlocal.site_code)
    granularity = getGranularity(start_time, end_time)
    sourceList = ["message.DB_SAMPLE_DATE"]
    sourceList.extend(fieldList)
    querybody = {
        "size": 10000,
        "sort": "message.DB_SAMPLE_DATE",
        "_source": sourceList,
        "query": {
            "bool": {
                "filter": [{"term": {"message.DB_HOST": host_name}},
                           {"term": {"message.DB_METRICS_TYPE.keyword": "OS-MEMINFO"}},
                           {"range": {"message.DB_SAMPLE_DATE": {
                               "gte": start_time,
                               "lte": end_time
                           }}
                           }
                           ]
            }
        }
    }
    if granularity is not None:
        aggdict = {}
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            aggdict[aggname] = {
                    "date_histogram": {
                        "field": "message.DB_SAMPLE_DATE",
                        "interval": granularity
                    },
                    "aggs": {
                        "field_value": {"max": {"field": fieldname}}
                    }
                }
        querybody["aggs"] = aggdict
        querybody["size"] = 0

    response = es.search(querybody)
    fieldDict = {}
    if granularity is None:
        for row in response["hits"]["hits"]:
            msg = row["_source"]["message"]
            for fieldname in fieldList:
                fname = fieldname.split('.')[-1]
                if fname not in fieldDict:
                    fieldDict[fname] = []
                fieldValList = fieldDict[fname]
                fieldValList.append({msg["DB_SAMPLE_DATE"]: round(msg[fname], 1)})
    else:
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            fname = fieldname.split('.')[-1]
            rowlist = response["aggregations"][aggname]["buckets"]
            valList = [{row["key_as_string"]:row["field_value"]["value"]}for row in rowlist]
            fieldDict[fname] = valList
    # print(datetime.datetime.now())
    return fieldDict

def getCPUUsageMetric(host_name, start_time, end_time, fieldList):
    esfactory = wbxelasticsearchfactory()
    es = esfactory.getElasticSearch(threadlocal.site_code)
    granularity = getGranularity(start_time, end_time)
    sourceList = ["message.DB_SAMPLE_DATE", "message.DB_HOST"]
    sourceList.extend(fieldList)
    querybody = {
        "size": 10000,
        "sort": "message.DB_SAMPLE_DATE",
        "_source": sourceList,
        "query": {
            "bool": {
                "filter": [{"term": {"message.DB_HOST": host_name}},
                           {"term": {"message.DB_METRICS_TYPE.keyword": "OS-MPSTAT"}},
                           {"term": {"message.DB_CPU_ID.keyword": "all"}},
                           {"range": {"message.DB_SAMPLE_DATE": {
                               "gte": start_time,
                               "lte": end_time
                           }}
                           }
                           ]
            }
        }
    }

    if granularity is not None:
        aggdict = {}
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            aggdict[aggname] = {
                "date_histogram": {
                    "field": "message.DB_SAMPLE_DATE",
                    "interval": granularity
                },
                "aggs": {
                    "field_value": {"avg": {"field": fieldname}}
                }
            }
        querybody["aggs"] = aggdict
        querybody["size"] = 0

    response = es.search(querybody)
    fieldDict = {}
    if granularity is None:
        for row in response["hits"]["hits"]:
            msg = row["_source"]["message"]
            for fieldname in fieldList:
                fname = fieldname.split('.')[-1]
                if fname not in fieldDict:
                    fieldDict[fname] = []
                fieldValList = fieldDict[fname]
                fieldValList.append({msg["DB_SAMPLE_DATE"]: round(msg[fname], 1)})
    else:
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            fname = fieldname.split('.')[-1]
            rowlist = response["aggregations"][aggname]["buckets"]
            valList = [{row["key_as_string"]: round(row["field_value"]["value"],1)} for row in rowlist]
            fieldDict[fname] = valList
    return fieldDict

def getTraceoutMetric(host_name, start_time, end_time, fieldList):
    esfactory = wbxelasticsearchfactory()
    es = esfactory.getElasticSearch(threadlocal.site_code)
    granularity = getGranularity(start_time, end_time)
    sourceList = ["message.DB_SAMPLE_DATE", "message.DB_HOST","message.DB_TARGET_HOSTNAME"]
    sourceList.extend(fieldList)
    querybody = {
        "size": 10000,
        "sort": "message.DB_SAMPLE_DATE",
        "_source": sourceList,
        "query": {
            "bool": {
                "filter": [{"term": {"message.DB_HOST": host_name}},
                           {"term": {"message.DB_METRICS_TYPE.keyword": "OS-PRVTNET"}},
                           {"range": {"message.DB_SAMPLE_DATE": {
                               "gte": start_time,
                               "lte": end_time
                           }}
                           }
                           ]
            }
        }
    }

    if granularity is not None:
        aggdict = {}
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            aggdict[aggname] = {"terms": {
                "field": "message.DB_TARGET_HOSTNAME.keyword",
                "size": 10
            },
                "aggs": {
                    "sample_date_bucket": {
                        "date_histogram": {
                            "field": "message.DB_SAMPLE_DATE",
                            "interval": granularity
                        },
                        "aggs": {
                            "field_value": {"max": {"field": fieldname}}
                        }
                    }
                }
            }
        querybody["aggs"] = aggdict
        querybody["size"] = 0

    response = es.search(querybody)
    fieldDict = {}
    if granularity is None:
        for row in response["hits"]["hits"]:
            msg = row["_source"]["message"]
            for fieldname in fieldList:
                fname = fieldname.split('.')[-1]
                if fname not in fieldDict:
                    fieldDict[fname] = {}
                fieldValDict = fieldDict[fname]
                db_device = msg["DB_TARGET_HOSTNAME"]
                if db_device not in fieldValDict:
                    fieldValDict[db_device] = []
                fieldDict[fname][db_device].append({msg["DB_SAMPLE_DATE"]: round(msg[fname]*1000)})
    else:
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            fname = fieldname.split('.')[-1]
            if fname not in fieldDict:
                fieldDict[fname] = {}
            keyDict = fieldDict[fname]
            rowList = response["aggregations"][aggname]["buckets"]
            for row in rowList:
                key = row["key"]
                valList = [{databucket["key_as_string"]: round(databucket["field_value"]["value"] * 1000)} for databucket in
                           row["sample_date_bucket"]["buckets"]]
                if key not in keyDict:
                    keyDict[key] = valList
                else:
                    keyDict[key].extend(valList)
    # print(datetime.datetime.now())
    return fieldDict

def getUserSessionMetric(host_name, db_name, start_time, end_time, fieldList):
    esfactory = wbxelasticsearchfactory()
    es = esfactory.getElasticSearch(threadlocal.site_code)
    granularity = getGranularity(start_time, end_time)
    sourceList = ["message.DB_SAMPLE_DATE", "message.DB_NAME"]
    sourceList.extend(fieldList)
    querybody = {
        "size": 10000,
        "sort": "message.DB_SAMPLE_DATE",
        "_source": sourceList,
        "query": {
            "bool": {
                "filter": [{"term": {"message.DB_HOST": host_name}},
                           {"term": {"message.DB_NAME.keyword": db_name}},
                           {"match": {"message.DB_METRICS_TYPE": "USERSESSION"}},
                           {"range": {"message.DB_SAMPLE_DATE": {
                               "gte": start_time,
                               "lte": end_time
                           }}
                           }
                           ]
            }
        }
    }

    if granularity is not None and granularity != "1m":
        aggdict = {}
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            aggdict[aggname] = {
                "date_histogram": {
                    "field": "message.DB_SAMPLE_DATE",
                    "interval": granularity
                },
                "aggs": {
                    "field_value": {"max": {"field": fieldname}}
                }
            }
        querybody["aggs"] = aggdict
        querybody["size"] = 0

    response = es.search(querybody)
    fieldDict = {}
    if granularity is None:
        for row in response["hits"]["hits"]:
            msg = row["_source"]["message"]
            for fieldname in fieldList:
                fname = fieldname.split('.')[-1]
                if fname not in fieldDict:
                    fieldDict[fname] = []
                fieldValList = fieldDict[fname]
                fieldValList.append({msg["DB_SAMPLE_DATE"]: round(msg[fname], 1)})
    else:
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            fname = fieldname.split('.')[-1]
            rowlist = response["aggregations"][aggname]["buckets"]
            valList = [{row["key_as_string"]: row["field_value"]["value"]} for row in rowlist]
            fieldDict[fname] = valList
    return fieldDict

def getShareplexProcessMetric(host_name, db_name, start_time, end_time, processType, fieldList):
    esfactory = wbxelasticsearchfactory()
    es = esfactory.getElasticSearch(threadlocal.site_code)
    granularity = getGranularity(start_time, end_time)
    if granularity is None and granularity == "1m":
        granularity = "5m"

    sourceList = ["message.DB_SAMPLE_DATE", "message.DB_NAME"]
    sourceList.extend(fieldList)
    querybody = {
        "size": 10000,
        "sort": [{"message.DB_SAMPLE_DATE": {"order": "desc"}}],
        "_source": sourceList,
        "query": {
            "bool": {
                "filter": [{"term": {"message.DB_HOST": host_name}},
                           {"term": {"message.DB_NAME.keyword": db_name}},
                           {"term": {"message.DB_PROCESS_TYPE.keyword": processType}},
                           {"term": {"message.DB_METRICS_TYPE.keyword": "DB_SP_PROCESSSTATUS"}},
                           {"range": {"message.DB_SAMPLE_DATE": {
                               "gte": start_time,
                               "lte": end_time
                           }}
                           }
                           ]
            }
        }
    }
    aggdict = {}
    for fieldname in fieldList:
        aggname = "agg_%s" % fieldname.split('.')[-1]
        aggdict[aggname] = {"terms": {
                "field": "message.DB_NETWORK_INTERFACE.keyword",
                "size": 30
            },
                "aggs": {
                    "sample_date_bucket": {
                        "date_histogram": {
                            "field": "message.DB_SAMPLE_DATE",
                            "interval": granularity
                        },
                        "aggs": {
                            "field_value": {"max": {"field": fieldname}},
                            "derivative_value": {"derivative": {"buckets_path": "field_value"}}
                        }
                    }
                }
            }
    querybody["aggs"] = aggdict
    querybody["size"] = 0


    for fieldname in fieldList:
        aggname = "agg_%s" % fieldname.split('.')[-1]
        aggdict[aggname] = {
            "date_histogram": {
                "field": "message.DB_SAMPLE_DATE",
                "interval": granularity
            },
            "aggs": {
                "field_value": {"max": {"field": fieldname}}
            }
        }
    querybody["aggs"] = aggdict
    querybody["size"] = 0

    response = es.search(querybody)
    fieldDict = {}
    if granularity is None:
        for row in response["hits"]["hits"]:
            msg = row["_source"]["message"]
            for fieldname in fieldList:
                fname = fieldname.split('.')[-1]
                if fname not in fieldDict:
                    fieldDict[fname] = []
                fieldValList = fieldDict[fname]
                fieldValList.append({msg["DB_SAMPLE_DATE"]: round(msg[fname], 1)})
    else:
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            fname = fieldname.split('.')[-1]
            rowlist = response["aggregations"][aggname]["buckets"]
            valList = [{row["key_as_string"]: row["field_value"]["value"]} for row in rowlist]
            fieldDict[fname] = valList
    print(datetime.datetime.now())
    return fieldDict

def getOSLoadMetric(host_name, start_time, end_time, fieldList):
    esfactory = wbxelasticsearchfactory()
    es = esfactory.getElasticSearch(threadlocal.site_code)
    granularity = getGranularity(start_time, end_time)
    sourceList = ["message.DB_SAMPLE_DATE"]
    sourceList.extend(fieldList)
    querybody = {
        "size": 10000,
        "sort": "message.DB_SAMPLE_DATE",
        "_source": sourceList,
        "query": {
            "bool": {
                "filter": [{"term": {"message.DB_HOST": host_name}},
                           {"term": {"message.DB_METRICS_TYPE.keyword": "OS-TOP"}},
                           {"range": {"message.DB_SAMPLE_DATE": {
                               "gte": start_time,
                               "lte": end_time
                           }}
                           }
                           ]
            }
        }
    }
    if granularity is not None:
        aggdict = {}
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            aggdict[aggname] = {
                    "date_histogram": {
                        "field": "message.DB_SAMPLE_DATE",
                        "interval": granularity
                    },
                    "aggs": {
                        "field_value": {"max": {"field": fieldname}}
                    }
                }
        querybody["aggs"] = aggdict
        querybody["size"] = 0

    response = es.search(querybody)
    fieldDict = {}
    if granularity is None:
        for row in response["hits"]["hits"]:
            msg = row["_source"]["message"]
            if len(msg) < 3:
                continue
            for fieldname in fieldList:
                fname = fieldname.split('.')[-1]
                if fname not in fieldDict:
                    fieldDict[fname] = []
                fieldValList = fieldDict[fname]
                fieldValList.append({msg["DB_SAMPLE_DATE"]: round(msg[fname], 1)})


    else:
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            fname = fieldname.split('.')[-1]
            rowlist = response["aggregations"][aggname]["buckets"]
            valList = [{row["key_as_string"]:round(row["field_value"]["value"],1)}for row in rowlist]
            fieldDict[fname] = valList
    # print(datetime.datetime.now())
    return fieldDict

def getOSProcessMetric(host_name, start_time, end_time, fieldList):
    esfactory = wbxelasticsearchfactory()
    es = esfactory.getElasticSearch(threadlocal.site_code)
    granularity = getGranularity(start_time, end_time)
    sourceList = ["message.DB_SAMPLE_DATE"]
    sourceList.extend(fieldList)
    querybody = {
        "size": 10000,
        "sort": "message.DB_SAMPLE_DATE",
        "_source": sourceList,
        "query": {
            "bool": {
                "filter": [{"term": {"message.DB_HOST": host_name}},
                           {"term": {"message.DB_METRICS_TYPE.keyword": "OS-VMSTAT"}},
                           {"range": {"message.DB_SAMPLE_DATE": {
                               "gte": start_time,
                               "lte": end_time
                           }}
                           }
                           ]
            }
        }
    }
    if granularity is not None:
        aggdict = {}
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            aggdict[aggname] = {
                    "date_histogram": {
                        "field": "message.DB_SAMPLE_DATE",
                        "interval": granularity
                    },
                    "aggs": {
                        "field_value": {"max": {"field": fieldname}}
                    }
                }
        querybody["aggs"] = aggdict
        querybody["size"] = 0

    response = es.search(querybody)
    fieldDict = {}
    if granularity is None:
        for row in response["hits"]["hits"]:
            msg = row["_source"]["message"]
            if len(msg) < 3:
                continue
            for fieldname in fieldList:
                fname = fieldname.split('.')[-1]
                if fname not in fieldDict:
                    fieldDict[fname] = []
                fieldValList = fieldDict[fname]
                fieldValList.append({msg["DB_SAMPLE_DATE"]: msg[fname]})


    else:
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            fname = fieldname.split('.')[-1]
            rowlist = response["aggregations"][aggname]["buckets"]
            valList = [{row["key_as_string"]:row["field_value"]["value"]}for row in rowlist]
            fieldDict[fname] = valList
    # print(datetime.datetime.now())
    return fieldDict

def getTablespaceMetric(host_name, db_name, start_time, end_time, fieldList):
    esfactory = wbxelasticsearchfactory()
    es = esfactory.getElasticSearch(threadlocal.site_code)
    sourceList = ["message.DB_SAMPLE_DATE", "message.DB_NAME"]
    sourceList.extend(fieldList)
    granularity = getGranularity(start_time, end_time)
    if  granularity == "1m" or granularity == "10m":
        granularity = None
    querybody = {
        "size":10000,
        "sort":"message.DB_SAMPLE_DATE",
        "query":{
            "bool":{
                "filter":[{"term":{"message.DB_HOST":host_name}},
                          {"term":{"message.DB_NAME.keyword":db_name}},
                        {"term":{"message.DB_METRICS_TYPE.keyword":"DB-TABLESPACE"}},
                        {"range":{"message.DB_SAMPLE_DATE": {
                                "gte": start_time,
                                "lte": end_time
                            }}
                        }
                        ]
            }
        }
    }

    if granularity is not None:
        aggdict = {}
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            aggdict[aggname] = {
                "terms": {
                    "field": "message.DB_TABLESPACE_NAME.keyword",
                    "size": 30,
                    "order": {"_key": "desc"}
                },
                "aggs": {
                    "sample_date_bucket": {
                        "date_histogram": {
                            "field": "message.DB_SAMPLE_DATE",
                            "interval": granularity
                        },
                        "aggs": {
                            "field_value": {"max": {"field": fieldname}}
                        }
                    }
                }
            }
        querybody["aggs"] = aggdict
        querybody["size"] = 0

    response = es.search(querybody)
    fieldDict = {}
    if granularity is None:
        for row in response["hits"]["hits"]:
            msg = row["_source"]["message"]
            for fieldname in fieldList:
                fname = fieldname.split('.')[-1]
                if fname not in fieldDict:
                    fieldDict[fname] = {}
                fieldValDict = fieldDict[fname]
                tablespace_name = msg["DB_TABLESPACE_NAME"]
                if tablespace_name not in fieldValDict:
                    fieldValDict[tablespace_name] = []
                fieldDict[fname][tablespace_name].append({msg["DB_SAMPLE_DATE"]: round(msg[fname], 1)})
    else:
        for fieldname in fieldList:
            aggname = "agg_%s" % fieldname.split('.')[-1]
            fname = fieldname.split('.')[-1]
            if fname not in fieldDict:
                fieldDict[fname] = {}
            keyDict = fieldDict[fname]
            rowList = response["aggregations"][aggname]["buckets"]
            for row in rowList:
                key = row["key"]
                valList = [{databucket["key_as_string"]: databucket["field_value"]["value"]} for databucket in row["sample_date_bucket"]["buckets"]]
                if key not in keyDict:
                    keyDict[key] = valList
                else:
                    keyDict[key].extend(valList)
    return fieldDict

def get_osw_data_from_es(metric_type, host_name, db_name, str_start_time, str_end_time):
    try:
        start_time = wbxutil.convertStringtoDateTime(str_start_time)
        end_time = min(wbxutil.convertStringtoDateTime(str_end_time), wbxutil.getcurrenttime())
    except Exception as e:
        start_time = wbxutil.convertESStringToDatetime(str_start_time)
        end_time = min(wbxutil.convertESStringToDatetime(str_end_time), wbxutil.getcurrenttime())

    daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
    server = daomanagerfactory.getServer(host_name)
    # threadlocal.site_code = "SJC02"
    threadlocal.site_code = server._site_code

    resDict = {"status": "success", "result":{}}
    try:
        if metric_type == "meminfo":
            fieldList = ["message.DB_HUGEPAGES_TOTAL","message.DB_HUGEPAGES_FREE","message.DB_MEMTOTAL",
                         "message.DB_MEMFREE","message.DB_SWAPTOTAL","message.DB_SWAPFREE"]
            resDict["result"] = getMemoryMetric(host_name, start_time, end_time, fieldList)

        elif metric_type == "usersession":
            fieldList = ["message.DB_SESSION_COUNT","message.DB_ACTIVE_SESSION_COUNT"]
            resDict["result"] = getUserSessionMetric(host_name, db_name, start_time, end_time, fieldList)

        elif metric_type == "mpstat":
            fieldList = ["message.DB_CPU_USED_TOTAL","message.DB_CPU_IOWAIT","message.DB_CPU_STEAL"]
            resDict["result"] = getCPUUsageMetric(host_name, start_time, end_time, fieldList)

        elif metric_type == "iostat":
            fieldList = ["message.DB_svctm","message.DB_w/s","message.DB_r/s","message.DB_await"]
            resDict["result"] = getDiskMetric(host_name, start_time, end_time, fieldList)

        elif metric_type == "ifconfig":
            fieldList = ["message.DB_TX_ERRORS","message.DB_RX_ERRORS","message.DB_TX_DROPPED",
                         "message.DB_RX_DROPPED","message.DB_TX_BYTES","message.DB_RX_BYTES"]
            resDict["result"] = getNetworkInterfaceMetric(host_name, start_time, end_time,fieldList)

        elif metric_type == "queuestatus":
            resDict = []
            # fieldList = ["message.DB_SP_TRANSACTION_COUNT"]
            # resDict["result"] = getShareplexProcessMetric(host_name, db_name, start_time, end_time, "Capture",fieldList)

        elif metric_type == "tablespacemetric":
            fieldList = ["message.DB_TABLESPACE_TOTALSIZE", "message.DB_TABLESPACE_USEDSIZE"]
            resDict["result"] = getTablespaceMetric(host_name, db_name, start_time, end_time, fieldList)

        elif metric_type == "traceoutmetric":
            fieldList = ["message.DB_RESPONSE_TIMES"]
            resDict["result"] = getTraceoutMetric(host_name, start_time, end_time, fieldList)

        elif metric_type == "osload":
            fieldList = ["message.DB_TOP_LOAD_5MIN", "message.DB_TOP_LOAD_1MIN", "message.DB_TOP_LOAD_15MIN"]
            resDict["result"] = getOSLoadMetric(host_name, start_time, end_time, fieldList)

        elif metric_type == "osprocess":
            fieldList = ["message.DB_R", "message.DB_B"]
            resDict["result"] = getOSProcessMetric(host_name, start_time, end_time, fieldList)
    except Exception as e:
        logger.error("metric_type=%s, host_name=%s,start_time=%s, end_time=%s, db_name=%s has error:" % (metric_type, host_name, start_time, end_time, db_name), exc_info = e)
        resDict["status"] = "fail"
    return resDict

def getDBListForDBHealth():
    daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daomanager = daomanagerfactory.getDefaultDaoManager()
    dbDict = {}
    try:
        daomanager.startTransaction()
        dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        resList = dao.getDBListForDBHealth()
        for row in resList:
            host_name = row[0]
            db_name = row[1]
            if db_name not in dbDict:
                dbDict[db_name] = []
            dbDict[db_name].append(host_name)
        daomanager.commit()
    except Exception as e:
        daomanager.rollback()
        pass
    finally:
        daomanager.close()
    return dbDict

