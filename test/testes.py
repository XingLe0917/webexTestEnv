from elasticsearch import Elasticsearch
import base64
from common.wbxelasticsearch import wbxelasticsearchfactory
import datetime
import sys
from common.wbxssh import wbxssh

if __name__ == "__main__":
    s = wbxssh("sjdbormt084",22, "oracle","eU3D#0nU6On")
    s.connect()
    print(s.exec_command("pwd"))
    s.close()

    startTime = datetime.datetime(2020, 3, 10, 10,0,0)
    startTime = min(startTime, datetime.datetime.now())
    print(startTime)
    sys.exit(0)

    uname = "ccpdbaadmin.gen"  # "yejfeng"#
    server_pwd = b"Y2NwZGJhYWRtaW4="  # b"WUp4bjM5NSM=" #
    pwd = "ccpdbaadmin"
    url = "https://clpsj-dba.webex.com/esapi"
    #
    query_dict1 = {
        "query": {
            "bool": {
                "must": [
                    {"query_string": {
                        "query": "message.DB_METRICS_TYPE.keyword:OS-IFCONFIG AND message.DB_HOST:sjdbormt046"}},
                    {
                        "range": {
                            "message.DB_SAMPLE_DATE": {
                                "gte": "2020-03-05T01:50:00.007",
                                "lte": "2020-03-05T04:20:00.007",
                                "format": "strict_date_hour_minute_second_millis"
                            }
                        }
                    }
                ],
                "filter": [],
                "should": [],
                "must_not": []
            }
        },
        "aggs":{
            "disk_stats":{
                "terms": {
                    "field": "message.DB_NETWORK_INTERFACE.keyword",
                    "size": 5,
                    "order": {
                        "_key": "desc"
                    }
                },
                "aggs":{
                    "sample_date_bucket":{
                        "date_histogram": {
                            "field": "message.DB_SAMPLE_DATE",
                            "interval": "1m"
                        },
                        "aggs": {
                            "max_readsize": {"max": {"field": "message.DB_RX_BYTES"}},
                            "increment_readsize": {"derivative": {"buckets_path": "max_readsize"}}
                        }
                    }
                }
            }
    }
    }

    query_dict = {'size': 10000,
                  'query': {
                      'bool': {
                          'must': [
                              {'query_string': {
                                  'query': 'message.DB_METRICS_TYPE.keyword:DB_QUEUESTATUS AND message.DB_HOST:sjdbormt046 AND message.DB_NAME:RACAVWEB AND message.DB_QueueType:Capture'}
                              },
                              {'range': {
                                  'message.DB_SAMPLE_DATE': {
                                      'gte': '2020-03-04T22:00:00.000',
                                      'lte': '2020-03-05T10:00:00.000',
                                      'format': 'strict_date_hour_minute_second_millis'}}}],
                          'filter': [],
                          'should': [],
                          'must_not': []}},
                  "aggs": {
                      "disk_stats": {
                          "terms": {
                              "field": "message.DB_SPLEX_PORT",
                              "size": 5,
                              "order": {
                                  "_key": "desc"
                              }
                          },
                    'aggs': {
                      'sample_date_bucket': {
                          'date_histogram': {
                              'field': 'message.DB_SAMPLE_DATE',
                              'interval': '10m'},
                          'aggs': {
                              'field_value': {
                                  'max': {'field': 'message.DB_SP_TRANSACTION_COUNT'}},
                                  'derivative_value': {'derivative': {'buckets_path': 'field_value'}}
                          }
                      }
                  }
                      }
                      }
                  }
    es = Elasticsearch(['https://clpsj-dba.webex.com/esapi'], http_auth=(uname, pwd), verify=False, verify_certs = False)
    # print(es.info)
    # res = es.search(index='metrics-clap_sj1-dba*', body=query_dict,docvalue_fields="message.DB_R,message.DB_B",sort="message.DB_SAMPLE_DATE:desc")
    res = es.search(index='metrics-clap_sj1-dba*', body=query_dict)
    print(res["took"])
    print(len(res["hits"]["hits"]))
    for row in res["hits"]["hits"]:
        if row["_source"]["message"]["DB_DEVICE:"] == "sdz":
            print("%s = %s" % (row["_source"]["message"]["DB_SAMPLE_DATE:"],row["_source"]["message"]["DB_svctm"]))

    agg = res["aggregations"]

    print(type(res))
    # jsdata = es.get(index='metrics-clap_sj1-dba*',id="LsJRvVVG3fZrlV9IAt34UqRg==")
    # 4 * 60 * 24 = 1440 * 4 = 5760
    # jsdata = es.get(index='metrics-clap_sj1-dba-*',id="LsJRvVVG3fZrlV9IAt34UqRg==")

    # jsdata = es.get(index='metrics-clap_sj1-dba-2020.03.02',id="LsJRvVVG3fZrlV9IAt34UqRg==")

    # server_usr = "zhiwliu"  # "yejfeng"#
    # server_pwd = "Flask$1357"  # b"WUp4bjM5NSM=" #
    # pwd =  base64.b64decode(server_pwd).decode("utf-8")

    # host_name = server_name

    # server_url = "https://clpsj-meeting.webex.com/elasticsearch/meeting-*/_search?"
    # server_url = "https://clpsj-dba.webex.com/elasticsearch/metrics-*-dba-*/_search?"
    # es = Elasticsearch([server_url],
    #                    http_auth=(server_usr, base64.b64decode(server_pwd).decode("utf-8")),
    #                    use_ssl=True,
    #                    timeout=3600)
    # # metric-clap_sj1-dba*
    # es.search(index='metrics-*-dba-*', filter_path=['hits.hits._id', 'hits.hits._type'])
    # res = es.get(index="metrics-*-dba-*", id="Ls5tPDbawYKFwU2uQyayQygQ==")

