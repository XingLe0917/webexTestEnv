from elasticsearch import Elasticsearch
import requests
import json
import logging
import urllib3
import base64
from common.singleton import Singleton
from common.wbxutil import wbxutil
from common.wbxexception import wbxexception
from collections import OrderedDict

logger = logging.getLogger("DBAMONITOR")

@Singleton
class wbxelasticsearchfactory:
    def __init__(self):
        self._site_code_dict = {
            "DFW01": {"ESURL":"tx","ESINDEX":"tx1"},
            "DFW02": {"ESURL":"ta","ESINDEX":"ta71"},
            "NRT03": {"ESURL":"jp","ESINDEX":"jp2"},
            "LHR03": {"ESURL":"ln","ESINDEX":"ln1"},
            "AMS01": {"ESURL":"am","ESINDEX":"am1"},
            "SIN01": {"ESURL":"sg","ESINDEX":"sg1"},
            "SYD01": {"ESURL":"sy","ESINDEX":"sy1"},
            "SJC02": {"ESURL":"sj","ESINDEX":"sj1"},
            "SJC03": {"ESURL": "sj", "ESINDEX": "sj1"},
            "YYZ01": {"ESURL":"ta","ESINDEX":"ta71"},
            "IAD02": {"ESURL":"ta","ESINDEX":"ta71"}
        }

        self._es_username = "ccpdbaadmin.gen"  # "yejfeng"#
        self._es_pwd = b'Q0NQZGJhYWRtaW4xMjMh'  # b"WUp4bjM5NSM="
        self._esDict = {}

    def getElasticSearch(self, site_code):
        if site_code not in self._esDict:
            esurl = 'https://clp%s-dba.webex.com/esapi' % self._site_code_dict[site_code]["ESURL"]
            esindx = 'metrics-clap_%s-dba-*'% self._site_code_dict[site_code]["ESINDEX"]
            es = wbxelasticsearch(esurl, esindx, self._es_username, self._es_pwd)
            self._esDict[site_code] = es

        return self._esDict[site_code]

class wbxelasticsearch:
    def __init__(self, esurl, esindex, esusername, espwd):
        self._es = Elasticsearch([esurl], http_auth=(esusername, base64.b64decode(espwd).decode("utf-8")), verify=False,verify_certs=False,timeout=3600)
        self._idx = esindex

    def search(self, querybody):
        response = self._es.search(index=self._idx, body=querybody)
        return  response

    def scroll(self, querybody):
        hasMore = True
        response = self._es.search(index=self._idx, body=querybody, scroll="1m")
        resList = []
        while hasMore:
            scroll_id = response["_scroll_id"]
            if "aggs" in querybody:
                if "aggregations" in response:
                    aggdict = response["aggregations"]
                    aggvaldict, = aggdict.values()
                    rowlist = aggvaldict["buckets"]
                else:
                    hasMore = False
                    continue
            else:
                if len(response["hits"]["hits"]) > 0:
                    rowlist = response["hits"]["hits"]
                else:
                    hasMore = False
                    continue
            resList.extend(rowlist)
            response = self._es.scroll(scroll_id=scroll_id, scroll="1m")
        else:
            self._es.clear_scroll(scroll_id=response["_scroll_id"])
        return resList
