import sys
from common.wbxssh import wbxssh
import time, datetime
import logging
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from common.wbxcache import curcache
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from sqlalchemy import Table, Column, MetaData, String, DateTime, Integer, and_, create_engine
from sqlalchemy.exc import  DBAPIError, DatabaseError
from sqlalchemy.pool import NullPool
import pandas as pd
from collections import OrderedDict

logger = logging.getLogger("DBAMONITOR")


src_tgt_dict = {
    "src": "tadbrpt2",
    "tgt": "rsdboradiag002"
}


def get_gg_comparision_performance_monitor(starttime, endtime):
    errormsg = {}
    status = "SUCCEED"
    gg_src_cpu_consumption_data = {}
    gg_tgt_cpu_consumption_data = {}
    srcdata = {}
    tgtdata = {}
    start_time = wbxutil.convertStringtoDateTime(starttime)
    end_time = wbxutil.convertStringtoDateTime(endtime)
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        gg_delay_data = depotdbDao.getwbxggreplicationdelaytime(start_time, end_time)
        if not gg_delay_data:
            errormsg["gg_delay_data"] = "Failed to find goldengate delay time info from depot by starttime: %s endtime:%s " % (start_time, end_time)

        df = pd.DataFrame({"rep_time": list(gg_delay_data.values())})
        aggdate = df["rep_time"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
        aggdict = OrderedDict()
        aggdict["count"] = len(gg_delay_data)
        aggdict["mean"] = round(aggdate["mean"], 2)
        aggdict["50%"] = round(aggdate["50%"], 2)
        aggdict["90%"] = round(aggdate["90%"], 2)
        aggdict["99.5%"] = round(aggdate["99.5%"], 2)
        aggdict["99.9%"] = round(aggdate["99.9%"], 2)
        df["label"] = pd.cut(df["rep_time"], [0, 2, 4, 10, 60, 120, sys.maxsize * 1.0],
                             labels=["count_2s", "count_4s", "count_10s", "count_60s", "count_120s", "count_more"])
        gg_cpu_consumption_data = depotdbDao.getwbxggcpuconsumption(start_time, end_time)
        if not gg_cpu_consumption_data:
            errormsg["gg_cpu_consumption_data"] = "Failed to find goldengate cpu consumption info from depot by starttime: %s endtime:%s " % (start_time, end_time)
        gg_cpu_consumption_percent_data = get_percent_cpu_consumption_data(gg_cpu_consumption_data)
        if gg_cpu_consumption_data.get(src_tgt_dict["src"]) and gg_cpu_consumption_percent_data.get(
                src_tgt_dict["src"]):
            src_dict = convert_dict(gg_cpu_consumption_data[src_tgt_dict["src"]])
            df_src = pd.DataFrame({"src": src_dict})
            srcaggdate = df_src["src"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
            src_per_dict = convert_dict(gg_cpu_consumption_percent_data[src_tgt_dict["src"]])
            df_src_per = pd.DataFrame({"src_per": src_per_dict})
            srcperaggdate = df_src_per["src_per"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
            srcdata = {
                "maxdata": round(srcaggdate["max"], 2),
                "mindata": round(srcaggdate["min"], 2),
                "meandata": round(srcaggdate["mean"], 2),
                "maxpercent": round(srcperaggdate["max"], 2),
                "minpercent": round(srcperaggdate["min"], 2),
                "meanpercent": round(srcperaggdate["mean"], 2)
            }
        if gg_cpu_consumption_data.get(src_tgt_dict["tgt"]) and gg_cpu_consumption_percent_data.get(src_tgt_dict["tgt"]):
            tgt_dict = convert_dict(gg_cpu_consumption_data[src_tgt_dict["tgt"]])
            df_tgt = pd.DataFrame({"tgt": tgt_dict})
            tgtaggdate = df_tgt["tgt"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
            tgt_per_dict = convert_dict(gg_cpu_consumption_percent_data[src_tgt_dict["tgt"]])
            df_tgt_per = pd.DataFrame({"tgt_per": tgt_per_dict})
            tgtperaggdate = df_tgt_per["tgt_per"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
            tgtdata = {
                "maxdata": round(tgtaggdate["max"], 2),
                "mindata": round(tgtaggdate["min"], 2),
                "meandata": round(tgtaggdate["mean"], 2),
                "maxpercent": round(tgtperaggdate["max"], 2),
                "minpercent": round(tgtperaggdate["min"], 2),
                "meanpercent": round(tgtperaggdate["mean"], 2)
            }
        depotDaoManager.commit()
    except DatabaseError as e:
        logger.error("gg_performance_comparision_data met error %s" % e)
        raise wbxexception(
            "Error ocurred when get info on gg_performance_comparision_data in DepotDB with msg %s" % e)
    if errormsg:
        status = "FAIL"
    else:
        gg_src_cpu_consumption_data = {
            "detailed_data": gg_cpu_consumption_data[src_tgt_dict["src"]],
            "percent_data": gg_cpu_consumption_percent_data[src_tgt_dict["src"]]}
        gg_tgt_cpu_consumption_data = {
            "detailed_data": gg_cpu_consumption_data[src_tgt_dict["tgt"]],
            "percent_data": gg_cpu_consumption_percent_data[src_tgt_dict["tgt"]]}
    return {
        "status": status,
        "gg_delay_data": gg_delay_data,
        "gg_src_cpu_consumption_data": gg_src_cpu_consumption_data,
        "gg_tgt_cpu_consumption_data": gg_tgt_cpu_consumption_data,
        "errormsg": errormsg,
        "data": df.groupby("label").count().to_dict()["rep_time"],
        "srcdata": srcdata,
        "tgtdata": tgtdata
    }


def convert_dict(data):
    new_list = []
    for item in data:
        for k, v in item.items():
            new_list.append(v)
    return new_list


def get_splex_comparision_performance_monitor(starttime, endtime):
    splex_src_cpu_consumption_data = {}
    splex_tgt_cpu_consumption_data = {}
    srcdata = {}
    tgtdata = {}
    errormsg = {}
    status = "SUCCEED"
    start_time = wbxutil.convertStringtoDateTime(starttime)
    end_time = wbxutil.convertStringtoDateTime(endtime)
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        splex_delay_data = depotdbDao.getwbxsplexdelaytime(start_time, end_time)
        if not splex_delay_data:
            errormsg["splex_delay_data"] = "Failed to find shareplex delay time info from depot by starttime: %s endtime:%s " % (start_time, end_time)

        df = pd.DataFrame({"rep_time": list(splex_delay_data.values())})
        aggdate = df["rep_time"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
        aggdict = OrderedDict()
        aggdict["count"] = len(splex_delay_data)
        aggdict["mean"] = round(aggdate["mean"], 2)
        aggdict["max"] = round(aggdate["max"], 2)
        aggdict["min"] = round(aggdate["min"], 2)
        aggdict["50%"] = round(aggdate["50%"], 2)
        aggdict["90%"] = round(aggdate["90%"], 2)
        aggdict["99.5%"] = round(aggdate["99.5%"], 2)
        aggdict["99.9%"] = round(aggdate["99.9%"], 2)
        df["label"] = pd.cut(df["rep_time"], [0, 2, 4, 10, 60, 120, sys.maxsize * 1.0],
                             labels=["count_2s", "count_4s", "count_10s", "count_60s", "count_120s", "count_more"])
        splex_cpu_consumption_data = depotdbDao.getwbxsplexcpuconsumption(start_time, end_time)
        if not splex_cpu_consumption_data:
            errormsg["splex_cpu_consumption_data"] = "Failed to find shareplex cpu consumption info from depot by starttime: %s endtime:%s " % (start_time, end_time)
        splex_cpu_consumption_percent_data = get_percent_cpu_consumption_data(splex_cpu_consumption_data)
        if splex_cpu_consumption_data.get(src_tgt_dict["src"]) and splex_cpu_consumption_percent_data.get(src_tgt_dict["src"]):
            src_dict = convert_dict(splex_cpu_consumption_data[src_tgt_dict["src"]])
            df_src = pd.DataFrame({"src": src_dict})
            srcaggdate = df_src["src"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
            src_per_dict = convert_dict(splex_cpu_consumption_percent_data[src_tgt_dict["src"]])
            df_src_per = pd.DataFrame({"src_per": src_per_dict})
            srcperaggdate = df_src_per["src_per"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
            srcdata = {
                "maxdata": round(srcaggdate["max"], 2),
                "mindata": round(srcaggdate["min"], 2),
                "meandata": round(srcaggdate["mean"], 2),
                "maxpercent": round(srcperaggdate["max"], 2),
                "minpercent": round(srcperaggdate["min"], 2),
                "meanpercent": round(srcperaggdate["mean"], 2)
            }
        if splex_cpu_consumption_data.get(src_tgt_dict["tgt"]) and splex_cpu_consumption_percent_data.get(src_tgt_dict["tgt"]):
            tgt_dict = convert_dict(splex_cpu_consumption_data[src_tgt_dict["tgt"]])
            df_tgt = pd.DataFrame({"tgt": tgt_dict})
            tgtaggdate = df_tgt["tgt"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
            tgt_per_dict = convert_dict(splex_cpu_consumption_percent_data[src_tgt_dict["tgt"]])
            df_tgt_per = pd.DataFrame({"tgt_per": tgt_per_dict})
            tgtperaggdate = df_tgt_per["tgt_per"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
            tgtdata = {
                "maxdata": round(tgtaggdate["max"], 2),
                "mindata": round(tgtaggdate["min"], 2),
                "meandata": round(tgtaggdate["mean"], 2),
                "maxpercent": round(tgtperaggdate["max"], 2),
                "minpercent": round(tgtperaggdate["min"], 2),
                "meanpercent": round(tgtperaggdate["mean"], 2)
            }
        depotDaoManager.commit()
    except DatabaseError as e:
        logger.error("splex_performance_comparision_data met error %s" % e)
        raise wbxexception(
            "Error ocurred when get info on splex_performance_comparision_data in DepotDB with msg %s" % e)
    if errormsg:
        status = "FAIL"
    else:
        splex_src_cpu_consumption_data = {
            "detailed_data": splex_cpu_consumption_data[src_tgt_dict["src"]],
            "percent_data": splex_cpu_consumption_percent_data[src_tgt_dict["src"]]}
        splex_tgt_cpu_consumption_data = {
            "detailed_data": splex_cpu_consumption_data[src_tgt_dict["tgt"]],
            "percent_data": splex_cpu_consumption_percent_data[src_tgt_dict["tgt"]]}
    return {
        "status": status,
        "splex_delay_data": splex_delay_data,
        "splex_src_cpu_consumption_data": splex_src_cpu_consumption_data,
        "splex_tgt_cpu_consumption_data": splex_tgt_cpu_consumption_data,
        "errormsg": errormsg,
        "data": df.groupby("label").count().to_dict()["rep_time"],
        "srcdata": srcdata,
        "tgtdata": tgtdata
    }

def get_percent_cpu_consumption_data(data):
    cpu_core_num_dict = {
        "rsdboradiag002": 16,
        "tadbrpt2": 24
    }
    new_dict = {}
    for k, v in data.items():
        new_dict[k] = []
        for item in v:
            for k1, v1 in item.items():
                new_dict[k].append({
                    k1: round(v1 * 100 / (cpu_core_num_dict[k] * 60), 2)
                })
    return new_dict



# def get_splex_comparision_performance_monitor2(starttime, endtime):
#     splex_delay_data = {}
#     splex_cpu_consumption_data = {}
#     errormsg = {}
#     status = "SUCCEED"
#     start_time = wbxutil.convertStringtoDateTime(starttime)
#     end_time = wbxutil.convertStringtoDateTime(endtime)
#     daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
#     depotDaoManager = daoManagerFactory.getDefaultDaoManager()
#     depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
#     try:
#         depotDaoManager.startTransaction()
#         splex_delay_data = depotdbDao.getwbxsplexdelaytime(start_time, end_time)
#         if not splex_delay_data:
#             errormsg[
#                 "splex_delay_data"] = "Failed to find shareplex delay time info from depot by starttime: %s endtime:%s " % (
#             start_time, end_time)
#         splex_cpu_consumption_data = depotdbDao.getwbxsplexcpuconsumption(start_time, end_time)
#         df = pd.DataFrame({"rep_time": repTimeList})
#         aggdate = df["rep_time"].describe(percentiles=[.5, .9, .995, .999]).to_dict()
#         aggdict = OrderedDict()
#         aggdict["count"] = aggdate["count"]
#         aggdict["mean"] = round(aggdate["mean"], 2)
#         aggdict["50%"] = round(aggdate["50%"], 2)
#         aggdict["90%"] = round(aggdate["90%"], 2)
#         aggdict["99.5%"] = round(aggdate["99.5%"], 2)
#         aggdict["99.9%"] = round(aggdate["99.9%"], 2)
#         df["label"] = pd.cut(df["rep_time"], [0, 2, 4, 10, 60, 120, sys.maxsize * 1.0],
#                              labels=["count_2s", "count_4s", "count_10s", "count_60s", "count_120s", "count_more"])
#         dbresdict = {"DB_NAME": dbid,
#                      "subtext": aggdict,
#                      "data": df.groupby("label").count().to_dict()["rep_time"]}
#         if not splex_cpu_consumption_data:
#             errormsg[
#                 "splex_cpu_consumption_data"] = "Failed to find shareplex cpu consumption info from depot by starttime: %s endtime:%s " % (
#             start_time, end_time)
#         depotDaoManager.commit()
#     except DatabaseError as e:
#         logger.error("splex_performance_comparision_data met error %s" % e)
#         raise wbxexception(
#             "Error ocurred when get info on splex_performance_comparision_data in DepotDB with msg %s" % e)
#     if errormsg:
#         status = "FAIL"
#     return {
#         "status": status,
#         "splex_delay_data": splex_delay_data,
#         "splex_cpu_consumption_data": splex_cpu_consumption_data,
#         "errormsg": errormsg
#     }
