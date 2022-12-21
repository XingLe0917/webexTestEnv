from common.wbxutil import wbxutil
import datetime
from datetime import timedelta
import subprocess

def get_hourly_chime(dt, step=0, rounding_level="s"):
    if rounding_level == "days":
        td = timedelta(days=-step, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute, hours=dt.hour, weeks=0)
        new_dt = dt - td
    elif rounding_level == "hour":
        td = timedelta(days=0, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute, hours=-step, weeks=0)
        new_dt = dt - td
    elif rounding_level == "min":
        td = timedelta(days=0, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=-step, hours=0, weeks=0)
        new_dt = dt - td
    elif rounding_level == "s":
        td = timedelta(days=0, seconds=-step, microseconds=dt.microsecond, milliseconds=0, minutes=0, hours=0, weeks=0)
        new_dt = dt - td
    else:
        new_dt = dt
    return new_dt

import re
if __name__ == "__main__":
    str="123456789"
    resList = re.findall(".{9}", str)
    print(resList)

    #
    # cmd = '''source /sjdbormt/shareplex863/bin/.profile_u19062;
    #          /sjdbormt/shareplex863/bin/sp_ctrl << EOF
    #          qstatus
    #          lstatus
    #          EOF
    #       '''
    # pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
    # for line in pipe.readlines():
    #     if line.startswith("  Name:"):
    #         print(line.split())
    #
    # start_time = datetime.datetime(2020, 2, 28, 1, 50, 0)
    # end_time = datetime.datetime(2020, 3, 5, 3, 50, 0)
    # df = pd.DataFrame({"rep_time":[6.02,5.73,6.77,6.74,6.2,6.3,6.23,6.84,6.65,5.27,6.97,5.96,5.55,5.33,6.05,5.88,5.8,6.14,6.14,6.02,6.17,8.42,6.6,6.86,6.17,5.76,6.44,6.32,6.25,5.94,6.96,7.46,5.34,5.82,5.67,6.6,6,6.98,6.29,5.89,6.29,6.4,6.79,5.95,8.76,6.85,6.45,5.87,9.51,9.99,6.54,6.06,6.14,5.92,5.91,6.18,6.84,6.11,6.67,6.13,5.6,11.21,6.08,5.99,6.31,6.07,6.9,6.99,6.83,6.54,9.13,5.52,6.46,9.65,6.18,5.03,4.95,5.46,5.49,10.07,6.41,6.38,4.33,4.74,6.27,5.31,5.19,5.24,6.69,5.2,5.45,5.73,5.46,5.88,5.9,5.8,6.14,6.4,5.75,5.59,5.59,5.47,6.79,5.53,5.83,6.07,5.98,5.89,6.03,6.22,5.9,9,6.51,6.6,6.82,9.07,6.98,6.03,6.08,8.3,6.5,6.39,7.28,5.51,5.48,8.01,5.42,5.88,6.46,5.39,6.39,6.01,5.92,6.51,5.8,5.87,5.78]})
    # # print(df)
    # df["label"]  = pd.cut(df["rep_time"],[0,5,6,7,100], labels=["count_5s","count_6s","count_7s","count_more"])
    # # print(df["rep_time"].describe(percentiles=[.5,.9,.995,.999]).to_dict())
    # dbresdict = {"DB_NAME":"RACBWEB","subtext":df["rep_time"].describe(percentiles=[.5,.9,.995,.999]).to_dict(),
    #              "data":df.groupby("label").count().to_dict()["rep_time"]}
    # print(dbresdict)