import datetime, time
import json
import logging
import http.client
import string
import random
import calendar
from dateutil import tz
import subprocess
import os


timeformat = "%Y-%m-%d %H:%M:%S"
dateformat = "%Y-%m-%d"

logger = logging.getLogger("DBAMONITOR")

class wbxutil:

    @staticmethod
    def isNoneString(str):
        if str is None or str.strip() == '':
            return True
        else:
            return False

    @staticmethod
    def getcurrenttime(timerange = None):
        if timerange is None:
            return datetime.datetime.utcnow()
        else:
            mytime = datetime.datetime.utcnow() - datetime.timedelta(seconds=timerange)
            return mytime

    @staticmethod
    def gettimestr(timerange = None):
        if timerange is None:
            return datetime.datetime.utcnow().strftime(timeformat)
        else:
            mytime = datetime.datetime.utcnow() + datetime.timedelta(seconds=timerange)
            return mytime.strftime(timeformat)

    @staticmethod
    def convertStringtoDateTime(str):
        if str is None or str == '':
            return None
        return datetime.datetime.strptime(str, timeformat)

    @staticmethod
    def convertStringToGMTString(str):
        if str is None:
            return None
        from_tz = tz.gettz('America/Los_Angeles')
        to_tz = tz.tzutc()
        dt = datetime.datetime.strptime(str, timeformat)
        dt = dt.replace(tzinfo=from_tz)
        central = dt.astimezone(to_tz)
        return wbxutil.convertDatetimeToString(central)


    @staticmethod
    def convertDatetimeToString(dtime):
        if dtime is None:
            return ""
        return datetime.datetime.strftime(dtime, timeformat)

    @staticmethod
    def convertStringToDate(ddate):
        if ddate is None:
            return ""
        return datetime.datetime.strptime(ddate, dateformat)

    @staticmethod
    def convertTimeToDateTime(ttime):
        return  datetime.datetime(*ttime[0:6])

    @staticmethod
    def convertTimeToString(ttime):
        time.strftime(timeformat, ttime)

    @staticmethod
    def convertTimestampToString(ttimestamp):
        curdatetime = datetime.datetime.utcfromtimestamp(ttimestamp)
        return wbxutil.convertDatetimeToString(curdatetime)

    @staticmethod
    def convertDateTimeToStringForES(dt):
        NEW_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
        return dt.strftime(NEW_FORMAT)[:-3]

    def convertESStringToDatetime(str_datetime):
        NEW_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
        return datetime.datetime.strptime(str_datetime, NEW_FORMAT)

    @staticmethod
    def convertStringToTimestamp(str):
        d = wbxutil.convertStringtoDateTime(str)
        return calendar.timegm(d.timetuple())

    @staticmethod
    def convertTimestampToDatetime(ttimestamp):
        curdatetime = datetime.datetime.utcfromtimestamp(ttimestamp)
        return curdatetime

    @staticmethod
    def convertTZtimeToDatetime(strTZtime):
        NEW_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
        curdatetime = datetime.datetime.strptime(strTZtime, NEW_FORMAT)
        return curdatetime

    @staticmethod
    def convertString(xstr):
        if xstr is None:
            return xstr
        elif xstr.strip() == '':
            return None
        else:
            return str(xstr)

    @staticmethod
    def convertRow2Dict(row):
        dict = {}
        for column in row.__table__.columns:
            dict[column.name] = json.dumps(getattr(row, column.name))
        return dict

    @staticmethod
    def convertRowProxy2Dict(obj):
        objdict = {}
        for name, val in obj.items():
            if isinstance(val, str):
                objdict[name] = val
            else:
                objdict[name]  = json.dumps(val)
        return objdict

    @staticmethod
    def getShareplexSchemanamebyPort(port):
        return "splex%s" % port


    @staticmethod
    def sendRequest(url, path):
        conn = None
        try:
            conn = http.client.HTTPSConnection(url)
            conn.request("POST", path)
            res = conn.getresponse()
            data = res.read().decode("ascii")
            print(data)
        except Exception as e :
            print(e)
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def generateNewPassword():
        chars = string.ascii_letters + string.digits
        length = random.randint(8,12)
        while True:
            pos = random.randint(2, length - 3)
            hasupper = haslower = hasdigit = False
            pwd = ''.join([random.choice(chars) if i != pos else '#' for i in range(length)])
            if pwd[0] in string.digits:
                continue

            for c in pwd:
                if c in string.ascii_uppercase:
                    hasupper = True
                elif c in string.ascii_lowercase:
                    haslower = True
                elif c in string.digits:
                    hasdigit = True

            if hasupper and haslower and hasdigit:
                return pwd

    @staticmethod
    def installdbpatch(releasenumber):
        release_name = "WBXdbpatch-%s" % releasenumber
        p = subprocess.Popen('sudo rpm -qa | grep %s' % release_name, shell=True, stdout=subprocess.PIPE)
        out, err = p.communicate()
        vres = out.decode("ascii").strip().replace("\n", "")
        if not (vres is None or "" == vres):
            subprocess.Popen("sudo rpm -e %s" % vres, shell=True, stdout=subprocess.PIPE)
        release_dir = os.path.join("/tmp", str(releasenumber))
        if os.path.isdir(release_dir):
            subprocess.Popen('sudo rm -rf %s' % release_dir, shell=True, stdout=subprocess.PIPE)
        os.system("sudo yum -y install %s" % release_name)

    @staticmethod
    def converttodict(obj):
        is_list = obj.__class__ == [].__class__
        is_set = obj.__class__ == set().__class__

        if is_list or is_set:
            obj_arr = []
            for o in obj:
                dict = wbxutil.converttodict(o)
                obj_arr.append(dict)
            return obj_arr
        else:
            dict = {}
            dict.update(obj.to_dict())
            return dict

if __name__ == "__main__":
    ddate1 = wbxutil.convertStringToDate("2018-09-04")
    ddate2 = wbxutil.convertStringToDate("2018-09-06")
    datedelta = (ddate2 - ddate1).days
    print(datedelta)

