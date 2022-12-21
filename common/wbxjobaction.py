# from wbxdatabase import wbxdatabase
import cx_Oracle
import sys
from common.wbxmail import wbxemailmessage, wbxemailtype, sendemailasync
import logging
from datetime import datetime
import time



def verifyuserschema(dbinfo):
    logging.info("verifyuserschema db_name=%s" % dbinfo.getdbname())
    connectstr = dbinfo.getconnectstring()
    schemadict = dbinfo.getschemadict()
    for schemainfo in schemadict.values():
        conn = None
        csr = None
        try:
            username = schemainfo["username"]
            pwd = schemainfo["password"]
            type = schemainfo["schematype"]
            # if type == wbxdatabase.SCHEMATYPE_SHAREPLEX_SPLEX:
            #     continue

            conn = cx_Oracle.connect("%s/%s@%s" % (username, pwd, connectstr))
            csr = conn.cursor()
            csr.execute('select sysdate from dual')
            csr.fetchone()
        except:
            topic = "VerifyUserSchema with %s failed" % username
            content = sys.exc_info()[0]
            content = "%s, %s" % (connectstr, content)
            msg = wbxemailmessage(wbxemailtype.EMAILTYPE_VERIFY_SCHEMA, content)
            sendemailasync(msg)
        finally:
            if csr is not None:
                csr.close()
            if conn is not None:
                conn.close()

def verifyshareplexreplication(tgtchanneldict):
    for port, channel in tgtchanneldict.items():
        srcdb = channel.srcdb
        tgtdb = channel.tgtdb
        srchost = channel.srchost
        tgthost = channel.tgthost
        srcdbname = srcdb.getdbname()
        tgtdbname = tgtdb.getdbname()
        direction = "%s2%s_%s" % (srcdbname, tgtdbname, datetime.strftime(datetime.now(), '%Y%m%d%H%M%S'))
        srcschema = channel.srcschema
        tgtschema = channel.tgtschema
        srcconnectstr = srcdb.getconnectstring()
        tgtconnectstr = tgtdb.getconnectstring()

        srcconn = None
        srccsr = None
        tgtconn = None
        tgtcsr = None
        try:
            srcpwd = srcdb.getuserpassword(srcschema)
            srcconn = cx_Oracle.connect("%s/%s@%s" % (srcschema, srcpwd, srcconnectstr))
            srccsr = srcconn.cursor()
            srccsr.execute('insert into SPLEX_MONITOR_ADB(direction,src_host, src_db, logtime, port_number) values(:1,:2,:3,sysdate, :4)',[direction, srchost, tgthost, port])
            srcconn.commit()
        except:
            srcconn.rollback()
            content = "schema=%s" % (srcschema)
            content = "%s; connection_string=%s" % (content, srcconnectstr)
            content = "%s, %s" % (content, sys.exc_info()[0])
            msg = wbxemailmessage(wbxemailtype.EMAILTYPE_VERIFY_SCHEMA, content)
            sendemailasync(msg)
        finally:
            if srccsr is not None:
                srccsr.close()
            if srcconn is not None:
                srcconn.close()

        time.sleep(10)
        try:
            tgtpwd = tgtdb.getuserpassword(tgtschema)

            tgtconn = cx_Oracle.connect("%s/%s@%s" % (tgtschema, tgtpwd, tgtconnectstr))
            tgtcsr = tgtconn.cursor()
            tgtcsr.execute('SELECT count(1) SPLEX_MONITOR_ADB where direction=:1',[direction])
            rows = tgtcsr.fetchone()
            if len(rows) == 0:
                content = "port=%s" % port
                content = "source_host=%s" % srchost
                content = "target_host=%s" % tgthost
                msg = wbxemailmessage(wbxemailtype.EMAILTYPE_SHAREPLEX_REPLICATION, content)
                sendemailasync(msg)
            else:
                tgtcsr.close()
                tgtcsr = tgtconn.cursor()
                tgtcsr.execute('DELETE FROM SPLEX_MONITOR_ADB where direction=:1', [direction])
                tgtconn.commit()
        except:
            tgtconn.rollback()
            content = "schema=%s" % (tgtschema)
            content = "%s; connection_string=%s" % (content, tgtconnectstr)
            content = "%s, %s" % (content, sys.exc_info()[0])
            msg = wbxemailmessage(wbxemailtype.EMAILTYPE_VERIFY_SCHEMA, content)
            sendemailasync(msg)
        finally:
            if tgtcsr is not None:
                tgtcsr.close()
            if tgtconn is not None:
                tgtconn.close()

if __name__ == "__main__":
    verifyuserschema()




