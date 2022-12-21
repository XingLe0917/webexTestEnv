import json
import logging
import copy
import base64
from dao.wbxdaomanager import DaoKeys
from biz.metadata import get_session

logger = logging.getLogger("DBAMONITOR")


def checkcpk(payload):
    src_host = payload.get("src_host")
    src_db = payload.get("src_db")
    src_monitor_tablename = payload.get("src_monitor_tablename")
    port = payload.get("port")
    tgt_host = payload.get("tgt_host")
    tgt_db = payload.get("tgt_db")
    tgt_monitor_tablename = payload.get("tgt_monitor_tablename")
    qname = payload.get("qname")

    if all([
        src_host, src_db, src_monitor_tablename, 
        "%s" % port if port == 0 else port,
        tgt_host, tgt_db, tgt_monitor_tablename,
        qname
    ]):
        return True
    return False


def b64pk(payload):
    """
    base64 to '{"src_host":"","src_db","port":"","tgt_host":"","tgt_db":""}'
    """
    strs = ''
    strs+='{'
    strs+='"src_host":"{}",'.format(payload.get("src_host"))
    strs+='"src_db":"{}",'.format(payload.get("src_db"))
    strs+='"src_monitor_tablename":"{}",'.format(payload.get("src_monitor_tablename"))
    strs+='"port":{},'.format(payload.get("port"))
    strs+='"tgt_host":"{}",'.format(payload.get("tgt_host"))
    strs+='"tgt_db":"{}",'.format(payload.get("tgt_db"))
    strs+='"tgt_monitor_tablename":"{}",'.format(payload.get("tgt_monitor_tablename"))
    strs+='"qname":"{}"'.format(payload.get("qname"))
    strs+='}'
    return str(base64.b64encode(strs.encode("utf8")), 'utf8')
    

def db64pk(sec):
    return base64.b64decode(sec).decode("utf8")


def get_shareplex(cpk):
    """check data in pg auditdb"""
    conditions = json.loads(db64pk(cpk))
    try:
        session = get_session(DaoKeys.DAO_PGDEPOTDBDAO)
        sql = """
        select
            src_host,
            src_db,
            src_monitor_tablename,
            port,
            tgt_host,
            tgt_db,
            tgt_monitor_tablename,
            src_splex_sid,
            tgt_splex_sid,
            src_schema,
            tgt_schema,
            qname,
            created_by,
            modified_by,
            createtime,
            lastmodifiedtime      
        from shareplex_info
        where 
            src_host='{src_host}' and 
            src_db='{src_db}' and
            src_monitor_tablename = '{src_monitor_tablename}' and
            port = {port} and 
            tgt_host = '{tgt_host}' and
            tgt_db = '{tgt_db}' and
            tgt_monitor_tablename = '{tgt_monitor_tablename}' and
            qname = '{qname}'
        """.format(**conditions)

        row = session.execute(sql).fetchone()
        if not row:
            return
        res = dict(zip(row.keys(), row))
    except Exception as e:
        logger.error(str(e))
    else:
        return res
    finally:
        session.close()


def process_oracle_data(payload):
    oracle_data = copy.deepcopy(payload)
    oracle_data.pop("createtime", None)
    oracle_data.pop("lastmodifiedtime", None)
    oracle_data.pop("src_monitor_tablename", None)
    oracle_data.pop("tgt_monitor_tablename", None)
    oracle_data["date_added"] = "sysdate"
    oracle_data["lastmodifieddate"] = "sysdate"

    return oracle_data

def persistance_oracle_data(sess, payload):
    oracle_data = process_oracle_data(payload)

    # column var: list[col, ]
    column_var = ",".join(list(oracle_data.keys()))

    # value var: list[:col, ]
    value_var = ",".join([":%s" % col for col in list(oracle_data.keys())])
    sql = """
    insert into shareplex_info({})
    values({})
    """.format(column_var, value_var)
    
    print("########## create oracle shareplex info ###########")
    print("create oracle shareplex sql: ", sql)
    print("create oracle data: ", oracle_data)
    print("########## create oracle shareplex info ###########")
    # sess.execute(sql, oracle_data)


def update_oracle_data(sess, payload):
    oracle_data = process_oracle_data(payload)
    oracle_data.pop("date_added")
    src_host = oracle_data.pop("src_host")
    src_db = oracle_data.pop("src_db")
    port = oracle_data.pop("port")
    tgt_host = oracle_data.pop("tgt_host")
    tgt_db = oracle_data.pop("tgt_db")

    col_val = []

    for col, val in oracle_data.items():
        if isinstance(val, (int, float)):
            col_val.append("%s=%s" % (col, val))
        else:
            col_val.append("%s='%s'" % (col, val))

    col_val_str = ",\n".join(col_val)

    sql = """
    update shareplex_info set
    {col_val_str}
    where 
    src_host='{src_host}' and 
    src_db='{src_db}' and
    port={port} and
    tgt_host='{tgt_host}' and
    tgt_db='{tgt_db}'
    """.format(
            col_val_str=col_val_str, 
            src_host=src_host, 
            src_db=src_db,
            port=port,
            tgt_host=tgt_host,
            tgt_db=tgt_db
        )
    
    print("########## update oracle shareplex info ###########")
    print("update oracle shareplex sql: ", sql)
    print("########## update oracle shareplex info ###########")
    # sess.execute(sql)



def persistance_pg_data(sess, payload):
    pg_data = copy.deepcopy(payload)
    column_var = ",".join(list(pg_data.keys()))
    # value_var = ",".join(["'{%s}'" % col for col in column_var.split(",")])

    value_var = []
    for col in column_var.split(","):
        if isinstance(payload[col], (int, float)):
            value_var.append("{%s}" % col)
        else:
            value_var.append("'{%s}'" % col)
    
    value_var = ",".join(value_var)

    sql = """
    insert into depot.shareplex_info({})
    values
    ({})
    """.format(column_var, value_var)

    print("########## create pg shareplex info ###########")
    print("create pg shareplex info sql: ", sql.format(**payload))
    print("########## create pg shareplex info ###########")

    # sess.execute(sql.format(**pg_data))


def update_pg_data(sess, payload):
    pg_data = copy.deepcopy(payload)
    src_host = pg_data.pop("src_host")
    src_db = pg_data.pop("src_db")
    port = pg_data.pop("port")
    tgt_host = pg_data.pop("tgt_host")
    tgt_db = pg_data.pop("tgt_db")

    col_val = []

    for col, val in pg_data.items():
        if isinstance(val, (int, float)):
            col_val.append("%s=%s" % (col, val))
        else:
            col_val.append("%s='%s'" % (col, val))

    col_val_str = ",\n".join(col_val)

    sql = """update depot.shareplex_info 
    set
    {col_val_str}
    where 
    src_host = '{src_host}' and 
    src_db = '{src_db}' and
    port = {port} and
    tgt_host = '{tgt_host}' and
    tgt_db = '{tgt_db}';
    """.format(
            col_val_str=col_val_str, 
            src_host=src_host, 
            src_db=src_db,
            port=port,
            tgt_host=tgt_host,
            tgt_db=tgt_db
        )

    print("########## update pg shareplex info ###########")
    print("update pg shareplex info sql: ", sql.format(**payload))
    print("########## update pg shareplex info ###########")

    # sess.execute(sql)


def create_shareplex(payload):
    oracle_sess = get_session(DaoKeys.DAO_DEPOTDBDAO)
    sess = get_session(DaoKeys.DAO_PGDEPOTDBDAO)

    try:
        persistance_oracle_data(oracle_sess, payload)
        persistance_pg_data(sess, payload)

        oracle_sess.commit()
        sess.commit()
    except Exception as e:
        logger.error(str(e))
        oracle_sess.rollback()
        sess.rollback()
        raise e
    else:
        ins = get_shareplex(b64pk(payload))
        return ins
    finally:
        oracle_sess.close()
        sess.close()


def update_shareplex(cpk, payload):
    oracle_sess = get_session(DaoKeys.DAO_DEPOTDBDAO)
    sess = get_session(DaoKeys.DAO_PGDEPOTDBDAO)

    try:
        update_oracle_data(oracle_sess, payload)
        update_pg_data(sess, payload)        

        # commit after sql execution
        oracle_sess.commit()
        sess.commit()
    except Exception as e:
        logger.error(str(e))
        oracle_sess.rollback()
        sess.rollback()
        raise e
    else:
        ins = get_shareplex(cpk)
        return ins
    finally:
        oracle_sess.close()
        sess.close()