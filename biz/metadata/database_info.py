import logging
import copy
from dao.wbxdaomanager import DaoKeys
from biz.metadata import get_session

logger = logging.getLogger("DBAMONITOR")


def get_database(db_name):
    try:
        session = get_session(DaoKeys.DAO_PGDEPOTDBDAO)
        sql = """
        select
            db_name,
            host_name,
            cluster_name,
            db_vendor,
            db_version,
            db_type,
            application_type,
            appln_support_code,
            db_home,
            listener_port,
            monitored,
            wbx_cluster,
            web_domain,
            createtime,
            lastmodifiedtime
        from depot.database_info
            where db_name='{}'
        """.format(db_name)
        row = session.execute(sql).fetchone()
        if not row:
            session.close()
            return
        res = dict(zip(row.keys(), row))
    except Exception as e:
        logger.error(str(e))
    else:
        session.close()
        return res


def list_database(
    db_name=None,
    host_name=None,
):
    try:
        sess = get_session(DaoKeys.DAO_PGDEPOTDBDAO)
    except Exception as e:
        return {"error": str(e)}
    else:
        sess.close()
        return {}


def process_oracle_data(payload):
    """transmit to oracle not null data"""

    oracle_payload = copy.deepcopy(payload)
    host_name = oracle_payload.pop("host_name")
    oracle_payload["trim_host"] = host_name[:-1]
    if oracle_payload.get("db_version", None) is None:
        oracle_payload["db_version"] = "null"
    if oracle_payload.get("db_type", None) is None:
        oracle_payload["db_type"] = "null"
    if oracle_payload.get("application", None) is None:
        oracle_payload["application_type"] = "null"
    if oracle_payload.get("appln_support_code", None) is None:
        oracle_payload["appln_support_code"] = "WEB"
    oracle_payload["date_added"] = "sysdate"
    oracle_payload["lastmodifieddate"] = "sysdate"
    oracle_payload["created_by"] = "AutomationTool"
    oracle_payload["modified_by"] = "AutomationTool"

    return oracle_payload


def persistance_oracle_data(sess, payload):
    oracle_data = process_oracle_data(payload)

    # column var: list[col, ]
    column_var = ",".join(list(oracle_data.keys()))

    # value var: list[:col, ]
    value_var = ",".join([":%s" % col for col in list(oracle_data.keys())])
    sql = """
    insert into database_info({})
    values({})
    """.format(column_var, value_var)

    print("########## create oracle database_info ###########")
    print("create oracle database sql: \n", sql)
    print("create oracle database data: \n", oracle_data)
    print("########## create oracle database_info ###########")
    # sess.execute(sql, oracle_data)


def update_oracle_data(sess, payload):
    oracle_data = process_oracle_data(payload)
    trim_host = oracle_data.pop("trim_host")
    db_name = oracle_data.pop("db_name")

    col_val = []

    for col, val in oracle_data.items():
        if isinstance(val, (int, float)):
            col_val.append("%s=%s" % (col, val))
        else:
            col_val.append("%s='%s'" % (col, val))

    col_val_str = ",\n".join(col_val)

    sql = """
    update database_info set
    {col_val_str}
    where 
    db_name = '{db_name}' and trim_host = '{trim_host}'
    """.format(col_val_str=col_val_str, db_name=db_name, trim_host=trim_host)
    
    # print("update oracle database sql: ", sql)
    print("########## update oracle database_info ###########")
    print("update oracle database sql: \n", sql)
    print("########## update oracle database_info ###########")
    # sess.execute(sql)
    return {}


def persistance_pg_data(sess, payload):
    pg_data = copy.deepcopy(payload)

    column_var = ",".join(list(pg_data.keys()))
    value_var = ",".join(["'{%s}'" % col for col in column_var.split(",")])

    sql = """
        insert into depot.database_info({})
        values
        ({})
        """.format(column_var, value_var)

    print("########## create pg database_info ###########")
    print("create pg database sql: ", sql.format(**pg_data))
    print("########## create pg database_info ###########")

    # sess.execute(sql.format(**pg_data))


def update_pg_data(sess, payload):
    pg_data = copy.deepcopy(payload)
    db_name = pg_data.pop("db_name")
    col_val = []

    for col, val in pg_data.items():
        if isinstance(val, (int, float)):
            col_val.append("%s=%s" % (col, val))
        else:
            col_val.append("%s='%s'" % (col, val))

    col_val_str = ",\n".join(col_val)

    sql = """update depot.host_info 
    set
    {col_val_str}
    where 
    db_name = '{db_name}';
    """.format(col_val_str=col_val_str, db_name=db_name)

    print("########## update pg database_info ###########")
    print("update pg database sql: ", sql.format(**pg_data))
    print("########## update pg database_info ###########")
    
    # sess.execute(sql)


def create_database(payload):
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
        ins = get_database(payload["db_name"])
        oracle_sess.close()
        sess.close()
        return ins


def update_database(db_name, payload):
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
        ins = get_database(db_name)
        oracle_sess.close()
        sess.close()
        return ins

def list_database_related_to_host(host_name):
    return {}