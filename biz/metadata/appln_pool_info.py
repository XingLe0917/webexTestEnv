import logging
import copy
from dao.wbxdaomanager import DaoKeys
from biz.metadata import get_session

logger = logging.getLogger("DBAMONITOR")


def get_user(db_name, schemaname):
    try:
        session = get_session(DaoKeys.DAO_PGDEPOTDBDAO)
        sql = """
        select
            db_name,
            appln_support_code,
            schemaname,
            password,
            password_vault_path,
            schematype,
            createtime,
            lastmodifiedtime
        from depot.appln_pool_info
            where db_name='{}' and schemaname='{}';
        """.format(db_name, schemaname)
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
    return

def process_oracle_data(payload):
    oracle_data = copy.deepcopy(payload)
    oracle_data.pop("password_vault_path")
    oracle_data.pop("created_by")
    oracle_data.pop("modified_by")
    oracle_data.pop("createtime")
    oracle_data.pop("lastmodifiedtime")

    # TODO how to get host_name
    oracle_data["trim_host"] = "null" 
    oracle_data["schema"] = oracle_data.pop("schemaname") 
    if oracle_data.get("password", None) is None:
        oracle_data["password"] = "null" 
    oracle_data["date_added"] = "sysdate"
    oracle_data["lastmodifieddate"] = "sysdate"

    oracle_data["km_version"] = "null" 
    oracle_data["new_password"] = "null" 
    oracle_data["change_status"] = "null"    
    return oracle_data

def persistance_oracle_data(sess, payload):
    oracle_data = process_oracle_data(payload)

    # column var: list[col, ]
    column_var = ",".join(list(oracle_data.keys()))

    # value var: list[:col, ]
    value_var = ",".join([":%s" % col for col in list(oracle_data.keys())])
    sql = """
    insert into appln_pool_info({})
    values({})
    """.format(column_var, value_var)
    
    print("########## create oracle appln_pool_info ###########")
    print("create oracle user sql: ", sql)
    print("create oracle data: ", oracle_data)
    print("########## create oracle appln_pool_info ###########")
    # sess.execute(sql, oracle_data)

def update_oracle_data(sess, payload):
    oracle_data = process_oracle_data(payload)
    trim_host = oracle_data.pop("trim_host")
    db_name = oracle_data.pop("db_name")
    schema = oracle_data.pop("schema")

    col_val = []

    for col, val in oracle_data.items():
        if isinstance(val, (int, float)):
            col_val.append("%s=%s" % (col, val))
        else:
            col_val.append("%s='%s'" % (col, val))

    col_val_str = ",\n".join(col_val)

    sql = """
    update appln_pool_info set
    {col_val_str}
    where 
    db_name='{db_name}' and 
    trim_host='{trim_host}' and
    schema='{schema}'
    """.format(
            col_val_str=col_val_str, 
            db_name=db_name, 
            trim_host=trim_host,
            schema=schema
        )
    
    print("########## update oracle appln_pool_info ###########")
    print("update oracle user sql: ", sql)
    print("########## update oracle appln_pool_info ###########")
    # sess.execute(sql)


def persistance_pg_data(sess, payload):
    pg_data = copy.deepcopy(payload)
    column_var = ",".join(list(pg_data.keys()))
    value_var = ",".join(["'{%s}'" % col for col in column_var.split(",")])

    sql = """
    insert into depot.appln_pool_info({})
    values
    ({})
    """.format(column_var, value_var)

    print("########## create pg appln_pool_info ###########")
    print("create pg appln_pool_info sql: ", sql.format(**payload))
    print("########## create pg appln_pool_info ###########")

    # sess.execute(sql.format(**pg_data))


def update_pg_data(sess, payload):
    pg_data = copy.deepcopy(payload)

    db_name = pg_data.pop("db_name")
    schemaname = pg_data.pop("schemaname")
    col_val = []

    for col, val in pg_data.items():
        if isinstance(val, (int, float)):
            col_val.append("%s=%s" % (col, val))
        else:
            col_val.append("%s='%s'" % (col, val))

    col_val_str = ",\n".join(col_val)

    sql = """update depot.appln_pool_info 
    set
    {col_val_str}
    where 
    db_name = '{db_name}' and schemaname = '{schemaname}';
    """.format(col_val_str=col_val_str, db_name=db_name, schemaname=schemaname)

    print("########## update pg appln_pool_info ###########")
    print("update pg appln_pool_info sql: ", sql.format(**payload))
    print("########## update pg appln_pool_info ###########")

    # sess.execute(sql)

def create_user(payload):
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
        ins = get_user(payload["db_name"], payload["schemaname"])
        oracle_sess.close()
        sess.close()
        return ins


def update_user(db_name, schemaname, payload):
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
        ins = get_user(db_name, schemaname)
        oracle_sess.close()
        sess.close()
        return ins