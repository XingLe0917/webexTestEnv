import logging
import copy
from dao.wbxdaomanager import DaoKeys
from biz.metadata import get_session

logger = logging.getLogger("DBAMONITOR")


def get_instance(host_name, instance_name):
    """instance_info only for oracle db"""
    try:
        session = get_session(DaoKeys.DAO_DEPOTDBDAO)
        sql = """
        select
            trim_host,
            host_name,
            db_name,
            instance_name,
            date_added,
            lastmodifieddate,
            created_by,
            modified_by
        from instance_info
        where host_name='{}' and instance_name='{}'
        """.format(host_name, instance_name)

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

def process_oracle_data(payload):
    """transmit to oracle not null data"""
    oracle_payload = copy.deepcopy(payload)
    return oracle_payload


def persistance_oracle_data(sess, payload):
    oracle_data = process_oracle_data(payload)

    # column var: list[col, ]
    column_var = ",".join(list(oracle_data.keys()))

    # value var: list[:col, ]
    value_var = ",".join([":%s" % col for col in list(oracle_data.keys())])
    sql = """
    insert into instance_info({})
    values({})
    """.format(column_var, value_var)

    print("########## create oracle instance_info ###########")
    print("create oracle instance_info sql: ", sql)
    print("create oracle data: ", oracle_data)
    print("########## create oracle instance_info ###########")

    # sess.execute(sql, oracle_data)


def update_oracle_data(sess, payload):
    oracle_data = process_oracle_data(payload)
    host_name = oracle_data.pop("host_name")
    instance_name = oracle_data.pop("instance_name")

    col_val = []

    for col, val in oracle_data.items():
        if isinstance(val, (int, float)):
            col_val.append("%s=%s" % (col, val))
        else:
            col_val.append("%s='%s'" % (col, val))

    col_val_str = ",\n".join(col_val)

    sql = """
    update instance_info set
    {col_val_str}
    where 
    host_name = '{host_name}' and instance_name = '{instance_name}'
    """.format(col_val_str=col_val_str, host_name=host_name, instance_name=instance_name)
    

    print("########## update oracle instance_info ###########")
    print("update oracle instance_info sql: ", sql)
    print("########## update oracle instance_info ###########")
    # sess.execute(sql)
    return {}

def create_instance(payload):
    oracle_sess = get_session(DaoKeys.DAO_DEPOTDBDAO)
    
    try:
        persistance_oracle_data(oracle_sess, payload)

        oracle_sess.commit()
    except Exception as e:
        logger.error(str(e))
        oracle_sess.rollback()
        raise e
    else:
        ins = get_instance(payload["host_name"], payload["instance_name"])
        oracle_sess.close()
        return ins

def update_instance(host_name, instance_name, payload):
    oracle_sess = get_session(DaoKeys.DAO_DEPOTDBDAO)

    try:
        update_oracle_data(oracle_sess, payload)

        oracle_sess.commit()
    except Exception as e:
        logger.error(str(e))
        oracle_sess.rollback()
        raise e
    else:
        ins = get_instance(host_name, instance_name)
        oracle_sess.close()
        return ins
