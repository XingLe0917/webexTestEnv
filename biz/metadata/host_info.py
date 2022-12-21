import copy
import logging
from dao.wbxdaomanager import DaoKeys
from biz.metadata import get_session

logger = logging.getLogger("DBAMONITOR")


def get_host(host_name):
    try:
        session = get_session(DaoKeys.DAO_PGDEPOTDBDAO)
        sql = """
        select 
            cname,            
            host_name,        
            domain,           
            site_code,        
            region_name,      
            public_ip,        
            private_ip,       
            os_type_code,     
            processor,        
            kernel_release,   
            hardware_platform,
            physical_cpu,     
            cores,            
            cpu_model,        
            flag_node_virtual,
            install_date,     
            comments,         
            ssh_port,         
            createtime,       
            lastmodifiedtime 
        from depot.host_info
            where host_name='{}'
        """.format(host_name)
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


def list_host(
    host_name=None,
    domain=None,
    site_code=None,
    page=1,
    page_size=20,
    order_by="host_name"
):
    page = int(page)
    page_size = int(page_size)
    result = {
        "header": [],
        "rows":[],
    }

    try:
        session = get_session(DaoKeys.DAO_DEPOTDBDAO)
        sql = """
        select 
            trim_host,host_name,domain,site_code,host_ip,vip_name,
            vip_ip,priv_name,priv_ip,scan_name,scan_ip1,scan_ip2,
            scan_ip3,os_type_code,processor,kernel_release,hardware_platform,
            physical_cpu,cores,cpu_model,flag_node_virtual,install_date,date_added,
            lastmodifieddate,comments,lc_code,ssh_port,created_by,modified_by
        from depot.host_info
        where 1=1
        """
        if host_name:
            sql+="""and host_name='{}'
            """.format(host_name)
        
        if site_code:
            sql+="""and site_code='{}'
            """.format(site_code)

        if order_by:
            sql+="""order by {}
            """.format(order_by)

        sql+="""offset {} rows fetch next {} rows only
        """.format((page-1)*page_size, page_size)

        data = session.execute(sql).fetchall()
    except Exception as e:
        logger.error(str(e))
        return {"error": str(e)}
    else:
        result["header"] = list(data[-1].keys())
        for row in data:
            result["rows"].append(dict(zip(row.keys(), row)))
        session.close()
        return result


def process_oracle_data(payload):
    oracle_payload = copy.deepcopy(payload)
    oracle_payload.pop("cname", None)
    oracle_payload.pop("region_name", None)
    oracle_payload.pop("public_ip", None)
    oracle_payload.pop("private_ip", None)
    oracle_payload.pop("createtime", None)
    oracle_payload.pop("lastmodifiedtime", None)
    if oracle_payload.get("physical_cpu", None) is None:
        oracle_payload["physical_cpu"] = "null"
    oracle_payload["priv_ip"] = payload["private_ip"]
    oracle_payload["trim_host"] = payload["host_name"][:-1]
    oracle_payload["host_ip"] = payload["public_ip"]
    oracle_payload["date_added"] = "sysdate"
    oracle_payload["lastmodifieddate"] = "sysdate"
    oracle_payload["lc_code"] = "null"
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
    insert into host_info({})
    values({})
    """.format(column_var, value_var)

    print("########## create oracle host_info ###########")
    print("create oracle host_info sql: \n", sql)
    print("create oracle host_info data: \n", oracle_data)
    print("########## create oracle host_info ###########")
    # sess.execute(sql, oracle_data)

def update_oracle_data(sess, payload):
    oracle_data = process_oracle_data(payload)
    trim_host = oracle_data.pop("trim_host")
    host_name = oracle_data.pop("host_name")
    col_val = []

    for col, val in oracle_data.items():
        if isinstance(val, (int, float)):
            col_val.append("%s=%s" % (col, val))
        else:
            col_val.append("%s='%s'" % (col, val))

    col_val_str = ",\n".join(col_val)

    sql = """
    update host_info set
    {col_val_str}
    where 
    host_name = '{host_name}' and trim_host = '{trim_host}'
    """.format(col_val_str=col_val_str, host_name=host_name, trim_host=trim_host)
    print("########## update oracle host_info ###########")
    print("update oracle host_info sql: \n", sql)
    print("########## update oracle host_info ###########")
    sess.execute(sql)
    return {}

def persistance_pg_data(sess, payload):
    pg_data = copy.deepcopy(payload)

    column_var = ",".join(list(pg_data.keys()))
    value_var = ",".join(["'{%s}'" % col for col in column_var.split(",")])

    sql = """
        insert into depot.host_info({})
        values
        ({})
        """.format(column_var, value_var)

    print("########## create pg host_info ###########")
    print("create pg host_info sql: \n", sql.format(**payload))
    print("########## create pg host_info ###########")
    # sess.execute(sql.format(**payload))


def update_pg_data(sess, payload):
    pg_data = copy.deepcopy(payload)
    host_name = pg_data.pop("host_name")
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
    host_name = '{host_name}';
    """.format(col_val_str=col_val_str, host_name=host_name)

    print("########## update pg host_info ###########")
    print("update pg host_info sql: \n", sql)
    print("########## update pg host_info ###########")

    # sess.execute(sql)
    return {}


def create_host(payload):
    """the creation logic:
    payload is postgres host data,so will persistance both oracle and postgres
    """
    oracle_sess = get_session(DaoKeys.DAO_DEPOTDBDAO)
    sess = get_session(DaoKeys.DAO_PGDEPOTDBDAO)

    try:
        persistance_oracle_data(oracle_sess, payload)
        persistance_pg_data(sess, payload)

        # commit after all session query
        oracle_sess.commit()
        sess.commit()
    except Exception as e:
        logger.error(str(e))
        oracle_sess.rollback()
        sess.rollback()
        raise e
    else:
        ins = get_host(payload["host_name"])
        oracle_sess.close()
        sess.close()
        return ins

def update_host(host_name, payload):
    """full data update
    """
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
        ins = get_host(host_name)
        oracle_sess.close()
        sess.close()
        return ins

def list_host_related_to_database(db_name):
    return {}