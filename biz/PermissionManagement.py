import logging
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from common.wbxexception import wbxexception

logger = logging.getLogger("DBAMONITOR")


def address_role_page_list(info):
    dict = pre_address_role_page_list(info)
    print(dict)
    rst_list = []
    for k, v in dict.items():
        rst_list.append(
            {
                "route": k,
                "roleInfo": v
            }
        )
    return rst_list


def address_access_dir_list(dir_list):
    rst = []
    for item in dir_list:
        rst.append("/".join(item[1].split("/")[1:]))
        if item[0] not in rst:
            rst.append(item[0])
    return rst


def address_full_access_dir_list(dir_list):
    rst = []
    for item in dir_list:
        rst.append(item[0] + item[1])
        if item[0] not in rst:
            rst.append(item[0])
    return rst


def pre_address_role_page_list(info):
    dict = {}
    map = {
        "1": True,
        "0": False
    }
    for item in info:
        if item[0] + item[1] not in dict.keys():
            dict[item[0] + item[1]] = {}
        dict[item[0] + item[1]][item[2]] = map[item[3]]
    return dict


def get_changed_role_page_list(exist_list, new_list):
    changed_list = []
    map = {
        True: "1",
        False: "0"
    }
    for item in new_list:
        if item["roleInfo"] != exist_list[item["route"]]:
            for key in item["roleInfo"]:
                if item["roleInfo"].get(key, None) != exist_list[item["route"]].get(key, None):
                    page_dir = "/" + "/".join(item["route"].split("/")[2:])
                    parent_page_dir = "/" + item["route"].split("/")[1]
                    changed_list.append(
                        [parent_page_dir, page_dir, key, map[item["roleInfo"][key]]])
    return changed_list


def get_user_role_dict_from_depot():
    user_role_info_dict = {}
    status = "success"
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        role_list = depotdbDao.get_role_list()
        for role_name in role_list:
            user_list = depotdbDao.get_user_list_by_rolename(role_name)
            user_role_info_dict[role_name] = user_list
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
        user_role_info_dict = "cannot get role list from depot"
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return {
        "status": status,
        "databaseDict": user_role_info_dict
    }


def assign_role_to_user_to_depot(username, role_name):
    status = "success"
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        depotdbDao.assign_role_to_user(username, role_name)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return {
        "status": status
    }


def delete_user_from_role_to_depot(username, role_name):
    status = "success"
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        depotdbDao.delete_user_from_role(username, role_name)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return {
        "status": status
    }


def get_role_page_dict_from_depot(url_list):
    role_page_list = []
    depot_url_list = []
    errormsg = ""
    status = "success"
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        depot_url_list = depotdbDao.get_existed_url_list()
        extra_url_key_list = list(set(url_list.keys()) - set(depot_url_list.keys()))
        logger.info("to be added pages: %s" % extra_url_key_list)
        if extra_url_key_list:
            for key_item in extra_url_key_list:
                url_item = url_list[key_item]
                page_dir = "/" + "/".join(url_item.split("/")[2:])
                parent_page_dir = "/" + url_item.split("/")[1]
                depotdbDao.add_page_to_depot(page_dir, parent_page_dir, key_item)
        deleted_url_key_list = list(set(depot_url_list.keys()) - set(url_list.keys()))
        logger.info("to be deleted_url_key_list pages: %s" % extra_url_key_list)
        if deleted_url_key_list:
            for key_item in deleted_url_key_list:
                url_item = depot_url_list[key_item]
                page_dir = "/" + "/".join(url_item.split("/")[2:])
                parent_page_dir = "/" + url_item.split("/")[1]
                depotdbDao.delete_page_from_depot(page_dir, parent_page_dir, key_item)
        depotDaoManager.commit()
        role_page_info = depotdbDao.get_page_role_list()
        if role_page_info:
            role_page_list = address_role_page_list(role_page_info)
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return {
        "status": status,
        "routeArray": role_page_list if role_page_list else errormsg
    }


def change_role_to_page_to_depot(route_list):
    changed_list = []
    role_page_list = []
    status = "success"
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        role_page_info = depotdbDao.get_page_role_list()
        if role_page_info:
            role_page_list = pre_address_role_page_list(role_page_info)
        changed_list = get_changed_role_page_list(role_page_list, route_list)
        for url_info_item in changed_list:
            depotdbDao.update_role_permission(url_info_item[1], url_info_item[0], url_info_item[2], url_info_item[3])
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return {
        "status": status
    }


def add_role_to_page_to_depot(role_name):
    status = "success"
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        role_list = depotdbDao.get_role_list()
        if role_name in role_list:
            raise wbxexception('the role name exists')
        depot_url_list = depotdbDao.get_existed_url_list()
        if not depot_url_list:
            raise wbxexception('there is no page info in depot')
        for page_name, url_item in depot_url_list.items():
            page_dir = "/" + "/".join(url_item.split("/")[2:])
            parent_page_dir = "/" + url_item.split("/")[1]
            depotdbDao.add_role_to_page(page_dir, parent_page_dir, role_name, "0")
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return {
        "status": status
    }


def identify_user_access_from_depot(username):
    status = "success"
    identity_flag = True
    role_list = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        role_list = depotdbDao.get_role_list_by_name(username)
        if not role_list:
            raise wbxexception("fail to get role info of %s from depot" % username)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
        identity_flag = False
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return {
        "status": status,
        "identity": identity_flag,
        "roles": role_list
    }


def check_login_user_from_depot(username):
    status = "success"
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        userrst = depotdbDao.check_login_user(username)
        if not userrst:
            raise wbxexception("there is no user info in depot")
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return { "status": status }


def get_role_list_from_depot():
    status = "success"
    role_info = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        role_info = depotdbDao.get_role_list()
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return {
        "status": status,
        "roleNameArray": role_info
    }


def get_access_dir_from_depot(username):
    status = "success"
    dir_list= []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        dir_list = depotdbDao.get_access_dir(username)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return {
        "status": status,
        "accessedRoutes": address_access_dir_list(dir_list),
        "fullaccessRouter": address_full_access_dir_list(dir_list)
    }

def get_favourit_page_by_username(username):
    status = "success"
    dir_list = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        dir_list = depotdbDao.get_access_favourite_dir(username)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return {
        "status": status,
        "accessedRoutes": address_access_dir_list(dir_list),
        "fullaccessRouter": address_full_access_dir_list(dir_list)
    }

def add_favourite_page(username, page_name, url):
    status = "success"
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        page_dir = "/" + "/".join(url.split("/")[2:])
        parent_page_dir = "/" + url.split("/")[1]
        depotdbDao.add_user_favourite_page(username, page_name, page_dir, parent_page_dir)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return {"status": status}


def delete_favourite_page(username, page_name, url):
    status = "success"
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        page_dir = "/" + "/".join(url.split("/")[2:])
        parent_page_dir = "/" + url.split("/")[1]
        depotdbDao.delete_user_favourite_page(username, page_name, page_dir, parent_page_dir)
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        logger.error(errormsg)
        status = "fail"
    finally:
        if depotDaoManager:
            depotDaoManager.close()
    return {"status": status}


def get_health():
    import socket
    ip = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('pccp.webex.com', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return 'LocalIP:%s' % ip
