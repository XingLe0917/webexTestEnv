import logging
from flasgger import swag_from
from flask import Blueprint, jsonify, request
from biz.metadata.host_info import (
    list_host, update_host, 
    create_host, list_host_related_to_database,
    get_host
)
from biz.metadata.database_info import (
    list_database, create_database, 
    update_database, list_database_related_to_host,
    get_database
)
from biz.metadata.appln_pool_info import (
    get_user, create_user, update_user
)
from biz.metadata.instance_info import (
    get_instance, create_instance, update_instance
)
from biz.metadata.shareplex_info import (
    get_shareplex, create_shareplex, update_shareplex,
    checkcpk, b64pk
)

logger = logging.getLogger("DBAMONITOR")

route = Blueprint("metadata", __name__, url_prefix="/api/metadata")

@route.route("/hosts", methods=["GET"])
# @swag_from("/apidocs/metadata/host_info/list_host.yml")
def api_list_host():
    return jsonify(list_host(**request.args))

@route.route("/hosts", methods=["POST"])
@swag_from("/apidocs/metadata/host_info/create_host.yml")
def api_create_host():
    host_name = request.json.get("host_name", None)
    if host_name is None:
        return jsonify({"error": "parameter error"}), 400
    
    ins = get_host(host_name)
    if ins:
        return jsonify(ins), 200
    try:
        ins = create_host(request.json)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    else:
        return jsonify(ins), 201

@route.route("/hosts/<host_name>/update", methods=["POST"])
@swag_from("/apidocs/metadata/host_info/update_host.yml")
def api_update_host(host_name):

    if not host_name:
        return jsonify({"error": "parameter error, need host_name"}), 400

    if host_name != request.json.get("host_name"):
        return jsonify({"error": "the host_name of path and body not match"}), 400

    if get_host(host_name) is None:
        return jsonify({"error": "host not found!"}), 404

    # full update
    try:
        ins = update_host(host_name, request.json)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    else:
        return jsonify(ins)

@route.route("/hosts/<host_name>/databases", methods=["GET"])
# @swag_from("/apidocs/metadata/database_info/database_related_to_host.yml")
def api_list_host_related_database(host_name):
    result = list_database_related_to_host(host_name)
    return jsonify(result)

@route.route("/databases", methods=["GET"])
# @swag_from("/apidocs/metadata/database_info/list_database.yml")
def api_list_database():
    data = list_database()
    return jsonify(data)

@route.route("/databases", methods=["POST"])
@swag_from("/apidocs/metadata/database_info/create_database.yml")
def api_create_database():
    db_name = request.json.get("db_name", None)
    if db_name is None:
        return jsonify({"error": "parameter error"}), 400
    
    ins = get_database(db_name)
    if ins:
        return jsonify(ins), 200

    try:
        ins = create_database(request.json)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    else:
        return jsonify(ins), 201

@route.route("/databases/<db_name>/update", methods=["POST"])
@swag_from("/apidocs/metadata/database_info/update_database.yml")
def api_update_database(db_name):
    if db_name is None:
        return jsonify({"error": "parameter error, need db_name"}), 400

    if db_name != request.json.get("db_name"):
        return jsonify({"error": "the db_name of path and body not match!"}), 400

    if get_database(db_name) is None:
        return jsonify({"error": "database not found!"}), 404

    # full update
    try:
        ins = update_database(db_name, request.json)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    else:
        return jsonify(ins)


@route.route("/databases/<db_name>/hosts", methods=["GET"])
# @swag_from("/apidocs/metadata/host_info/host_related_to_database.yml")
def api_list_database_related_host(db_name):
    ins = list_host_related_to_database(db_name)
    return jsonify(ins)


@route.route("/db-users", methods=["GET"])
# @swag_from("/apidocs/metadata/appln_pool_info/list_user.yml")
def api_list_db_user():
    return jsonify()


@route.route("/db-users", methods=["POST"])
@swag_from("/apidocs/metadata/appln_pool_info/create_user.yml")
def api_create_db_user():
    db_name = request.json.get("db_name", None)
    schemaname = request.json.get("schemaname", None)
    if db_name is None or schemaname is None:
        return jsonify({"error": "parameter error"}), 400
    
    ins = get_user(db_name, schemaname)
    if ins:
        return jsonify(ins), 200

    try:
        ins = create_user(request.json)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    else:
        return jsonify(ins), 201

@route.route("/db-users/<db_name>/<schemaname>/update", methods=["POST"])
@swag_from("/apidocs/metadata/appln_pool_info/update_user.yml")
def api_update_db_user(db_name, schemaname):
    if db_name is None or schemaname is None:
        return jsonify({"error": "parameter error, need db_name, schemaname"}), 400

    if db_name != request.json.get("db_name") or schemaname != request.json.get("schemaname"):
        return jsonify({"error": "the db_name and schemaname of path and body not match!"}), 400

    if get_user(db_name, schemaname) is None:
        return jsonify({"error": "user not found!"}), 404

    # full update
    try:
        ins = update_user(db_name, schemaname, request.json)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    else:
        return jsonify(ins)


@route.route("/databases/<db_name>/db-users", methods=["GET"])
# @swag_from("/apidocs/metadata/appln_pool_info/list_user_related_to_database.yml")
def api_list_user_related_to_database(db_name, schemaname):
    return jsonify()

@route.route("/instances", methods=["GET"])
# @swag_from("/apidocs/metadata/instance_info/list_instance.yml")
def api_list_instance():
    return jsonify()

@route.route("/instances", methods=["POST"])
@swag_from("/apidocs/metadata/instance_info/create_instance.yml")
def api_create_instance():
    host_name = request.json.get("host_name", None)
    instance_name = request.json.get("instance_name", None)
    if host_name is None or instance_name is None:
        return jsonify({"error": "parameter error"}), 400

    ins = get_instance(host_name, instance_name)
    if ins:
        return jsonify(ins), 200

    try:
        ins = create_instance(request.json)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    else:
        return jsonify(ins), 201    


@route.route("/instances/<host_name>/<instance_name>/update", methods=["POST"])
@swag_from("/apidocs/metadata/instance_info/update_instance.yml")
def api_update_instance(host_name, instance_name):
    if host_name is None or instance_name is None:
        return jsonify({"error": "parameter error, need host_name, instance_name"}), 400

    if host_name != request.json.get("host_name") or instance_name != request.json.get("instance_name"):
        return jsonify({"error": "the host_name and instance_name of path and body not match!"}), 400

    if get_instance(host_name, instance_name) is None:
        return jsonify({"error": "user not found!"}), 404

    # full update
    try:
        ins = update_instance(host_name, instance_name, request.json)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    else:
        return jsonify(ins)


@route.route("/databases/<db_name>/instances", methods=["GET"])
# @swag_from("/apidocs/metadata/instance_info/list_instance_related_to_database.yml")
def api_list_database_related_instance(db_name, schemaname):
    return jsonify()

@route.route("/shareplexs", methods=["GET"])
# @swag_from("/apidocs/metadata/shareplex_info/list_shareplex.yml")
def api_list_shareplex_info():
    return jsonify()

@route.route("/shareplexs", methods=["POST"])
@swag_from("/apidocs/metadata/shareplex_info/create_shareplex.yml")
def api_create_shareplex_info():
    if not checkcpk(request.json):
        return jsonify({"error": "parameter error"}), 400

    ins = get_shareplex(b64pk(request.json))
    if ins:
        return jsonify(ins), 200

    try:
        ins = create_shareplex(request.json)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    else:
        return jsonify(ins), 201  

@route.route("/shareplexs/update", methods=["POST"])
@swag_from("/apidocs/metadata/shareplex_info/update_shareplex.yml")
def api_update_shareplex():
    if not checkcpk(request.json):
        return jsonify({"error": "parameter error"}), 400
    
    if not get_shareplex(b64pk(request.json)):
        return jsonify({"error": "shareplex_info not found!"}), 404
    
    try:
        ins = update_shareplex(b64pk(request.json), request.json)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    else:
        return jsonify(ins), 200

@route.route("/databases/<db_name>/shareplexs", methods=["GET"])
# @swag_from("/apidocs/metadata/shareplex_info/list_shareplex_related_to_database.yml")
def api_list_shareplex_related_to_database(dbname, schemaname):
    return jsonify()
