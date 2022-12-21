import sys
import logging
from biz.autotask.wbxdbcutover import wbxdbcutover
from common.Config import Config
# Do not remove this import, they will be initialize dynamically. otherwise, the project will start failed
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.vo.depotdbvo import wbxdatabase
from dao.vo.depotdbvo import wbxserver
from dao.vo.depotdbvo import wbxschema


# logging.basicConfig(format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',level=logging.WARNING)


def init():
    depotdb = wbxdatabase()
    depotdb.appln_support_code = "DEFAULT"
    config = Config.getConfig()
    (username, pwd, url) = config.getDepotConnectionurl()
    depotdb.connectioninfo = url
    server = wbxserver()
    depotdb.addServer(server)
    schema = wbxschema(schema=username, password=pwd)
    depotdb.addSchema(schema)
    daomanagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daomanagerFactory.addDatabase("DEFAULT", depotdb)
    # loadDepotDBInfo()

if __name__ == "__main__":
    old_host_name = "sjdborasystool001"
    new_host_name = "sjdbormt086"
    db_name = "RACPSYT"
    db_splex_sid = "PSYTOOL_SPLEX"
    env = "PROD"
    if len(sys.argv) < 4:
        logging.error("There is at least 3 input parameter")
        sys.exit(-1)
    role = sys.argv[1]
    module = sys.argv[2]
    action = sys.argv[3]
    logfilename="C:\\Users\\zhiwliu\\logs\\wbxdbcutover_%s_%s_%s.log" %(role, module,action)
    # logfilename = "/tmp/dbmanagement/wbxdbcutover_%s_%s_%s.log" % (role, module, action)
    logging.basicConfig(filename=logfilename, filemode='a', level=logging.WARNING,format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    if role not in ("OLD","NEW","SRC"):
        logging.error("Error: The first parameter allowable value is OLD/NEW/SRC")
        sys.exit(-1)

    if module not in ("DB","SHAREPLEX","OTHER"):
        logging.error("Error: The 2nd parameter allowable value is OLD/NEW/SRC")
        sys.exit(-1)

    if action not in ("PRECUTOVER","PREPARE","SETUP","START","STOP"):
        logging.error("Error: The 3th parameter allowable value can be PRECUTOVER/PREPARE/START/POST/SETUP")
        sys.exit(-1)

    init()
    logging.warning("role=%s, module=%s, action=%s" % (role, module, action))
    if module == "SHAREPLEX" and action == "POST" and role == "SRC":
        old_host_name = new_host_name
    cutover = wbxdbcutover(old_host_name, new_host_name, db_name, env, db_splex_sid)
    cutover.preverify()
    # cutover.cutover("OLD", "DB", "PRECUTOVER")
    # cutover.cutover("OLD", "SHAREPLEX", "PRECUTOVER")
    # cutover.cutover("NEW", "SHAREPLEX", "PRECUTOVER")
    # cutover.cutover("SRC", "SHAREPLEX", "PRECUTOVER")
    # cutover.cutover("NEW", "OTHER", "STOP")
    # cutover.cutover("NEW", "OTHER", "START")
    # cutover.cutover("NEW", "OTHER", "POSTREGISTER")
    # cutover.cutover("OLD", "SHAREPLEX", "PREPARE")
    # cutover.cutover("SRC", "SHAREPLEX", "PREPARE")
    cutover.cutover("SRC", "SHAREPLEX", "POST")
    # cutover.cutover("NEW", "SHAREPLEX", "POST")
    # cutover.cutover("NEW", "OTHER", "SETUP")
