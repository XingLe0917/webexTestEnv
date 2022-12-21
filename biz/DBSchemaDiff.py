import logging

from common.sshConnection import SSHConnection

logger = logging.getLogger("DBAMONITOR")

dbname = ['configdbha','racbwebha','racibwebha','racijwebha','racuwebha','raciewebha','iwwebha',
          'racswebha','racewebha','racacwebha','racawwebha','racrvwebha','racvwebha','raccvwebha',
          'racmvwebha','racevwebha','racavwebha','racvvwebha','racrwebha','racahwebha','raclwebha',
          'racmwebha','racjwebha','racaawebha','racaowebha','racaswebha','racwwebha','raciwebha',
          'racaiwebha','racbiwebha','racftwebha','racctwebha','racdtwebha','racatwebha','racixwebha',
          'racgtwebha','rachtwebha','racltwebha','racjtwebha','racetwebha','racktwebha','racsywebha',
          'racauwebha','racbuwebha','racmuwebha','raccuwebha']

def getDiffFileNameByDate(diff_date):
    conn = SSHConnection('sjgrcabt107.webex.com', 22, 'cassandra', 'oNu9hq%y5W')
    # cmd = 'cd /usr/local/nginx/html/diff/;ls'
    cmd = 'cd /usr/local/nginx/html/diff/;ls | grep dbdiff_'+diff_date+'-*'
    ls = conn.exec_command(cmd)
    file_name = ''
    if ls:
        files = ls.split()
        print(len(files))
        conn.close()
        for i in files:
            if i.startswith("dbdiff_"+diff_date):
                logger.info(i)
                if i>file_name:
                    file_name = i
        logger.info("getDiffFileNameByDate(diff_date=%s) return file_name=%s" % (
            diff_date, file_name))
    file = {"file_name": file_name}
    return file

def getDiffFileByDataDB(conn,target_db,diff_date):
    cmd = 'cd /usr/local/nginx/html/diff/;ls | grep dbdiff_' + target_db + '_' + diff_date + '-*'
    f = conn.exec_command(cmd)
    file_name = ''
    if f:
        files = f.split()
        for i in files:
            if i.startswith("dbdiff_" + target_db + '_' + diff_date):
                if i > file_name:
                    file_name = str(i)
    return file_name

def dbSchemaDiff(target_db,diff_date):
    logger.info(
        "dbSchemaDiff, target_db=%s diff_date=%s " % (target_db, diff_date))
    data = {}
    if target_db not in dbname:
        data["file_name"] = ''
        data["message"] = 'target db incorrect'
        return data
    target_dbname = target_db+".webex.com"
    conn = SSHConnection('sjgrcabt107.webex.com', 22, 'cassandra', 'oNu9hq%y5W')
    file = getDiffFileByDataDB(conn,target_dbname,diff_date)
    if file:
        data["file_name"] = str(file)
        data["message"] = ''
    else:
        logging.debug("start diff job on target_db {0}" .format(target_dbname))
        sh_cmd = "sh /opt/dbvertool/dbdiff.sh immediate " + target_dbname
        cmd = 'sudo su - dbaadmin -c ' + "'" + sh_cmd + "'"
        logging.debug(cmd)
        f = conn.exec_command(cmd)
        logging.debug(f)
        data["file_name"] = ''
        data["message"] = str(f,"utf-8")
        logging.debug(data)
    return data

if __name__ == "__main__":
    getDiffFileNameByDate("2020-04-26")
    # data = dbSchemaDiff("racaiwebha","2020-01-21")
    # print(data)



