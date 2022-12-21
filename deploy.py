import cx_Oracle
import base64
import paramiko
import os
import datetime, time
import sys
import math


filter_file_list= ["__pycache__", "WebexTestEnv/test/", "WebexTestEnv/.idea", "WebexTestEnv/conf/logger.conf"]

def progress_bar(portion, total):
    part = total / 50
    count = math.ceil(portion / part)
    sys.stdout.write('\r')
    sys.stdout.write(('[%-50s]%.2f%%' % (('>' * count), portion / total * 100)))
    sys.stdout.flush()


class ConnServer(object):
    def __init__(self):
        self.server_usr = "oracle"
        self.server_pwd = b"Um1hbiQxMzU3"
        self.local_dir = os.getcwd()
        self.remote_dir = "/home/oracle/zhiwliu/WebexTestEnv"
        self.host_name = "sjgrcabt102"
        self.port = 22
        self.ssh = None

    def conn_server(self):
        if self.ssh:
            return "", True
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.ssh.connect(hostname=self.host_name + ".webex.com", port=22, username=self.server_usr, password=str(base64.b64decode(self.server_pwd), "utf-8"))
        except Exception as e:
            return str(e), False
        return "", True

    def disconn_server(self):
        self.ssh.close()

    def exec_ssh_in_server(self, exec_cmd):
        rst, rst_bool = self.conn_server()
        if not rst_bool:
            return rst, False
        try:
            std, stdout, stderr = self.ssh.exec_command(exec_cmd)
            res, err = stdout.read(), stderr.read()
            res_code = stdout.channel.recv_exit_status()  # exiting code for executing
            if res_code == 0:
                return str(res.decode()), True
            return str(err.decode()), False
        except Exception as e:
            return str(e), False

    def get_all_files_in_local_dir(self, local_dir):
        all_files = []
        files = os.listdir(local_dir)
        for x in files:
            filename = os.path.join(local_dir, x)
            if os.path.isdir(filename):
                all_files.extend(self.get_all_files_in_local_dir(filename))
            else:
                all_files.append(filename)
        return all_files

    def transfer_file_by_sftp(self, source_file, target_file):
        try:
            sftp = paramiko.SFTPClient.from_transport(self.ssh.get_transport())
            sftp.put(source_file, target_file)
        except Exception as e:
            Log("!!! " + str(e))
            self.safe_exit()

    def sftp_put_dir(self):
        res, res_bool = self.conn_server()
        if not res_bool:
            Log("!!! " + res)
            self.safe_exit()
        all_files = self.get_all_files_in_local_dir(self.local_dir)
        portion = 0
        total = len(all_files)
        Log("Starting to deploy...")
        for file_item in all_files:
            continue_flag = False
            portion += 1
            file_name_list = self.get_file_name_list(file_item)
            remote_filename = "/".join([self.remote_dir, *file_name_list])
            for filter_item in filter_file_list:
                if filter_item in remote_filename:
                    continue_flag = True
            if continue_flag:
                continue
            parent_dir = "/".join(remote_filename.split("/")[:-1])
            if len(file_name_list) > 0 and not self.isDirectory(parent_dir):
                self.mkdir_remote_dir(parent_dir)
            self.transfer_file_by_sftp(file_item, remote_filename)
            progress_bar(portion, total)
        sys.stdout.write("\n")
        self.change_target_script_own(self.remote_dir)

    def isDirectory(self, pathname):
        cmd = "if [ -d %s ]; then echo 'Y'; else echo 'N'; fi" % pathname
        res, res_bool = self.exec_ssh_in_server(cmd)
        if res.split("\n")[0] == "Y":
            return True
        else:
            return False

    def change_target_script_own(self, dir_name):
        # Log("Starting to change ownership of %s" % dir_name)
        cmd = "sudo chmod -R 777 %s" % dir_name
        res, res_bool = self.exec_ssh_in_server(cmd)
        if not res_bool:
            Log("!!! " + res)
            self.safe_exit()
        cmd = "sudo chown oracle:root %s" % dir_name
        res, res_bool = self.exec_ssh_in_server(cmd)
        if not res_bool:
            Log("!!! " + res)
            self.safe_exit()

    def get_file_name_list(self, filename):
        file_name_list = []
        if filename == self.local_dir:
            return []
        if "\\" in filename:
            file_name_list = filename.split("\\")
            local_dir_list = self.local_dir.split("\\")
        elif "\\\\" in filename:
            file_name_list = filename.split("\\\\")
            local_dir_list = self.local_dir.split("\\\\")
        elif "/" in filename:
            file_name_list = filename.split("/")
            local_dir_list = self.local_dir.split("/")
        file_name_list = file_name_list[len(local_dir_list):]
        return file_name_list

    def backup_Webex(self):
        cmd = "rm -fr %s" % self.remote_dir + ".bak"
        res, res_bool = self.exec_ssh_in_server(cmd)
        if not res_bool:
            Log("!!! " + res)
            self.safe_exit()
        Log("Starting to backup webexTestEnv...")
        cmd = "cp -r %s %s" % (self.remote_dir, self.remote_dir + ".bak")
        res, res_bool = self.exec_ssh_in_server(cmd)
        if not res_bool:
            Log("!!! " + res)
            self.safe_exit()

    def safe_exit(self):
        self.disconn_server()
        exit(0)

    def mkdir_remote_dir(self, dir_name):
        # Log("Starting to mkdir new %s" % dir_name)
        cmd = "sudo mkdir -p %s" % dir_name
        res, res_bool = self.exec_ssh_in_server(cmd)
        if not res_bool:
            Log("!!! " + res)
            self.safe_exit()
        self.change_target_script_own(self.remote_dir)

    def restart_ccp(self):
        cmd = "ps aux | grep ccpserver.py | grep python | grep -v bash | awk '{print $2}'"
        kill_num, kill_num_bool = self.exec_ssh_in_server(cmd)
        if not kill_num_bool:
            Log("!!! " + kill_num)
            self.safe_exit()
        kill_num = kill_num.split("\n")[0]
        print(kill_num)
        Log("Starting to stop ccp...")
        if kill_num:
            cmd = "kill %s" % kill_num
            res, res_bool = self.exec_ssh_in_server(cmd)
            if not res_bool:
                Log("!!! " + res)
                self.safe_exit()
        Log("Starting to restart ccp...")
        # cmd = """
        # source %s/venv/bin/activate; nohup `python %s/ccpserver.py` &
        # """ % (self.remote_dir, self.remote_dir)
        cmd = """
        sh %s/start_nohup.sh
        """ % self.remote_dir
        res, res_bool = self.exec_ssh_in_server(cmd)
        print(cmd, res, res_bool)
        time.sleep(1)
        if not res_bool:
            Log("!!! " + res)
            self.safe_exit()
        cmd = "ps aux | grep ccpserver.py | grep python | grep -v bash | awk '{print $2}'"
        kill_num, kill_num_bool = self.exec_ssh_in_server(cmd)
        if not kill_num_bool:
            Log("!!! " + kill_num)
            self.safe_exit()
        kill_num = kill_num.split("\n")[0]
        Log("New process id: %s" % kill_num)


def execute_self_server_cmd(cmd):
    try:
        res = os.popen(cmd)
    except Exception as e:
        Log(e)
        return e, False
    res_list = res.readlines()
    res_list = [i.split("\n")[0] for i in res_list]
    for item in res_list:
        Log(item)
    return res_list, True


def split_script_dir_and_filename(input_script):
    script_name_list = input_script.split("/")
    script_name = input_script
    script_dir = ""
    if len(script_name_list) > 2:
        script_name = script_name_list[-1]
        script_dir = "/".join(script_name_list[0:-1]) + "/"
    return script_name, script_dir


def get_server_name_list_from_config_file(filename):
    with open(filename, "r") as f:
        line = f.readlines()
        line = [x.strip() for x in line if x.strip() != '']
    return line

def Log(info):
    msg = "  >>  %s :: %s" % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), info)
    # with open(self.log_file, "a+") as f:
    #     f.write("\n" + msg)
    print(msg)


def check_git_status():
    cmd = "git status"
    res_list, res_bool = execute_self_server_cmd(cmd)
    if not res_bool or "Your branch is up to date with 'origin/master'" not in "".join(res_list):
        Log("!!! Please git pull the updated code first!")
        exit(0)


if __name__=="__main__":
    Log("Starting deploy CCP!")
    check_git_status()
    server = ConnServer()
    server.backup_Webex()
    server.sftp_put_dir()
    server.restart_ccp()
    server.disconn_server()
