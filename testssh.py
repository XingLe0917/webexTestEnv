import paramiko
from paramiko import SSHClient
import socket


def callback(str):
    print(str)

if __name__ == "__main__":
    client = SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    host_name = "txdbormt040"
    pwd = "Rman$1357"
    try:
        client.connect("%s.webex.com" % host_name, port=22, username="oracle",password=pwd)
        cmd = "sh /home/oracle/test.sh"
        stdin, stdout, stderr = client.exec_command(cmd, bufsize=10)
        stdout_iter = iter(stdout.readline, '')
        stderr_iter = iter(stderr.readline, '')

        for out, err in izip_longest(stdout_iter, stderr_iter):
            print("iterator")
            if out: callback(out.strip())
            if err: callback(err.strip())
        #
        # return stdin, stdout, stderr
    except (paramiko.BadHostKeyException, paramiko.BadAuthenticationType, paramiko.SSHException, socket.error) as e:
        print(str(e))

    # print("end")