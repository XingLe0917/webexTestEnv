#!/bin/bash

###############This script is only used for oracle server, not verified on other type server
source /staging/gates/bash_common.sh
. /home/oracle/.bash_profile
if [ $# -eq 1 ]; then
    datastore_type="${1}"
else
    datastore_type="ORACLE"
fi
db_env="${2}"
if [ -z ${db_env} ]; then
    db_env="PROD"
fi

localhostname=`hostname -s`
logfilename=`getlogfilename "install_telegraf" ""`
osver=`uname -r | awk -F\. '{print $(NF-1)}'`
if [ -z ${osver} ]; then
    osver="el7"
fi
echo "current logfilename is ${logfilename}"
depotDBConnectStr="`getdepotdbconnectinfo`"
SQL="select distinct case when instr(site_code,'AMS') > 0 then 'AMS01'
                     when instr(site_code,'IAD') > 0 then 'IAD03'
                     when instr(site_code,'LHR') > 0 then 'LHR03'
                     when instr(site_code,'NRT') > 0 then 'NRT03'
                     when instr(site_code,'SJC') > 0 then 'SJC02'
                     when instr(site_code,'SIN') > 0 then 'SIN01'
                     when instr(site_code,'DFW01') > 0 then 'DFW01'
                     when instr(site_code,'DFW02') > 0 then 'DFW02'
                     when instr(site_code,'YYZ') > 0 then 'YYZ01'
                     when instr(site_code,'SYD') > 0 then 'SYD01'
                     else site_code end
    from host_info where host_name='${localhostname}';"
dc_name=`execSQL "${depotDBConnectStr}" "${SQL}"`
if [ "x${dc_name}" == "x" ]; then
    echo "WBXERROR: do not get this server ${localhostname} from depotdb"
    exit -1
fi

kafka_url=""
printmsg "dc_name=${dc_name}, datastore_type=${datastore_type}, db_env=${db_env}"

if [ ! -d /var/log/dbmonitor/data ]; then
    sudo mkdir -p /var/log/dbmonitor/data
fi
if [ ! -d /var/log/dbmonitor/bin ]; then
    sudo mkdir -p /var/log/dbmonitor/bin
fi
sudo chown -R oracle:oinstall /var/log/dbmonitor/
chmod -R 755 /var/log/dbmonitor

if [ $(sudo rpm -qa | grep telegraf | wc -l) -eq 0 ]; then
    printmsg "WBXINFO:Install telegraf..."
  if [ "${osver}" == "el6" ]; then
        sudo rpm -ivh http://repo.webex.com/prod/storage/redhat6.9_64/telegraf-1.12.4-1.x86_64.rpm
  elif [ "${osver}" == "el7" ]; then
      sudo rpm -ivh https://repo.webex.com/prod/ansibletower/centos7_64/telegraf-1.16.1-1.x86_64.rpm
  fi
else
    printmsg "WBXINFO: telegraf already installed"
fi

TELEGRAF_CONF_FILE="/etc/telegraf/telegraf.conf"
if [ ! -f ${TELEGRAF_CONF_FILE} ]; then
    printmsg "WBXERROR: The telegraf config file ${TELEGRAF_CONF_FILE} does not exist. WBXEXIT"
    exit -1
else
    sudo cp ${TELEGRAF_CONF_FILE} /etc/telegraf/telegraf.conf.`date '+%Y%m%d%H%M%S'`
fi

if [ $(id telegraf | wc -l) -eq 0 ]; then
    printmsg "WBXERROR: telegraf user does not exist.WBXERROR"
    exit -1
fi
printmsg "WBXINFO: telegraf has been installed"

PIP_PROG=`which pip`
haspip="Y"
if [ "x${PIP_PROG}" == "x" ]; then
    if [ "${osver}" == "el6" ]; then
        sudo wget --no-check-certificate https://bootstrap.pypa.io/2.6/get-pip.py
    elif [ "${osver}" == "el7" ]; then
        sudo wget https://bootstrap.pypa.io/get-pip.py
    fi
    if [ -f get-pip.py ]; then
        sudo python get-pip.py
        if [ "${osver}" == "el6" ]; then
            sudo pip install tzlocal==1.5.1 futures APScheduler==3.0.6 DBUtils==1.3 pyinotify==0.9.6 kafka-python==0.9.5 threadpool==1.3.2
        elif [ "${osver}" == "el7" ]; then
            sudo pip install tzlocal futures APScheduler==3.6.3 sqlalchemy==1.3.20 cx_Oracle==7.3.0 pexpect requests kafka-python==2.0.2
        fi
        printmsg "Already installed python lib"
    else
        printmsg "WBXWARNING: Can not download get-pip.py file. so can not install logparser_plugin"
        haspip="N"
    fi
fi

vstatus=$(checkServiceStatus "telegraf")
if [ "${vstatus}" == "RUNNING" ]; then
    printmsg "telegraf is running, reload it..."
    stopService telegraf
    sudo rm -f /var/run/telegraf/telegraf.pid
fi

if [ ! -f /staging/gates/telegraf/bin/shareplex_job.py ]; then
    printmsg "WBXERROR: the shareplex job file /staging/gates/telegraf/bin/shareplex_job.py does not exist.WBXERROR"
    exit -1
fi
if [ ! -f /staging/gates/telegraf/bin/remove_db_metric_data.sh ]; then
    printmsg "WBXERROR: the file /staging/gates/telegraf/bin/remove_db_metric_data.sh does not exist.WBXERROR"
    exit -1
fi
if [ ! -f /staging/gates/telegraf/bin/logparser_plugin.py ]; then
    printmsg "WBXERROR: the file /staging/gates/telegraf/bin/logparser_plugin.py does not exist.WBXERROR"
    exit -1
fi
## replace telegraf with LMA team provided version
#if [ -f /staging/gates/telegraf/telegraf ]; then
#    sudo cp /staging/gates/telegraf/telegraf /usr/bin/
#    case ${dc_name} in
#    SJC02)
#        kafka_url="lmabufwsjc2-kf-internal.prodlma.wbx2.com:9092"
#        ;;
#    DFW01)
#          kafka_url="lmabufwdfw2-kf-internal.prodlma.wbx2.com:9092"
#          ;;
#    DFW02)
#        kafka_url="lmabufwdfw2-kf-internal.prodlma.wbx2.com:9092"
#        ;;
#    *)
#        kafka_url=""
#        ;;
#    esac
#  printmsg "WBXINFO: kafka broker=${kafka_url}"
#else
#    printmsg "WBXERROR: /staging/gates/telegraf/telegraf does not exist"
#    exit -1
#fi
printmsg "WBXINFO: copy plugin script file"
cp -R /staging/gates/telegraf/bin /var/log/dbmonitor/
chmod -R 755 /var/log/dbmonitor/bin

. /home/oracle/.bash_profile

urls=""
nodes=$(olsnodes)
for node in ${nodes}
do
    vip0="${node}-vip"
    vip1="${node}-HAIP1"
    vip2="${node}-HAIP2"
    c=$(ping -c 3 ${vip1} 2>/dev/null | wc -l)
    if [ $c -gt 0 ]; then
        urls="\"${vip1}\",${urls}"
        cv=$(ping -c 3 ${vip2} 2>/dev/null | wc -l)
        if [ $cv -gt 0 ]; then
            urls="\"${vip2}\",${urls}"
        fi
    else
        cv=$(ping -c 3 ${vip0} 2>/dev/null | wc -l)
        if [ $cv -gt 0 ]; then
            urls="\"${vip0}\",${urls}"
        fi
    fi
done
urls=${urls%?}
if [ "x${urls}" == "x" ]; then
    printmsg "WBXERROR: does not find vip. WBXEXIT"
    exit -1
fi
printmsg "urls=${urls}"

if [ ! -f /etc/sudoers.d/00_telegraf ]; then
cat << EOF | sudo tee /etc/sudoers.d/00_telegraf
# /etc/sudoers.d/00_telegraf
# override of 00_telegraf provided at build to allow passwordless sudo su
# required to facilitate sudo access when using proxied login
# when telegraf is a non-password user which is owned by DBA team

telegraf        ALL=(root) NOPASSWD: ALL, PASSWD: /bin/sh,/bin/bash,/usr/bin/man
EOF
sudo chmod 440 /etc/sudoers.d/00_telegraf
fi
printmsg "Already enable sudo privilege for telegraf user"

printmsg "Update telegraf.conf file..."
cat << EOF | sudo tee ${TELEGRAF_CONF_FILE}
[global_tags]
  datastore="${datastore_type}"
  datacenter="${dc_name}"
  db_env="${db_env}"
[agent]
  interval = "15s"
  round_interval = true
  metric_batch_size = 1000
  metric_buffer_limit = 10000
  collection_jitter = "0s"
  flush_interval = "10s"
  flush_jitter = "0s"
  precision = ""
  logfile = "/var/log/telegraf/telegraf.log"
  hostname = ""
  omit_hostname = false
[[inputs.cpu]]
  percpu = true
  totalcpu = true
  collect_cpu_time = false
  report_active = false
[[inputs.disk]]
  ignore_fs = ["tmpfs", "devtmpfs", "devfs", "iso9660", "overlay", "aufs", "squashfs"]
[[inputs.diskio]]
[[inputs.kernel]]
[[inputs.mem]]
[[inputs.processes]]
[[inputs.swap]]
[[inputs.system]]
[[inputs.net]]
[[inputs.netstat]]
[[inputs.interrupts]]
[[inputs.ntpq]]
  dns_lookup = true
[[inputs.ping]]
  urls = [${urls}]
[[inputs.exec]]
commands = ["sudo python /var/log/dbmonitor/bin/shareplex_job.py"]
timeout = "240s"
interval = "300s"
data_format = "influx"
[[inputs.exec]]
commands = ["sudo ksh /var/log/dbmonitor/bin/remove_db_metric_data.sh"]
data_format = "influx"
interval = "4h"
[[inputs.tail]]
files = ["/var/log/dbmonitor/data/*.log"]
pipe = false
from_beginning = false
data_format ="influx"
watch_method = "inotify"
tagexclude = ["path"]
[[outputs.influxdb]]
  urls = ["http://10.250.86.165:8086"]
  database = "telegraf"
  username = "admin"
  password = "influx@Nihao"
EOF

#if [ "${haspip}" == "Y" ]; then
#cat << EOF | sudo tee -a ${TELEGRAF_CONF_FILE}
#[[inputs.exec]]
#commands = ["sudo python /var/log/dbmonitor/bin/logparser_plugin.py ${dc_name}"]
#timeout = "300s"
#interval = "24h"
#name_override = "os_log"
#data_format = "influx"
#EOF
#
#fi

#if [ "x${kafka_url}"  != "x" ]; then
#cat << EOF | sudo tee -a ${TELEGRAF_CONF_FILE}
#[[outputs.kafka_avro]]
#  topic = "metrics_dbaas"
#  brokers = ["${kafka_url}"]
#  data_format = "influx"
#  avro_magic_byte_required = true
#  schema_registry = "https://non-tls-client-auth-schmreg.wbx2.com/subjects/LmaEventSchema/versions/2"
#  source = "dbaas"
#  type = "dbaas"
#  payload_format = "line"
#EOF
#
#fi

if [ "${dc_name}" == "AMS01" ]; then
    brokers='"10.253.113.154:9092","10.253.113.143:9092"'
elif [ "${dc_name}" == "LHR03" ]; then
    brokers='"10.242.43.171:9092","10.242.43.165:9092"'
elif [ "${dc_name}" == "IAD03" ]; then
    brokers='"10.244.40.128:9092","10.244.40.129:9092"'
elif [ "${dc_name}" == "SJC02" ]; then
    brokers='"10.250.85.220:9092","10.250.85.225:9092"'
elif [ "${dc_name}" == "DFW01" ]; then
    brokers='"10.240.63.95:9092","10.240.63.96:9092"'
elif [ "${dc_name}" == "DFW02" ]; then
    brokers='"10.250.78.94:9092","10.250.78.85:9092"'
elif [ "${dc_name}" == "YYZ01" ]; then
    brokers='"10.250.88.30:9092","10.250.88.31:9092"'
elif [ "${dc_name}" == "NRT03" ]; then
    brokers='"10.254.223.186:9092","10.254.223.217:9092"'
elif [ "${dc_name}" == "SIN01" ]; then
    brokers='"10.254.206.27:9092","10.254.206.79:9092"'
elif [ "${dc_name}" == "SYD01" ]; then
    brokers='"10.246.52.64:9092","10.246.52.126:9092"'
else
    brokers='"10.250.85.220:9092","10.250.85.225:9092"'
fi
cat << EOF | sudo tee -a ${TELEGRAF_CONF_FILE}
[[outputs.kafka]]
  topic = "dbmetric-oracle-${dc_name}"
  brokers = [${brokers}]
  required_acks = 1
  max_retry = 3
EOF

startService telegraf
sleep 5
vstatus=$(checkServiceStatus telegraf)
if [ "${vstatus}" == "RUNNING" ]; then
    printmsg "WBXINFO: Telegraf has been installed successfully"
else
    printmsg "WBXERROR: Telegraf has been installed but not started"
    exit -1
fi


