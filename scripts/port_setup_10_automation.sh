#!/bin/bash
############################################################
#
#  port_setup_921.sh
#
#     This script is used to configure to new shareplex
#     port ,
#
#     input: port.resp -- Response file
#
#  1.0 Annapurna Dantu Venkata (adantuve@cisco.com)  -- 2010-May-22
#       -- Created.
#
#
############################################################

##########################
## 00.0 Functions
##########################

getInput() {
  # Arg 1: variable name
  # Arg 2: prompt
  # Arg 3: all the available option to select
  # Arg 4: the default value from the avaiable option
  # Arg 5: strict_check, to check if the input is only from the avaiable options; (optional), default: strict_check enable(y), y/n

  local _var _prompt _options _default _opt _strict_check

  _var=$1
  _prompt="$2"
  _options="$3"
  _default="$4"

  if [ $# -eq 4 ]; then
    _strict_check="y"
  else
    _strict_check="$5"
  fi
#echo "_var: ${_var}"
#echo "_prompt: ${_prompt}"
#echo "_options: ${_options}"
#echo "_default: ${_default}"
#echo "_strict_check: ${_strict_check}"

  if [ -z "${_options}" ]; then _options="${_default}"; fi

  _msg="${_prompt} (${_options}) [${_default}]"

  _isDefault=0

  _retry=0
  while [ ${_retry} -lt 3 ]; do

    printf "\n%s :" "${_msg}" | tee -a ${_LOG_FILE}
    read _opt

    if [ -z "${_opt}" ]; then
      if [ -n "${_default}" ]; then
        _opt="${_default}"
      fi
    else
      # user has proivided the input
      if [ -n "${_options}" ]; then
        if [ "${_strict_check}" = "y" -a `echo "${_options}" | tr ',' ' ' | grep -wc "${_opt}"` -eq 0 ]; then
          # provided option is not in the list of options
          echo "${_opt} is not in the list of provided options"
          _opt=""
        fi
      fi
    fi

    if [ -n "${_opt}" ]; then
      break ;
    fi

    _retry=`expr ${_retry} + 1`
  done

  if [ "${_default}" = "n" -o "${_default}" = "y" ]; then _opt="`echo ${_opt} | tr [:upper:] [:lower:]`"; fi
  Log "User Selected: ${_opt}"

  eval "${_var}=\${_opt}"
}

# calling the function

#echo "teting the function"
#getInput _ret "testing the function" "1,2,3,4" "1" "y"
#echo "ret: ${_ret}"
#exit 0

Log() {
  # Arg 1: message to output/log

  _msg="  >>  `date '+%Y-%m-%d %H:%M:%S'` :: $1"
  echo "${_msg}" | tee -a ${_LOG_FILE}
}

DONE() {

  local _opt

  Log ""
  Log ""
  if [ $# -gt 0 ]; then
    # this function has been called at the END
    Log "PORT ${_port} HAS BEEN SETUP SUCCESSFULLY!!!"
    Log ""
  fi
  Log "VERIFY THE LOG FILE ${_LOG_FILE} FOR ANY DETAILED INFO."
  Log ""
  Log "Done!!!"
  Log ""
}

##########################
## 01.0 prerequisites and constants
##########################

if [ "${USER}" != "oracle" ]; then
  echo ""
  echo "This script should be executed as oracle, it cannot be executed as ${USER}. Exiting..."
  echo ""
  exit -1
fi

_Now=`date '+%Y%m%d%H%M%S'`

## host/vip ip
_this_host="`hostname -s`"
_trim_host="`echo ${_this_host} | sed s/.$//`"
_this_host_vip="${_this_host}-vip"
source /staging/gates/bash_common.sh
#_other_hosts="`cat /etc/hosts | grep ${_trim_host} | egrep -v "${_this_host}|vip|priv|scan|HAIP" | awk '{print $2}' | cut -d'.' -f1 | tr '\n' ' ' | sed s/.$//`"
 _grid_home="`echo $ORA_ASM_HOME`"
_other_hosts="`$_grid_home/bin/olsnodes |grep -v ${_this_host} | tr '\n' ' '`"
#echo -e "_this_host=${_this_host}|\n_trim_host=${_trim_host}|\n_this_host_vip=${_this_host_vip}|\n_other_hosts=${_other_hosts}|\n"

# script directory
_dir_name="`dirname $0`"
#printf "%s:%s:\n" "_dir_name" "${_dir_name}"

_periord="."
#if [ "${_dir_name}" = "${_periord}" ]; then
#  _SCRIPT_DIR="`pwd`"
#elif [ `echo "${_dir_name}" | grep -c "\."` -ge 1 ] ; then
#  _SCRIPT_DIR="`pwd`/${_dir_name}"
#else
#  _SCRIPT_DIR="${_dir_name}"
#fi
_SCRIPT_DIR="{INPUTVAR_SCRIPT_DIR}"

# create the log directory
if [ ! -d ${_SCRIPT_DIR}/logs ]; then
  sudo mkdir ${_SCRIPT_DIR}/logs
  sudo chown oracle:oinstall ${_SCRIPT_DIR}/logs
fi

# create the log directory
if [ ! -d ${_grid_home}/crs/script ]; then
  sudo mkdir ${_grid_home}/crs/script
 # sudo chown oracle:oinstall ${_grid_home}/crs/script
fi

if [ ! -d ${_grid_home}/crs/profile ]; then
  sudo mkdir ${_grid_home}/crs/profile
 # sudo chown oracle:oinstall ${_grid_home}/crs/profile
fi


_LOG_FILE="${_SCRIPT_DIR}/logs/splex_port_setup_${_this_host}_${_Now}.log"

Log "SCRIPT_DIR: ${_SCRIPT_DIR}"
Log "LOG_FILE: ${_LOG_FILE}"
Log "This Host: ${_this_host}"
Log "Trim Host: ${_trim_host}"
Log "This Host VIP: ${_this_host_vip}"
Log "Other Hosts: ${_other_hosts}"

# check for some prerequisites
# check if root and oracle are part of the spadmin and oinstall
_id_root="`sudo /usr/bin/id`"
_root_grs="`echo ${_id_root} | awk '{split($0, val, " "); split(val[3], gr, "="); print gr[2];}'`"
_root_cnt=0
_root_cnt=$((${_root_cnt} + `echo "${_root_grs}" | grep -c "oinstall"` + `echo "${_root_grs}" | grep -c "spadmin"`))

_id_oracle="`/usr/bin/id`"
_oracle_grs="`echo ${_id_oracle} | awk '{split($0, val, " "); split(val[3], gr, "="); print gr[2];}'`"
_oracle_cnt=0
_oracle_cnt=$((${_oracle_cnt} + `echo "${_oracle_grs}" | grep -c "oinstall"` + `echo "${_oracle_grs}" | grep -c "spadmin"`))

if [ ${_root_cnt} -ne 2 -o ${_oracle_cnt} -ne 2 ]; then
  echo ""
  echo "User oracle OR root is not part of either oinstall OR spadmin group."
  echo "Verify if oracle and root are part of the oinstall and spadmin in /etc/group"
  echo "After adding to /etc/group, re-login as root and oracle to take effect."
  echo "As of now exiting..."
  echo ""
#  exit 0
fi

_license_value="DZHCEZ8VJ8V54WPGAJ2NL73N8SQVZR6Z7B"
_license_customer="CISCO SYSTEMS INC"

depotDBConnectStr="`getdepotdbconnectinfo`"
if [ "x${depotDBConnectStr}" == "x" ]; then
    Log "WBXERROR: can not get DepotDB connection info. EXIT"
    DONE
    exit -1
fi
##########################
## 02.0 port number
##########################

Log "*** SPLEX PORT ***"
_default_port="20002"
#_default_port="31000"

_port="{INPUTVAR_PORT}"

_skip_menu_option="n"

Log "Port #: ${_port}"

# verify port is already running
_is_running="no"
## ========================================================================================
## Edwin Zhang, 09/08/2020. Change the grep option for the port is there or not.
## ========================================================================================

# if [ `ps -ef | grep sp_ | grep -c ${_port}` -ge 1 ]; then
if [ `ps -ef | grep sp_ | grep -e "-u *${_port}" | wc -l` -ge 1 ]; then
  _is_running="yes"
  _host=${_this_host}
else
  # check if the port is running on other nodes
  for _host in ${_other_hosts}
  do
    # _sp_process="`ssh ${_host} ps -ef 2>/dev/null | grep ${_port} | grep sp_ | head -1`"
    _sp_process="`ssh ${_host} ps -ef 2>/dev/null | grep -e "-u *${_port}" | grep sp_ | head -1`"
    if [ -n "${_sp_process}" ]; then
      _is_running="yes"
      break ;
    fi
  done
fi

if [ "${_is_running}" = "yes" ]; then
  Log ""
  Log "Port ${_port} is alredy running on ${_host}, so cannot continue with the setup. Exiting..."
  DONE
  exit -1
fi

##########################
## 03.0 product directory
##########################

Log "*** SPLEX PRODUCT DIRECTORY ***"
# check if any sp_ process running
_ps_cnt=`ps -ef | grep sp_ | egrep -vc "grep|sp_ctrl|splex_action"`
if [ ${_ps_cnt} -eq 0 ]; then
  # check if any port running on other nodes
  for _host in ${_other_hosts}
  do
    _sp_cnt=`ssh ${_host} ps -ef 2>/dev/null | grep sp_ | egrep -vc "grep|sp_ctrl|splex_action"`
    if [ ${_sp_cnt} -gt 0 ]; then
      _host_1=${_host}
      _prod_dirs="`ssh ${_host} ps -ef 2>/dev/null | grep sp_ | egrep -v "grep|sp_ctrl|splex_action" | awk '{print $8}' | awk -F'/' '{for(i=2;i<=NF-2;i++){printf("/%s", $i);} print ","; }' | grep -v '^$' | sort -u | tr -d '\n'`"
      break ;
    fi
  done
else
  _prod_dirs="`ps -ef | grep sp_ | egrep -v "grep|sp_ctrl|splex_action" | awk '{print $8}' | awk -F'/' '{for(i=2;i<=NF-2;i++){printf("/%s", $i);} print ","; }' | grep -v '^$' | sort -u | tr -d '\n'`"
fi

if [ -z "${_prod_dirs}" ]; then
  # to get the shareplex mount point
  _mps=`df -h | egrep -v "Filesystem|sd|dev|staging|arc|vol|backup|off|spare" | grep "^ " | awk '{print $5}' | tr '\n' ' '`

  _prod_dirs=""
  for _mp in ${_mps};
  do
    _param_defaults="`find ${_mp} -name param-defaults 2>/dev/null`"
    for _pd in ${_param_defaults}
    do
      _prod_dirs="${_prod_dirs}`echo ${_pd} | awk -F'/' '{for(i=2; i<=NF-2; i++) printf("/%s", $i);} '` ";
    done
  done
fi

_prod_dirs="`echo "${_prod_dirs}" | tr ' ' ',' | sed s/.$//`"
Log "Product directories: ${_prod_dirs}"

_options=""
_default=""
if [ `echo ${_prod_dirs} | grep -ic ","` -eq 0 ]; then
  _default="${_prod_dirs}"
#--newly added
_default=""
_options="${_prod_dirs}"
else
  _options="${_prod_dirs}"
fi

#if [ -n "${_default}" -a "${_skip_menu_option}" = "y" ]; then
_prod_dir="{INPUTVAR_PROD_DIR}"
  if [ ! -f "${_prod_dir}/data/param-defaults" ]; then
    Log ""
    Log "${_prod_dir} is not a valid shareplex product directory. Exiting..."
    DONE
    exit -1
  fi


Log "Splex Prod Dir: ${_prod_dir}"
_profile_configured="n"
_bin_dir="${_prod_dir}/bin"
Log "SPLEX Bin directory: ${_bin_dir}"

# verify if port is already configured
if [ -f ${_bin_dir}/.profile_u${_port} ]; then
  Log ""
  Log "The shareplex profile for the ${_port} port is already configured."
#  DONE
#  exit -1
  _profile_configured="y"
fi

##########################
## 04.0 vardir
##########################

Log "*** SPLEX VARDIR ***"
_vardir_tmp="`echo ${_prod_dir} | awk -F'/' '{for(i=2; i<=NF-1; i++) printf("/%s", $i); }'`/vardir_${_port}"

_vardir="${_vardir_tmp}"

Log "SPLEX vardir: ${_vardir}"
_vardir_configured="n"
if [ -f ${_vardir} ]; then
  Log ""
  Log "The shareplex vardir for the ${_port} port is already configured."
#  DONE
#  exit -1
  _vardir_configured="y"
fi



##########################
## 05.0 data_source (splex oracle_sid)
##########################

Log "*** SPLEX DATASOURCE ***"
#_datasources="`cat /etc/oratab  | grep -i splex | awk -F':' '{print $1}' | tr '\n' ',' | sed s/.$// | tr [:lower:] [:upper:]`"
#
#_options=""
#_default=""
#if [ `echo ${_datasources} | grep -ic ","` -eq 0 ]; then
#  _default="${_datasources}"
#else
#  _options="${_datasources}"
#fi
#
##if [ -n "${_default}" -a "${_skip_menu_option}" = "y" ]; then
#if [ -n "${_default}" ]; then
#  _datasource="${_default}"
#else
_datasource="{INPUTVAR_DATASOURCE}_SPLEX"
#fi
Log "Splex ORACLE_SID: ${_datasource}"

_datasource_tns="`grep ${_datasource} /u00/app/oracle/product/19.3.0/db/network/admin/tnsnames.ora`"
if [ -z "${_datasource_tns}" ]; then
  # to get the shareplex mount point
  echo "Cannot find tns for ${_datasource}.Exiting..."
  exit -1
fi

# get the splex ORACLE_HOME value
_splex_ora_home="`cat /etc/oratab | grep "^${_datasource}" | awk -F':' '{print $2}' | tr -d '\n'`"
if [ -z "${_splex_ora_home}" ]; then
  Log "Could not determine the shareplex SID (${_datasource}) home from /etc/oratab. PLEASE MAKE SURE /etc/oratab HAS VALID ENTRY. Exiting..."
  exit -1
fi
Log "_splex_ora_home=${_splex_ora_home}"

_connection_configured=0
if [ -f "${_vardir}/data/connections.yaml" ]; then
  _connection_configured="`cat ${_vardir}/data/connections.yaml | grep ${_datasource} -c`"
fi
if [ "${_vardir_configured}" = "y" -a "${_profile_configured}" = "y" -a ${_connection_configured} -ne 0 ]; then
  Log "the profile vardir and db connection have configured. Exiting..."
  exit -1
fi

##########################
## 06.0 get oracle info
##########################

Log "*** ORACLE INFO ***"
_ora_home="${_splex_ora_home}"
if [ -z "${_ora_home}" -o ! -d "${_ora_home}" ]; then
  Log "Could not determine ORACLE_HOME. Exiting..."
  DONE
  exit -1
fi
Log "ORACLE_HOME: ${_ora_home}"

# get the oracle version
_ora_version="`${_ora_home}/bin/sqlplus /nolog << EOSQL  | grep "Release" | awk '{print $3}' | cut -d'.' -f1
exit
EOSQL`"

_ora_base=""
if [ -f "${_ora_home}/bin/orabase" ]; then
  _ora_base="`${_ora_home}/bin/orabase`"
else
  # get the installActions.log
  _install_action_log_file="`ls -altr ${_ora_home}/cfgtoollogs/oui/installActions*log 2>/dev/null | tail -1 | awk '{print $9}'`"
  Log "Install Action Log File: ${_install_action_log_file}"
  if [ -n "${_install_action_log_file}" ]; then
    Log "`grep -A1 "name = ORACLE_BASE" ${_install_action_log_file} | tr '\n' '|'`"
    _ora_base="`grep -A1 "name = ORACLE_BASE" ${_install_action_log_file} | tail -1 | cut -d'=' -f2 | cut -d':' -f3 | tr -d ' '`"
  else
    # check on the other hosts for the installAction File
    Log "Could not file the installActions file on ${_this_host}. Verifing on the ${_other_hosts}"
    for _host in ${_other_hosts}
    do
      Log "  -- Verifing on the ${_host}"
      if [ -z "${_ora_base}" ]; then
        _install_action_log_file="`ssh ${_host} ls -altr ${_ora_home}/cfgtoollogs/oui/installActions*log 2>/dev/null | tail -1 | awk '{print $9}'`"
        if [ -n "${_install_action_log_file}" ]; then
          _cmd="grep -A1 \"name = ORACLE_BASE\" ${_install_action_log_file} | tail -1 | cut -d'=' -f2 | cut -d':' -f3 | tr -d ' '"
          Log "`ssh ${_host} ${_cmd}`"
          _ora_base="`ssh ${_host} ${_cmd}`"
        fi
      fi
    done
  fi
fi
Log "ORACLE_BASE: ${_ora_base}"
if [ -z "${_ora_base}" -o ! -d "${_ora_base}" ]; then
  Log "Could not determine ORACLE_BASE. Exiting..."
  DONE
  exit -1
fi
Log "ORACLE_BASE: ${_ora_base}"

_asm_sid="`ps aux | grep lgwr | grep ASM | awk '{print $NF}' | awk -F_ '{print $NF}'`"
if [ -z "${_asm_sid}" ]; then
  Log "Could not determine ASM SID. Exiting..."
  DONE
  exit -1
fi
Log "ASM SID: ${_asm_sid}"

if [ "${_ora_version}" = "10" ]; then
  _grid_home="`echo ${_ora_home} | awk -F'/' '{for(i=2; i<=NF-1; i++) printf("/%s", $i); print "/crs"; }'`"
else
  _grid_home="`grep "+ASM" /etc/oratab | cut -d':' -f2 | uniq`"
fi
if [ -z "${_grid_home}" -o ! -d ${_grid_home} ]; then
  Log "Could not determine GRID HOME. Exiting..."
  DONE
  exit -1
fi
Log "GRID HOME: ${_grid_home}"

##########################
## 07.0 verify the SPLEX SID
##########################
#_system_p="sysnotallow@${_datasource}"
_system_p="sysnotallow"

export ORACLE_HOME=${_splex_ora_home}
_splex_db_name="`$ORACLE_HOME/bin/sqlplus -s /nolog << EOSQL
conn system@${_datasource}/${_system_p}
set pages 0
select name from v\\$database ;
exit ;
EOSQL`"

if [ `echo ${_splex_db_name} | egrep -c "SP2-"` -ne 0 ]; then
  Log "Could not verify the connectivity as system user to SPLEX SID (${_datasource}). PLEASE MAKE SURE THE TNS ENTRY EXISTS FOR ${_datasource} or system user has default password. Exiting..."
  exit -1
fi
Log "_splex_db_name=${_splex_db_name}"

# check the shareplex user not exist
export ORACLE_HOME=${_splex_ora_home}
_splex_user_exist="`$ORACLE_HOME/bin/sqlplus -s /nolog << EOSQL
conn system@${_datasource}/${_system_p}
set pages 0
select username from dba_users where lower(username)='splex${_port}';
exit ;
EOSQL`"
echo "_splex_user_exist: ${_splex_user_exist}"
if [ `echo ${_splex_user_exist} | grep ${_port} -c` -eq 1 ]; then
  echo "user splex${_port} has existed in db. Exiting..."
  exit -1
fi
Log "_splex_db_name=${_splex_db_name}"

##########################
## 08.0 splex password
##########################

Log "*** SPLEX PASSWORDS ***"
#_splex_p="Tk07P#FBfT@${_datasource}"
#_splex_p="BLyBt21VtEdmu7X"
SQL="select distinct f_get_deencrypt(password) from appln_pool_info where schema='splex${_port}' and db_name='${_splex_db_name}';"
_splex_p=`execSQL "${depotDBConnectStr}" "${SQL}"`
if [ -z ${_splex_p} ]; then
    Log "WBXWARN: fail to execute ${SQL}"
    _splex_p="b4mdm4kZQMnvl"
fi
echo ${_splex_p}
#_splex_p="Tk07P#FBfT"

Log "System PWD: *******"
Log "SPLEX  PWD: *******"


##########################
## 09.0 tablespace (splex_data & splex_indx and temp)
##########################

Log "*** TABLESPACES FOR SPLEX PORT ***"
_splex_data_tblsps="`${_ora_home}/bin/sqlplus -s /nolog << EOSQL
conn system/sysnotallow@${_datasource}
set head off termout off feedback off echo off lines 200 pages 0
spool ${_SCRIPT_DIR}/logs/splex_port_setup_tblsp_data.txt
select tablespace_name from dba_tablespaces where ( (tablespace_name like '%SPLEX%' or tablespace_name like '%SPLX%') and (tablespace_name like  '%DATA%' or tablespace_name like '%DAT%') ) ;
spool off
spool ${_SCRIPT_DIR}/logs/splex_port_setup_tblsp_indx.txt
select tablespace_name from dba_tablespaces where ( (tablespace_name like '%SPLEX%' or tablespace_name like '%SPLX%') and (tablespace_name like  '%INDX%' or tablespace_name like '%IND%' or tablespace_name like '%IDX%' or tablespace_name like '%INDEX%' or tablespace_name like '%IDE%') ) ;
spool off
spool ${_SCRIPT_DIR}/logs/splex_port_setup_tblsp_temp.txt
select tablespace_name from dba_tablespaces where contents = 'TEMPORARY' ;
EOSQL`"
_splex_data_tblsps="`cat ${_SCRIPT_DIR}/logs/splex_port_setup_tblsp_data.txt | egrep -v '^$|^ ' | tr -d ' ' | head -1`"
_splex_indx_tblsps="`cat ${_SCRIPT_DIR}/logs/splex_port_setup_tblsp_indx.txt | egrep -v '^$|^ ' | tr -d ' ' | head -1`"
_splex_temp_tblsps="`cat ${_SCRIPT_DIR}/logs/splex_port_setup_tblsp_temp.txt | egrep -v '^$|^ ' | tr -d ' ' | head -1`"

Log "SPLEX DATA Tablespace(s): ${_splex_data_tblsps}"
Log "SPLEX INDX Tablespace(s): ${_splex_indx_tblsps}"
Log "SPLEX TEMP Tablespace(s): ${_splex_temp_tblsps}"

###


_splex_user_perm="`${_ora_home}/bin/sqlplus -s /nolog << EOSQL
conn sys/${_system_p}@${_datasource} as sysdba
set head off termout off feedback off echo off lines 200 pages 0
grant select on sys.user\$ to system with grant option;
EOSQL`"

_options=""
_default=""
if [ `echo ${_splex_data_tblsps} | grep -ic ","` -eq 0 ]; then
  _default="${_splex_data_tblsps}"
else
  _options="${_splex_data_tblsps}"
fi
if [ -n "${_default}" ]; then
  _splex_data_tblsp="${_default}"
else
  Log "Could not determine the shareplex DATA tablespace. Exiting..."
  DONE
  exit -1
fi
if [ -z "${_splex_data_tblsp}" ]; then
  Log "Could not determine SPLEX DATA tablespace. Exiting..."
  DONE
  exit -1
fi
Log "SPLEX DATA tablespace: ${_splex_data_tblsp}"

_options=""
_default=""
if [ `echo ${_splex_indx_tblsps} | grep -ic ","` -eq 0 ]; then
  _default="${_splex_indx_tblsps}"
else
  _options="${_splex_indx_tblsps}"
fi
if [ -n "${_default}" ]; then
  _splex_indx_tblsp="${_default}"
else
  Log "Could not determine the shareplex INDEX tablespace. Exiting..."
  DONE
fi
if [ -z "${_splex_indx_tblsp}" ]; then
  Log "Could not determine SPLEX INDEX tablespace. Exiting..."
  DONE
  exit -1
fi
Log "SPLEX INDX tablespace: ${_splex_indx_tblsp}"

_options=""
_default=""
if [ `echo ${_splex_temp_tblsps} | grep -ic ","` -eq 0 ]; then
  _default="${_splex_temp_tblsps}"
else
  _options="${_splex_temp_tblsps}"
fi
if [ -n "${_default}" ]; then
  _splex_temp_tblsp="${_default}"
else
  Log "Could not determine the shareplex TEMP tablespace. Exiting..."
  DONE
  exit -1
fi
if [ -z "${_splex_temp_tblsp}" ]; then
  Log "Could not determine SPLEX TEMP tablespace. Exiting..."
  DONE
  exit -1
fi
Log "SPLEX TEMP tablespace: ${_splex_temp_tblsp}"

##########################
## 10.0 support for oracle ASM
##########################

Log "*** SPLEX ASM SUPPORT ***"
_asm_support="n"
if [ -n "${_asm_sid}" ]; then
  _asm_support="y"
fi

Log "ASM Support: ${_asm_support}"

##########################
## 11.0 needed CRS setup
##########################

Log "*** CRS SETUP NEEDED ***"
if [ "${_port}" = "${_default_port}" ]; then
  _crs_setup_needed="n"
else
_crs_setup_needed="y"
fi
Log "CRS setup needed: ${_crs_setup_needed}"

##########################
## 12.0 needed monitoring
##########################

Log "*** MONITORING SETUP NEEDED ***"
if [ "${_port}" = "${_default_port}" ]; then
  _monitoring_needed="n"
else
_monitoring_needed="y"
fi
Log "Need Monitoring: ${_monitoring_needed}"

##########################
## 13.0 get the splex version and host id
##########################

Log "*** SPLEX HOST IDS ***"
_splex_uname=""
if [ -f "${_prod_dir}/util/splex_uname" ]; then
  _splex_uname="${_prod_dir}/util/splex_uname"
elif [ -f "${_prod_dir}/install/splex_uname" ]; then
  _splex_uname="${_prod_dir}/install/splex_uname"
fi
Log "SPLEX Uname binary file: ${_splex_uname}"

_splex_uname_out_file="${_SCRIPT_DIR}/logs/splex_uname_${_this_host}.out"
${_splex_uname} > ${_splex_uname_out_file}

_splex_version=$(cat ${_splex_uname_out_file} | grep -w version | awk '{print $NF}' | awk -F'.' '{print $1$2$3}')

if [ ${_splex_version} -lt 863 ]; then
  Log "Shareplex version is ${_splex_version}, which is not >= 863. Exiting..."
  DONE
  exit -1
fi
Log "SPLEX Version: ${_splex_version}"

_host_ids="$(cat ${_splex_uname_out_file} | grep "Host ID" | awk -F'=' '{print $2}' | tr -d ' ')"

for _host in ${_other_hosts}
do
  _host_ids="${_host_ids} `ssh ${_host} ${_splex_uname} 2>/dev/null | grep "Host ID" | awk -F'=' '{print $2}' | tr -d ' '`"
done
Log "SPLEX Host IDs: ${_host_ids}"

##########################
## 14.0 main program
##########################

Log "*** MAIN PROGRAM ***"

# creating the profile
if [ "${_profile_configured}" = "n" ]; then
  Log "Creating the SPLEX profile (${_bin_dir}/.profile_${_port}) ..."
  cat /dev/null > ${_bin_dir}/.profile_${_port}
  echo "ORACLE_SID=${_datasource}; export ORACLE_SID
  SP_COP_TPORT=${_port}; export SP_COP_TPORT
  SP_COP_UPORT=${_port}; export SP_COP_UPORT
  SP_SYS_VARDIR=${_vardir}; export SP_SYS_VARDIR
  SP_SYS_HOST_NAME=${_this_host_vip}; export SP_SYS_HOST_NAME
  SP_SYS_PRODDIR=${_prod_dir}; export SP_SYS_PRODDIR
  ORACLE_BASE=${_ora_base}; export ORACLE_BASE
  ORACLE_HOME=${_ora_home}; export ORACLE_HOME
  NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1; export NLS_LANG
  EDITOR=vi; export EDITOR
  ulimit -n 1024"      >> ${_bin_dir}/.profile_${_port}
fi

Log "Verifying if the profile has created properly..."
if [ ! -f ${_bin_dir}/.profile_${_port} ]; then
  Log "${_bin_dir}/.profile_${_port} has not been created properly. Exiting..."
  DONE
  exit -1
fi

if [ "${_profile_configured}" = "n" ]; then
  Log "Creating link for the SPLEX profile (${_bin_dir}/.profile_u${_port}) ..."
  ln -s ${_bin_dir}/.profile_${_port} ${_bin_dir}/.profile_u${_port}
fi

# make the vardir and structure
if [ "${_vardir_configured}" = "n" ]; then
  Log "Creating the vardir and all the other directories ..."
  sudo mkdir -p ${_vardir}
  sudo chown -R oracle:oinstall ${_vardir}
  cd ${_vardir}
  mkdir config data db dump idx log rim save state temp

  # create the param db
  Log "Creating the paramdb (${_vardir}/data/paramdb) ..."
  cat /dev/null > ${_vardir}/data/paramdb
  for _h_id in ${_host_ids}
  do
    echo "SP_SYS_LIC_${_h_id} \"${_license_value}:${_license_customer}\""  >> ${_vardir}/data/paramdb
  done
fi

Log "Sourcing the profile ..."
cd ${_bin_dir}
. ./.profile_u${_port}

Log "Running the ora_setup for ${_port} ..."
./ora_setup << EOCMD 1>${_SCRIPT_DIR}/logs/ora_setup_${_port}.log 2>${_SCRIPT_DIR}/logs/ora_setup_${_port}.err
n
n
$ORACLE_HOME
${_datasource}
system
${_system_p}
y
splex${_port}
${_splex_p}
${_splex_p}
n
${_splex_data_tblsp}
${_splex_temp_tblsp}
${_splex_indx_tblsp}
y
${_asm_support}
${_asm_sid}
EOCMD

# verify there is not error for ora_setup
echo "tail -4 ${_SCRIPT_DIR}/logs/ora_setup_${_port}.err | head -1 | grep -c 'Setup of ${_datasource} completed successfully'"
if [ `tail -4 ${_SCRIPT_DIR}/logs/ora_setup_${_port}.err | head -1 | grep -c "Setup of ${_datasource} completed successfully"` -ne 1 ]; then
  # there is error
  Log ""
  Log "ERROR OCCURED WHILE EXECUTING ORA_SETUP."
  Log "REFER TO ${_SCRIPT_DIR}/logs/ora_setup_${_port}.err FILE FOR detail info"
  DONE
  exit -1
fi

# verify the parameter is in the param-defaults before adding
if [ "${_splex_db_name}" == "CONFIGDB" ] || [ "${_splex_db_name}" == "GCFGDB" ]; then
    if [ "${_port}" != 33333 ]; then
        splex_deny_user="SPLEX33333";
    else
        splex_deny_user="SPLEX_DENY";
    fi
else
    splex_deny_user="SPLEX_DENY";
fi
_splex_deny_user_id="`$ORACLE_HOME/bin/sqlplus -s /nolog << EOSQL | sed "s/[[:space:]]//g"
conn system@${_datasource}/${_system_p}
set pages 0
select user_id from dba_users where username='${splex_deny_user}';
exit ;
EOSQL`"

if [ "x${_splex_deny_user_id}" == "x" ]; then
    Log "ERROR Does not get ${splex_deny_user} user from db ${_SQL_DBNAME}."
	exit -1
fi

_sp_oct_reduced_key_value=0
_SP_OCT_REDUCED_KEY_value="`$ORACLE_HOME/bin/sqlplus -s /nolog << EOSQL
conn system@${_datasource}/${_system_p}
set pages 0
select upper(supplemental_log_data_min) || upper(supplemental_log_data_pk) || upper(supplemental_log_data_ui) from v$database;
exit ;
EOSQL`"
if [ "${_SP_OCT_REDUCED_KEY_value}" = "YESYESYES" ]; then
  _sp_oct_reduced_key_value=1
fi

if [ "${_vardir_configured}" = "n" ]; then
  Log "#################################################"
  Log "# The below parameters are added to the paramdb #"
  Log "#################################################"

  _param_values=(
  "SP_OPO_READRELEASE_INTERVAL:1000"
  "SP_OPO_SQL_CACHE_DISABLE:1"
  "SP_XPT_KEEPALIVE:1"
  "SP_OCT_REPLICATE_DDL:0"
  "SP_OCT_REPLICATE_ALL_DDL:0"
  "SP_OCT_AUTOADD_ENABLE:0"
  "SP_OCT_DDL_UPDATE_CONFIG:0"
  "SP_OCT_REDOLOG_ENSURE:2"
  "SP_OCT_USE_DST:0"
  "SP_OCT_AUTOADD_ENABLE:0"
  "SP_OCT_OLP_TRACE:0"
  "SP_OPO_REDUCED_KEY:1"
  "SP_ORD_BATCH_ENABLE:0"
  "SP_OPO_SUPPRESSED_OOS:0"
  "SP_SYS_TARGET_COMPATIBILITY:9.2.0"
  "SP_OCT_OLOG_USE_OCI:1"
  "SP_OCT_REDUCED_KEY:${_sp_oct_reduced_key_value}"
  "SP_OCT_DENIED_USERID:${_splex_deny_user_id}"
  "SP_OPO_SYNC_LOG_FREQUENCY:1000"
  )

  _len=${#_param_values[*]}
  _i=0
  while [ ${_i} -lt ${_len} ]; do
    _p_v=${_param_values[${_i}]}
    _param=$(echo ${_p_v} | cut -d ':' -f1)
    _value=$(echo ${_p_v} | cut -d ':' -f2)

    if [ $(grep -ciw ${_param} ${_prod_dir}/data/param-defaults) -gt 0 -a $(grep -icw ${_param} ${_vardir}/data/paramdb) -eq 0 ]; then
      echo "${_param} \"${_value}\""           >> ${_vardir}/data/paramdb
      Log "${_param}\" ${_value}\""
    fi
    _i=$(expr ${_i} + 1)
  done
fi

# prepare the CRS registeration files
# cap file
if [ "${_crs_setup_needed}" = "y" ]; then

  Log ".cap files for CRS registration are being prepared..."
  _cap_file=${_grid_home}/crs/profile/shareplex${_port}.cap
  if [ -f ${_cap_file} ]; then
    sudo mv ${_cap_file} ${_cap_file}.${_Now}
  fi

  cp ${_SCRIPT_DIR}/conf/shareplexport_template_new.cap /tmp/shareplex${_port}.cap
  sed -i "s|{PORT}|${_port}|g" /tmp/shareplex${_port}.cap
  sed -i "s|{THIS_HOST}|${_this_host}|g" /tmp/shareplex${_port}.cap
  sed -i "s|{OTHER_HOSTS}|${_other_hosts}|g" /tmp/shareplex${_port}.cap

  sudo cp /tmp/shareplex${_port}.cap ${_cap_file}
  sudo chmod 666 ${_cap_file}

  # action file
  Log "action .sh files for CRS registration are being prepared..."
  _action_file=${_grid_home}/crs/script/splex_action_${_port}.sh
  if [ -f ${_action_file} ]; then
    sudo mv ${_action_file} ${_action_file}.${_Now}
  fi

  cp ${_SCRIPT_DIR}/conf/splex_action_template.sh /tmp/splex_action_${_port}.sh
  sed -i "s|{PROD_DIR}|${_prod_dir}|g" /tmp/splex_action_${_port}.sh
  sed -i "s|{ORA_BASE}|${_ora_base}|g" /tmp/splex_action_${_port}.sh
  sed -i "s|{ORA_HOME}|${_ora_home}|g" /tmp/splex_action_${_port}.sh
  sed -i "s|{DATASOURCE}|${_datasource}|g" /tmp/splex_action_${_port}.sh
  sed -i "s|{THIS_HOST_VIP}|${_this_host_vip}|g" /tmp/splex_action_${_port}.sh
  sed -i "s|{PORT}|${_port}|g" /tmp/splex_action_${_port}.sh
  sed -i "s|{VARDIR}|${_vardir}|g" /tmp/splex_action_${_port}.sh
  sed -i "s|{GRID_HOME}|${_grid_home}|g" /tmp/splex_action_${_port}.sh

  sudo cp /tmp/splex_action_${_port}.sh ${_action_file}
  sudo chown oracle:oinstall ${_action_file}
  sudo chmod 755 ${_action_file}

  # scp the cap and action files and copy to the respective directory
  Log "scp the .cap and action .sh to ${_other_hosts} ..."
  for _host in ${_other_hosts}
  do
    scp ${_cap_file} ${_host}:/tmp/ 2>/tmp/null
    scp ${_action_file} ${_host}:/tmp/ 2>/tmp/null

    ssh ${_host} 2>/dev/null << EOCMD
sudo mkdir -p ${_grid_home}/crs/script
sudo mkdir -p ${_grid_home}/crs/profile
sudo cp /tmp/shareplex${_port}.cap ${_cap_file}
sudo cp /tmp/splex_action_${_port}.sh ${_action_file}
sudo chmod 666 ${_cap_file}
sudo chown oracle:oinstall ${_action_file}
sudo chmod 755 ${_action_file}
EOCMD
  done

  # register with the crs
  Log "Registering with the CRS ..."
#  sudo ${_grid_home}/bin/crs_register shareplex${_port}
  sudo ${_grid_home}/bin/crsctl add resource shareplex${_port} -type application -file ${_cap_file}
  sudo ${_grid_home}/bin/crsctl setperm resource shareplex${_port}  -o oracle

  # start the port
  Log "Trying to Start the port using CRS..."
#  ${_grid_home}/bin/crs_start shareplex${_port}
   ${_grid_home}/bin/crsctl start resource  shareplex${_port}
else
  # crs setup not needed
  Log ".cap files for CRS registration are NOT prepared."
  Log "action .sh files for CRS registration are NOT prepared."
  Log "scp the .cap and action .sh to ${_other_hosts} is NOT done."
  Log "Registering with the CRS is NOT done."
  Log "Starting the port manually (NOT USING CRS)."

  . ${_bin_dir}/.profile_u${_port}
  ${_bin_dir}/sp_cop -u${_port} &
fi

sleep 5
Log "Verifying if the port has started..."
if [ `ps -ef | grep -v grep | grep -c ${_port}` -gt 0 ]; then Log "Port has started properly."; fi

Log "Creating the /u00/app/admin/dbarea/bin/splex10_restart_proc.sh ..."
if [ ! -f "/u00/app/admin/dbarea/bin/splex10_restart_proc.sh" ]; then
  sudo mkdir -p /u00/app/admin/dbarea/
  sudo chown -R oracle:oinstall /u00/app/admin/
  mkdir -p /u00/app/admin/dbarea/bin/ /u00/app/admin/dbarea/log/ /u00/app/admin/dbarea/conf/ /u00/app/admin/dbarea/sql/
  cp ${_SCRIPT_DIR}/conf/splex7_restart_proc_template.sh /u00/app/admin/dbarea/bin/splex10_restart_proc.sh
  chmod 755 /u00/app/admin/dbarea/bin/splex10_restart_proc.sh
  sed -i "s|{BIN_DIR}|${_bin_dir}|g" /u00/app/admin/dbarea/bin/splex10_restart_proc.sh
else
  Log "File /u00/app/admin/dbarea/bin/splex10_restart_proc.sh already exists"
fi

# add to auto restart
if [ "${_monitoring_needed}" = "y" ]; then
  Log "Adding the port to the monitoring config (${_bin_dir}/WbxSplexAutoStartStoppedProcess.config) ..."
  if [ ! -f "${_bin_dir}/WbxSplexAutoStartStoppedProcess.config" ]; then
    Log "  -- File does not exists."
    echo "${_port}:Y" >> ${_bin_dir}/WbxSplexAutoStartStoppedProcess.config
    Log "  -- Port added to (${_bin_dir}/WbxSplexAutoStartStoppedProcess.config)."
  else
    if [ `grep -c ${_port} ${_bin_dir}/WbxSplexAutoStartStoppedProcess.config` -eq 0 ]; then
      echo "${_port}:Y" >> ${_bin_dir}/WbxSplexAutoStartStoppedProcess.config
      Log "  -- Port added to (${_bin_dir}/WbxSplexAutoStartStoppedProcess.config)."
    else
      Log "  -- Port Already present in (${_bin_dir}/WbxSplexAutoStartStoppedProcess.config)."
    fi
  fi

  # add to the crontab
#  Log "Trying to add auto restart to crontab ..."
#  /usr/bin/crontab -l > ${_SCRIPT_DIR}/logs/crontab_${_this_host}_b4.txt
#  if [ `cat ${_SCRIPT_DIR}/logs/crontab_${_this_host}_b4.txt | egrep -v "^#|^$" | grep "splex9_restart_proc.sh" | grep -c ${_port}` -eq 0 ]; then
#    cp ${_SCRIPT_DIR}/logs/crontab_${_this_host}_b4.txt ${_SCRIPT_DIR}/logs/crontab_${_this_host}.txt
#    echo -e "\n# auto restart for ${_port}\n0,15,30,45 * * * * /u00/app/admin/dbarea/bin/splex10_restart_proc.sh ${_port}" >> ${_SCRIPT_DIR}/logs/crontab_${_this_host}.txt
#    /usr/bin/crontab ${_SCRIPT_DIR}/logs/crontab_${_this_host}.txt
#    Log "Auto restart added to crontab, trying to verify crontab ..."
#    /usr/bin/crontab -l > ${_SCRIPT_DIR}/logs/crontab_${_this_host}_after.txt
#    if [ `cat ${_SCRIPT_DIR}/logs/crontab_${_this_host}_after.txt | egrep -v "^#|^$" | grep "splex10_restart_proc.sh" | grep -c ${_port}` -ne 1 ]; then
#      Log "Auto restart entry has NOT BEEN ADDED TO CRONTAB PROPERLY. PLEASE VERIFY ..."
#    else
#      Log "Auto restart entry has been added to crontab properly."
#    fi
#  else
#    Log "Auto restart entry in crontab for port ${_port} already exits."
#  fi
else
  Log "As monitoring setup is not needed, port NOT added to the monitoring config (${_bin_dir}/WbxSplexAutoStartStoppedProcess.config)."
fi

Log "Adding to /etc/oraport"
if [ ! -f /etc/oraport ]; then
  sudo touch /etc/oraport
fi

sudo chown oracle:oinstall /etc/oraport
chmod 644 /etc/oraport
if [ `grep -c ${_port} /etc/oraport` -eq 0 ]; then
  echo "${_port}:${_bin_dir}" >> /etc/oraport
fi

# setup shsetport
Log "Trying to setup /usr/bin/shsetport"
if [ ! -f "/usr/bin/shsetport" ]; then
  sudo cp ${_SCRIPT_DIR}/conf/shsetport_template.sh /usr/bin/shsetport
  sudo chown oracle:oinstall /usr/bin/shsetport
  chmod 755 /usr/bin/shsetport

else
  Log "shsetport already exists"
fi

#### Shareplex ignore trans table

Log "Trying to create  shareplex Ignore Trans"
${_ora_home}/bin/sqlplus /nolog << EOSQL
connect splex${_port}/${_splex_p}@${_datasource}
@$SP_SYS_PRODDIR/util/create_ignore.sql
CREATE TABLE SPLEX_MONITOR_ADB
   (    "DIRECTION" VARCHAR2(30),
        "SRC_HOST" VARCHAR2(30),
        "SRC_DB" VARCHAR2(30),
        "LOGTIME" DATE,
        "PORT_NUMBER" NUMBER,
         CONSTRAINT PK_SPLEX_MONITOR_ADB PRIMARY KEY ("DIRECTION", "SRC_HOST", "SRC_DB")
 USING INDEX
  NOCOMPRESS
  LOGGING
)
/
set pages 0
set feedback off
grant all on SPLEX_MONITOR_ADB to public;
EOSQL

Log "Trying to encrypt shareplex port"
if [ ${#_port} -eq 5 ]; then
    _aes_key="${_port}33E283116B1165BD049DC94181C96CE76ED9AAC3AFD495636650780A9AB"
elif [ ${#_port} -eq 4 ]; then
    _aes_key="0${_port}33E283116B1165BD049DC94181C96CE76ED9AAC3AFD495636650780A9AB"
else
    Log "Could not encrypt port ${_port}. Exiting..."
    DONE
    exit -1
fi
cd ${_bin_dir}
. ./.profile_u${_port}
./sp_ctrl << EOSQL
set encryption key ${_aes_key}
set param SP_XPT_ENABLE_AES 1
stop export
start export
set param SP_IMP_ENABLE_AES 1
stop import
start import
show encryption key
list param all export
list param all import
EOSQL

_splex_error="`cat ${_vardir}/log/event_log | grep Error | grep -v license | wc -l`"
if [ ${_splex_error} -ne 0 ]
    Log "Shareplex met error with ${_splex_error}. Please check."
    exit -1


DONE "END"


