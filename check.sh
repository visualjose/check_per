#!/bin/bash

# Be strict about the file permissions for logs
umask 077

LOG_DIR="/var/log/$(basename $0 .sh)"
mkdir -p $LOG_DIR
LOG_FILE="${LOG_DIR}/$(basename $0 .sh)_`date +"%Y_%m_%d_%H_%M_%S"`.log"

#HADOOP_CMD="/usr/local/hadoop-2.3.0/bin/hadoop"
HADOOP_CMD=`type -p hadoop`

trap '' ERR

usage() {
  cat >&2<<!!
Usage: $(basename $0) [-h] <file>

Sets correct directory and file permissions on HDFS based on the patterns supplied in the <file>. 
Patterns should go line-by-line in a file using the following syntax: 
<path pattern> [ACL|<hdfs user> <hdfs group> <hdfs permissions>] <acl SPEC>
Permissions should follow the unix file permissions numerical description. ACL spec should follow the 
hdfs dfs -setfacl syntax as per https://hadoop.apache.org/docs/r2.4.0/hadoop-project-dist/hadoop-common/FileSystemShell.html#setfacl

You can omit execute bit for the directory, it will be handled automatically. E.g. if you supply 644 as 
your permissions for /*/*/* pattern files fill got 644 permission, while directories will receive 755.
'+/-' specifications are also valid, however, they would not be translated by any means. 
Acl specs will also be parsed and exec permission would automatically be added, unless -x (--notranslate) is specified. 

Options: 

 -h --help
  Display this help message.
 -n --nochange
  Do not actually change anything, just print the intent.
 -l --log_current
  Log current state of the file before applying the new permissions
 -c
  Do not log into the log file, just output to console. 
 -q --quiet
  Turn off verbosity. Do not write any log messages, etc.
 -v --verbose
  Be verbose. For now just output the hadoop command before executing it.
 -x --notranslate
  Do not automatically translate permissions to add execute ("x") bit for directories.

Examples: 

/test/Lev1/*/* hdfs hdfs 640 user::rw-,group::rw-,other::---
/test/Lev1/*/something_else/* hdfs hdfs 420
/test/Lev1/*/something/* ACL user::hadoop:rw-,group:staff::rw-
/test/Lev1/*/* hdfs hdfs 640 user::rw-,group::rw-,other::---  --notranslate
/test/Lev1/*/something/* ACL user::hadoop:rw-,group:staff::rw-  --notranslate
/test/Lev1/vendor/something ACL user::hadoop:rw-,group:staff::rw-  -R

!!
}

syntax_error() {
  echo -e "$*" >&2
  usage
  exit 1
}


translate_dir_perm() {
  res=
  if [[ $1 =~ ^-?[0-9]+$ ]]; then
    perm=$1
  else
    return $1
  fi

  # check if perm string has super-user bits, etc.
  if [ ${#perm} -gt 3 ]; then
    i=$((${#perm}-3))
    res=${perm:0:$i}
  else
    i=0
  fi

  while [ $i -lt ${#perm} ]; do
    tst=`expr ${perm:$i:1} % 2`
    if [ ${perm:$i:1} -ne "0" ] && [ $tst == "0" ]; then
      c=${perm:$i:1}
      res="${res}$((c+1))"
    else
      res="${res}${perm:$i:1}"
    fi
    i=$((i+1))
  done
}


translate_acl_perm() {
  res=
  lines=$(echo $1 | tr "," "\n")

  first=1
  for i in $lines; do
    len=${#i}
    p_len=$((len-3))

    if [ "${i:$p_len:$len}" != "---" ]; then
      # Do not add execute bit if initial set of permissions was empty.
      len=$((len-1))
      suffix="x"
    else
      suffix=""
    fi

    if [ "$first" = "1" ]; then
      res="${i:0:$len}$suffix"
      first=0
    else
      res="${res},${i:0:$len}$suffix"
    fi
  done
}


log_message() {
  echo "`date +"%D %H:%M --"` $@"
  echo "`date +"%D %H:%M --"` $@" >> $LOG_FILE
}


log_verbose() {
  if [ "$VERBOSE" = "1" ]; then
    log_message $@
  fi
}


log_error() {
  echo "`date +"%D %H:%M --"` $@" >&2
  echo "`date +"%D %H:%M --"` $@" >> $LOG_FILE  
}


update_by_pattern() {
  pattern=$1

  if [[ "$pattern" =~ [^A-Za-z0-9_\-/] ]]; then
    log_verbose "Updating by pattern: $pattern"
    LS_PARAM=" -R "
  else
    log_verbose "Updating single item: $pattern"
    LS_PARAM=" -d "
  fi

 ${HADOOP_CMD} fs -ls $LS_PARAM $pattern | \
  while read prm _ _ _ _ _ _ file; do
    if [ "$prm" != "Found" ]; then
      update_permissions $file $prm $2 $3 $4 $5 $6 $7
    fi
  done 
}


exec_hadoop_command() {
  if [ "$NOCHANGE" = "1" ] || [ "$VERBOSE_HADOOP" = "1" ]; then
    echo "**** $HADOOP_CMD $@" | tee -a $LOG_FILE
  fi

  if [ "$NOCHANGE" = "0" ]; then
    ${HADOOP_CMD} $@ 2>&1 | tee -a $LOG_FILE
  fi
}


update_permissions() {
  file=$1
  prm=$2
  ACL_PARAM=
  res=
  NO_TRANSLATE=
  HDFS_PARAM=
  acl_spec=


  if [ "$3" == "ACL" ]; then
    UPDATE_FILE=0
    ACL_PARAM=" -m "
    acl_spec=$4
    while true; do
      case "$5" in 
        -R|--recursive) 
          shift
          ACL_PARAM="${ACL_PARAM} -R "
          ;;
        --notranslate)
          shift
          NO_TRANSLATE=1
          ;;
        *) break
        ;;
      esac
    done
  else
    UPDATE_FILE=1
    ACL_PARAM=" --set "
    hdfs_user=$3
    hdfs_group=$4
    hdfs_perm=$5

    if [ -z $6 ] || [ "${6:0:1}" != "-" ]; then
      acl_spec=$6
      shift
    fi    

    while true; do
      case "$6" in 
        -R|--recursive) 
          shift
          HDFS_PARAM="${HDFS_PARAM} -R "
          ACL_PARAM="${ACL_PARAM} -R "
          ;;
        --notranslate)
          shift
          NO_TRANSLATE=1
          ;;
        *) break
        ;;
      esac
    done
  fi

  if [ "${prm:0:1}" == "d" ] && [ -z $NO_TRANSLATE ]
  then
    # this is a directory. update permissions adding the exec bit if necessary. 
    if [ "$UPDATE_FILE" = "1" ]; then
      translate_dir_perm $hdfs_perm
      hdfs_perm=$res
#      log_verbose "update_perm: $file --FILE--> $hdfs_perm"
    fi
    if [ -n $acl_spec ]; then
      translate_acl_perm $acl_spec
      acl_spec=$res
#      log_verbose "update_perm: $file --ACL--> $acl_spec"
    fi
  fi

  if [ "$LOG_CURRENT" = "1" ] && [ "$VERBOSE" = "1"]; then
    log_message "------- Current --------"
    if [ "$UPDATE_FILE" = "1" ]; then
      exec_hadoop_command fs -ls $file >> $LOG_FILE
    fi
    exec_hadoop_command fs -getfacl $file >> LOG_FILE
    log_message "------- New --------"
  fi

  if [ "$UPDATE_FILE" = "1" ]; then
    # Update HDFS permissions
    log_verbose "$file --hdfs--> $hdfs_user:$hdfs_group:$hdfs_perm"
    exec_hadoop_command fs -chown $HDFS_PARAM $hdfs_user:$hdfs_group $file
    exec_hadoop_command fs -chmod $HDFS_PARAM $hdfs_perm $file
  fi
  if [ -n $acl_spec ]; then
    # Update HDFS ACLs
    if [[ "$acl_spec" =~ .*default.* ]]; then 
      if [ "${prm:0:1}" == "d" ]; then
        log_verbose "$file --acl--> $acl_spec"  
        exec_hadoop_command fs -setfacl ${ACL_PARAM} $acl_spec $file
      fi
    else
      log_verbose "$file --acl--> $acl_spec"
      exec_hadoop_command fs -setfacl ${ACL_PARAM} $acl_spec $file
    fi
  fi
}


VERBOSE=1
VERBOSE_HADOOP=0
NOCHANGE=0
LOG_CURRENT=0
NO_EXEC=0

# Read command line parameters
while true; do
  case "$1" in
    -h | --help)
      shift
      usage
      exit 0
      ;;
    -q | --quiet)
      shift
      VERBOSE=0     
      ;;
    -v | --verbose)
      shift
      VERBOSE_HADOOP=1     
      ;;      
    -n | --nochange)
      shift
      NOCHANGE=1
      ;;
    -l | --log_current)
      shift
      LOG_CURRENT=1
      ;;
    -c)
      shift
      LOG_FILE=/dev/null
      ;;
    -x | --notranslate)
      shift
      NO_EXEC=1
      ;;
    -*) echo "ERROR: Unknown option: $1" >&2
      exit 1
      ;;
    *)  break
      ;;
  esac
done

FILE=$1

if [ x"$FILE" = "x" ]; then
  echo "No patterns file parameter found. Please run me with -h for usage"
  exit 1
fi

log_verbose "Reading patterns list from file: $FILE"

while read pattern hdfs_user hdfs_group hdfs_perm acl_spec translate recursive; do
    # Skip comments
    [[ "$pattern" =~ \#.* ]] && continue
    # Skip empty lines
    [ -z "$pattern" ] && continue
    update_by_pattern $pattern $hdfs_user $hdfs_group $hdfs_perm $acl_spec $translate $recursive
done < $FILE


