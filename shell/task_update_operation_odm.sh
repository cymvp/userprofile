#!/bin/bash

logger_file_path=`find . -name logger.sh | head -1`

. "${logger_file_path}"

if [ "${d}" = "" ];then
        d=`date -d yesterday +%Y%m%d`
        #d_format=`date -d yesterday +%Y_%m_%d`
fi

while getopts "d:" opt;
do
    case $opt in
        d) print_debug_info "Indicating the date."
            d=$OPTARG
            ;;
    esac
done

print_debug_info "${d}"
d_format="${d:0:4}_${d:4:2}_${d:6:2}"

print_debug_info "${d_format}"

URL="http://cloud.os.baidu.com/common_ds/data/odmsync/data_ops_odm_${d_format}"

curl -O  "${URL}"

odm_file_name="${URL##*/}"
LOG_DIR="./python_log/"

print_debug_info "${URL}"
print_debug_info "${odm_file_name}"

if [ "$?" = 0 ];then
    find . -name "${odm_file_name}"
    if [ "$?" != "" ];then
        mv "${odm_file_name}" "${LOG_DIR}"
        print_debug_info "Start executing import action."
        python3 import_odm.py "${LOG_DIR}${odm_file_name}"
    fi
fi
