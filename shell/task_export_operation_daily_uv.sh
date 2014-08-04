#!/bin/bash

LOG_FILE_PATH="../python_log/"

FILE_PATH="${0%/*}"

CALLED_PATH="${PWD}"

cd ${FILE_PATH}

echo "$PWD"


logger_file_path=`find . -name logger.sh | head -1`

. "${logger_file_path}"

if [ "${d}" = "" ];then
        d=`date -d yesterday +%Y%m%d`
fi

while getopts "d:" opt;
do
    case $opt in
        d) print_debug_info "Indicating the date."
            d=$OPTARG
            ;;
    esac
done

cd "${CALLED_PATH}"
print_debug_info "${d}"
python3  export_operation_daily_uid.py "${d}" 

