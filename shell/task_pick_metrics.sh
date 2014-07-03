#!/bin/bash

LOG_FILE_PATH="../python_log/"

FILE_PATH="${0%/*}"

CALLED_PATH="${PWD}"

cd ${FILE_PATH}

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

str_dir='/data/web/upload/ubc_data/'
str_prefix='chunlei_ubc_'

declare -a arr

eval $(ls ${str_dir} | awk 'BEGIN{i=0;}$1~/'${str_prefix}$d'/{a[i]=$1; i++} END{ for(i in a) print "arr["i"]="a[i]}')

print_debug_info "${arr[@]}"

declare -i i

i=0
for s in ${arr[@]}
do
    print_debug_info "file is: ${str_dir}${arr[i]}"
    print_debug_info "python3 pf_pick_metric_old.py  ./config/metric_map_metrics.txt "${str_dir}${arr[i]}" "${d}""
    cd "${CALLED_PATH}"
    python3 pf_pick_metric_old.py  ./config/metric_map_metrics.txt "${str_dir}${arr[i]}" "${d}"
    cd "${FILE_PATH}"
    i=$(($i+1))
done

if [ ${i} -lt 3 ];then
    echo "error log file."
    echo "error log file." >> "${LOG_FILE_PATH}${d}_error.txt"
fi


