#!/bin/bash

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

days='7'
top_items="3"

python3 pf_calc_profile_linked_info.py "${days}" "${top_items}"  ./config/metric_map_app.txt
python3 pf_calc_profile_linked_info.py "${days}" "${top_items}"  ./config/metric_map_province.txt
python3 pf_calc_profile_linked_info.py "${days}" "${top_items}"  ./config/metric_map_hwv.txt

python3 pf_calc_profile_device.py  "${days}" "${top_items}" ./config/metric_map_device.txt
python3 pf_calc_profile_account.py "${days}" "${top_items}" ./config/metric_map_account.txt

python3 pf_calc_tags.py 
