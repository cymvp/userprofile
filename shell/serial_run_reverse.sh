#!/bin/bash
shopt -s expand_aliases

while getopts ":wd:sp:" opt;
do
    case $opt in
        w) echo "multi day calculating"
        cycle=7
        ;;
        d) day_duration=$OPTARG
        
        ;;
        s) bingfa='1'
        ;;
        p) scrpt=$OPTARG
    esac
done

if [ "$scrpt" = ""  ];then
    echo "No script requesetd running!!!!!!"
    exit
fi
if [ "$day_duration" = "" ];then
    echo "yesterday date."
    startDay=`date -d yesterday +%Y%m%d`
    endDay=''
else
    startDay=`echo $day_duration | awk -F '-' '{print $1}'`
    endDay=`echo $day_duration | awk -F '-' '{print $2}'`
fi

if [ "$cycle" = "" ]; then
    echo "cycle is 1"
    cycle=1
fi
if [ "$endDay" = "" ];then
    echo "Single date."
    interval=0
else
    begin_s=$(date --date=${startDay} +%s)
    end_s=$(date --date=${endDay} +%s)
    interval=$(((${end_s}-${begin_s})/3600/24))
fi

echo "Start day is: $startDay"
echo "End day is :$endDay"
echo "Total days is : $interval"
echo "Cycle is: $cycle"
echo "Bingfa is: $bingfa"
echo "Script is: $scrpt"

declare -i interval
declare -i cycle
interval=interval+1
let "d = $startDay"
for((i = 0; i < interval; i = i + $cycle));do
    d=$(date --date="$i days ago ${endDay}" +%Y%m%d)
    echo $d
    if [ "$bingfa" = "1" ]; then
        sh "$scrpt" -d "$d" &
    else
        sh "$scrpt" -d "$d"
    fi
done




