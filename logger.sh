#! /bin/sh

print_debug_info(){
    local content=${1:-"No content..."}
    local current_time=`date "+%Y-%m-%d %H:%M:%S"`
    printf "[%s %s %s %s]:%s\n" "${current_time}" "$0" "$$"  "D" "${content}"
}
