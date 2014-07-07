#coding:utf-8
import sys
import util.utils
import libs.util.logger
import libs.util.my_utils
import util.calc_tag
import ubc.pf_metric_helper
from config.const import *
from mythread.write_thread import write_thread
import datetime

import config.const

from dbmanager.pf_hwv_collection import PFHWVCollectionManager
from dbmanager.pf_province_collection import PFProvinceCollectionManager
from dbmanager.pf_app_collection import PFAPPCollectionManager
from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager
from dbmanager.pf_device_collection_manager import PFDeviceCollectionManager
from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager

def __consist_of_foreign_data(key_list, foreign_tuple, collection_name):
    '''
    result is:
        {
          "deviceinfo_4096_7d3" : {
                'linked_collection': 
                'linkde_doc':['1', '2',...]
            } 
        }
    '''
    app_id = foreign_tuple[0]
    metric_id = foreign_tuple[1]
    metricHandler = ubc.pf_metric_helper.getMetricHandler('%x' % int(metric_id, 16),  str(app_id))
    foreign_label =  metricHandler.get_profile_metric_label()
    l = len(key_list)
    return {foreign_label:{'linked_collection' : collection_name, 'linked_list' : key_list[0: l]}}


def __create_4096_1807_linked_node(cur):
    appId,  metricId = ('4096', '0x1807')
    result_linked_node_map = {}
    metricHandler = ubc.pf_metric_helper.getMetricHandler(metricId[2:],  appId)
    
    if cur is not None:
        appid_metric_label = metricHandler.get_profile_metric_label()
        old_lines = cur.get(appid_metric_label)
        if old_lines is not None and len(old_lines) > 0:
            old_lines = old_lines[0]
            for package_name in old_lines:
                if result_linked_node_map.get((appId, metricId)) is None:
                    result_linked_node_map[(appId, metricId)] = []
                result_linked_node_map[(appId, metricId)].append(package_name)
    return result_linked_node_map

def __parse_date_params(str_date_duration):
    str_day_duration_arr = str_date_duration.split('-')
    if len(str_day_duration_arr) == 2:
        str_start_day = str_day_duration_arr[0]
        str_end_day = str_day_duration_arr[1]
    elif len(str_day_duration_arr) == 1:
        str_start_day = str_day_duration_arr[0]
        str_end_day = str_day_duration_arr[0]
    else:
        str_start_day = None
        str_end_day = None   
    return (str_start_day, str_end_day)  

if __name__ == "__main__":  # nDays,  mTop
    if len(sys.argv) != 4:
        print("Error params...... python pf_calc__profile_device.py nDays, mTop ")
        sys.exit(1)
    
    libs.util.logger.Logger.getInstance().setLogFilePrefixName('')
    libs.util.logger.Logger.getInstance().setLogFileSurfixName('_device')
    
    total_device_count = 0
    
    g_foreign_tuple_list = [('1', '0x1'), ('1', '0x2')]
    g_foreign_collection_map = {g_foreign_tuple_list[0]: PFHWVCollectionManager.getCollectionName(),  g_foreign_tuple_list[1]: PFProvinceCollectionManager.getCollectionName()} 
    
    g_final_foreign_tuple_list = g_foreign_tuple_list.copy()
    g_final_foreign_collection_map = g_foreign_collection_map.copy()
    g_final_foreign_tuple_list.append(('4096', '0x1807'))
    g_final_foreign_collection_map[('4096', '0x1807')] = PFAPPCollectionManager.getCollectionName()
      
    fMetricMap = libs.util.my_utils.openFile(sys.argv[3], 'r')
    
    metricMap = util.utils.loadMap(fMetricMap)
    
    part1_total_duration = 0
    part2_total_duration = 0
    part3_total_duration = 0
    part4_total_duration = 0
    part5_total_duration = 0
    
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    recordTime.startTime()
        
    metricManager = PFMetricCollectionManager()
    userProfileManager = PFDeviceCollectionManager()
    tagManager = PFTagsCollectionManager()
    
    str_start_day, str_end_day = __parse_date_params(sys.argv[1])
    if str_start_day == None or str_end_day == None:
        print("Date params error...... python pf_calc__profile_device.py 20140621-20140625, mTop ")
        sys.exit(1)
        
        
    #start: 获得stat doc的
    c = userProfileManager.isDocExist('stat')
    
    #不存在,则创建一个;
    if c is None or c.count() == 0:
        m = {'_id': 'stat', 'is_working': 0, 'last_update_date':str_start_day, 'first_update_date': str_start_day}
        userProfileManager.create_stat_doc(m)
        str_first_date =  userProfileManager.get_stat_first_date() 
        str_last_update_date = userProfileManager.get_stat_update_date()
    else:
        str_first_date =  userProfileManager.get_stat_first_date()     
        str_last_update_date = userProfileManager.get_stat_update_date()
        
        #因为只记录了第一个更新日期和最后一个更新日期,所以如果不连续更新的话, 就会出现不知道中间有哪些天没有做更新.所以要求必须按天连续导入日志数据.
        str_start_day, str_end_day = util.utils.get_intersect_day(str_start_day, str_end_day, str_first_date, str_last_update_date)
        if str_start_day == None or str_end_day == None:
            libs.util.logger.Logger.getInstance().debugLog("No valid start day and end day, so just quit!")
            sys.exit(1) 
    
    is_busy = userProfileManager.get_stat_busy()
    
    if is_busy > 0:
        libs.util.logger.Logger.getInstance().debugLog("metrics_collection is busy, so just quit!")
        sys.exit(1)
    else:
        userProfileManager.set_stat_busy(0)
        
    write_thread_obj = write_thread()
    write_thread_obj.start()               
    
    date_src = datetime.date(int(str_start_day[0:4]), int(str_start_day[4:6]), int(str_start_day[6:8]))
    date_des = datetime.date(int(str_end_day[0:4]), int(str_end_day[4:6]), int(str_end_day[6:8]))
    diff_days = date_des - date_src
    diff_days = diff_days.days
    
    monthLst = util.utils.calc_months(str_end_day,  diff_days)
    
    #Get all documents of indicated months; documents is user metrics.
    cursorLst = util.utils.getCursorLst(metricManager, monthLst)
    #Parse all user and info,and save to map.
    
    # For calculating tags of profile collection.
    tagCursors = tagManager.getAllDoc()
    tagLst = PFTagsCollectionManager.final_convertCursor(tagCursors)
    
    tagObjectLst = []
    for t in tagLst:
        tagObjectLst.append(PFTagsCollectionManager.final_convert2Tag(t)) 
    
    ellapsedTime = recordTime.getEllapsedTime()
    libs.util.logger.Logger.getInstance().debugLog("Call buildMetricData(): take %.3fs" % ellapsedTime)
    
    #cursorLst contains all cursor whch located in diffferent metric collections, seperated by month.
    for curs in cursorLst:
        # curs contains cursor which located in same metric collections.
        max_count = get_actural_count(curs.count(), PERFORMANCE_DEVICE_COUNT)
        for cur in curs:
            total_device_count += 1
            if total_device_count % 1000 == 0:
                libs.util.logger.Logger.getInstance().debugLog("Has handled %d device." % total_device_count)
                libs.util.logger.Logger.getInstance().debugLog("total time of part1 is: %.3fs ." % part1_total_duration)
                libs.util.logger.Logger.getInstance().debugLog("total time of part2 is: %.3fs ." % part2_total_duration)
                libs.util.logger.Logger.getInstance().debugLog("total time of part3 is: %.3fs ." % part3_total_duration)
                libs.util.logger.Logger.getInstance().debugLog("total time of part4 is: %.3fs ." % part4_total_duration)
            
            if total_device_count >= max_count:
                break
            
            uid = cur[PFCollectionManager.final_getUidLabel()]
            
            if uid == 'stat':
                continue

            uidMap, userInfoMap = util.utils.buildMetricData_new(metricManager,  cur, metricMap, g_foreign_tuple_list)
            part1_total_duration += recordTime.getEllaspedTimeSinceLast()
            
            cur = userProfileManager.isDocExist(uid)
            if cur is None or cur.count() == 0:
                old_linked_node_map = {}
            else:
                cur = next(cur.__iter__())
                old_linked_node_map = PFCollectionManager.final_get_linked_node_by_cur(cur)
            #till here, Performance: 4000/10s on PC.
            
            #sys.argv[2] is refered days.
            
            resultMap = util.utils.calc_profile(uidMap, str_start_day, str_end_day, cur, None, sys.argv[2])
            part2_total_duration += recordTime.getEllaspedTimeSinceLast()

            cuid = userInfoMap[uid].get(PFCollectionManager.final_getCUidLabel())
            imei = userInfoMap[uid].get(PFCollectionManager.final_getIMEILabel())

            if userInfoMap[uid].get('foreign_key_list') is None or len(userInfoMap[uid].get('foreign_key_list')) == 0:
                foreign_map = {}
            else:
                foreign_map = userInfoMap[uid]['foreign_key_list']
                
            '''userInfoMap[uid]['foreign_key_list'] is: {(1,1) : ['ZTE N807', 'ZTE N807'], (1,2):['beijing', 'shanghai']}'''
            linked_node_1807_map = __create_4096_1807_linked_node(resultMap[uid])
            if linked_node_1807_map is not None and len(linked_node_1807_map) > 0:
                foreign_map[next(linked_node_1807_map.keys().__iter__())] = next(linked_node_1807_map.values().__iter__())
            
            for foreign_tuple in foreign_map:
                if foreign_tuple not in g_final_foreign_tuple_list:
                    continue
                foreign_collection_name = g_final_foreign_collection_map[foreign_tuple]
                key_list = foreign_map[foreign_tuple]
                
                foreign_data_map = __consist_of_foreign_data(key_list, foreign_tuple, foreign_collection_name)

                if resultMap[uid].get(userProfileManager.final_get_label_linkde_node()) is None:
                    resultMap[uid][userProfileManager.final_get_label_linkde_node()] = {} 
                resultMap[uid][userProfileManager.final_get_label_linkde_node()][next(foreign_data_map.__iter__())] = next(foreign_data_map.values().__iter__())
            part3_total_duration += recordTime.getEllaspedTimeSinceLast()
            
            cur, is_insert = userProfileManager.merge_new_data_map(cur, uid, cuid, imei, str_start_day, str_end_day, resultMap[uid])
            
            tagMap, userInfoMap = util.calc_tag.calc_tags(userProfileManager, cur,  tagLst, tagObjectLst)
            
            '''profileMap is:
                "profile_tags":[  
                       {'tagid1':123, 'name': n880e, 'category': model},  
                       {'tagid2':456, 'name': n880e, 'category': model}  
                ] ,
                "profile_tags":[
                                       
                ] , 
                ......
            }'''
                
            for uid in tagMap:
                profileMap = {PFDeviceCollectionManager.final_getProfileTagLabel():tagMap[uid]}
                cur, temp_flag = userProfileManager.merge_new_data_map(cur, uid, cuid, imei, str_start_day, str_end_day, profileMap)
                write_thread.write_to_queue(userProfileManager.getCollectionName(), cur, is_insert)
            
            #till here, Performance: 4000/5.5s on PC.
            part4_total_duration += recordTime.getEllaspedTimeSinceLast()
    
    if str_first_date > str_start_day:
        userProfileManager.set_stat_first_date(str_start_day)
    
    if str_last_update_date < str_end_day:
        userProfileManager.set_stat_update_date(str_end_day)
        
    userProfileManager.set_stat_busy(0)
            
    libs.util.logger.Logger.getInstance().debugLog("start waiting for write thread finished.: %.3fs ." % part1_total_duration)
    write_thread_obj.set_interrupt()

    write_thread.get_queue().join()

    part5_total_duration += recordTime.getEllaspedTimeSinceLast()
    
    libs.util.logger.Logger.getInstance().debugLog("total time of part1 is: %.3fs ." % part1_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part2 is: %.3fs ." % part2_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part3 is: %.3fs ." % part3_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part4 is: %.3fs ." % part4_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part5 is: %.3fs ." % part5_total_duration)
    
    ellapsedTime = recordTime.getEllapsedTime()
    printProcess.printCurrentProcess(ellapsedTime, total_device_count)
               

    
