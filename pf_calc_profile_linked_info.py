import sys
import platform
import time
import traceback 
import libs.util.my_utils
import os
from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager
from dbmanager.pf_province_collection import PFProvinceCollectionManager
from dbmanager.pf_app_collection import PFAPPCollectionManager
from dbmanager.pf_hwv_collection import PFHWVCollectionManager
from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager
import util.utils
import ubc.pf_metric_helper

import libs.util.logger
from config.const import *
import util.calc_tag

def consist_of_foreign_data(_id, foreign_tuple):
    '''
    result is:
        {
          "deviceinfo_4096_7d3" : [
                'ZTE N807' 
            ] #Only one element in list, because this is key of collection.
        }

    '''
    app_id = foreign_tuple[0]
    metric_id = foreign_tuple[1]
    metricHandler = ubc.pf_metric_helper.getMetricHandler('%x' % int(metric_id, 16),  str(app_id))
    foreign_label =  metricHandler.get_profile_metric_label()
    return {foreign_label: [_id]}
    
    
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Error params...... python pf_calc_profile.py nDays, mTop, ./config/metric_map_app.txt")
        sys.exit(1)
    
    libs.util.logger.Logger.getInstance().setLogFilePrefixName('')
    libs.util.logger.Logger.getInstance().setLogFileSurfixName(sys.argv[3])
        
    part1_total_duration = 0
    part2_total_duration = 0
    part3_total_duration = 0
    part4_total_duration = 0
    
    g_CollectionManager = None
    g_foreign_tuple_list = None
    total_device_count = 0
    
    metricManager = PFMetricCollectionManager()
    tagManager = PFTagsCollectionManager()
    
    # For calculating tags of profile collection.
    tagCursors = tagManager.getAllDoc()
    tagLst = PFTagsCollectionManager.final_convertCursor(tagCursors)
    
    tagObjectLst = []
    for t in tagLst:
        tagObjectLst.append(PFTagsCollectionManager.final_convert2Tag(t)) 

    if os.path.basename(sys.argv[3]) == 'metric_map_app.txt':
        g_foreign_tuple_list = [('1', '0x3')]
        g_CollectionManager = PFAPPCollectionManager()
    if os.path.basename(sys.argv[3]) == 'metric_map_province.txt':
        g_foreign_tuple_list = [('1', '0x2')]
        g_CollectionManager = PFProvinceCollectionManager()
    if os.path.basename(sys.argv[3]) == 'metric_map_hwv.txt':
        g_foreign_tuple_list = [('1', '0x1')]
        g_CollectionManager = PFHWVCollectionManager()
    
    fMetricMap = libs.util.my_utils.openFile(sys.argv[3], 'r')
    
    metricMap = util.utils.loadMap(fMetricMap)
    
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess(g_CollectionManager.getCollectionName())
    
    recordTime.startTime()
    
    #Fixed me!!!!!!
    #g_CollectionManager.drop_table()
    
    # For calculating metric data of profile collection.
    strToday = util.utils.getStrToday()
    monthLst = util.utils.calc_months(strToday,  int(sys.argv[1]))
    
    #Get all documents of indicated months; documents is user metrics.
    cursorLst = util.utils.getCursorLst(metricManager, monthLst)
    #Parse all user and info,and save to map.
    
    #buildMetricData: Search from metrics_collection, loop in uid doc.
    #cursorLst contains all cursor whch located in diffferent metric collections, seperated by month.
    
    resultMap = {}     
    
    for curs in cursorLst:
        libs.util.logger.Logger.getInstance().debugLog("Start calculating %s: %d items." % ("metrics_collection", curs.count()))  
        # curs contains cursor which located in same metric collections.
        max_count = get_actural_count(curs.count(), PERFORMANCE_DEVICE_COUNT)
        for cur in curs:
            total_device_count += 1
            if total_device_count % 1000 == 0:
                ellapsedTime = recordTime.getEllapsedTime()
                printProcess.printCurrentProcess(ellapsedTime, total_device_count) 
            #Fixed me!!!!!! temp py.
            if total_device_count >= max_count:
                break

            uid = cur[PFCollectionManager.final_getUidLabel()]
    
            uidMap, userInfoMap = util.utils.buildMetricData_new(metricManager, cur, metricMap, g_foreign_tuple_list)
            part1_total_duration += recordTime.getEllaspedTimeSinceLast()
            #sys.argv[2] is top.
            #calc_profile: Search from metrics_collection, loop in uid doc.
            #calc_profile() will not distinct linked foreign key, so we should exclude them from uidMap in calling buildMetricData().
            device_metric_map = util.utils.calc_profile(uidMap, sys.argv[2])
           
            #resultMap is doc of certain collection, distinct by _id.
            part2_total_duration += recordTime.getEllaspedTimeSinceLast()

            #if uid has no related keys with this collection, just skip it.
            if userInfoMap[uid].get('foreign_key_list') is None or len(userInfoMap[uid].get('foreign_key_list')) == 0:
                continue
            
            #list(userInfoMap[uid]['foreign_key_list'].values())[0] is an list of doc key.
            for _id in list(userInfoMap[uid]['foreign_key_list'].values())[0]:
                if _id is None or len(_id) < 1 or resultMap.get(_id) is not None:
                    continue
                
                #merge_new_data_map()
                cur = g_CollectionManager.isDocExist(_id)
                if cur is not None and  cur.count() > 0:
                    cur = next(cur.__iter__())  
                part3_total_duration += recordTime.getEllaspedTimeSinceLast()
                #userInfoMap[uid]['foreign_key_list'] is: ['ZTE N807', 'ZTE N807']
                foreign_data_map = consist_of_foreign_data(_id, g_foreign_tuple_list[0])
                if len(foreign_data_map) > 0:
                    #Get first element.
                    resultMap[_id] = device_metric_map[uid]
                    resultMap[_id][list(foreign_data_map.keys())[0]] = next(foreign_data_map.values().__iter__())
                    #g_CollectionManager.insertOrUpdateCollection(_id, resultMap[_id])
                cur, is_insert = g_CollectionManager.merge_new_data_map(cur, _id, resultMap[_id])
                tagMap, userInfoMap = util.calc_tag.calc_tags(g_CollectionManager, cur,  tagLst, tagObjectLst)
                for _id in tagMap:
                    profileMap = {g_CollectionManager.__class__.final_getProfileTagLabel():tagMap[_id]}
                    cur, temp_flag = g_CollectionManager.merge_new_data_map(cur, _id, profileMap)
                    g_CollectionManager.insertOrUpdateCollection(cur,  is_insert)
                part4_total_duration += recordTime.getEllaspedTimeSinceLast()
    
    #libs.util.logger.Logger.getInstance().debugLog("Start calculating tags.")
    #calc_tags: Search from profle_device_collection, loop in uid doc.
    #tagMap, userInfoMap = util.calc_tag.calc_tags_old(g_CollectionManager,  tagLst)
    #part4_total_duration += recordTime.getEllaspedTimeSinceLast()
   
    libs.util.logger.Logger.getInstance().debugLog("total time of part1 is: %.3fs ." % part1_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part2 is: %.3fs ." % part2_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part3 is: %.3fs ." % part3_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part4 is: %.3fs ." % part4_total_duration)   
    ellapsedTime = recordTime.getEllapsedTime()
    printProcess.printFinalInfo(ellapsedTime)
                
            


