import sys
import platform
import time
import traceback
import libs.util.my_utils
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager
from dbmanager.pf_hwv_collection import PFHWVCollectionManager
from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager
import util.utils
import ubc.pf_metric_helper

import libs.util.logger
import config.const
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
        print("Error params...... python pf_calc_profile.py nDays, mTop, ./config/metric_map_hwv.txt")
        sys.exit(1)
    
    g_foreign_tuple_list = [('1', '0x1')]
    
    #g_foreign_label = consist_of_foreign_data(g_foreign_tuple)
    
    fMetricMap = libs.util.my_utils.openFile(sys.argv[3], 'r')
    
    metricMap = util.utils.loadMap(fMetricMap)
    
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    recordTime.startTime()
        
    metricManager = PFMetricCollectionManager()
    g_hwvCollectionManager = PFHWVCollectionManager()
    tagManager = PFTagsCollectionManager()
    
    #Fixed me!!!!!!
    #g_hwvCollectionManager.drop_table()
    
    # For calculating metric data of profile collection.
    strToday = util.utils.getStrToday()
    monthLst = util.utils.calc_months(strToday,  int(sys.argv[1]))
    
    #Get all documents of indicated months; documents is user metrics.
    cursorLst = util.utils.getCursorLst(metricManager, monthLst)
    #Parse all user and info,and save to map.
    
    uidMap, userInfoMap = util.utils.buildMetricData(metricManager,  cursorLst, metricMap, g_foreign_tuple_list)
    
    libs.util.logger.Logger.getInstance().debugLog("Start calculating user profile: %d uid document." % len(uidMap))
    #sys.argv[2] is top.
    resultMap = util.utils.calc_profile(uidMap, sys.argv[2])
    #resultMap is doc of certain collection, distinct by _id.
    
    libs.util.logger.Logger.getInstance().debugLog("Start insertng into MongoDB user profile: %d uid profile result." % len(resultMap))    
    for uid in resultMap:
        if userInfoMap[uid].get('foreign_key_list') is None or len(userInfoMap[uid].get('foreign_key_list')) == 0:
            continue
        for _id in list(userInfoMap[uid]['foreign_key_list'].values())[0]:
            if _id is None or len(_id) < 1:
                continue
            #userInfoMap[uid]['foreign_key_list'][(app_id, metric_id)] is: ['ZTE N807', 'ZTE N807']
            foreign_data_map = consist_of_foreign_data(_id, g_foreign_tuple_list[0])
            if len(foreign_data_map) > 0:
                #Get first element.
                resultMap[uid][list(foreign_data_map.keys())[0]] = next(foreign_data_map.values().__iter__())
                g_hwvCollectionManager.insertOrUpdateCollection(_id, resultMap[uid])
    
    ellapsedTime = recordTime.getEllapsedTime()
    printProcess.printCurrentProcess(ellapsedTime, len(uidMap))
    
    # For calculating tags of profile collection.
    tagCursors = tagManager.getAllDoc()
    tagLst = PFTagsCollectionManager.final_convertCursor(tagCursors)
    
    libs.util.logger.Logger.getInstance().debugLog("Start calculating tags.")
    tagMap, userInfoMap = util.calc_tag.calc_tags(g_hwvCollectionManager,  tagLst)
    
    for _id in tagMap:
        profileMap = {PFHWVCollectionManager.final_getProfileTagLabel():tagMap[_id]}
        g_hwvCollectionManager.insertOrUpdateCollection(_id,  profileMap)
    
                
            


