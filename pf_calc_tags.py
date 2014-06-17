#coding:utf-8
import sys
import util.utils
import libs.util.logger
import libs.util.my_utils
import util.calc_tag
import ubc.pf_metric_helper
from config.const import *

from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager
from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_device_collection_manager import PFDeviceCollectionManager
from dbmanager.pf_taguid_collection_manager import PFTagUidsCollectionManager

MAX_UID_COUNT_PER_DOC = 500000

def __get_devices_by_tag(tag_manager, device_manager, total_tag_map):
    result_map = {}
    total_device_count = 0
    
    libs.util.logger.Logger.getInstance().setLogFilePrefixName('')
    libs.util.logger.Logger.getInstance().setLogFileSurfixName('_tag_uids')
    
    device_cursors = device_manager.getAllDoc()
    max_count = get_actural_count(device_cursors.count(), PERFORMANCE_DEVICE_COUNT)
    
    for cur in device_cursors:
        total_device_count += 1  
        if total_device_count % 1000 == 0:
            libs.util.logger.Logger.getInstance().debugLog("Has handled %d device." % total_device_count)
            #Fixed me!!!!!! temp py.
            if total_device_count >= max_count:
                break
        
        #Get device list from profile_device_collection, which marked as this tag.
       
        device_id = cur[PFCollectionManager.final_getUidLabel()]
        tag_lst = cur[PFDeviceCollectionManager.final_getProfileTagLabel()]
        if tag_lst == None:
            continue
        for tg_map in tag_lst:
            tag_id = tg_map[PFTagsCollectionManager.final_getUidLabel()]
            if total_tag_map.get(tag_id) is not None:
                if result_map.get(tag_id) is None:
                    result_map[tag_id] = []
                result_map[tag_id].append(device_id)
                
    return result_map
    #Insert device list of tag into tag_uids_collection.

def __get_tag_device_map(tag_manager):
    cursors = tag_manager.getAllDoc()
    if cursors is None or cursors.count() == 0:
        return None
    tag_map = {}
    for cur in cursors:
        tag_map[cur[PFTagsCollectionManager.final_getUidLabel()]] = cur
    return tag_map

if __name__ == "__main__": 
    tag_manager = PFTagsCollectionManager()
    device_manager = PFDeviceCollectionManager()
    tag_deviceid_manager = PFTagUidsCollectionManager()
    #Get all tag doc.
    total_tag_map = __get_tag_device_map(tag_manager)
    
    #for x in total_tag_map:
    #    if x.__contains__('game_category_世界杯'):
    #        print(x)
    #sys.exit()
    
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    recordTime.startTime()
    
    libs.util.logger.Logger.getInstance().debugLog("total length of tag is: %d ." % len(total_tag_map))
    if total_tag_map is not None:
        result_map = __get_devices_by_tag(tag_manager, device_manager, total_tag_map)
        if result_map is not None and len(result_map) > 0:
            for tag_id in result_map:
                tag_map = total_tag_map.get(tag_id)
                
                tag_deviceid_manager.remove_doc(tag_map)
                
                device_list = result_map.get(tag_id)
                page_size = len(device_list)
                if page_size > MAX_UID_COUNT_PER_DOC:
                    page_size = MAX_UID_COUNT_PER_DOC
                
                page_count = int(len(device_list) / page_size)
                start_index = 0
                if len(device_list) % page_size != 0:
                    page_count += 1
                for i in range(page_count):
                    start_index = page_size * i
                    current_page_size = 0
                    if len(device_list) - start_index > page_size:
                        current_page_size = page_size
                    else:
                        current_page_size = len(device_list) - start_index
                    tag_deviceid_manager.insert_tag_devices_list(tag_map, device_list[start_index:page_size * i + current_page_size], len(device_list), i + 1)
                    
    ellapsedTime = recordTime.getEllapsedTime()
    printProcess.printFinalInfo(ellapsedTime)  