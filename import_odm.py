#!D:\WorkSpace\Python33\python.exe
import time
import libs.util.logger
import libs.util.my_utils
import sys
import config.const
import tags.pf_tags

from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager
from dbmanager.pf_taguid_collection_manager import PFTagUidsCollectionManager


if __name__ == "__main__":
    
    #lpush("sadd", 'big', (1,2,3))
    
    lineNumber = 0
    errorNumber = 0
    
    libs.util.logger.Logger.getInstance().setLogFilePrefixName('')
    libs.util.logger.Logger.getInstance().setLogFileSurfixName('_importodmdata')
    
    if len(sys.argv) < 2:
        sys.exit()
    
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    recordTime.startTime()
    
    f_odm_file = libs.util.my_utils.openFile(sys.argv[1], 'r')
    
    tag_deviceid_manager = PFTagUidsCollectionManager()
    
    g_odm_uids_map = {} 
    
    for line in f_odm_file:
        #line = 'o123\tabcde'
        arr = line.strip().split('\t')
        if len(arr) < 2:
            errorNumber += 1
            continue
        
        if errorNumber > 100:
            libs.util.logger.Logger.getInstance().debugLog("error count is to much! so I quit.")
            break;
        
        lineNumber += 1
        if lineNumber % 10000 == 0:
            ellapsedTime = recordTime.getEllapsedTime()
            printProcess.printCurrentProcess(ellapsedTime, lineNumber)
        
        arr[0] = arr[0].strip().replace(r'\n', '')
        
        
        unique_name = arr[0]
        category_name = 'operationodm'
        tag_id = category_name + '_' + unique_name

        if g_odm_uids_map.get(tag_id) is None:
            t = {}
            t[PFTagsCollectionManager.getNameLabel()] = (arr[0])
            t[PFTagsCollectionManager.getCategoryLabel()] = category_name
            t[PFTagsCollectionManager.getUniqueNameLabel()] = unique_name
            g_odm_uids_map[tag_id] = (t, [arr[1]])
        else:
            g_odm_uids_map[tag_id][1].append(arr[1])
        
    for tag_id in g_odm_uids_map:   
        tag_map, device_list = g_odm_uids_map.get(tag_id)
        tag_deviceid_manager.insert_tag_devices_list(tag_map, device_list)
        
