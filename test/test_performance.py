import sys
import platform
import time
import traceback
import gzip
import libs.util.my_utils
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager
from dbmanager.pf_hwv_collection import PFHWVCollectionManager
from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager
import util.utils
import ubc.pf_metric_helper
import libs.redis.redis_manager

import libs.util.logger
import config.const


def insertOrUpdateUser(manager, uid, data, collection = None):
    if collection is None:
        collection = manager.mDBManager.getCollection(manager.__getCollectionName__())

    manager.mDBManager.insert({'data': data},  collection)


if __name__ == "__main__":
    
    part1_total_duration = 0
    part2_total_duration = 0
    part3_total_duration = 0
    part4_total_duration = 0
    part5_total_duration = 0
    part6_total_duration = 0
    
    total_line = 0
    
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    libs.util.logger.Logger.getInstance().setLogFilePrefixName('')
    libs.util.logger.Logger.getInstance().setLogFileSurfixName('_testperformance')
    
    metricCollectionManager = PFMetricCollectionManager()
    #metricCollectionManager.drop_table()
    recordTime.startTime()
    
    for i in range(10000):
        total_line = i
        if total_line % 1000 == 0:
            libs.util.logger.Logger.getInstance().debugLog("Processed insert: %d lines." % total_line)
        uid = str(i)
        data = '0x7d3,4096,359538053624678YCo3i,359538053624678,10.242.41.243,I8552_R_2.2.44.25,GT-I8552,官方,1399131349,0,,,,480:800:160.42105:160.0:240:5.826904660976051;846;1561;ARMv7 Processor rev 1 (v7l):QRD MSM8625Q SKUD:4:245760:1209600;2:491:30;UMTS;16:4.1.2,,,,,,,,,,,,,,,,,,DC9681BEDD83E036FF9D7CD2F79A436A|876426350835953,null,1,0,3,北京,北京'
        part1_total_duration += recordTime.getEllaspedTimeSinceLast()
        insertOrUpdateUser(metricCollectionManager, uid, data)
        part2_total_duration += recordTime.getEllaspedTimeSinceLast()
        
    ellapsedTime = recordTime.getEllapsedTime()
    printProcess.printCurrentProcess(ellapsedTime, total_line) 
    
    libs.util.logger.Logger.getInstance().debugLog("total time of part1 is: %.3fs ." % part1_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part2 is: %.3fs ." % part2_total_duration)
    
    
                    