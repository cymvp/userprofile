#!D:\WorkSpace\Python33\python.exe
import time
import libs.util.logger
import libs.util.my_utils
import sys
import config.const
import tags.pf_tags
import pymongo
import util
import traceback

from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager

MAX_UID_COUNT_PER_DOC = 100000

class PFOperationDailyUVCollectionManager(PFCollectionManager):
    __PROFILE_COLLCETION_PREFIX = 'operation_daily_uv_collection'
    
    @staticmethod
    def getCollectionName():
        return PFOperationDailyUVCollectionManager.__PROFILE_COLLCETION_PREFIX
    
    def update_doc(self, str_date, device_list, total_uid_count, current_page = -1):
        try:
            collection_= self.mDBManager.getCollection(PFOperationDailyUVCollectionManager.__PROFILE_COLLCETION_PREFIX)
            if len(device_list) != total_uid_count:  
                if current_page > 0:
                    tag_id = str_date + '(' + str(current_page) + ')'
                else:
                    raise util.pf_exception.PFExceptionFormat()
            else:
                tag_id = str_date + '(' + str(current_page) + ')'
            self.mDBManager.save({'_id': tag_id, tag_id:device_list}, collection_)
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            libs.util.logger.Logger.getInstance().errorLog(traceback.format_exc())
            libs.util.logger.Logger.getInstance().errorLog('!!!! %s' % e)
            raise util.pf_exception.PFExceptionWrongStatus

    def remove_doc(self, str_date):
        collection = self.mDBManager.getCollection(PFOperationDailyUVCollectionManager.__PROFILE_COLLCETION_PREFIX)
        tag_id = str_date
        spec_map = {'$regex': tag_id}
        uidMap = self.__buildUid__(spec_map)
        self.mDBManager.remove(uidMap, collection)            

if __name__ == "__main__":
    
    #libs.util.logger.Logger.getInstance().setDirectToFile(False)
    
    if len(sys.argv) < 2:
        print("Error params...... python3  export_operation_daily_uid.py 20140717")
        sys.exit(1)
    
    libs.util.logger.Logger.getInstance().setLogFilePrefixName('')
    libs.util.logger.Logger.getInstance().setLogFileSurfixName('_dailyoperation')
    
    g_date = sys.argv[1]
    
    total_line = 0
    
    if len(g_date) != 8:
        print("Date format is error......")
        sys.exit(1) 
    
    metricCollectionManager = PFMetricCollectionManager()
    
    current_collection = metricCollectionManager.final_getCollection(g_date)
    
    
    #curs = metricCollectionManager.getDocWithCondition({'_id':'8B56711E115C8791C71CBCA28A301F84|362505320019268'}, current_collection)
    
    curs = metricCollectionManager.getDocWithCondition({'metrics.metric_id':'0x1808'}, current_collection)
    
    device_list = []
    
    for cur in curs:
        
        total_line += 1    
        if total_line % 100000 == 0:
            libs.util.logger.Logger.getInstance().debugLog("Processed total: %d lines." % total_line)
         
        uid = cur[PFCollectionManager.final_getUidLabel()]
        for metric_map in cur[PFMetricCollectionManager.final_getMetricsLabel()]:
            if metric_map.get(PFMetricCollectionManager.final_getMetricIdLabel()) != '0x1808' or metric_map.get(PFMetricCollectionManager.final_getAppIdLabel()) != '36864':
                continue
            metric_data_list = metric_map[PFMetricCollectionManager.final_getMetricDataLabel()]
            if len(metric_data_list) == 0:
                continue
            metric_data_map = metric_data_list[0]
            for channel_ in metric_data_map:
                if metric_data_map[channel_].get(g_date) is not None:
                    version = metric_data_map[channel_][g_date]['version']
                    device_list.append("%s:%s:%s" % (uid, channel_, version))
    
    daily_collection_manager = PFOperationDailyUVCollectionManager()
    daily_collection_manager.remove_doc(g_date)

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
        daily_collection_manager.update_doc(g_date, device_list[start_index:page_size * i + current_page_size], len(device_list), i + 1)
     
    
   
    #daily_collection_manager.update_doc({g_date:result_list})

        
