import sys
import platform
import time
import traceback
import threading
from queue import Queue
import queue
import libs.util.my_utils
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager
from dbmanager.pf_hwv_collection import PFHWVCollectionManager
from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager
import util.utils
import ubc.pf_metric_helper
import libs.redis.redis_manager

import libs.util.logger
import config.const

#Fix me. Should read from config file.
metricCollector_4096_1807 =  {12: ('packagename',)}
metricCollector_4096_7d3 =  {13: ('deviceinfo',)}

class work_thread(threading.Thread):
    
    
    def __init__(self, metricMap, str_tag):
        threading.Thread.__init__(self)
        self.__queue = Queue()
        self.interrupt_flag = False
        self.__metric_map = metricMap
        self.__tag = str_tag
        pass 
    
    def set_interrupt(self):
        self.interrupt_flag = True
        
    def get_queue(self):
        return self.__queue
    
    def write_to_queue(self, line):
        self.__queue.put(line)
    
    def read_from_queue(self):
        return self.__queue.get(timeout = 6)
    
    def __convert_appid_metricid(self, arr, appid, metricid):
        arr[1] = appid
        arr[0] = metricid
    
    def run(self):
        g_last_uid = ''
        g_last_imei = ''
        g_last_cuid = ''
        g_last_metric_data_list = []
        
        handled_count = 0
        
        while True:
            if self.interrupt_flag == True and self.__queue.empty():
                break
            try:
                line = self.read_from_queue() 
            except queue.Empty:
                if self.interrupt_flag == True:
                    break
            else:
                handled_count += 1
                if handled_count % 1000 == 0:
                    libs.util.logger.Logger.getInstance().errorLog('thread %s has handled count: %d' % (self.__tag, handled_count))
                arr = line.strip().split(',')
                #Parse raw data.
                tupl = handle_data(arr,  self.__metric_map)   
                if tupl is None:
                    continue
                
                #One metric log data, for only one metricMap.
                appid_metricid, valueMap = tupl
                app_id = appid_metricid[0]
                metric_id = appid_metricid[1]
                             
                appid_lst = []
                metricid_lst = []
                value_map_lst = []
                
                appid_lst.append(app_id)
                metricid_lst.append(metric_id)
                value_map_lst.append(valueMap)
                
                #Ensure 0x7d3 has model.
                if app_id == '4096' and metric_id in ('0x1807', '0x7d3', '0x7d2', '0x7d1'):
                    raw_metric_id = metric_id
                    #model
                    self.__convert_appid_metricid(arr, '1', '0x1')
                    tupl = handle_data(arr,  metricMap)
                    if tupl is not None:
                        appid_metricid, valueMap = tupl
                        app_id = appid_metricid[0]
                        metric_id = appid_metricid[1]
                        appid_lst.append(app_id)
                        metricid_lst.append(metric_id)
                        value_map_lst.append(valueMap)
                    #province
                    self.__convert_appid_metricid(arr, '1', '0x2')
                    tupl = handle_data(arr,  metricMap)
                    if tupl is not None:
                        appid_metricid, valueMap = tupl
                        app_id = appid_metricid[0]
                        metric_id = appid_metricid[1]
                        appid_lst.append(app_id)
                        metricid_lst.append(metric_id)
                        value_map_lst.append(valueMap)
                    #package_name
                    if raw_metric_id == '0x1807':
                        self.__convert_appid_metricid(arr, '1', '0x3')
                        tupl = handle_data(arr,  metricMap)
                        if tupl is not None:
                            appid_metricid, valueMap = tupl
                            app_id = appid_metricid[0]
                            metric_id = appid_metricid[1]
                            appid_lst.append(app_id)
                            metricid_lst.append(metric_id)
                            value_map_lst.append(valueMap)
                    #calc chunleiid only if this user has 0x7d2 metric. 0x7d2 is account metric;
                    if raw_metric_id == '0x7d2':
                        self.__convert_appid_metricid(arr, '1', '0x4')
                        tupl = handle_data(arr,  metricMap)
                        if tupl is not None:
                            appid_metricid, valueMap = tupl
                            app_id = appid_metricid[0]
                            metric_id = appid_metricid[1]
                            appid_lst.append(app_id)
                            metricid_lst.append(metric_id)
                            value_map_lst.append(valueMap)
                        
                if value_map_lst is not None:
                    uid = util.utils.getUid(line)
                    imei = util.utils.getIMEI(line)         
                    #Add CUID on 20140425.
                    cuid = util.utils.getCUID(line)
                    
                    if g_last_uid == '':
                        g_last_uid = uid
                    
                    if g_last_uid != uid:
                        writeSuccess = False
                        while writeSuccess != True:      
                            try:
                                pass
                                #libs.util.logger.Logger.getInstance().debugLog(line)
                            except:
                                libs.util.logger.Logger.getInstance().errorLog(self.__tag + ': ' + 'line can not be shown.')
                            try:
                                #metricCollectionManager.insertOrUpdateUser(g_last_uid,  g_last_cuid,  g_last_imei, metricid_lst,  appid_lst,  value_map_lst)
                                #g_last_uid = "$123"
                                metricCollectionManager.insertOrUpdateUser(g_last_uid,  g_last_cuid,  g_last_imei,  g_last_metric_data_list)
                                #raise util.pf_exception.PFExceptionWrongStatus()
                            except util.pf_exception.PFExceptionWrongStatus:
                                libs.util.logger.Logger.getInstance().errorLog(self.__tag + ': ' + 'mongodb happened some error, wait reconnect......')
                                time.sleep(10)
                            except util.pf_exception.PFExceptionFormat:
                                libs.util.logger.Logger.getInstance().errorLog(self.__tag + ': ' + 'Format is Wrong: ' + ','.join([g_last_uid, str(g_last_metric_data_list)]))
                                writeSuccess = True
                                g_last_imei = imei
                                g_last_cuid = cuid
                                g_last_uid = uid
                                g_last_metric_data_list = []
                            else:
                                writeSuccess = True
                                g_last_imei = imei
                                g_last_cuid = cuid
                                g_last_uid = uid
                                g_last_metric_data_list = []
                        
                    i = 0
                    while i < len(metricid_lst):
                        metricId = metricid_lst[i]
                        appId = appid_lst[i]
                        valueMap = value_map_lst[i]
                        metric_map = metricCollectionManager.isMetricExist(g_last_metric_data_list, metricId, appId)
                        if metric_map is None:
                            metric_map = metricCollectionManager.get_metric_data_list(uid, appId, metricId)
                            if metric_map is None:
                                metric_map = metricCollectionManager.buildsubDocMetric(metricId,  appId)
                            #userMap[PFMetricCollectionManager.final_getMetricsLabel()].append(metricMap)
                            
                        metricHandler = ubc.pf_metric_helper.getMetricHandler('%x'% int(metricId, 16),  str(appId)) # hex(metricId)[2:]
                        metric_map[PFMetricCollectionManager.final_getMetricDataLabel()] = metricHandler.handle_raw_data(metric_map[PFMetricCollectionManager.final_getMetricDataLabel()], valueMap) 
                        
                        g_last_metric_data_list.append(metric_map)
                        
                        i += 1
                    
                    #deviceProfileManager.insertOrUpdateUser(cur, is_insert)
            self.__queue.task_done()
        
        libs.util.logger.Logger.getInstance().errorLog('thread %s total handled count is: %d' % (self.__tag, handled_count))



def handle_data(arr,  metricMap):
    '''
    metricCollector is : 
    {
        8: ('timestamp',), 
        17: ('collect_date',), 
        18: ('launch_count',), 
        19: ('duration',), 
        12: ('packagename',)
    }
    
    valueMap is:
        { time:"2013-06-07 15:34:22", 
          field_1:"value", field_2:"value" 
        }
    '''
    valueMap = {}

    arr = util.utils.isValidArr(arr) 
    
    if arr == None:
        return None
    
    # '0x511' -> 0x511
    metricId = int(arr[0], 16)
    #4096
    appId = int(arr[1])
    metricCollector = metricMap.get((appId,metricId))
    #This metricId is not the candidate.
    if metricCollector is None:
        return None
    metricHandler = ubc.pf_metric_helper.getMetricHandler('%x'%metricId,  str(appId)) # hex(metricId)[2:]    
    for index in metricCollector:
        strDescriptor = metricCollector[index][0]
        try:
            value = metricHandler.convertData(index,  arr[index])
        except Exception as e:
            print(e)
        valueMap[strDescriptor] = value

    #((appid,metricid), field map.)
    
    return ((arr[1], arr[0]),  valueMap)
    
# -*- coding:utf-8 -*-
def __convert_n_bytes(n, b):
    bits = b*8
    return (n + 2**(bits-1)) % 2**bits - 2**(bits-1)

def __convert_4_bytes(n):
    return __convert_n_bytes(n, 4)

def getHashCode(s):
    h = 0
    n = len(s)
    for i, c in enumerate(s):
        h = h + ord(c)*31**(n-1-i)
    return __convert_4_bytes(h)


if __name__ == "__main__":
    
    #print (getHashCode('http://outofmemory.cn/'))
    #sys.exit(1)
    
    part1_total_duration = 0
    part2_total_duration = 0
    part3_total_duration = 0
    part4_total_duration = 0
    part5_total_duration = 0
    
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    recordTime.startTime()
    
    libs.util.logger.Logger.getInstance().setLogFilePrefixName('')
    libs.util.logger.Logger.getInstance().setLogFileSurfixName('_pickmetric')

    fInputFile = None
     
    metricCollectionManager = PFMetricCollectionManager()
    
    #fixme!
    #metricCollectionManager.drop_table()
    
    # python pf_pick_metric.py ./config/MetricMap_part.txt D:\mongodb\p_data\ubc_100
    fMetricMap = libs.util.my_utils.openFile(sys.argv[1], 'r')
    if platform.system() == 'Windows_xxx':
        #chunlei_ubc_20130729_part
        fInputFile = libs.util.my_utils.openFile(sys.argv[2], 'r')
    else:
        redisManager = libs.redis.redis_manager.redis_manager(config.const.CONST_SERVER_ADDR, config.const.CONST_QUEUE_NAME, True)

    metricMap = util.utils.loadMap(fMetricMap)
    
    gCount= 10
    
    total_line = 0 
        
    g_work_thread = []
    for i in range(config.const.THREAD_COUNT_PICK_METRIC):
        write_thread_obj = work_thread(metricMap, str(i))
        write_thread_obj.start()
        g_work_thread.append(write_thread_obj)
    
    recordTime.getEllaspedTimeSinceLast()
    
    while gCount > 0:
        
        total_line += 1
        
        if config.const.PERFORMANCE_DEVICE_COUNT > 0 and total_line > config.const.PERFORMANCE_DEVICE_COUNT:
            break
        
        if total_line % 1000 == 0:
            libs.util.logger.Logger.getInstance().debugLog("Processed: %d lines." % total_line)
            if total_line > 500000000:
                total_line = 1
        if platform.system() == 'Windows_xxx':
            line = fInputFile.readline()
            if line == '':
                break
        else:
            try:
                part1_total_duration += recordTime.getEllaspedTimeSinceLast()
                line = redisManager.pop()
                part2_total_duration += recordTime.getEllaspedTimeSinceLast()
                if line is None or len(line) == 0:
                    time.sleep(2)
                    continue
            except Exception as e:
                # handle any exception to ensure the sub process exit normally
                libs.util.logger.Logger.getInstance().errorLog(traceback.format_exc())
                libs.util.logger.Logger.getInstance().errorLog('!!!! %s' % e)
                break

        '''tupl is ((4096,0x1807), { time:"2013-06-07 15:34:22", field_1:"value", field_2:"value" }) '''
        
        part3_total_duration += recordTime.getEllaspedTimeSinceLast()
        
        uid = util.utils.getUid(line)
        hash_code = getHashCode(uid)
        i = hash_code % config.const.THREAD_COUNT_PICK_METRIC
        g_work_thread[i].write_to_queue(line)
        
        part4_total_duration += recordTime.getEllaspedTimeSinceLast()
        
    libs.util.logger.Logger.getInstance().debugLog("total time of part1 is: %.3fs ." % part1_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part2 is: %.3fs ." % part2_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part3 is: %.3fs ." % part3_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part4 is: %.3fs ." % part4_total_duration)
    
    redisManager.printTime()
    
    for i in range(len(g_work_thread)):  
        g_work_thread[i].set_interrupt()
        
    redisManager.stopDemon()
        #g_work_thread[i].get_queue().join()
        
                        
                
        
                    