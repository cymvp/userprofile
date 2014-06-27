import sys
import time
import traceback
import gzip
import libs.util.my_utils
from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager
from externdata.app_filter import AppFilter
import util.utils
import ubc.pf_metric_helper
import libs.redis.redis_manager
import libs.util.logger
import config.const
import pymongo

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
            value = None
        if value == None:
            return None
        else:
            valueMap[strDescriptor] = value

    #((appid,metricid), field map.)
    
    return ((arr[1], arr[0]),  valueMap)

def __convert_appid_metricid(arr, appid, metricid):
    arr[1] = appid
    arr[0] = metricid
    
def __convert_virtual_metric(tupl, appid_lst, metricid_lst, value_map_lst):
    appid_metricid, valueMap = tupl
    app_id = appid_metricid[0]
    metric_id = appid_metricid[1]
    appid_lst.append(app_id)
    metricid_lst.append(metric_id)
    value_map_lst.append(valueMap)

def insertOrUpdateUser(self,  cur, chunleiId,  cuid,  imei, metrics_list, recordTime, collection = None, str_date = None):
    
    global part7_total_duration
    global part8_total_duration
    global part9_total_duration
    global part10_total_duration
    
    try:
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__(str_date))
        #check whether this document existed, checked by chunleiid.

        userMap = {}
        isInsert = 0
        
        part7_total_duration += recordTime.getEllaspedTimeSinceLast()
        
        if cur is None:
            #insert.
            userMap = self.__buildDocUser__(chunleiId,  cuid, imei)
            isInsert = 1
        else:
            #update
            #g_last_cur = next(g_last_cur.__iter__())
            #userMap = cur.__getitem__(0)
            userMap = cur 
        
        for metric_map in metrics_list:
            metric_id = metric_map[PFMetricCollectionManager.final_getMetricIdLabel()]
            app_id = metric_map[PFMetricCollectionManager.final_getAppIdLabel()]
            old_metric_map = self.isMetricExist(userMap[self.final_getMetricsLabel()],  metric_id,  app_id)
            if old_metric_map is None:
                userMap[PFMetricCollectionManager.final_getMetricsLabel()].append(metric_map)
            else:
                old_metric_map[PFMetricCollectionManager.final_getMetricDataLabel()] = metric_map[PFMetricCollectionManager.final_getMetricDataLabel()]

            #metricMap is sub element of metrics[] with certain appid and metricid, and metric_data[]
            
        part9_total_duration += recordTime.getEllaspedTimeSinceLast()    
        
        if isInsert == 1:
            self.mDBManager.insert(userMap,  collection)
        else:
            self.mDBManager.update(self.__buildUid__(chunleiId),  userMap,  collection)
        
        part10_total_duration += recordTime.getEllaspedTimeSinceLast()  
        
    except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
        print('111111')
        libs.util.logger.Logger.getInstance().errorLog(traceback.format_exc())
        libs.util.logger.Logger.getInstance().errorLog('!!!! %s' % e)
        raise util.pf_exception.PFExceptionWrongStatus

if __name__ == "__main__":
    
    #print (getHashCode('http://outofmemory.cn/'))
    #sys.exit(1)
    
    if len(sys.argv) < 4:
        print("Error params...... python3  pf_pick_metric_new.py ./config/metric_map_metrics.txt /data/web/upload/ubc_data/chunlei_ubc_20140604_ubc00_ai.tar.gz ")
        sys.exit(1)
    
    skip_line = -1  
    if len(sys.argv) == 5:
        skip_line = int(sys.argv[4])

    print(skip_line)
    
    g_date = sys.argv[3]
    
    
    part1_total_duration = 0
    part2_total_duration = 0
    part3_total_duration = 0
    part4_total_duration = 0
    part5_total_duration = 0
    part6_total_duration = 0
    part7_total_duration = 0
    part8_total_duration = 0
    part9_total_duration = 0
    part10_total_duration = 0
    
    
    part11_total_duration = 0
    part12_total_duration = 0
    part13_total_duration = 0
    
    
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    recordTime.startTime()
    
    libs.util.logger.Logger.getInstance().setLogFilePrefixName('')
    libs.util.logger.Logger.getInstance().setLogFileSurfixName('_pickmetric')

    fInputFile = None
     
    metricCollectionManager = PFMetricCollectionManager()
    
    current_collection_name = metricCollectionManager.final_getCollection(g_date)
    
    app_filter = AppFilter()
    
    #fixme!!!!!!!
    #metricCollectionManager.drop_table()
    
    # python pf_pick_metric.py ./config/MetricMap_part.txt D:\mongodb\p_data\ubc_100
    #sys.argv[2] = 'd:/test.tar.gz'
    fMetricMap = libs.util.my_utils.openFile(sys.argv[1], 'r')
    if config.const.FLAG_PICK_METRICS_FROM_REDIS == False:
        #chunlei_ubc_20130729_part
        libs.util.logger.Logger.getInstance().debugLog("File name is %s" % sys.argv[2])
        fInputFile = gzip.open(sys.argv[2], 'rt', encoding = 'utf-8')
        for i in range(3):
            try:
                fInputFile.readline()
            except  Exception as e:
                print(e)
    else:
        redisManager = libs.redis.redis_manager.redis_manager(config.const.CONST_SERVER_ADDR, config.const.CONST_QUEUE_NAME, True)

    metricMap = util.utils.loadMap(fMetricMap)
    
    gCount= 10
    
    total_line = 0
    valid_line = 0
    insert_count = 0
    
    g_blacklist_map = {}
    g_uid_size_map = {}
    
    g_last_uid = ''
    g_last_imei = ''
    g_last_cuid = ''
    g_last_metric_data_list = []
    g_last_cur = None
    
        
    while gCount > 0:
        
        total_line += 1
        
        if config.const.PERFORMANCE_DEVICE_COUNT > 0 and valid_line > config.const.PERFORMANCE_DEVICE_COUNT:
            break
        
        if total_line % 100000 == 0:
            libs.util.logger.Logger.getInstance().debugLog("Processed total: %d lines." % total_line)
        
        if config.const.FLAG_PICK_METRICS_FROM_REDIS == False:
            line = fInputFile.readline()
            part1_total_duration += recordTime.getEllaspedTimeSinceLast()
            if line is None or len(line) <= 0:
                break
            if skip_line > 0 and total_line < skip_line:
                continue         
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
        
        arr = line.strip().split(',')                
        
        #Fixed me!!!!!!
        if arr[0] != '0x1807':
            continue
        
        if arr[0] == '0x1807':
            package_name = arr[12]
            package_name = util.utils.format_2_mongo_key_(package_name)
            
            #Filter the app which is no tag.
            if app_filter.is_fit(package_name) is False:
                continue
            #If the app is in white list, we will raise it weight.
            if app_filter.is_in_white_list(package_name) is False:
                 arr[20] = '1'
                 
        #Parse raw data.
        tupl = handle_data(arr,  metricMap)   
        if tupl is None:
            continue
        
        valid_line += 1
        if valid_line % 1000 == 0:
            libs.util.logger.Logger.getInstance().debugLog("Processed valid: %d lines." % valid_line)
        if valid_line % 50000 == 0:
            libs.util.logger.Logger.getInstance().debugLog("total time of part1 is: %.3fs ." % part1_total_duration)
            libs.util.logger.Logger.getInstance().debugLog("total time of part2 is: %.3fs ." % part2_total_duration)
            libs.util.logger.Logger.getInstance().debugLog("total time of part3 is: %.3fs ." % part3_total_duration)
            libs.util.logger.Logger.getInstance().debugLog("total time of part4 is: %.3fs ." % part4_total_duration)
            libs.util.logger.Logger.getInstance().debugLog("total time of part5 is: %.3fs ." % part5_total_duration)
            libs.util.logger.Logger.getInstance().debugLog("total time of part6 is: %.3fs ." % part6_total_duration)
            libs.util.logger.Logger.getInstance().debugLog("total time of part7 is: %.3fs ." % part7_total_duration)
            libs.util.logger.Logger.getInstance().debugLog("total time of part8 is: %.3fs ." % part8_total_duration)
            libs.util.logger.Logger.getInstance().debugLog("total time of part9 is: %.3fs ." % part9_total_duration)
            libs.util.logger.Logger.getInstance().debugLog("total time of part10 is: %.3fs ." % part10_total_duration)
            
            libs.util.logger.Logger.getInstance().debugLog("total time of part11 is: %.3fs ." % part11_total_duration)
            libs.util.logger.Logger.getInstance().debugLog("total time of part12 is: %.3fs ." % part12_total_duration)
            libs.util.logger.Logger.getInstance().debugLog("total time of part13 is: %.3fs ." % part13_total_duration)
            
            libs.util.logger.Logger.getInstance().debugLog("total count of black uid is: %d" % len(g_blacklist_map))
            libs.util.logger.Logger.getInstance().debugLog(str(g_blacklist_map))
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
            __convert_appid_metricid(arr, '1', '0x1')
            tupl = handle_data(arr,  metricMap)
            if tupl is not None:
                __convert_virtual_metric(tupl, appid_lst, metricid_lst, value_map_lst)
            #province
            __convert_appid_metricid(arr, '1', '0x2')
            tupl = handle_data(arr,  metricMap)
            if tupl is not None:
                __convert_virtual_metric(tupl, appid_lst, metricid_lst, value_map_lst)
            #calc chunleiid only if this user has 0x7d2 metric. 0x7d2 is account metric;
            
            if raw_metric_id == '0x7d2':
                __convert_appid_metricid(arr, '1', '0x4')
                tupl = handle_data(arr,  metricMap)
                if tupl is not None:
                    __convert_virtual_metric(tupl, appid_lst, metricid_lst, value_map_lst)
        
        part2_total_duration += recordTime.getEllaspedTimeSinceLast()
                
        if value_map_lst is not None:
            uid = util.utils.getUid(line)
            imei = util.utils.getIMEI(line)         
            #Add CUID on 20140425.
            cuid = util.utils.getCUID(line)
            
            if g_blacklist_map.get(uid) is not None:
                g_blacklist_map[uid] += 1
                continue
    
            # g_last_uid is empty only if the uid is the first uid..
            if g_last_uid == '':
                g_last_uid = uid
                g_last_imei = imei
                g_last_cuid = cuid
                g_last_metric_data_list = []
                
                #merge_new_data_map()
                g_last_cur = metricCollectionManager.isDocExist(uid, current_collection_name)
                
                libs.util.logger.Logger.getInstance().debugLog("Must call once!") 
     
                if g_last_cur is None or g_last_cur.count() == 0:
                    g_last_metric_data_list = []
                    g_last_cur = None
                else:
                    g_last_cur = next(g_last_cur.__iter__())
                    g_last_metric_data_list = g_last_cur[PFMetricCollectionManager.final_getMetricsLabel()]
                
            if g_last_uid != uid:
                writeSuccess = False
                while writeSuccess != True:      
                    try:
                        pass
                        #libs.util.logger.Logger.getInstance().debugLog(line)
                    except:
                        libs.util.logger.Logger.getInstance().errorLog('line can not be shown.')
                    try:
                        #metricCollectionManager.insertOrUpdateUser(g_last_uid,  g_last_cuid,  g_last_imei, metricid_lst,  appid_lst,  value_map_lst)
                        #g_last_uid = "$123"
                        insert_count += 1
                        if insert_count % 1000 == 0:
                            libs.util.logger.Logger.getInstance().debugLog("Processed insert: %d lines." % insert_count)
                        part3_total_duration += recordTime.getEllaspedTimeSinceLast()
                        #metricCollectionManager.insertOrUpdateUser(g_last_uid,  g_last_cuid,  g_last_imei,  g_last_metric_data_list)
                        #Fixed me. Should updating using $set, because doc is so big.
                        
                        insertOrUpdateUser(metricCollectionManager, g_last_cur, g_last_uid,  g_last_cuid,  g_last_imei,  g_last_metric_data_list, recordTime, None, g_date)
                        
                    except util.pf_exception.PFExceptionWrongStatus:
                        print('PFExceptionWrongStatus.')
                        libs.util.logger.Logger.getInstance().errorLog('mongodb happened some error, wait reconnect......')
                        time.sleep(10)
                    except util.pf_exception.PFExceptionFormat:
                        print('PFExceptionFormat.')
                        libs.util.logger.Logger.getInstance().errorLog('Format is Wrong: ' + ','.join([g_last_uid, ]))
                        writeSuccess = True
                        g_last_uid = ""
                    else:
                        writeSuccess = True
                        g_last_uid = ""
            
            part4_total_duration += recordTime.getEllaspedTimeSinceLast()
            
            if g_last_uid == "":
                g_last_uid = uid
                g_last_cuid = cuid
                g_last_imei = imei
                g_last_metric_data_list = []
                
                #merge_new_data_map()
                g_last_cur = metricCollectionManager.isDocExist(g_last_uid, current_collection_name)
                     
                if g_last_cur is None or g_last_cur.count() == 0:
                    g_last_metric_data_list = []
                    g_last_cur = None
                else:
                    g_last_cur = next(g_last_cur.__iter__())
                    g_last_metric_data_list = g_last_cur[PFMetricCollectionManager.final_getMetricsLabel()]
            i = 0
            part5_total_duration += recordTime.getEllaspedTimeSinceLast()
            while i < len(metricid_lst):
                metricId = metricid_lst[i]
                appId = appid_lst[i]
                valueMap = value_map_lst[i]
                
                metric_map = metricCollectionManager.isMetricExist(g_last_metric_data_list, metricId, appId)
                if metric_map is None:
                    #metric_map = metricCollectionManager.get_metric_data_list(uid, appId, metricId)
                    #if metric_map is None:
                    metric_map = metricCollectionManager.buildsubDocMetric(metricId,  appId)
                    g_last_metric_data_list.append(metric_map)
                    #userMap[PFMetricCollectionManager.final_getMetricsLabel()].append(metricMap)
                
                metricHandler = ubc.pf_metric_helper.getMetricHandler('%x'% int(metricId, 16),  str(appId)) # hex(metricId)[2:]
                
                part11_total_duration += recordTime.getEllaspedTimeSinceLast()
                
                metric_map[PFMetricCollectionManager.final_getMetricDataLabel()] = metricHandler.handle_raw_data(metric_map[PFMetricCollectionManager.final_getMetricDataLabel()], valueMap) 
                
                part12_total_duration += recordTime.getEllaspedTimeSinceLast()
                
                i += 1
            
            part13_total_duration += recordTime.getEllaspedTimeSinceLast()
            
            if g_uid_size_map.get(g_last_uid) is None:
                g_uid_size_map[g_last_uid] = [0, 0]
            #Calculate size of cur per 100 times;Including first time.
            if g_uid_size_map[g_last_uid][0] % 100 == 0:
                cur_size = len(str(g_last_cur))
                if cur_size > 1000000:
                    libs.util.logger.Logger.getInstance().debugLog("read big size: length of current cur is %s, size is %d, line num is: %d." % (g_last_uid, len(str(g_last_cur)), total_line))
                    if g_blacklist_map.get(g_last_uid) is None:
                        g_blacklist_map[g_last_uid] = 1
                    else:
                        g_blacklist_map[g_last_uid] += 1         
                g_uid_size_map[g_last_uid][1] = cur_size
            g_uid_size_map[g_last_uid][0] += 1 
            
            part6_total_duration += recordTime.getEllaspedTimeSinceLast()
                
    ellapsedTime = recordTime.getEllapsedTime()
    printProcess.printCurrentProcess(ellapsedTime, total_line) 
    
    printProcess.printCurrentProcess(ellapsedTime, valid_line) 
    
    printProcess.printCurrentProcess(ellapsedTime, insert_count) 
    
    libs.util.logger.Logger.getInstance().debugLog("total time of part1 is: %.3fs ." % part1_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part2 is: %.3fs ." % part2_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part3 is: %.3fs ." % part3_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part4 is: %.3fs ." % part4_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part5 is: %.3fs ." % part5_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part6 is: %.3fs ." % part6_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part7 is: %.3fs ." % part7_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part8 is: %.3fs ." % part8_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part9 is: %.3fs ." % part9_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part10 is: %.3fs ." % part10_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part11 is: %.3fs ." % part11_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part12 is: %.3fs ." % part12_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part13 is: %.3fs ." % part13_total_duration)

                    
