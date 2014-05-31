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
import libs.redis.redis_manager

import libs.util.logger
import config.const

#Fix me. Should read from config file.
metricCollector_4096_1807 =  {12: ('packagename',)}
metricCollector_4096_7d3 =  {13: ('deviceinfo',)}

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

def __convert_appid_metricid(arr, appid, metricid):
    arr[1] = appid
    arr[0] = metricid


if __name__ == "__main__":
    
    libs.util.logger.Logger.getInstance().setLogFilePrefixName('')
    libs.util.logger.Logger.getInstance().setLogFileSurfixName('pickmetric')

    fInputFile = None
     
    metricCollectionManager = PFMetricCollectionManager()
    g_hwvCollectionManager = PFHWVCollectionManager()
    
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
    
    g_last_uid = ''
    g_last_imei = ''
    g_last_cuid = ''
    g_last_metric_data_list = []
        
    while gCount > 0:
        
        total_line += 1
        
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
                line = redisManager.pop()
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
                
        #Parse raw data.
        tupl = handle_data(arr,  metricMap)   
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
            __convert_appid_metricid(arr, '1', '0x1')
            tupl = handle_data(arr,  metricMap)
            if tupl is not None:
                appid_metricid, valueMap = tupl
                app_id = appid_metricid[0]
                metric_id = appid_metricid[1]
                appid_lst.append(app_id)
                metricid_lst.append(metric_id)
                value_map_lst.append(valueMap)
            #province
            __convert_appid_metricid(arr, '1', '0x2')
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
                __convert_appid_metricid(arr, '1', '0x3')
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
                __convert_appid_metricid(arr, '1', '0x4')
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
    
            # g_last_uid is empty only if the uid is the first uid..
            #if g_last_uid == '':
            #    g_last_uid = uid
                #g_last_imei = imei
                #g_last_cuid = cuid
                #g_last_metric_data_list = []
            
            if g_last_uid == '':
                g_last_uid = uid
            
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
                        metricCollectionManager.insertOrUpdateUser(g_last_uid,  g_last_cuid,  g_last_imei,  g_last_metric_data_list)
                    except util.pf_exception.PFExceptionWrongStatus:
                        libs.util.logger.Logger.getInstance().errorLog('mongodb happened some error, wait reconnect......')
                        time.sleep(3)
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
                    