import datetime
import time
import libs.util.logger
import ubc.pf_metric_helper
import util.pf_exception
import dbmanager.pf_metric_collection_manager
import sys
from config.const import *
def loadMap(lines):
    """ 
    load mapping rules from ubc metrics to udw model
    {mtriecid:{index:(label in profile_device_collection,),....}
    rules is:
    { 
        (4096,801):{
            9:('location',), 
            10:('lat',)
        },  
        (4096,802):{
            9:('aaa',), 
            10:('bbb')
        }
    }
    """
    rules = {}
    for line in lines:
        cc = line.strip().split(',')
        mm = (cc[6],  )
        mid = int(cc[1], 16)   # a hex value
        appid = int(cc[0])
        col = int(cc[4])
        if rules.get((appid,mid)):
            rules[(appid, mid)][col] = mm 
        else:
            rules[(appid, mid)] = {col: mm }
    return rules 

def isValidArr(arr):
    if len(arr) != 38 or arr[0].startswith('0x') == False:
        libs.util.logger.Logger.getInstance().errorLog('error format data!')
        try:
            libs.util.logger.Logger.getInstance().debugLog(','.join(arr))
        except:
            libs.util.logger.Logger.getInstance().errorLog('error but no shown...')
        return None
    return arr

def isValidLine(line):
    arr = line.strip().split(',')
    return isValidArr(arr)

    
def getStrToday(): 
    return datetime.datetime.now().strftime('%Y%m%d')

def getStrCurrentTime():
    stamp = int(time.time())
    return str(datetime.datetime.fromtimestamp(stamp))

def getUid(line):
    return line.strip().split(',')[2]

def getAppId(line):
    return line.strip().split(',')[1]

def getIMEI(line):
    return line.strip().split(',')[3]

def getCUID(line):
    return line.strip().split(',')[31]

def getModel(line):
    return line.strip().split(',')[6]

def isStrEmpty(s):
    if s is None or len(s) == 0:
        return True
    else:
        return False
    
def calc_months(strStartDay,  nDays):
    if nDays >= 32: # can not calculate over 1 years, because if sM > dM, we should judge whether the year is same.
        raise util.pf_exception.PFExceptionFormat
    
    strTargetDay = libs.util.utils.getNextDate(strStartDay,  nDays,  0)
    libs.util.logger.Logger.getInstance().debugLog("First day is:" + strTargetDay)
    sM = int(strStartDay[4:6])
    dM = int(strTargetDay[4:6])
    month = []
    if sM < dM:
        while dM <= 12:
            t = strTargetDay[0:4] + '%02d' %  dM  + '01'
            month.append(t)
            dM += 1
        x = 1    
        while sM >= x:
            t = strStartDay[0:4] + '%02d' %  x  + '01'
            month.append(t)
            x += 1
        month[0] = strTargetDay
        month[-1] = strStartDay
    elif sM > dM:
        while sM >= dM:
            t = strStartDay[0:4] + '%02d' %  sM  + '01'
            month.append(t) 
            sM -= 1
        month[0] = strTargetDay
        month[-1] = strStartDay
    else:
        month.append(strTargetDay)
        month.append(strStartDay)
    return month
    
def getCursorLst(manager,  monthLst):
    cursorLst = []
    monthMap = {}
    for month in monthLst:
        if monthMap.get(month[:6]) is None:
            monthMap[month[:6]] = month[:6]
        else:
            continue
        curs = manager.getAllDoc(manager.final_getCollection(month))
        if curs is not None:
            cursorLst.append(curs)
    return cursorLst


def buildMetricData(manager,  cursorLst, appId_metricId_map, foreign_tuple_lst = None):
    '''userInfoMap is the map, which contains the cuid, imei, model of uid.
       uidMap is the map, which contains all metrics list of certain user.
    '''
    uidMap = {}
    userInfoMap = {}
    total_device_count = 0
    
    #part1_total_duration = 0
    #part2_total_duration = 0
    
    #recordTime = libs.util.my_utils.RecordTime()
    
    #recordTime.startTime()
    
    #cursorLst contains all cursor whch located in diffferent metric collections, seperated by month.
    for curs in cursorLst:
        # curs contains cursor which located in same metric collections.
        max_count = get_actural_count(curs.count(), PERFORMANCE_DEVICE_COUNT)
        for cur in curs:
            #part1_total_duration += recordTime.getEllaspedTimeSinceLast()
            total_device_count += 1
            if total_device_count % 100 == 0:
                libs.util.logger.Logger.getInstance().debugLog("Has handled %d device." % total_device_count)
                #sys.exit(1)
            #Fixed me!!!!!! temp test.
            if total_device_count >= max_count:
                break
            #part2_total_duration += recordTime.getEllaspedTimeSinceLast()

            uid = cur[dbmanager.pf_collection_manager.PFCollectionManager.final_getUidLabel()]
            
            # one uid -> one userInfo, which got from manager represent as certain collection.
            userInfoMap[uid] = manager.getMetaInfo(cur)
            
            if uidMap.get(uid) is None:
                uidMap[uid] = {}
            # metrics is a list.
            metrics = cur.get(dbmanager.pf_metric_collection_manager.PFMetricCollectionManager.final_getMetricsLabel())
            if metrics is None:
                metrics = []
            #metric is a map.
            for metric in metrics:
                metricId = metric[dbmanager.pf_metric_collection_manager.PFMetricCollectionManager.final_getMetricIdLabel()]
                appId = metric[dbmanager.pf_metric_collection_manager.PFMetricCollectionManager.final_getAppIdLabel()]
                
                if appId_metricId_map.get((int(appId),  int(metricId, 16))) is None:
                    continue
                
                #metricDataLst is a list.
                metricDataLst = metric.get(dbmanager.pf_metric_collection_manager.PFMetricCollectionManager.final_getMetricDataLabel())
                if metricDataLst is not None and len(metricDataLst) > 0:
                    if foreign_tuple_lst is not None and (appId,  metricId) in foreign_tuple_lst:
                        if userInfoMap[uid].get('foreign_key_list') is None:
                            userInfoMap[uid]['foreign_key_list'] = {}
                        '''forgien key must is this format: 
                            { metric_data: [
                                {"ZTE N807" : "xxx"
                                 "ZTE N808" : "xxx"
                                } #only one element in metric_data!
                              ],
                            }
                            metric_data must have only one map element; each key of map is the foreign key of referenced collection.
                            "ZTE N807" is foreign key, which is _id or doc key of referenced collection.
                            One uid may be referenced more than one foreign keys, such as package_name, so result is a list.
                        '''
                        
                        reference_data_map = metricDataLst[0]
                        sortedTuple = sorted(reference_data_map.items(),  key=lambda d:d[1], reverse = True)
                        i = 0
                        tops = 30
                        appLst = []
                        if tops > len(sortedTuple):
                            tops = len(sortedTuple)
                        while i <= tops - 1:
                            appLst.append(sortedTuple[i][0])
                            i += 1
                        #Fixed me. should get top 10 foreign key from list.
                        #Because some foreign key likd package name, will be much than expecting.
                        userInfoMap[uid]['foreign_key_list'][(appId,  metricId)] = list(appLst)
                    else:
                        # foreign data can not be inserted , because, it maybe contains more than one foreign keys.
                        # foreign data will be consisted as "model_1_1:[{}] after calc_profile() function later.
                                        
                        if uidMap[uid].get((appId,  metricId)) is None:
                            uidMap[uid][(appId,  metricId)] = []
                        uidMap[uid][(appId,  metricId)].extend(metricDataLst)
                        
    #uidMap is immidiate struct.
    # uidMap contains all uid metric data, key is uid, value is a map which key is (appid, metricid), value is array of metric data.
    ''' 
    {
        uid1:{
            (appid, metricid): [
                {'timestamp':123456, 'strdesc': 10}, 
                {'timestamp':876543, 'strdesc': 11}]}, 
            (appid, metricid):[
            ], 
        uid2:{
        }
        ......
    }    
    '''        
    
    #libs.util.logger.Logger.getInstance().debugLog("total num is: %s ." % total_device_count)
    #libs.util.logger.Logger.getInstance().debugLog("recordTime time is: %s ." % recordTime.getEllapsedTime()) 
    #libs.util.logger.Logger.getInstance().debugLog("total time of part1 is: %.3fs ." % part1_total_duration)
    #libs.util.logger.Logger.getInstance().debugLog("total time of part2 is: %.3fs ." % part2_total_duration)
    return (uidMap, userInfoMap)


def calc_profile(uidMap,  tops = 3): 
    '''
    uidMap is: 
    {
        uid1:{
            (appid, metricid): [
                {'timestamp':123456, 'strdesc': 10}, 
                {'timestamp':876543, 'strdesc': 11}], 
            (appid, metricid):[
            ], 
        uid2:{
        }
        ......
    }    
    '''        
       
    '''
    resultMap is:
    {uid1:{
            'location':[
                {location: xxx, 'lat':xxx, 'lon': xxx}, 
                {location: yyy, 'lat':yyy, 'lon': yyy}], 
                {}
            ],
            'applist':[
                {'name': xxx, 'duration': 10}
            ],
        }, 
        uid2:{
        }, 
        ......
      }'''
    resultMap = {}
    #Get each uid from metrics_collection immediate struct.
    for uid in uidMap:
        if resultMap.get(uid) is None:
            resultMap[uid] = {}
        #Get (appid, metricid) from user doc of metrics_collection immediate struct.
        for appIdMetricId in uidMap[uid]:
            #this is a tuple.
            appId,  metricId = appIdMetricId
            metricHandler = ubc.pf_metric_helper.getMetricHandler(metricId[2:],  appId)
        
            #uidMap[uid][appIdMetricId] is a list of certain metric data list.
            dataMap = metricHandler.calculateProfile(uidMap[uid][appIdMetricId])
            
            if dataMap is not None and len(dataMap) > 0:
                '''
                dataMap is like:
                {accounts: [ 
                    { account: '124345533@qq.com'},
                    { account: 'ddac@gmail.com'}
                    ]
                }
                '''
                #dataMap is a map which key is like 'location' and so on,  value is a list(array) of map which is calculated by some stat.
                #Overwrite k-v of user doc.
                resultMap[uid][next(dataMap.keys().__iter__())] = next(dataMap.values().__iter__())
                
                '''
                dataMap is:
                 deviceinfo_4096_7d3: [{
                            RAM:''
                            ROM:''
                            resolution:''
                            android_os: '4.1'
                        }],
                '''
    return resultMap

def format_2_mongo_key_(str_key):
    str_key = str_key.replace('.', '_')
    return str_key
    
