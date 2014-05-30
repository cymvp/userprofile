import datetime
import tags.pf_tag_helpers
import util.pf_exception

CONST_METRIC_HANDLER_PREFIX = 'PFMetricHandler_'


class PFMetricHandler_Appid_Metricid:
    ''' update sequence: 
            redis -> metric_collection-> metric data of profile -> tags of profile.
    '''
    __PROFILE_METRIC_LABEL = ''
    
   
    
    def __init__(self):
        a = self.__class__.__name__
        if a.count('_') < 2:
            raise util.pf_exception.PFExceptionFormat
    
    #override
    def convertData(self,  index,  value):
        '''{field_1:"value" }'''
        #timestamp
        if index == 8:
            t = datetime.datetime.fromtimestamp(int(value))
            return str(t)
        return value
    
    #@override
    #metric_data_lst is the old data list already existing in metrics_collection;
    #value_map is the new data list, which generates from raw data pool.
    def handle_raw_data(self, metric_data_lst, value_map):
        metric_data_lst.append(value_map)
        return metric_data_lst
        
    #override    
    def calculateProfile(self,  lines,  tops = 3): 
        '''result is :
        locations: [
            { locaton: '五道口', lat: '', lng: '', method: 'gps', time: '', ip: '151.23.21.109'},
            { locaton: '望京', lat: '', lng: '', method: 'network', time: '', ip: '151.23.21.109'},
            { locaton: '上地', lat: '', lng: '', method: 'network', time: '', ip: '151.23.21.109'}
        ],
        return None means this metric handler don't  need update metric info of profile.
        
        lines must be metric data of metric collection; it is an array:
            [
                {'timestamp':123456, 'strdesc': 10}, 
                {'timestamp':876543, 'strdesc': 11}
            ]
        '''
        return None
    
    def calculateTag(self,  lines,  tagLst):
        
        #if PFMetricHandler_Appid_Metricid.__tag_cagetory_map is None:
        #    PFMetricHandler_Appid_Metricid.__tag_cagetory_map = PFMetricHandler_Appid_Metricid.final_set_tag_category_map(tagLst)
        
        '''lines must be metric area of profile collection; it is  an array:
        [
            {'timestamp':123456, 'strdesc': 10}, 
            {'timestamp':876543, 'strdesc': 11}
        ]
        calculate fit tag through 
        return None means this metric handler don't  need update metric info of profile.
        return is:
        [
            {'tagid:': 1231231, 'category': memory, ...}
            {}
        ]
        one metric data may convert to several tags, so result return a array of tags.
        '''
        return None
        
    def __getProfileMetricLabel__(self,  prefix):
        ''' return prefix_4096_1807 '''
        a = self.__class__.__name__
        i = a.find('_')
        return prefix + a[i:]
    
    def get_profile_metric_label(self):
        return self.__getProfileMetricLabel__(self.__class__.__PROFILE_METRIC_LABEL__)
    
    @staticmethod 
    def final_getAppIdMetricId(s):
        arr = s.split('_')
        if len(arr) < 3:
            return None
        return (arr[-2],  arr[-1])
    
    @staticmethod 
    def final_getAppIdMetricId(s):
        arr = s.split('_')
        if len(arr) < 3:
            return None
        return (arr[-2],  arr[-1])
    
    
class PFMetricHandler_4096_1807(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'app_used'
    
    def calculateProfile(self,  lines,  tops = 3):
        '''
        lines is:
        [
          {
            com_android_mms:{'timestamp': '2014-05-14 08:14:09', 'collect_date': '20140513', 'launch_count': '1', 'duration': '5', 
 'packagename': 'com.android.mms', 'upload_count' : 3,
            },
            com_xxx_xxx:{},
          } #Only one element in lines for performance.
        ] 
        '''
        countMap = {}
        tempMap = {}
        appLst = []
        if len(lines[0]) == 0:
            return None
        dataMap = lines[0]
        for packageName in dataMap:
            #todo: should not use 'packagename' directly.
            #packageName = dataMap['packagename']
            launch_count = int(dataMap[packageName]['launch_count'])
            duration = int(dataMap[packageName]['duration'])
            upload_count = int(dataMap[packageName]['upload_count'])
            
            countMap[packageName] = launch_count

            tempMap[packageName] = {'packagename':  packageName,  'launch_count': launch_count / upload_count,  'duration': duration / upload_count, 'upload_count' : upload_count}

        #sortedTuple is : [('com.android.mms', 4), ('com.android.m', 1)] 
        #countMap is {'com.android.mms': 4, 'com.android.m':1}      
        #pass an iteratable object countMap.items() to sorted function, and for each element of this iterable object,  get the second element of each element.
        sortedTuple = sorted(countMap.items(),  key=lambda d:d[1], reverse = True)
        #print(tops)
        #print(countMap)
        i = 0
        if tops > len(sortedTuple):
            tops = len(sortedTuple)
        while i <= tops - 1:
            appLst.append(tempMap[sortedTuple[i][0]])
            i += 1
        #print(appLst)
        return {self.__getProfileMetricLabel__('app_used'):appLst}
    
    def calculateTag(self,  lines,  tagLst):
        
        return None
        #don't calculate tag using thie metricid.
        '''
        resultLst = []
        if lines is None or len(lines) == 0:
            return None
        categoryName = 'usedapp'
        l = len(lines)
        if l > 3:
            l = 3
        i = 0
        while i < l:
            line = lines[i]
            packagename = line['packagename']
            tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, categoryName)
            for tg in tgs:
                if tg.isFitTag(packagename) :
                    resultLst.append(tg)
            i += 1
                
        return resultLst
        '''
    #override
    def handle_raw_data(self, metric_data_lst, value_map):
        '''
        metric_data_lst is:
        [
          {
            com_android_mms:{'timestamp': '2014-05-14 08:14:09', 'collect_date': '20140513', 'launch_count': '1', 'duration': '5', 
 'packagename': 'com.android.mms'
            },
            com_xxx_xxx:{},
          }
        ] 
        value_map is:
        {'timestamp': '2014-05-14 08:14:09', 'launch_count': '1', 'collect_date': '20140513', 'duration': '5', 
 'packagename': 'com.android.mms'
        }
        '''
        #collect_date is latest date.
        #timestamp is useless.
        #For performance.
        package_name = value_map.get('packagename')
        package_name = util.utils.format_2_mongo_key_(package_name)
        if len(metric_data_lst) == 0:
            metric_data_lst.append({})
        if metric_data_lst[0].get(package_name) is None:
            metric_data_lst[0][package_name] = value_map
            value_map['upload_count'] = 1
        else:
            metric_data_lst[0][package_name]['duration'] = int(metric_data_lst[0][package_name]['duration']) + int(value_map['duration'])
            metric_data_lst[0][package_name]['launch_count'] = int(metric_data_lst[0][package_name]['launch_count']) + int(value_map['launch_count'])
            metric_data_lst[0][package_name]['upload_count'] += 1
            if metric_data_lst[0][package_name]['collect_date'] <  value_map['collect_date']:
                metric_data_lst[0][package_name]['collect_date'] = value_map['collect_date']
        return metric_data_lst

#Handle Model
class PFMetricHandler_1_1(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'deviceinfo'
    
    def calculateProfile(self,  lines,  tops = 3):
        #Never called now.
        return None
        #raise util.pf_exception.PFExceptionWrongStatus
        #dataMap = lines[-1]
        #return {self.__getProfileMetricLabel__('deviceinfo'): [dataMap]}
    
    def handle_raw_data(self, metric_data_lst, value_map):
        v = next(value_map.values().__iter__())
        v = util.utils.format_2_mongo_key_(v)
        if len(metric_data_lst) == 0:
            metric_data_lst.append({v:1})
        else:
            if metric_data_lst[0].get(v) is None:
                metric_data_lst[0][v] = 1
            else:
                metric_data_lst[0][v] += 1
        return metric_data_lst

    def calculateTag(self,  lines,  tagLst):
        super(PFMetricHandler_1_1,  self).calculateTag(lines,  tagLst)
        '''
         model_1_1: [
            "ZTE N807"
        ]
        '''
        resultLst = []
        str_model = lines[-1]
        category_name = 'model'
        tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, category_name)
        for tg in tgs:
            if tg.isFitTag(str_model) :
                resultLst.append(tg)    
        return resultLst   
    
    
#Handle Province
class PFMetricHandler_1_2(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'province'
    
    def calculateProfile(self,  lines,  tops = 3):
        #Never called now.
        return None
        #dataMap = lines[-1]
        #return {self.__getProfileMetricLabel__('province'): [dataMap]}
    
    def handle_raw_data(self, metric_data_lst, value_map):
        v = next(value_map.values().__iter__())
        v = util.utils.format_2_mongo_key_(v)
        if len(metric_data_lst) == 0:
            metric_data_lst.append({v:1})
        else:
            if metric_data_lst[0].get(v) is None:
                metric_data_lst[0][v] = 1
            else:
                metric_data_lst[0][v] += 1
        return metric_data_lst

    def calculateTag(self,  lines,  tagLst):
        super(PFMetricHandler_1_2,  self).calculateTag(lines,  tagLst)
        '''
         province_1_2: [
            "beijing"
        ]
        '''
        resultLst = []
        str_model = lines[-1]
        category_name = 'province'
        tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, category_name)
        for tg in tgs:
            if tg.isFitTag(str_model) :
                resultLst.append(tg)    
        return resultLst  
    
           
#Handle App
class PFMetricHandler_1_3(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'package_name'
    
    def calculateProfile(self,  lines,  tops = 3):
        #Never called now.
        return None
        #dataMap = lines[-1]
        #return {self.__getProfileMetricLabel__('package_name'): [dataMap]}
    
    def handle_raw_data(self, metric_data_lst, value_map):
        #means Should unique by v.
        package_name = value_map.get('package_name')
        launch_count = value_map.get('launch_count')
        package_name = util.utils.format_2_mongo_key_(package_name)
        if len(metric_data_lst) == 0:
            metric_data_lst.append({})

        if metric_data_lst[0].get(package_name) is None:
            metric_data_lst[0][package_name] = int(launch_count)
        else:
            metric_data_lst[0][package_name] += int(launch_count)
            
        return metric_data_lst

    def calculateTag(self,  lines,  tagLst):
        super(PFMetricHandler_1_3,  self).calculateTag(lines,  tagLst)
        '''
         package_name_1_3: [
            "com.baidu.BaiduMap"
        ]
        '''   
        return None   
 
#Handle deviceid(chunleiid)   
class PFMetricHandler_1_4(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'device'
    
    def calculateProfile(self,  lines,  tops = 3):
        return None
    
    def handle_raw_data(self, metric_data_lst, value_map):
        v = next(value_map.values().__iter__())
        v = util.utils.format_2_mongo_key_(v)
        if len(metric_data_lst) == 0:
            metric_data_lst.append({v:1})
        else:
            if metric_data_lst[0].get(v) is None:
                metric_data_lst[0][v] = 1
            else:
                metric_data_lst[0][v] += 1
        return metric_data_lst

    def calculateTag(self,  lines,  tagLst):
        return None    

#Handle App Tags.
class PFMetricHandler_2_1(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'package_name'

    def calculateTag(self,  lines,  tagLst):
        resultLst = []
        super(PFMetricHandler_2_1,  self).calculateTag(lines,  tagLst)
        '''
         package_name_1_3: [
            "com.baidu.BaiduMap"
        ]
        '''
        if len(lines) < 0:
            return None  
        info_map = lines[-1]
        
        for k in info_map:
            category_name = None
            #k应该是tag的category_name,而value应该是具体的unique_name.
            if k == 'app_type':
                category_name = k
                tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, category_name)
                for tg in tgs:
                    if tg.isFitTag(info_map[k]) :
                        resultLst.append(tg)    
            elif k == info_map['app_type'] + '_' + 'category':
                category_name = k
            #elif k =='category':
                #category_name = info_map['type'] + '_' + k #'game_category or app_category'
                tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, category_name)
                for unique_name in info_map[k]:
                    for tg in tgs:
                        if tg.isFitTag(unique_name) :
                            resultLst.append(tg)
            elif k == 'gender':
                category_name = k
                tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, category_name)
                for tg in tgs:
                    if tg.isFitTag(info_map[k]) :
                        resultLst.append(tg)  
        return resultLst  

           
class PFMetricHandler_4096_7d3(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'deviceinfo'
    
    def convertData(self,  index,  value):                   
        '''
        value:
        {
                RAM:''
                ROM:''
                resolution:''
                android_os: '4.1'
        }
        '''
        if index == 8:
            return super(PFMetricHandler_4096_7d3,  self).convertData(index,  value)
        if index == 13:
            infoMap = {}
            arr = value.split(';')
            infoMap['resolution'] = ','.join(arr[0].split(':')[:2])
            infoMap['RAM'] = arr[1]
            infoMap['ROM'] = arr[2]
            return infoMap
        return value
    
    def calculateProfile(self,  lines,  tops = 3):
        dataMap = lines[-1]
        '''
        "deviceinfo_4096_7d3" : {
            [
                {
                    RAM:''
                    ROM:''
                    resolution:''
                    android_os: '4.1'
                },
            ]
            } 
        '''
        return {self.__getProfileMetricLabel__('deviceinfo'): [dataMap]}

    def calculateTag(self,  lines,  tagLst):
        super(PFMetricHandler_4096_7d3,  self).calculateTag(lines,  tagLst)
        resultLst = []
        deviceInfoMap = lines[-1]
        deviceInfoMap = deviceInfoMap['deviceinfo']
        for k in deviceInfoMap:
            if k == 'RAM':
                categoryName = 'memory'
                tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, categoryName)
                for tg in tgs:
                    if tg.isFitTag(deviceInfoMap[k]) :
                        resultLst.append(tg)    
        return resultLst
        
class PFMetricHandler_4096_7d5(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'network_usage'
    
    def calculateProfile(self,  lines,  tops = 3):
        tempMap = {}
        for dataMap in lines:
            t = dataMap['timestamp']
            if tempMap.get(t) is None:
                tempMap[t] = dataMap
        sortedTuple = sorted(tempMap.items(),  key=lambda d:d[0], reverse = True)
        #dataMap = lines[-1]
        return {self.__getProfileMetricLabel__('network_usage'): [sortedTuple[0][1]]}   
    def calculateTag(self,  lines,  tagLst):
        #just use the first metric data, because there is only one item in profile metric collection..
        super(PFMetricHandler_4096_7d5,  self).calculateTag(lines,  tagLst)
        resultLst = []
        line = lines[0]
        wifi = line['wifi']
        gprs = line['gprs']
        categoryName = 'wifistat'
        tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, categoryName)
       
        for tg in tgs:
            if tg.isFitTag(wifi) :
                resultLst.append(tg)
                
        categoryName = 'gprsstat'
        tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, categoryName)
        for tg in tgs:
            if tg.isFitTag(gprs) :
                resultLst.append(tg)
                
        return resultLst
        
class PFMetricHandler_4096_7d2(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'account'
    
    def calculateProfile(self,  lines,  tops = 3):
        '''
        lines:
        [
                {'timestamp':123456, 'account_list': com.baidu:account1;com.tencent.mm:account2}, 
                {'timestamp':876543, 'account_list': com.baidu:account1;com.tencent.mm:account2}
        ]
        '''
        # old:resultMap is '{com.baidu: account1, 'com.tencent.mm': account2, the same format of accountMap}.
        #
        resultMap = {}
        for dataMap in lines:
            accountStr = dataMap['account_list']
            accountStr = accountStr.replace('.',  '_')
            accountMap = self.__splitAccountList(accountStr)
            for account_tuple in accountMap:
                account_type, account_name = account_tuple
                accountId = account_type + '_' + account_name
                if resultMap.get(accountId) is None:
                    resultMap[accountId] = {self.__getProfileMetricLabel__('account'):[]}
                resultMap[accountId][self.__getProfileMetricLabel__('account')].append({'account_type' : account_type, 'account_name' : account_name})
                
        return resultMap
        #dataMap = lines[-1]
        #return {self.__getProfileMetricLabel__('accounts'):resultMap}   
        
    def calculateTag(self,  lines,  tagLst):
        super(PFMetricHandler_4096_7d2,  self).calculateTag(lines,  tagLst)
        resultLst = []
        categoryName = 'account'
        tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, categoryName)
        #for app in lines:
        #    for tg in tgs:
        #        if tg.isFitTag(app) :
        #            resultLst.append(tg)
        for accountInfo in lines:
            for tg in tgs:
                if tg.isFitTag(accountInfo['account_type']):
                    resultLst.append(tg)
                        
        return resultLst
        
    def __splitAccountList(self,  line):
        ''' line is:
            ''com.baidu:account1;com.tencent.mm:account2"
           resultMap is:
            {''com.baidu": account1, 'com.tencent.mm': account2}
        '''
        resultMap = {}
        arr = line.split(';')
        for element in arr:
            account_info = element.split(':')
            if len(account_info) >= 2:
                app, account = account_info[0:2]
                resultMap[(app, account)] = account
        return resultMap
        
        
class PFMetricHandler_4096_7d1(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'device_usage_time_by_day'
    
    def calculateProfile(self,  lines,  tops = 3):
        tempMap = {}
        hourCountMap = {}
        resultLst = []
        
        for dataMap in lines:
            t = dataMap['hour']
            if hourCountMap.get(t) is None:
                hourCountMap[t] = 1
            else:
                hourCountMap[t] += 1
                
            if tempMap.get(t) is None:
                tempMap[t] = int(dataMap['duration'])
            else:
                tempMap[t] += int(dataMap['duration'])
        
        
        
        for t in tempMap:
            tempMap[t] = int(tempMap[t]) / int(hourCountMap[t])
            resultLst.append({str(t) : tempMap[t]})

        
        
        resultLst = sorted(resultLst,  key=lambda d:d[next(d.__iter__())], reverse = True)
            
        #print(resultLst)    
        '''
                resultLst is sorted by duration;
                device_usage_time_by_day: [
                {00: '30'},  {01: '30'},  {02: '30'},  {03: '30'},
                {04: '1'},  {05: '30'},  {06: '30'},  {07: '30'},
                {08: '1'},  {09: '30'},  {10: '30'},  {11: '30'},
                {12: '1'},  {13: '30'},  {14: '30'},  {15: '30'},
                {16: '1'},  {17: '30'},  {18: '30'},  {19: '30'},
                {20: '1'},  {21: '30'},  {22: '30'},  {23: '30'}
                ]
            '''
        return {self.__getProfileMetricLabel__('device_usage_time_by_day'): resultLst}   
        
    def calculateTag(self,  lines,  tagLst):
        super(PFMetricHandler_4096_7d1,  self).calculateTag(lines,  tagLst)
        resultLst = []
        if lines is None or len(lines) == 0:
            return None
        categoryName = 'useclock'
        l = len(lines)
        if l > 3:
            l = 3
        i = 0
        tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, categoryName)
        while i < l:
            line = lines[i]
            t = next(line.__iter__())
            for tg in tgs:
                if tg.isFitTag(t) :
                    resultLst.append(tg)
            i += 1
                
        return resultLst
        
class PFMetricHandler_4096_640(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'content_query'
    
    def calculateProfile(self,  lines,  tops = 3):
        contentCountMap = {}
        resultLst = []
        
        for dataMap in lines:
            content = dataMap['query_content']
            
            if contentCountMap.get(content) is None:
                contentCountMap[content] = 1
            else:
                contentCountMap[content] += 1
            
        
        for content in contentCountMap:
            resultLst.append({content : contentCountMap[content]})
            
        resultLst = sorted(resultLst,  key=lambda d:d[next(d.__iter__())], reverse = True)
        '''
                resultLst is sorted by duration;
                device_usage_time_by_day: [
                {00: '30'},  {01: '30'},  {02: '30'},  {03: '30'},
                {04: '1'},  {05: '30'},  {06: '30'},  {07: '30'},
                {08: '1'},  {09: '30'},  {10: '30'},  {11: '30'},
                {12: '1'},  {13: '30'},  {14: '30'},  {15: '30'},
                {16: '1'},  {17: '30'},  {18: '30'},  {19: '30'},
                {20: '1'},  {21: '30'},  {22: '30'},  {23: '30'}
                ]
            '''
        return {self.__getProfileMetricLabel__('content_query'): resultLst}   

#Handle channel & version of operation 
class PFMetricHandler_36864_1808(PFMetricHandler_Appid_Metricid):
    
    def calculateProfile(self,  lines,  tops = 3):
        '''
        map.items(): {x:y, z:z1, m:n} => [{x:y}, {z:z1}, {m:n}]
        map.items() also like sortedTuple: a list which elements are map. Each k-v pair in map is the element in list.
        sortedTuple is as: [{}, {}]   
        '''
        result_lst = sorted(lines, key=lambda d:d['timestamp'], reverse = True)

        return {self.__getProfileMetricLabel__('operation'): result_lst}  
    
    def handle_raw_data(self, metric_data_lst, value_map):
        for line in metric_data_lst:
            if line.get('version') == value_map.get('version'):
                return metric_data_lst
        metric_data_lst.append(value_map) 
        return metric_data_lst

    def calculateTag(self,  lines,  tagLst):
        super(PFMetricHandler_36864_1808,  self).calculateTag(lines,  tagLst)
        resultLst = []
        if lines is None or len(lines) == 0:
            return None
        line = lines[-1]
        
        categoryName = 'operationversion'
        version = line.get('version')
        tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, categoryName)
        for tg in tgs:
            if tg.isFitTag(version):
                resultLst.append(tg)
        categoryName = 'operationchannel'
        version = line.get('channel')
        tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, categoryName)
        for tg in tgs:
            if tg.isFitTag(version):
                resultLst.append(tg)
        return resultLst       
        
        
