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
            return t.strftime("%Y%m%d-%H:%M:%S")
        return value
    
    #@override
    #metric_data_lst is the old data list already existing in metrics_collection;
    #value_map is the new data list, which generates from raw data pool.
    def handle_raw_data(self, metric_data_lst, value_map):
        metric_data_lst.append(value_map)
        return metric_data_lst
        
    #override    
    def calculateProfile(self,  lines, old_lines = None, param = None): 
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
        a = self.__class__.__name__
        i = a.find('_')
        return prefix + a[i:]
    
    def get_profile_metric_label(self):
        return self.__getProfileMetricLabel__(self.__class__.__PROFILE_METRIC_LABEL__)
    
    def split_data_by_date(self,str_start_day, str_end_day, lines):
        return lines
    
    def final_split_data_by_date(self, str_start_day, str_end_day, lines):
        #lines is all data of this metric, stored in metrics_collection.
        result_lines = [{}]
        if lines is None or len(lines) == 0:
            return lines
        for key_ in lines[0]:
            result_map = {}
            for str_date in lines[0][key_]:
                if str_date >= str_start_day and str_date <= str_end_day:
                    result_map[str_date] = lines[0][key_][str_date]
            if len(result_map) > 0:
                result_lines[0][key_] = result_map
        if len(result_lines[0]) <= 0:
            result_lines = None
        return result_lines
    
    '''
                        在handle_raw_data()中调用, 只能对时间相关的metric_data上调用;类似于如下格式:
        metric_data_lst:
        [
          {
            com_android_mms:{
                20140624:{'timestamp': '2014-05-14 08:14:09', 'launch_count': '1', 'duration': '5'}
                20140623:{'timestamp': '2014-05-14 08:14:09', 'launch_count': '1', 'duration': '5'}
            },
            com_xxx_xxx:{},
          }
        ] 
    '''
    '''
             在metrics_collection表里有两种metric data的数据格式:
      1 时间相关的metric data,这种metric类似上面的数据格式; 这种格式的数据,时间由时间戳生成;
      2 时间无关的metric data,这种metric没有固定的格式;
                       这种数据一般在device_collection等profile表里没有历史累积的需求(如duration, launch_count, wifi流量等), 所以按日计算profile表时,不需要精确到天, 所有不需要按日期来存储原始数据.
                       如果这种数据在metrics_collection表中有重复导入相同天的情况, 也不会影响profile表数据的生成,因为不需要在profile表里的累加(其实也有相同问题, 比如model按出现次数进行权值累加,影响profile表外键的生成,但是不需太多考虑,因为一般uid的model基本固定,且都会进行外键计算).
                      这种数据一般为如下两种:
        (1) 外键metric_data,如model, chunleiid, province;
        (2) 普通metric,如0x7d2这种帐户数据.
                     一般这种数据, 依靠metrics_collection表的总doc来保证数据不重复导入,也就是在总doc里会记录最近导入的数据的日期.
        
    '''
    def final_consist_metric_data(self, metric_data_lst, new_raw_value_map, new_merge_value_map, str_key_name, str_date_name = None):
        should_remove_timestamp = False
        if str_date_name == None or str_date_name == 'timestamp':
            str_date = new_raw_value_map.get('timestamp')[0:8]
            should_remove_timestamp = True
        else:
            str_date = new_raw_value_map.get(str_date_name)
        
        if util.utils.DateController.get_instance(None).is_valid_date(str_date) == False:
            return metric_data_lst
            
        str_key = new_raw_value_map.get(str_key_name)
        
        #This means this metric only one key, so we just use the str_key_name as the key name.
        if str_key is None:
            str_key = str_key_name
        
        if len(metric_data_lst) == 0:
            metric_data_lst.append({})
        if metric_data_lst[0].get(str_key) is None:
            metric_data_lst[0][str_key] = {}
        if metric_data_lst[0][str_key].get(str_date) is None:
            data_map = new_merge_value_map
            #Because timestamp is as the key, so we remove timestamp data for cut down disk space.
            if should_remove_timestamp == True and data_map.get('timestamp') is not None:
                data_map.pop('timestamp')
            metric_data_lst[0][str_key][str_date] = data_map
               
        return metric_data_lst
    
    @staticmethod 
    def final_getAppIdMetricId(s):
        arr = s.split('_')
        if len(arr) < 3:
            return None
        return (arr[-2],  arr[-1])
    
    @staticmethod
    def final_consist_data_by_key(raw_data_map_by_date, old_data_map, value_name_list):
        min_str_date = '20300101'
        max_str_date = '19700101'
        new_total_upload_count = 0
        
        
        new_data_map = None
        
        if old_data_map == None:
            new_data_map = {'first_upload_date':min_str_date, 'last_upload_date': max_str_date, 'total_upload_count': 0}
            for i in range(len(value_name_list)):
                new_data_map['avg_' + value_name_list[i]] = 0
        else:
            new_data_map = old_data_map
                
        old_last_upload_date = new_data_map.get('last_upload_date')
        old_first_upload_date = new_data_map.get('first_upload_date')
        value_list = []  
        for str_date in raw_data_map_by_date:
            if str_date >= old_first_upload_date and str_date <= old_last_upload_date:
                continue
            if min_str_date > str_date:
                min_str_date = str_date
            if max_str_date < str_date:
                max_str_date = str_date
            new_total_upload_count += 1
            
            for i in range(len(value_name_list)):
                if len(value_list) <= i:
                    value_list.append(int(raw_data_map_by_date[str_date].get(value_name_list[i])))
                else:
                    value_list[i] += int(raw_data_map_by_date[str_date].get(value_name_list[i]))
        
        #Means no fit date for calculating.
        if new_total_upload_count <= 0:
            return old_data_map
                    
        if max_str_date > old_last_upload_date:
            new_data_map['last_upload_date'] = max_str_date
        if min_str_date < old_first_upload_date:
            new_data_map['first_upload_date'] = min_str_date
            
        for i in range(len(value_name_list)):
            avg_value_name = 'avg_' + value_name_list[i]
            new_data_map[avg_value_name] = int((new_data_map[avg_value_name] * new_data_map['total_upload_count'] + value_list[i]) / (new_total_upload_count + old_data_map['total_upload_count']))
        
        new_data_map['total_upload_count'] += new_total_upload_count    
        return new_data_map

        
class PFMetricHandler_4096_1807(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'app_used'
    
    #override
    def split_data_by_date(self,str_start_day, str_end_day, lines):
        return self.final_split_data_by_date(str_start_day, str_end_day, lines)
    
    def calculateProfile(self,  lines, old_lines = None, param = None):
        
        '''
        lines is:
        [
          {
            com_android_mms:{
                20140624:{'timestamp': '2014-05-14 08:14:09', 'launch_count': '1', 'duration': '5'}
                20140623:{'timestamp': '2014-05-14 08:14:09', 'launch_count': '1', 'duration': '5'}
            }
            com_xxx_xxx:{},
          } #Only one element in lines for performance.
        ]
        old_lines is:
        [ 
          {
            com_android_mms:{first_upload_date:'20140624', 'last_upload_date':'20140624', total_upload_count = '100', avg_launch_count = '10', "avg_duration" : 20, is_important = '1'}
            com_xxx_xxx:{}
          } #Only one element in old_lines for performance.
        ]    
        '''
        #countMap = {}
        tempMap = {}
        appLst = [{}]
        if lines is None or len(lines[0]) == 0:
            return old_lines
        dataMap = lines[0]
        
        for packageName in dataMap:
            
            if old_lines == None:
                old_app_data = None
            else:
                old_app_data = old_lines[0].get(packageName)
            
            new_data_map = self.final_consist_data_by_key(dataMap[packageName], old_app_data, ['launch_count', 'duration'])
            
            if new_data_map is None or len(new_data_map) == 0:
                continue
                
            tempMap[packageName] = new_data_map
            
            import_flag = 0
            for str_date in dataMap[packageName]:
                import_flag  = int(dataMap[packageName][str_date].get('is_important'))
                if import_flag is None or import_flag < 1:
                    import_flag = 0
                else:
                    break
            tempMap[packageName]['is_important'] = import_flag
            tempMap[packageName]['weight'] = tempMap[packageName]['is_important'] * 100000 + tempMap[packageName]['avg_launch_count']
            
        #pass an iteratable object countMap.items() to sorted function, and for each element of this iterable object,  get the second element of each element.  
        result_lst = sorted(tempMap.items() , key=lambda d:d[1]['weight'], reverse = True)

        i = 0
        tops = 50
        if tops > len(result_lst):
            tops = len(result_lst)
        while i <= tops - 1:
            appLst[0][result_lst[i][0]] = tempMap[result_lst[i][0]]
            i += 1
       
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
        def __consist_profile_data(value_map):
            result_map = {}
            result_map['timestamp'] = value_map['timestamp']
            result_map['launch_count'] = value_map['launch_count']
            result_map['duration'] = value_map['duration']
            
            try:
                if len(value_map['is_important']) == 0:
                    result_map['is_important'] = '0'
                else:
                    result_map['is_important'] = value_map['is_important']
            except:
                result_map['is_important'] = value_map['is_important']
            
            return result_map
        
        '''
        metric_data_lst is(as metric_data in metrics_collection, only one element which is a map):
        [
          {
            com_android_mms:{
                20140624:{'timestamp': '2014-05-14 08:14:09', 'launch_count': '1', 'duration': '5'}
                20140623:{'timestamp': '2014-05-14 08:14:09', 'launch_count': '1', 'duration': '5'}
            },
            com_xxx_xxx:{},
          }
        ] 
        value_map is:
        {'timestamp': '2014-05-14 08:14:09', 'launch_count': '1', 'collect_date': '20140513', 'duration': '5', 
 'packagename': 'com.android.mms'
        }
        '''
        data_map = __consist_profile_data(value_map)
        
        return self.final_consist_metric_data(metric_data_lst, value_map, data_map, 'packagename', 'collect_date')  
        
        #collect_date is latest date.
        #timestamp is useless.
        #For performance.
        '''
        package_name = value_map.get('packagename')
        str_date = value_map.get('collect_date')
        package_name = util.utils.format_2_mongo_key_(package_name)
        if len(metric_data_lst) == 0:
            metric_data_lst.append({})
        if metric_data_lst[0].get(package_name) is None:
            metric_data_lst[0][package_name] = {}
            #value_map['upload_count'] = 1
        if metric_data_lst[0][package_name].get(str_date) is None:
            data_map = __consist_profile_data(value_map)
            metric_data_lst[0][package_name][str_date] = data_map
            
        return metric_data_lst
        '''

#Handle Model
class PFMetricHandler_1_1(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'deviceinfo'
    
    def calculateProfile(self,  lines, old_lines = None, param = None):
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
            if tg.isFitTag(str_model):
                resultLst.append(tg)    
        return resultLst   
    
    
#Handle Province
class PFMetricHandler_1_2(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'province'
    
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
#Invoked by app_collection.
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
            return super(PFMetricHandler_4096_7d3,  self).convertData(index, value)
        if index == 13:
            infoMap = {}
            arr = value.split(';')
            infoMap['resolution'] = ','.join(arr[0].split(':')[:2])
            infoMap['RAM'] = arr[1]
            infoMap['ROM'] = arr[2]
            return infoMap
        return value
    
    def calculateProfile(self,  lines, old_lines = None, param = None):
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
            if k == 'resolution':
                categoryName = k
                xy_dot = deviceInfoMap[k].split(',')
                if xy_dot is None or len(xy_dot) == 2:
                    x_dot, y_dot = xy_dot
                    tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, categoryName)
                    for tg in tgs:
                        if tg.isFitTag((x_dot, y_dot)) :
                            resultLst.append(tg)
        return resultLst
        
class PFMetricHandler_4096_7d5(PFMetricHandler_Appid_Metricid):
    
    __PROFILE_METRIC_LABEL__ = 'network_usage'
    
    #override
    def handle_raw_data(self, metric_data_lst, value_map):
        return self.final_consist_metric_data(metric_data_lst, value_map, value_map, PFMetricHandler_4096_7d5.__PROFILE_METRIC_LABEL__)  
    
    #override
    def split_data_by_date(self, str_start_day, str_end_day, lines):
        #根据日期段,选出在metrics_collection表里的metrics.metric_data数组里,符合要求的数据; lines为: metrics.metric_data数组.
        return self.final_split_data_by_date(str_start_day, str_end_day, lines)
    
    def calculateProfile(self,  lines, old_lines = None, param = None):
        if lines is None or len(lines) == 0:
            return old_lines
        lines = lines[0]
        
        if old_lines == None or len(old_lines) == 0:
            old_data_map = None
        else:
            old_data_map = old_lines[0]
        
        for str_single_label in lines:
            
            single_data_map = lines[str_single_label]
            
            sorted_list_by_date = sorted(single_data_map.items(),  key=lambda d:d[0][6:8], reverse = True)
            
            has_update = False
            
            #选出给定时间段内的metric数据中(如20140628-20140702), 最接近月末的那一天(按day排序,且大于28号), 且该月没有做过计算(由last_upload_date保证);
            #一般不会跨月进行数据计算,如20140628-20140702,如果跨月,也会分月进行计算,也就是先读201406，再读201407;
            for i in range(len(sorted_list_by_date)):
                str_date = sorted_list_by_date[i][0]
                data_map = sorted_list_by_date[i][1]
                if str_date[6:8] >= '28':
                    if data_map.get('gprs') is None or int(data_map.get('gprs')) < 0 or data_map.get('wifi') is None or int(data_map.get('wifi')) < 0:
                        continue
                    if old_data_map == None:
                        old_data_map = {'first_upload_date':str_date, 'last_upload_date': '19700101', 'total_upload_count': 0, 'avg_gprs': 0, 'avg_wifi': 0}
                    if str_date[0:6] > old_data_map['last_upload_date'][0:6]:
                        old_data_map['last_upload_date'] = str_date 
                        old_data_map['avg_gprs'] = int((old_data_map['avg_gprs'] * old_data_map['total_upload_count'] + int(data_map['gprs'])) / (1 + old_data_map['total_upload_count']))
                        old_data_map['avg_wifi'] = int((old_data_map['avg_wifi'] * old_data_map['total_upload_count'] + int(data_map['wifi'])) / (1 + old_data_map['total_upload_count']))
                        old_data_map['total_upload_count'] += 1 
                        break
                    
        if old_data_map is None or len(old_data_map) <= 0:
            return None 
        else:
            return {self.__getProfileMetricLabel__('network_usage'): [old_data_map]}  
        
    def calculateTag(self,  lines,  tagLst):
        #just use the first metric data, because there is only one item in profile metric collection..
        super(PFMetricHandler_4096_7d5,  self).calculateTag(lines,  tagLst)
        resultLst = []
        if len(lines) <= 0:
            return resultLst
        line = lines[0]
        wifi = line['avg_wifi']
        gprs = line['avg_gprs']
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
    
    def calculateProfile(self,  lines, old_lines = None, param = None):
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
    
    #override
    def split_data_by_date(self, str_start_day, str_end_day, lines):
        #根据日期段,选出在metrics_collection表里的metrics.metric_data数组里,符合要求的数据; lines为: metrics.metric_data数组.
        return self.final_split_data_by_date(str_start_day, str_end_day, lines)
    
    #override
    def handle_raw_data(self, metric_data_lst, value_map):
        def __consist_profile_data(value_map):
            result_map = {}
            result_map['duration'] = value_map['duration']
            return result_map
        
        new_merge_value_map = __consist_profile_data(value_map)
        return self.final_consist_metric_data(metric_data_lst, value_map, new_merge_value_map, 'hour')  
    
    #
    def calculateProfile(self,  lines, old_lines = None, param = None):
           
        tempMap = {}
        if lines is None or len(lines[0]) == 0:
            return old_lines
        dataMap = lines[0]
        
        for str_hour in dataMap:
            
            if old_lines == None:
                old_app_data = None
            else:
                old_app_data = old_lines[0].get(str_hour)
            
            new_data_map = self.final_consist_data_by_key(dataMap[str_hour], old_app_data, ['duration'])
                        
            if new_data_map is None or len(new_data_map) == 0:
                continue
            
            tempMap[str_hour] = new_data_map
              
        #result_lst = sorted(tempMap.items() , key=lambda d:d[1]['avg_duration'], reverse = True)
        '''
        i = 0
        tops = 50
        if tops > len(result_lst):
            tops = len(result_lst)
        while i <= tops - 1:
            appLst[0][result_lst[i][0]] = tempMap[result_lst[i][0]]
            i += 1
        '''
        return {self.__getProfileMetricLabel__(self.__PROFILE_METRIC_LABEL__):[tempMap]}
        
 
    def calculateTag(self,  lines,  tagLst):
        super(PFMetricHandler_4096_7d1,  self).calculateTag(lines,  tagLst)
        resultLst = []
        if lines is None or len(lines) == 0:
            return None
        lines = lines[0]
        categoryName = 'useclock'
        l = len(lines)
        if l > 3:
            l = 3
        #lines is a map, which key is hour.    
        lines = sorted(lines.items() , key=lambda d:d[1]['avg_duration'], reverse = True)
        
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
    
    def calculateProfile(self,  lines, old_lines = None, param = None):
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
    
    __PROFILE_METRIC_LABEL__ = 'operation'
    
    #override
    def handle_raw_data(self, metric_data_lst, value_map):
        return self.final_consist_metric_data(metric_data_lst, value_map, value_map, 'channel')  
    
    #override
    def split_data_by_date(self, str_start_day, str_end_day, lines):
        #根据日期段,选出在metrics_collection表里的metrics.metric_data数组里,符合要求的数据; lines为: metrics.metric_data数组.
        return self.final_split_data_by_date(str_start_day, str_end_day, lines)
    
    def calculateProfile(self,  lines, old_lines = None, param = None):
        tempMap = {}
        if lines is None or len(lines[0]) == 0:
            return old_lines
        dataMap = lines[0]
        
        '''
        map.items(): {x:y, z:z1, m:n} => [{x:y}, {z:z1}, {m:n}]
        map.items() also like sortedTuple: a list which elements are map. Each k-v pair in map is the element in list.
        sortedTuple is as: [{}, {}]   
        '''
        for str_channel in dataMap:
            
            if old_lines == None:
                old_app_data = None
            else:
                old_app_data = old_lines[0].get(str_channel)
            
            new_data_map = self.final_consist_data_by_key(dataMap[str_channel], old_app_data, [])   
            
            if new_data_map is None or len(new_data_map) == 0:
                continue
            
            tempMap[str_channel]  = new_data_map
            
            for str_date in dataMap[str_channel]:
                str_version  = dataMap[str_channel][str_date].get('version')
                tempMap[str_channel]['version'] = str_version 
                break
            
        return {self.__getProfileMetricLabel__('operation'):[tempMap]}

    def calculateTag(self,  lines,  tagLst):
        super(PFMetricHandler_36864_1808,  self).calculateTag(lines,  tagLst)
        resultLst = []
        if lines is None or len(lines) == 0:
            return None
        lines = lines[0]
        
        #lines is a map, which key is hour.    
        sorted_list = sorted(lines.items() , key=lambda d:d[1]['last_upload_date'], reverse = True)
        
        if len(sorted_list) <= 0:
            return None
        else:
            data_map = sorted_list[0][1]
            str_channel = sorted_list[0][0]
        ''' {'total_upload_count': 1, 'version': 'null', 'first_upload_date': '20140604', 'last_upload_date': 
 '20140604'}
        '''
        categoryName = 'operationversion'
        version = data_map.get('version')
        tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, categoryName)
        for tg in tgs:
            if tg.isFitTag(version):
                resultLst.append(tg)
        categoryName = 'operationchannel'
        tgs = tags.pf_tag_helpers.PFTagsHelper.final_getTagLstByCategory(tagLst, categoryName)
        for tg in tgs:
            if tg.isFitTag(str_channel):
                resultLst.append(tg)
        return resultLst       
        
        
