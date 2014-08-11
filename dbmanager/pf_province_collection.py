import pymongo
import traceback
import util
import libs.util.logger
from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager

class PFProvinceCollectionManager(PFCollectionManager):
    __PROFILE_COLLCETION_PREFIX = 'profile_province_collection'
    cache_cursors = None
    cache_data = {}
    stat_doc = None
    
    @staticmethod
    def final_getLabel_model():
        return 'model'
   
    @staticmethod
    def getCollectionName():
        return PFProvinceCollectionManager.__PROFILE_COLLCETION_PREFIX
    
    #override           
    def __getCollectionName__(self,   params = None):
        return PFProvinceCollectionManager.__PROFILE_COLLCETION_PREFIX
    
    '''
    def insertOrUpdateCollection(self,  _id,  valueMap,  collection = None):
        #_id is model.
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        #check whether this document existed, checked by chunleiid.
        c = self.isDocExist(_id)
        
        if c is None:
            #insert.
            userMap = self.__buildDocUser__(_id)
            #Because len(userMap) is 1.
            valueMap[next(userMap.__iter__())] = userMap[next(userMap.__iter__())]
            self.mDBManager.insert(valueMap,  collection)
            valueMap.pop(PFCollectionManager.final_getUidLabel())
        else:
            #update
            tmpLst = []
            userMap = c.__getitem__(0)
            for statName in valueMap: 
                    userMap[statName] = valueMap[statName]
            self.mDBManager.update(self.__buildUid__(_id),  userMap,  collection)  
    '''        
   
    def __set_stat_doc(self, data_map, first_update_date, last_update_date):
        label_stat = PFCollectionManager.final_get_stat_label()
        label_first_date = PFCollectionManager.final_get_stat_first_date_label()
        label_last_date = PFCollectionManager.final_get_stat_last_date_label()
        if data_map.get(label_stat) is None:
            data_map[label_stat] = {}
        if data_map[label_stat].get(label_first_date) is None or data_map[label_stat][label_first_date] > first_update_date:
            data_map[label_stat][label_first_date] = first_update_date
        if data_map[label_stat].get(label_last_date) is None or data_map[label_stat][label_last_date] < last_update_date:
            data_map[label_stat][label_last_date] = last_update_date
        return data_map 
   
    def __buildDocUser__(self,  _id):
        userMap = {}
        userMap[PFProvinceCollectionManager.final_getUidLabel()] = _id
        return userMap

    def merge_new_data_map(self, cur, _id, str_start_day, str_end_day, value_map):
        isInsert = 0
        data_map = {}
        if cur is None:
            data_map = self.__buildDocUser__(_id)
            isInsert = 1
        else:
            #update
            data_map = cur
        '''For each of stat map: 
                {
                  statName1: [
                    {'launch_count': 8, 'packagename': com.baidu.map, 'duration': 20}, 
                    {}, 
                    {}
                  ], 
                  statName2: [],
                  .......
                } 
        ''' 
        
        data_map = self.__set_stat_doc(data_map, str_start_day, str_end_day)  
            
        for statName in value_map:  #valueMap[chunleiId]
            data_map[statName] = value_map[statName]
        return (data_map, isInsert)
    
    def insertOrUpdateCollection(self, cur, is_insert, collection = None): 
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        if is_insert == 1:
            self.mDBManager.insert(cur,  collection)
        else:
            self.mDBManager.update(self.__buildUid__(cur[PFCollectionManager.final_getUidLabel()]),  cur,  collection)
       
    def __get_stat_doc(self):
        #初始时;
        _id = 'stat'
        if PFProvinceCollectionManager.stat_doc is None:     
            c = self.isDocExist(_id)
            #不存在,则创建一个,并赋值给stat_doc
            if c is None or c.count() == 0:
                return None
            else:     
                PFProvinceCollectionManager.stat_doc = next(c.__iter__())        
        return PFProvinceCollectionManager.stat_doc 
    
    def get_stat_busy(self):
        m = self.__get_stat_doc()
        if m is None:
            return None
        return m['is_working']

    def set_stat_busy(self, is_working):
        m = self.__get_stat_doc()
        if m is None:
            return None
        m['is_working'] = is_working
        self.__update_stat_doc(m)
    
    def get_stat_update_date(self):
        m = self.__get_stat_doc()
        if m is None:
            return None
        return m['last_update_date']
    
    def set_stat_update_date(self, update_date):
        m = self.__get_stat_doc()
        if m is None:
            return None        
        m['last_update_date'] = update_date
        self.__update_stat_doc(m)
    
    def get_stat_first_date(self):
        m = self.__get_stat_doc()
        if m is None:
            return None
        return m['first_update_date']
    
    def set_stat_first_date(self, first_date):
        m = self.__get_stat_doc()
        if m is None:
            return None        
        m['first_update_date'] = first_date
        self.__update_stat_doc(m)
        
    def create_stat_doc(self, m):
        self.__update_stat_doc(m)
    
    def __update_stat_doc(self, m):
        try:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
            self.mDBManager.save(m, collection)
            PFProvinceCollectionManager.stat_doc = m
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            libs.util.logger.Logger.getInstance().errorLog(traceback.format_exc())
            libs.util.logger.Logger.getInstance().errorLog('!!!! %s' % e)
            raise util.pf_exception.PFExceptionWrongStatus   
