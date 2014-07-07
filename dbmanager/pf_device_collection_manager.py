import pymongo
import traceback
import libs.util.logger
import util.pf_exception
from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_app_collection import PFAPPCollectionManager
from dbmanager.pf_hwv_collection import PFHWVCollectionManager
from dbmanager.pf_province_collection import PFProvinceCollectionManager
        
class PFDeviceCollectionManager(PFCollectionManager):
    __PROFILE_COLLCETION_PREFIX = 'test_profile_device_collection'
    cache_cursors = None
    cache_data = {}
    stat_doc = None
    
    @staticmethod
    def final_getProfileTagLabel():
        return 'profile_tags'
    
    @staticmethod
    def final_getUserGeneralInfoLabel():
        return 'user_general_info'
    
                  
    def __getCollectionName__(self,   params = None):
        return PFDeviceCollectionManager.__PROFILE_COLLCETION_PREFIX
    
    @staticmethod
    def getCollectionName():
        return PFDeviceCollectionManager.__PROFILE_COLLCETION_PREFIX
    
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
    
    def merge_new_data_map(self, cur, chunleiId,  cuid, imei, str_start_day, str_end_day, value_map):
        isInsert = 0
        data_map = {}
        if cur is None:
            data_map = self.__buildDocUser__(chunleiId,  cuid,  imei)
            isInsert = 1
        else:
            #update
            data_map = cur
        
        data_map = self.__set_stat_doc(data_map, str_start_day, str_end_day)
        
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
        for statName in value_map:  #valueMap[chunleiId]
            data_map[statName] = value_map[statName]
        return (data_map, isInsert)
            
    def insertOrUpdateUser(self, cur, is_insert, collection = None): 
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        if is_insert == 1:
            self.mDBManager.insert(cur,  collection)
        else:
            self.mDBManager.update(self.__buildUid__(cur[PFCollectionManager.final_getUidLabel()]),  cur,  collection)
    
    
    def insertOrUpdateUser_old(self,  chunleiId,  cuid,  imei,  valueMap,  collection = None):
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        #check whether this document existed, checked by chunleiid.
        c = self.isDocExist(chunleiId)
        userMap = {}
        isInsert = 0
        if c is None:
            #insert.
            userMap = self.__buildDocUser__(chunleiId,  cuid,  imei)
            isInsert = 1
        else:
            #update
            userMap = c.__getitem__(0)
         
                    
        for statName in valueMap:  #valueMap[chunleiId]
    
    
            #update the whole metric data area, so just use '='.
            userMap[statName] = valueMap[statName]
        
        if isInsert == 1:
            self.mDBManager.insert(userMap,  collection)
        else:
            self.mDBManager.update(self.__buildUid__(chunleiId),  userMap,  collection)
    
            
    @staticmethod         
    def __get_tag_from_collection__(collection_name, key_list):
        collection_manager = None
        tag_list = []
        tag_id_map = {}
        if collection_name == PFHWVCollectionManager.getCollectionName():
            collection_manager = PFHWVCollectionManager()
        if collection_name == PFProvinceCollectionManager.getCollectionName():
            collection_manager = PFProvinceCollectionManager()
        if collection_name == PFAPPCollectionManager.getCollectionName():
            collection_manager = PFAPPCollectionManager()
        if collection_name == PFDeviceCollectionManager.getCollectionName():
            collection_manager = PFDeviceCollectionManager()
        for k in key_list:
            tg_list = collection_manager.final_getTagsByUidWithCache(k)
            for tg in tg_list:
                tag_id = tg.get(collection_manager.final_getUidLabel())
                if tag_id_map.get(tag_id) is None:
                    tag_list.append(tg)
                    tag_id_map[tag_id] = 1
        return tag_list
    
    def __get_stat_doc(self):
        #初始时;
        _id = 'stat'
        if PFDeviceCollectionManager.stat_doc is None:     
            c = self.isDocExist(_id)
            #不存在,则创建一个,并赋值给stat_doc
            if c is None or c.count() == 0:
                return None
            else:     
                PFDeviceCollectionManager.stat_doc = next(c.__iter__())        
        return PFDeviceCollectionManager.stat_doc 
    
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
            PFDeviceCollectionManager.stat_doc = m
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            libs.util.logger.Logger.getInstance().errorLog(traceback.format_exc())
            libs.util.logger.Logger.getInstance().errorLog('!!!! %s' % e)
            raise util.pf_exception.PFExceptionWrongStatus