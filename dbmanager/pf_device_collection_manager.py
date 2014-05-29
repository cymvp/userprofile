from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_app_collection import PFAPPCollectionManager
from dbmanager.pf_hwv_collection import PFHWVCollectionManager
from dbmanager.pf_province_collection import PFProvinceCollectionManager
        
class PFDeviceCollectionManager(PFCollectionManager):
    __PROFILE_COLLCETION_PREFIX = 'profile_device_collection'
    cache_cursors = None
    cache_data = {}
    
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
    
    
    def merge_new_data_map(self, cur, chunleiId,  cuid,  imei, value_map):
        isInsert = 0
        data_map = {}
        if cur is None:
            data_map = self.__buildDocUser__(chunleiId,  cuid,  imei)
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
            tg_list = collection_manager.__final_getTagsByUidWithCache__(k)
            for tg in tg_list:
                tag_id = tg.get(collection_manager.final_getUidLabel())
                if tag_id_map.get(tag_id) is None:
                    tag_list.append(tg)
                    tag_id_map[tag_id] = 1
        return tag_list