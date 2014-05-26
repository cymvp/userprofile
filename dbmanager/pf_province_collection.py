from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager

class PFProvinceCollectionManager(PFCollectionManager):
    __PROFILE_COLLCETION_PREFIX = 'profile_province_collection'
    cache_cursors = None
    cache_data = {}
    
    @staticmethod
    def final_getProfileTagLabel():
        return 'profile_tags'
    
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
   
    def __buildDocUser__(self,  _id):
        userMap = {}
        userMap[PFProvinceCollectionManager.final_getUidLabel()] = _id
        return userMap

    def merge_new_data_map(self, cur, _id, value_map):
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
       
       
