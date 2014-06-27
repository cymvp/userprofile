from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager

class PFAPPCollectionManager(PFCollectionManager):
    __PROFILE_COLLCETION_PREFIX = 'profile_app_collection'
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
        return PFAPPCollectionManager.__PROFILE_COLLCETION_PREFIX
    
    #override           
    def __getCollectionName__(self,   params = None):
        return PFAPPCollectionManager.__PROFILE_COLLCETION_PREFIX
    
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
         
   
    def __buildDocUser__(self,  _id):
        userMap = {}
        userMap[PFAPPCollectionManager.final_getUidLabel()] = _id
        return userMap

    
        
       
       
