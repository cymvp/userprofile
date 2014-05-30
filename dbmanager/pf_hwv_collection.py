from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager

class PFHWVCollectionManager(PFCollectionManager):
    __PROFILE_COLLCETION_PREFIX = 'profile_hwv_collection'
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
        return PFHWVCollectionManager.__PROFILE_COLLCETION_PREFIX
    
    #override           
    def __getCollectionName__(self,   params = None):
        return PFHWVCollectionManager.__PROFILE_COLLCETION_PREFIX
    
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
            valueMap.pop(PFHWVCollectionManager.final_getUidLabel())
        else:
            #update
            tmpLst = []
            userMap = c.__getitem__(0)
            for statName in valueMap: 
                    userMap[statName] = valueMap[statName]
            self.mDBManager.update(self.__buildUid__(_id),  userMap,  collection)  
            
   
    def __buildDocUser__(self,  _id):
        userMap = {}
        userMap[PFHWVCollectionManager.final_getUidLabel()] = _id
        return userMap
    
        
    #override
    #def getMetaInfo(self, dicMap):
        '''
        resultMap is:
        {
            ''foreign_key_list': [
            'C8812',
            ]
        }
        '''
        #resultMap = {}
        #resultMap['forgien_key_list'] = [dicMap.get(PFMetricCollectionManager.final_getLabelModel()),]
        #return resultMap
        
       
       
