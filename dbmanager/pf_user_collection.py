from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_device_collection_manager import PFDeviceCollectionManager

class PFUserCollectionManager(PFCollectionManager):
    __PROFILE_COLLCETION_PREFIX = 'profile_user_collection'
    cache_cursors = None
    cache_data = {}
    
    @staticmethod
    def final_getProfileTagLabel():
        return 'profile_tags'
    
    @staticmethod
    def final_getLabelAccountType():
        return 'account_type'
    
    @staticmethod
    def final_getLabelAccountName():
        return 'account_name'
    
    @staticmethod
    def final_getLabelDevices():
        return 'devices'
    
    #override           
    def __getCollectionName__(self,   params = None):
        return PFUserCollectionManager.__PROFILE_COLLCETION_PREFIX
    
    def insertOrUpdateCollectionDevice(self,  _id,  valueMap,  collection = None):
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        #check whether this document existed, checked by chunleiid.
        c = self.isDocExist(_id)
        
        if c is None:
            #insert.
            userMap = self.__buildDocUser__(_id)
            #userMap[PFUserCollectionManager.final_getLabelDevices()] = deviceIdLst
            valueMap[next(userMap.__iter__())] = userMap[next(userMap.__iter__())]
            self.mDBManager.insert(valueMap,  collection)
        else:
            #update
            tmpLst = []
            userMap = c.__getitem__(0)
            for statName in valueMap: 
                if statName == PFUserCollectionManager.final_getLabelDevices():
                    for deviceId in valueMap[PFUserCollectionManager.final_getLabelDevices()]:
                        if deviceId not in userMap[PFUserCollectionManager.final_getLabelDevices()]:
                            tmpLst.append(deviceId)
                    userMap[PFUserCollectionManager.final_getLabelDevices()].extend(tmpLst)
                else:
                    userMap[statName] = valueMap[statName]
            self.mDBManager.update(self.__buildUid__(_id),  userMap,  collection)  
            
    def updateCollectionTag(self,  _id,  tagMap,  collection = None):
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        #check whether this document existed, checked by chunleiid.
        c = self.isDocExist(_id)
        userMap = {}
        if c is None:
            pass
        else:
            #update
            userMap = c.__getitem__(0)
            userMap[PFUserCollectionManager.final_getProfileTagLabel()] = tagMap[PFUserCollectionManager.final_getProfileTagLabel()]
            self.mDBManager.update(self.__buildUid__(_id),  userMap,  collection)    
        
    #override
    def __buildDocUser__(self,  _id):
        userMap = {}
        userMap[PFUserCollectionManager.final_getUidLabel()] = _id
        return userMap
    
    def getTagsByAccountId(self,  accountId):     
        c = self.__getDocByUid__(accountId)
        
        uidData = next(c.__iter__())
        if uidData.get(PFUserCollectionManager.final_getProfileTagLabel()) is None:
            return []
        else:
            return uidData.get(PFUserCollectionManager.final_getProfileTagLabel())
            
                   
    @staticmethod           
    def __get_tag_from_collection__(collection_name, key_list):
            collection_manager = None
            tag_list = []
            if collection_name == PFDeviceCollectionManager.getCollectionName():
                collection_manager = PFDeviceCollectionManager()
            for k in key_list:
                tg_list = collection_manager.__final_getTagsByUidWithCache__(k)
                tag_list.extend(tg_list)
            return tag_list
    '''    
    def getTagsByUid(self,  uid):     
        c = self.__getDocByUid__(uid)
        if c is None or c.count() == 0:
            return []
        uidData = next(c.__iter__())
        if uidData.get(PFUserCollectionManager.final_getProfileTagLabel()) is None:
            return []
        else:
            return uidData.get(PFUserCollectionManager.final_getProfileTagLabel())
     '''   
       
       
