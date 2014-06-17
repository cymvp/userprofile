import dbmanager.pf_tags_collection_manager
from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager
import util.pf_exception

class PFTagUidsCollectionManager(PFCollectionManager):
    __COLLCETION_PREFIX = 'tag_uids_collection'
    cache_cursors = None
    cache_data = {}
    
    #override              
    def __getCollectionName__(self,   params = None):
        return PFTagUidsCollectionManager.__COLLCETION_PREFIX
        
    @staticmethod    
    def final_getUidLstLabel():
        return 'uids'
    
    @staticmethod    
    def final_getTagCategoryLabel():
        return 'tag_category'
    
    @staticmethod    
    def final_getTagUniqueNameLabel():
        return 'tag_unique_name'
    
    @staticmethod    
    def final_getTagDisplayNameLabel():
        return 'tag_name'  
    
    @staticmethod    
    def final_getPartitionLabel():
        return 'partition'
    
    '''
    def removeUid(self, tagMap, uid, collection = None):
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        tagManager = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager   
        category = tagMap[tagManager.getCategoryLabel()]
        uniqueName = tagMap.get(tagManager.getUniqueNameLabel())
        curs = self.__getDocByTag(category, uniqueName)
        if curs is None:
            return
        for cur in curs:
            uidLst = cur[PFTagUidsCollectionManager.final_getUidLstLabel()]
            if uidLst.__contains__(uid):
                uidLst.remove(uid) 
                self.mDBManager.update(PFTagUidsCollectionManager.__buildKey(category, uniqueName, cur[PFTagUidsCollectionManager.final_getPartitionLabel()]),  cur,  collection)
            
    
    def insertOrUpdateCollection(self, tagMap, uid, collection = None):
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        isInsert = False
        uidsMap = {}
        lastPatition = 1
        tagManager = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager    
        category = tagMap[tagManager.getCategoryLabel()]
        uniqueName = tagMap[tagManager.getUniqueNameLabel()]
        displayName = tagMap[tagManager.getNameLabel()]
        curs = self.__getDocByTag(category, uniqueName)
        if curs is None:
            isInsert = True
        else:
            cur, lastPatition = PFTagUidsCollectionManager.__getBestDoc(curs)
            if cur is None:
                isInsert = True
            else:        
                uidsMap = cur
        if isInsert == True:
            uidsMap = PFTagUidsCollectionManager.__buildDoc(category, uniqueName, displayName, lastPatition)
        
        if uidsMap[PFTagUidsCollectionManager.final_getUidLstLabel()].__contains__(uid) == False:
            uidsMap[PFTagUidsCollectionManager.final_getUidLstLabel()].append(uid)
        
        if isInsert == 1:
            self.mDBManager.insert(uidsMap,  collection)
        else:
            self.mDBManager.update(PFTagUidsCollectionManager.__buildKey(category, uniqueName),  uidsMap,  collection)
            
    @staticmethod
    def __buildKey(category, uniqueName, partition = 0):  
        query = {}
        query[PFTagUidsCollectionManager.final_getTagCategoryLabel()] = category
        query[PFTagUidsCollectionManager.final_getTagUniqueNameLabel()] = uniqueName
        if partition > 0:
            query[PFTagUidsCollectionManager.final_getPartitionLabel()] = partition
        return query
    
    def __getDocByTag(self,  category, uniqueName, collection = None): 
        query = PFTagUidsCollectionManager.__buildKey(category, uniqueName)
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())  
        curs = self.mDBManager.find(query,  collection)
        #print(dir(curs))
        if curs is None or curs.count() == 0:
            return  None
        else:
            return curs
    
    @staticmethod
    def __getBestDoc(curs):
        minLength = 5000000
        currentCur = None
        lastPartition = 1
        for cur in curs:
            uidLst = cur[PFTagUidsCollectionManager.final_getUidLstLabel()]
            if len(uidLst) < minLength:
                currentCur = cur
                minLength = len(uidLst)
            if cur[PFTagUidsCollectionManager.final_getPartitionLabel()] > lastPartition:
                lastPartition = cur[PFTagUidsCollectionManager.final_getPartitionLabel()]
        return (currentCur, lastPartition)
    '''
   
    def insert_tag_devices_list(self, tag_map, device_list, total_uid_count, current_page = -1):
        collection = self.mDBManager.getCollection(self.__getCollectionName__())
        tag_unique_name = tag_map[PFTagsCollectionManager.getUniqueNameLabel()]
        tag_category = tag_map[PFTagsCollectionManager.getCategoryLabel()]
        
        #split page.
        if len(device_list) != total_uid_count:  
            if current_page > 0:
                tag_id = tag_category + '_' + tag_unique_name + '(' + str(current_page) + ')'
            else:
                raise util.pf_exception.PFExceptionFormat()
        else:
            tag_id = tag_category + '_' + tag_unique_name 

                  
        c = self.isDocExist(tag_id)
        if c is None:
            #insert.
            tag_map = self.__buildDoc(tag_id, tag_category, tag_unique_name, tag_map[PFTagsCollectionManager.getNameLabel()], total_uid_count)
            tag_map[PFTagUidsCollectionManager.final_getUidLstLabel()].extend(device_list)
            self.mDBManager.insert(tag_map,  collection)
        else:
            #update
            tag_map = c.__getitem__(0)
            tag_map[PFTagUidsCollectionManager.final_getUidLstLabel()] = device_list
            tag_map['total_count'] = str(total_uid_count)
            self.mDBManager.update(self.__buildUid__(tag_id),  tag_map,  collection)  
   
    @staticmethod
    def __buildDoc(tag_id, category, uniqueName, displayName, total_uid_count):
        m = {}
        m[PFTagUidsCollectionManager.final_getUidLabel()] = tag_id
        m[PFTagUidsCollectionManager.final_getTagCategoryLabel()] = category
        m[PFTagUidsCollectionManager.final_getTagUniqueNameLabel()] = uniqueName
        m[PFTagUidsCollectionManager.final_getTagDisplayNameLabel()] = displayName
        #m[PFTagUidsCollectionManager.final_getPartitionLabel()] = lastPartition
        m[PFTagUidsCollectionManager.final_getUidLstLabel()] = []
        m['total_count'] = str(total_uid_count)
            
        return m
    
if __name__ == "__main__":
    mLst = [{'name':'512内存',  'type': 'raw',  'create_time': '2013-7-31 14:30:11', 'category': 'memory',  'unique_name':'512'}]
    tagManager = dbmanager.pf_tags_collection_manager()
    tagManager.mDBManager.update({'category': 'memory',  'unique_name':'512'},  mLst[0],  tagManager.mDBManager.getCollection(tagManager.__getCollectionName__()))
            
        
        
       
       
