from  dbmanager import pf_dbmanager

class Singleton(object):  
    def __new__(cls, *args, **kw):  
        if not hasattr(cls, '_instance'):  
            orig = super(Singleton, cls)  
            cls._instance = orig.__new__(cls, *args, **kw)
            cls._instance.mDBManager = pf_dbmanager.PFDBManager()
            cls._instance.mDBManager.startDB()
        return cls._instance    

class PFCollectionManager(Singleton):
    cache_cursors = None
    cache_data = {}
    cache_data_map = {}
    
    #Override
    def __init__(self):
        #self.mDBManager = pf_dbmanager.PFDBManager()
        #self.mDBManager.startDB()
        pass
    
    #__xx__() means protected method.
    def __buildUid__(self,  uid):
        return {PFCollectionManager.final_getUidLabel():  uid}
    
    #override
    def __buildDocUser__(self,  chunleiId,  cuid,  imei = ''):
        userMap = {}
        userMap[PFCollectionManager.final_getUidLabel()] = chunleiId
        userMap[PFCollectionManager.final_getCUidLabel()] = cuid
        userMap[PFCollectionManager.final_getIMEILabel()] = imei
        return userMap
    
    def final_get_linked_tag(self, id_map):
        #Get tag of linked foreign key, by _id of current collection;
        #Example: for device_collection doc, get tag list of apps. 
        i = 0
        #c = self.__getDocByUid__(uid)
        #if c is None or c.count() == 0:
        #    return None
        #uidData = next(c.__iter__())
        linked_node_map = PFCollectionManager.final_get_linked_node_by_cur(id_map)
        if linked_node_map is None:
            return None
        tag_list = []
        for linked_node_name in linked_node_map:
            linked_collection_name = linked_node_map[linked_node_name]['linked_collection']
            linkded_key_list = linked_node_map[linked_node_name]['linked_list']
            tg_list = self.__get_tag_from_collection__(linked_collection_name, linkded_key_list)
            tag_list.extend(tg_list)
            i += 1
        return tag_list
        
    
    #override
    def __getCollectionName__(self,  params = None): 
        return None
        
    #Override 
    def isDocExist(self,  uid,  collection = None):
        if uid is None:
            return None
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        uidMap = self.__buildUid__(uid)
        c = self.getDocWithCondition(uidMap,  collection, None)
        return c
    
    #override    
    def getAllDoc(self,  collection = None):
        return self.getDocWithCondition({}, collection, None)
    
    #override
    def getMetaInfo(self, dicMap):
        '''
        resultMap like:
        {
            '_id' : xxx,
            'cuid': xxxxxx,
            'imei': xxxxxx
        }
        '''
        resultMap = {}
        resultMap[PFCollectionManager.final_getUidLabel()] = dicMap.get(PFCollectionManager.final_getUidLabel())
        resultMap[PFCollectionManager.final_getCUidLabel()] = dicMap.get(PFCollectionManager.final_getCUidLabel())
        resultMap[PFCollectionManager.final_getIMEILabel()] = dicMap.get(PFCollectionManager.final_getIMEILabel())
        
        return resultMap
    
    def getTagsByCur(self,  cur):
        if cur is None or cur.get(PFCollectionManager.final_getProfileTagLabel()) is None:
            return []
        return cur.get(PFCollectionManager.final_getProfileTagLabel())
    
    #May be it need override, because some collection is too big to copy it all.
    def final_getTagsByUidWithCache(self,  uid):
        #Called by collection points to this collection;
        #Example: device collection calls this function of app collection to get tag list of app.
        #uid is the doc _id of app collection. 
        target_tag_list = []
        target_tag_map = {}
        cursors = None     
        uid_tag_list = self.__class__.cache_data.get(uid)
        if uid_tag_list is None:
            if self.__class__.cache_cursors == None:
                cursors = self.getAllDoc()
                if cursors is None:
                    return []
            else:
                cursors = self.__class__.cache_cursors
            self.__class__.cache_cursors = cursors  
            for cur in cursors:                
                _id = cur[self.__class__.final_getUidLabel()]
                #Whatever is useful or not, we copy all _id-tag_list map into cache data, means back up all. 
                self.__class__.cache_data[_id] = cur.get(self.__class__.final_getProfileTagLabel())
                if _id == uid:
                    if cur.get(self.__class__.final_getProfileTagLabel()) is not None:
                        target_tag_list = cur.get(self.__class__.final_getProfileTagLabel())
                        for tg in target_tag_list:   
                            target_tag_map[tg[self.__class__.final_getUidLabel()]] = 1
                    break
            #Whatever found it or not, we save uid into __cached_data, and not found will return empty [].
            self.__class__.cache_data[uid] = target_tag_list
            self.__class__.cache_data_map[uid] = target_tag_map
        else:
            target_tag_list = uid_tag_list
            target_tag_map = self.__class__.cache_data_map.get(uid)
        
        return target_tag_list

    def drop_table(self, collection = None):
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        self.mDBManager.drop_collection(collection)
    
    def getDocWithCondition(self, spec, collection = None, fields = None):
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        c = self.mDBManager.find(spec,  collection, fields)
        if c.count() > 0:
            return c
        else:
            return None   
        return c
    
    @staticmethod
    def final_getProfileTagLabel():
        return 'profile_tags'
    
    @staticmethod
    def final_getUidLabel():
        return '_id'
    
    @staticmethod
    def final_getCUidLabel():
        return 'cuid'
    
    @staticmethod
    def final_getIMEILabel():
        return 'imei'
    
    @staticmethod
    def final_get_label_linkde_node():
        return 'profile_linked_node'
    
    @staticmethod           
    def __get_tag_from_collection__(collection_name, key_list):
        return []
    
    @staticmethod   
    def final_get_linked_node_by_cur(cur):
        linked_node_map = cur.get(PFCollectionManager.final_get_label_linkde_node())
        if linked_node_map is None:
            return {}
        return linked_node_map
        
       
       
