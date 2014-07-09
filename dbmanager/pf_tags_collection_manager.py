import util.utils
import libs.util.logger
from tags.pf_tags import PFTags
import tags.pf_tag_helpers

from dbmanager.pf_collection_manager import PFCollectionManager

class PFTagsCollectionManager(PFCollectionManager):
    __COLLCETION_PREFIX = 'all_tags_collection'
    cache_cursors = None
    cache_data = {}
    
    #override              
    def __getCollectionName__(self,   params = None):
        return PFTagsCollectionManager.__COLLCETION_PREFIX
    
    def save(self, cur, collection = None): 
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        self.mDBManager.save(cur,  collection)
    
    def insertOrUpdateCollection(self, tag, collection = None):
        if collection is None:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())
        if len(tag.getType()) == 0:
            tag.setType('raw')
        if len(tag.getUpdateTime()) == 0:
            tag.setUpdateTime(util.utils.getStrCurrentTime())
        if len(tag.getCreateTime()) == 0:
            tag.setCreateTime(util.utils.getStrCurrentTime())
        
        m = PFTagsCollectionManager.final_buildTagMap(tag, False)
        self.mDBManager.save(m,  collection)
        
    @staticmethod
    def final_SlimTag(tagMap):
        d = {}
        try:
            d[PFTagsCollectionManager.getNameLabel()] = tagMap[PFTagsCollectionManager.getNameLabel()]
            d[PFTagsCollectionManager.getTagIdLabel()] = tagMap[PFTagsCollectionManager.getTagIdLabel()]
            d[PFTagsCollectionManager.getUniqueNameLabel()] = tagMap.get(PFTagsCollectionManager.getUniqueNameLabel())
            d[PFTagsCollectionManager.getCategoryLabel()] = tagMap[PFTagsCollectionManager.getCategoryLabel()]
            d[PFTagsCollectionManager.getTypeLabel()] = tagMap[PFTagsCollectionManager.getTypeLabel()]
        except NameError:
            libs.util.logger.Logger().getInstance.debugLog("Some tag is not correct!" + str(tagMap[PFTagsCollectionManager.getTagIdLabel()]))
        return d
        
    @staticmethod
    def final_convertCursor(curs):
        tagLst = []
        if curs is not None:
            for cur in curs:
                tagLst.append(cur)
        return tagLst
        
    @staticmethod
    def final_convert2Tag(tagMap):
        #Wrap the tag with tagCategory.
        tag = tags.pf_tag_helpers.PFTagsHelper.final_getTagClass(tagMap[PFTagsCollectionManager.getCategoryLabel()], tagMap[PFTagsCollectionManager.getUniqueNameLabel()])
        tagCategory = tags.pf_tag_helpers.PFTagsHelper.final_getTagCategoryInstance(tagMap[PFTagsCollectionManager.getCategoryLabel()], tag)
        tag.setId(tagMap[PFTagsCollectionManager.getTagIdLabel()])
        #tag.setUpdateTime(tagMap[PFTagsCollectionManager.getUpdateTimeLabel()])
        #tag.setCreateTime(tagMap[PFTagsCollectionManager.getCreateTimeLabel()])
        tag.setCategory(tagMap[PFTagsCollectionManager.getCategoryLabel()])
        tag.setUniqueName(tagMap.get(PFTagsCollectionManager.getUniqueNameLabel()))
        tag.setName(tagMap[PFTagsCollectionManager.getNameLabel()])
        tag.setType(tagMap[PFTagsCollectionManager.getTypeLabel()])
        return tagCategory


    @staticmethod    
    def final_buildTagMap(tg, convertId = True):
        m = {PFCollectionManager.final_getUidLabel() : tg.getCategory() + '_' + tg.getUniqueName(),
             PFTagsCollectionManager.getNameLabel():tg.getName(),  PFTagsCollectionManager.getCategoryLabel() :tg.getCategory() ,  PFTagsCollectionManager.getUniqueNameLabel() : tg.getUniqueName(),
                PFTagsCollectionManager.getTypeLabel():tg.getType() }
        if convertId == True:
            m[PFTagsCollectionManager.getTagIdLabel()] = tg.getId()
        return m
    #PFTagsCollectionManager.getCreateTimeLabel():tg.getCreateTime() ,  PFTagsCollectionManager.getUpdateTimeLabel() :tg.getUpdateTime(), 
            
    @staticmethod    
    def getTagIdLabel():
        return '_id'
    
    @staticmethod
    def getNameLabel():
        return 'name'
    
    @staticmethod
    def getUniqueNameLabel():
        return 'unique_name'
    
    @staticmethod
    def getTypeLabel():
        return 'type'
    
    @staticmethod
    def getCategoryLabel():
        return 'category'
    
    @staticmethod
    def getCreateTimeLabel():
        return 'first_update_date'
    
    @staticmethod
    def getUpdateTimeLabel():
        return 'last_update_date'    
 
    
if __name__ == "__main__":
    pass
    #mLst = [{'name':'512内存',  'type': 'raw',  'create_time': '2013-7-31 14:30:11', 'category': 'memory',  'unique_name':'512'}]
    #tagManager = dbmanager.pf_tags_collection_manager()
    #tagManager.mDBManager.update({'category': 'memory',  'unique_name':'512'},  mLst[0],  tagManager.mDBManager.getCollection(tagManager.__getCollectionName__()))
            
        
        
       
       
