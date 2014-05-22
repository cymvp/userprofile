from tags.pf_tags_account import *
from tags.pf_tags_net_stat import *
from tags.pf_tags_used_clock import *
from tags.pf_tags_usedapp import *
from tags.pf_tags import *

class PFTagsHelper:   
    
    __tag_cagetory_map = None
    
    @staticmethod
    def final_getTagClass(categoryName,  uniqueName):        
        try:
            a = eval(PFTags.CONST_TAG_PREFIX + str(categoryName) + '_' + str(uniqueName) + '()')
        except Exception as e:
            a = PFTags()
        return a
    
    @staticmethod    
    def final_returnTagCount(categoryName):
        try:
            a = eval(PFTags.CONST_TAG_PREFIX + str(categoryName))
            return a.getTagCount()
        except NameError:
            return PFTags_category.getTagCount()
    
    @staticmethod
    def final_getTagCategoryInstance(category, tag):
        a = None
        try:
            a = eval(PFTags.CONST_TAG_PREFIX + str(category) + '()')
        except NameError:
            a = PFTags_category()
        a.mTag = tag
        return a
    
    ''' 
    @staticmethod
    def final_getTagLstByCategory(tagLst,  categoryName):
        resultLst = []
        for tag in tagLst:
            if tag.getCategory() == categoryName:
                resultLst.append(tag)
        return resultLst
    '''  
    @staticmethod
    def final_getTagLstByCategory(tagLst,  categoryName):
        if PFTagsHelper.__tag_cagetory_map is None:
            PFTagsHelper.final_set_tag_category_map(tagLst)
        resultLst = PFTagsHelper.__tag_cagetory_map.get(categoryName)
        if resultLst is None:
            resultLst = [] 
        return resultLst
      
    @staticmethod
    def final_set_tag_category_map(tagLst):
        if PFTagsHelper.__tag_cagetory_map is not None:
            return PFTagsHelper.__tag_cagetory_map
        category_map = {}
        for tag in tagLst:
            categoryName = tag.getCategory()
            if  category_map.get(categoryName) is None:
                category_map[categoryName] = []
            category_map[categoryName].append(tag)
        PFTagsHelper.__tag_cagetory_map = category_map
        return category_map
    
    #@staticmethod
    #def final_get_tag_category_map():
    #    return PFTagsHelper.__tag_cagetory_map

    

