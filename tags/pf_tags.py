#from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager
#import dbmanager.pf_tags_collection_manager
####!!!!todo:  why can not I write 'from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager'!!!!!!
import util.pf_exception

class PFTags:
    CONST_TAG_PREFIX = 'PFTags_'
    def __init__(self):
        self.__mTagId = ""
        self.__mName = ""
        self.__mCategory = ""
        self.__mUniqueName = ""
        self.__mUpdateTime = ""
        self.__mCreateTime = ''
        self.__mType = ''

    def getId(self):
        return self.__mTagId
    
    def getName(self):
        return self.__mName
    
    def getCategory(self):
        return self.__mCategory
    
    def getUniqueName(self):
        return self.__mUniqueName
    
    def getUpdateTime(self):
        return self.__mUpdateTime
    
    def getCreateTime(self):
        return self.__mCreateTime
    
    def getType(self):
        return self.__mType
    
    def setId(self, _id):
        self.__mTagId = _id
    
    def setName(self, name):
        self.__mName = name
    
    def setCategory(self, category):
        self.__mCategory = category
    
    def setUniqueName(self, uniqueName):
        self.__mUniqueName = uniqueName
    
    def setUpdateTime(self, updateTime):
        self.__mUpdateTime = updateTime
    
    def setCreateTime(self, createTime):
        self.__mCreateTime = createTime
    
    def setType(self, tp):
        self.__mType = tp

    #override
    def isFitTag(self,  param):
        return False 
    
  
#Singleton, should be instanced by final_getTagCategoryInstance(), because it must be set mTag.
#Sub class should be named as PFTags_categoryname.
class PFTags_category(PFTags):
    def __init__(self):
        super(PFTags_category,  self).__init__()
        self.mTag = None
    
    def getId(self):
        return self.mTag.getId()
    
    def getName(self):
        return self.mTag.getName()
    
    def getCategory(self):
        return self.mTag.getCategory()
    
    def getUniqueName(self):
        return self.mTag.getUniqueName()
    
    def getUpdateTime(self):
        return self.mTag.getUpdateTime()
    
    def getCreateTime(self):
        return self.mTag.getCreateTime()
    
    def getType(self):
        return self.mTag.getType()
    
    def setId(self, _id):
        raise util.pf_exception.PFExceptionFormat
    
    def setName(self, name):
        raise util.pf_exception.PFExceptionFormat
    
    def setCategory(self, category):
        raise util.pf_exception.PFExceptionFormat
    
    def setUniqueName(self, uniqueName):
        raise util.pf_exception.PFExceptionFormat
    
    def setUpdateTime(self, updateTime):
        raise util.pf_exception.PFExceptionFormat
    
    def setCreateTime(self, createTime):
        raise util.pf_exception.PFExceptionFormat
    
    def setType(self, tp):
        raise util.pf_exception.PFExceptionFormat

    #This method should not override anymore.    
    #override
    def isFitTag(self,  param):
        #First pass parameter to tag, if not fit, then pass parameter to tag category.
        if self.mTag.isFitTag(param) is True:
            return True
        else:
            return self.isFitCategory(param)
    
    #override
    def isFitCategory(self, param):
        return False 
    
    #override
    @staticmethod    
    def getTagCount():
        return 3
    

#abstract class, should not be instanced.
class PFTags_tag(PFTags):
    def __init__(self):
        if self.__class__ == PFTags_tag:
            raise util.pf_exception.PFExceptionFormat
        super(PFTags_tag,  self).__init__()
        self.setCategory(self.__class__.__name__.split('_')[-2])
        self.setUniqueName(self.__class__.__name__.split('_')[-1])


class PFTags_model(PFTags_category):
    #override
    def isFitCategory(self, param):
        str_tag_value = self.mTag.getUniqueName()
        if str_tag_value == param:
            return True
        return False         

class PFTags_memory(PFTags_category):
    #override
    def isFitCategory(self, param):
        tagValue = int(self.mTag.getUniqueName())
        param = int(param)
        if int(param) >= tagValue:
            r = param - tagValue
        else:
            r = tagValue - param
        if r  / tagValue <= 0.5:
            return True
        return False 
    
class PFTags_province(PFTags_category):
    #override
    def isFitCategory(self, param):
        str_tag_value = self.mTag.getUniqueName()
        if str_tag_value == param:
            return True
        return False 

class PFTags_app_type(PFTags_category):
    #override
    def isFitCategory(self, param):
        str_tag_value = self.mTag.getUniqueName()
        if str_tag_value == param:
            return True
        return False
   
class PFTags_game_category(PFTags_category):
    #override
    def isFitCategory(self, param):
        str_tag_value = self.mTag.getUniqueName()
        if str_tag_value in param or param in str_tag_value:
            return True
        return False

class PFTags_app_category(PFTags_category):
    #override
    def isFitCategory(self, param):
        str_tag_value = self.mTag.getUniqueName()
        if str_tag_value in param or param in str_tag_value:
            return True
        return False

class PFTags_app_gender(PFTags_category):
    #override
    def isFitCategory(self, param):
        str_tag_value = self.mTag.getUniqueName()
        if str_tag_value == param:
            return True
        return False   

#should name PFTags_Category_UnigueName, extend PFTags_tag, extend PFTags.  
#class PFTags_memory_512(PFTags_tag):
#def isFitTag(self,  param):
#  if int(param) > 500 and int (param) < 600:
#      return True
# return False
        

if __name__ == "__main__":
    pass
    #a = PFTags_memory_512()
    
