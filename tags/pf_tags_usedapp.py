#import pf_tags
#import tags
import tags.pf_tags
class PFTags_usedapp(tags.pf_tags.PFTags_category): 
    #override
    def isFitCategory(self, param):
        #tagValue is packagename: com.baidu.map
        tagValue = str(self.mTag.getUniqueName())
        #param is packagename: com.baidu.map
        param = str(param)
        if param == tagValue:
            return True
        else:
            return False
    