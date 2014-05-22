#import pf_tags
import tags.pf_tags
class PFTags_useclock(tags.pf_tags.PFTags_category):#pf_tags.PFTags_category
    #override
    def isFitCategory(self, param):
        #tagValue is 0,1,2,3,...23
        tagValue = int(self.mTag.getUniqueName())
        #param is 0,1,2,3,...23
        param = int(param)
        if param == tagValue:
            return True
        else:
            return False