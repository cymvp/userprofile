#import pf_tags
#import tags
import tags.pf_tags
class PFTags_wifistat(tags.pf_tags.PFTags_category): #pf_tags.PFTags_category
    #override
    def isFitCategory(self, param):
        #tagValue is : the unique name of wifistat category.
        #param is the count of KB.
        param = int(param)
        if param > 512 * 1024:
            return True
        else:
            return False

class PFTags_gprsstat(PFTags_wifistat):
    pass
    