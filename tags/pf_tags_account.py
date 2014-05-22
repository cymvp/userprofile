#import pf_tags
#import tags
import tags.pf_tags
class PFTags_account(tags.pf_tags.PFTags_category): #pf_tags.PFTags_category
    #override
    def isFitCategory(self, param):
        #tagValue is like: tencent, baidu, google
        tagValue = str(self.mTag.getUniqueName())
        #param is like:com_android_email, com_tencent_mm_account, com_baidu
        param = str(param)
        if param.__contains__(tagValue):
            return True
        else:
            return False
    