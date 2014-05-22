import sys
import platform
import time
import traceback 
import libs.util.my_utils
from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_hwv_collection import PFHWVCollectionManager
from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager
import util.utils
import ubc.pf_metric_helper

import libs.util.logger
import config.const
import util.calc_tag
import tags.pf_tag_helpers


def consist_of_tag_doc(category_name, unique_name):
    t = tags.pf_tags.PFTags()
    t.setName(unique_name)
    t.setCategory(category_name)
    t.setUniqueName(unique_name)
    return t

def __exist_by_category(lst, category_name):
    for tg_map in lst:
        tg_category_name = tg_map.get(PFTagsCollectionManager.getCategoryLabel())
        if tg_category_name is not None and tg_category_name == category_name:
            return True
    return False


if __name__ == "__main__":

    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    recordTime.startTime()

    g_CollectionManager = PFHWVCollectionManager()
    tagManager = PFTagsCollectionManager()
    
    #g_CollectionManager.drop_table()
    
    curs = g_CollectionManager.getAllDoc()
    
    untaged_model_map = {}
    tag_object_map = {}
    category_name = 'model'
    
    force_add = True
    
    for cur in curs:
        model = cur[PFHWVCollectionManager.final_getUidLabel()]
        if force_add is False:
            tag_list = cur[PFHWVCollectionManager.final_getProfileTagLabel()]
            if tag_list is None or len(tag_list) == 0:
                untaged_model_map[model] = model
            else:
                if __exist_by_category(tag_list, category_name) == True:
                    continue
                else:
                    untaged_model_map[model] = model
        else:
            untaged_model_map[model] = model
    
    # For calculating tags of profile collection.
    tagCursors = tagManager.getAllDoc()
    tagLst = PFTagsCollectionManager.final_convertCursor(tagCursors)
    
    for t in tagLst:
        tag_object = PFTagsCollectionManager.final_convert2Tag(t)
        #Getting all tags with category is 'model'.
        if tag_object.getCategory() != category_name:
            continue
        tag_object_map[tag_object.getUniqueName()] = tag_object 
    
    libs.util.logger.Logger.getInstance().debugLog("Start calculating tags.")

    u = 0
    y = 0
    for model in untaged_model_map:
        tag_object = tag_object_map.get(model)
        if tag_object is None:
            #This model has not tag now.
            t = consist_of_tag_doc(category_name, model)
            tagManager.insertOrUpdateCollection(t)
            u += 1
        else:
            y += 1
    print(u)
    print(y)
            
            
            
                
            


