import sys
import platform
import time
import traceback 
import os
sys.path.append(os.path.abspath('./'))

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
    
    curs = g_CollectionManager.getAllDoc()
    
    g_solution_map = {}
    
    for cur in curs:
        deviceinfo_4096_7d3 = cur.get('deviceinfo_4096_7d3')
        if deviceinfo_4096_7d3 is None or len(deviceinfo_4096_7d3) <= 0:
            continue
        
        solution = deviceinfo_4096_7d3[0].get('deviceinfo').get('resolution')
        
        if g_solution_map.get(solution) == None:
            g_solution_map[solution] = 1
        else:
            g_solution_map[solution] += 1
    
    print(len(g_solution_map))   
    print(sorted(g_solution_map .items(),  key=lambda d:d[1], reverse = True))
            
            
            
                
            


