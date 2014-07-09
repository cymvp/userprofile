import ubc.pf_metric_helper
import dbmanager.pf_tags_collection_manager
from dbmanager.pf_device_collection_manager import PFCollectionManager
import libs.util.logger
import util.utils
import tags.pf_tag_helpers
import libs.util.my_utils
from config.const import *


def calc_tags(manager, str_start_day, str_end_day, cur,  tagLst, tagObjectLst):
    '''result is:
    { uid1:[  
               {'tagid1':123, 'name': n880e, 'category': model},  
               {'tagid2':456, 'name': n880e, 'category': model}  
        ] ,
     uid2:[
        ] , 
     ......
    }'''
    tagMap = {} 
    userInfoMap = {}

        
    #uid must not be None, and the key must existed.
    uid = cur[PFCollectionManager.final_getUidLabel()]
    #libs.util.logger.Logger.getInstance().debugLog("In Calculating tags, uid is %s" % uid)

    userInfoMap[uid] = manager.getMetaInfo(cur)  
    #Get the existed profile tags by certain cur.
    oldTagLst = manager.getTagsByCur(cur)
    newTagLst = []

    if tagMap.get(uid) is None:
        tagMap[uid] = []
    
    # For each metric data in user profile doc. item is name of column, which in user doc.
    for item in cur:
        appIdMetricId = ubc.pf_metric_handle_general.PFMetricHandler_Appid_Metricid.final_getAppIdMetricId(item)
        if appIdMetricId is None:
            #item name is not like: xxx_4096_1807:
            continue
        appId,  metricId = appIdMetricId  
        metricHandler = ubc.pf_metric_helper.getMetricHandler(metricId,  appId)
        #mapObjectLst is a list, which contains several tag object; it may include different category tags.
        #mapObjectLst = None
        mapObjectLst = metricHandler.calculateTag(cur[item],  tagObjectLst)
        # if mapObjectLst is None, it means that this metric dont't convert it to tag.
        if mapObjectLst is not None and len(mapObjectLst) > 0:
            if isinstance(mapObjectLst,  list) == False:
                raise util.pf_exception.PFExceptionFormat    
            for tg in mapObjectLst:
                #pass
                #newTagLst is sub collection of all tag collection, so all of newTagLst is newest.
                strTag = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager.final_buildTagMap(tg)
                newTagLst.append(strTag)

    linked_list = manager.final_get_linked_tag(cur) 

    if linked_list is not None and len(linked_list) > 0:
        newTagLst.extend(linked_list)
        
    #Merge old and new tag list; old tag list is from current user profile document, new tag list is from calculating by metric data.                          
    newTagLst = __pickTag(oldTagLst, newTagLst, str_start_day, str_end_day)
               
    tagMap[uid].extend(newTagLst)
    
    #part4_total_duration += recordTime.getEllaspedTimeSinceLast()
        
    return (tagMap, userInfoMap)
          
#Find the tag map,  which is in tagLst, but not in resultMap. compare with category and label, so must loop resultMap.
def __getCategoryMap(tagLst):
    '''compareMap and resultMap:
    {category1:[tag1, tag2]
     category2:{tag2, tag3]
    }
    '''
    resultMap = {}
    tagManager = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager
    for newTag in tagLst:
        category = newTag.get(tagManager.getCategoryLabel())

        if util.utils.isStrEmpty(category):
            continue
        if resultMap.get(category) is None:
            resultMap[category] = []
            
        resultMap[category].append(newTag)

    return  resultMap  

def __pickTag(oldTagLst,  newTagLst, str_start_day, str_end_day):
    resultTagLst = []
    
    split_index_map = {}
    result_map = {}
    
    tagMangaer = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager
    
    newTagMap = __getCategoryMap(newTagLst)        
    oldTagMap = __getCategoryMap(oldTagLst)
    
    for tag_category in newTagMap:
        if result_map.get(tag_category) is None:
            result_map[tag_category] = []
        result_map[tag_category].extend(newTagMap[tag_category])
        split_index_map[tag_category] = len(newTagMap[tag_category])
                    
    for tag_category in oldTagMap:
        if result_map.get(tag_category) is None:
            result_map[tag_category] = []
            result_map[tag_category].extend(newTagMap[tag_category])
            split_index_map[tag_category] = 0
        else:
            for old_tg in oldTagMap[tag_category]:
                is_existed = False
                for tg in result_map[tag_category]:
                    if old_tg.get(tagMangaer.getTagIdLabel()) == tg.get(tagMangaer.getTagIdLabel()):
                        is_existed = True
                        #新计算出的tag,如果已经在旧的tag列表里, 就需要重设"更新日期";
                        tg[tagMangaer.getCreateTimeLabel()] = old_tg.get(tagMangaer.getCreateTimeLabel())
                        tg[tagMangaer.getUpdateTimeLabel()] = old_tg.get(tagMangaer.getUpdateTimeLabel())
                        break
                if is_existed == False:
                    result_map[tag_category].append(old_tg)
                
    '''
     tagMap ,newTagMap, oldTagMap is :
     { 'memory':[
          {'name': 512, '_id': 'xxxxx', category:''}
          {'name': 1024, '_id': 'xxxxx', category:''}
        ],
       'app':[
          {'name': 'com.baidu.map', '_id': 'xxxxx', category:''}
          {'name': 'com.baidu.music', '_id': 'xxxxx', category:''}      
        ]
     }
    '''
    for tagCategory in result_map:
        tagLst = result_map[tagCategory]
        count = tags.pf_tag_helpers.PFTagsHelper.final_returnTagCount(tagCategory)
        if count > len(tagLst):
            count = len(tagLst)
        
        #new_tag_count是新计算得到的tag的个数;
        new_tag_count = count
        if split_index_map[tagCategory] < new_tag_count:
            new_tag_count = split_index_map[tagCategory]
        
        #新的tag需要重设"更新日期";
        for i in range(new_tag_count):
            tg = tagLst[i]
            if tg.get(tagMangaer.getCreateTimeLabel()) is None or tg[tagMangaer.getCreateTimeLabel()] > str_start_day:
                tg[tagMangaer.getCreateTimeLabel()] = str_start_day
            if tg.get(tagMangaer.getUpdateTimeLabel()) is None or tg[tagMangaer.getUpdateTimeLabel()] < str_end_day:
                tg[tagMangaer.getUpdateTimeLabel()] = str_end_day
        
        resultTagLst.extend(tagLst[:count])
        
    return resultTagLst 