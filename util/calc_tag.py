import ubc.pf_metric_helper
import dbmanager.pf_tags_collection_manager
from dbmanager.pf_device_collection_manager import PFCollectionManager
import libs.util.logger
import util.utils
import tags.pf_tag_helpers
import libs.util.my_utils
from config.const import *

temp_map = {}

'''
def calculateTag(handler, lines, tagObjectList, metricId,  appId, recordTime):
    global temp_map
    a =  handler.calculateTag(lines, tagObjectList)
    total_duration = recordTime.getEllaspedTimeSinceLast()
    
    if temp_map.get((str(handler.__class__), appId, metricId)) is None:
        temp_map[(str(handler.__class__), appId, metricId)] = total_duration
    else:
         temp_map[(str(handler.__class__), appId, metricId)] += total_duration

    return a
'''

def calc_tags(manager, cur,  tagLst, tagObjectLst):
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
    '''userInfoMap is: 
       {
           uid1:{
               cuid:xxx, 
               imei:xxx, 
               abondonTagList: [
                   {category:xxx, uniquename:xxx, _id:xxx}
                   {category:xxx, uniquename:xxx, _id:xxx}
               ]
           }
           uid2:{
              cuid:xxx, 
              imei:xxx, 
              abondonTagList: [
                   {category:xxx, uniquename:xxx, _id:xxx}
                   {category:xxx, uniquename:xxx, _id:xxx}
               ]
           }
       }
    '''
    userInfoMap[uid] = manager.getMetaInfo(cur)  
    #Get the existed profile tags by certain cur.
    oldTagLst = manager.getTagsByCur(cur)
    newTagLst = []
    #abandonLst contains all tags which is old, not existed in user profile for certain uid. 
    abandonLst = []

    if tagMap.get(uid) is None:
        tagMap[uid] = []
    
    #till here, Performance: 4000/0.03s on PC.
    #part2_total_duration += recordTime.getEllaspedTimeSinceLast()
    
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
        #mapObjectLst = calculateTag(metricHandler, cur[item], tagObjectLst, metricId,  appId, recordTime)
        # if mapObjectLst is None, it means that this metric dont't convert it to tag.
        if mapObjectLst is not None and len(mapObjectLst) > 0:
            if isinstance(mapObjectLst,  list) == False:
                raise util.pf_exception.PFExceptionFormat    
            for tg in mapObjectLst:
                #pass
                #newTagLst is sub collection of all tag collection, so all of newTagLst is newest.
                strTag = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager.final_buildTagMap(tg)
                newTagLst.append(strTag)
    #till here, Performance: 4000/0.076s on PC.
    #part3_total_duration += recordTime.getEllaspedTimeSinceLast()
    #till here, Performance: 4000/0.69s on PC.
    #linked_list = manager.get_linked_tag(uid)
    linked_list = manager.final_get_linked_tag(cur) 
    #part4_total_duration += recordTime.getEllaspedTimeSinceLast()
    if linked_list is not None and len(linked_list) > 0:
        newTagLst.extend(linked_list)
        
    #Merge old and new tag list; old tag list is from current user profile document, new tag list is from calculating by metric data.                          
    newTagLst, oldAvailableLst, oldOtherLst = __pickTag(oldTagLst,  newTagLst)
    
    #overlapLst is newest tag data which are set by tagLst.
    #oldUpdateLst is the tag list, which id of tag is changed.
    #oldUselessLst is the tag list, which tag is not valid anymore. 
    #overlapLst contains oldUpdateLst, which are still valid tag but tag id is changed.
    overlapLst, oldUpdateLst, oldUselessLst = findOverlapData(oldAvailableLst, tagLst)
    
    #oldPickedLst is final all(and data is newest) that selected from oldLst.
    oldPickedLst = []
    oldPickedLst.extend(oldOtherLst)
    oldPickedLst.extend(overlapLst)
    
    #L1 should equal oldPickedLst, L2 should equal oldUpdateLst, L3 is all of unselected, including in process of select tag,
    #in process of useless tag compared by tagLst; but not including need update tags.
    L1, L2, L3 = findOverlapData(oldTagLst, oldPickedLst)
    
    newTagLst.extend(oldPickedLst)
    
    #20140427: newTagLst can just contains total new tag list and 
    #tags which is in old tag list but tag id is changed.
    #But when update user profile, it need total tag list,so we use whole valid tag list.
    #newTagLst.extend(oldUpdateLst)
    
    abandonLst.extend(oldUpdateLst)
    abandonLst.extend(L3)
    
    userInfoMap[uid]['abondonTagList'] = abandonLst
               
    tagMap[uid].extend(newTagLst)
    
    #part4_total_duration += recordTime.getEllaspedTimeSinceLast()
    
    for i in temp_map:
        print(str(i) + ': ' + str(temp_map[i]))
        
    return (tagMap, userInfoMap)


def calc_tags_old(manager,  tagLst):
    tagMap = {} 
    userInfoMap = {}
    g_total_num = 0
    
    part1_total_duration = 0
    
    recordTime = libs.util.my_utils.RecordTime()
    recordTime.startTime()
    
    #Get all user doc of profile_collection
    profileCursors = manager.getAllDoc()
    
    if profileCursors is None:
        return (tagMap, userInfoMap)
    
    #Because tag attribute in tag_uids collection is more than other collection, so we slim them.
    #tmpLst = []
    #for t in tagLst:
    #    slimTag =  dbmanager.pf_tags_collection_manager.PFTagsCollectionManager.final_SlimTag(t)
    #    tmpLst.append(slimTag)
    #tagLst = tmpLst
    
    tagObjectLst = []
    for t in tagLst:
        tagObjectLst.append(dbmanager.pf_tags_collection_manager.PFTagsCollectionManager.final_convert2Tag(t)) 
    
    max_count = get_actural_count(profileCursors.count(), PERFORMANCE_DEVICE_TAG_COUNT)
    
    #for each user profile doc; cur is user doc.
    for cur in profileCursors:
        #till here, Performance: 4000/0.54s on PC.
        part1_total_duration += recordTime.getEllaspedTimeSinceLast()
        g_total_num += 1
        if g_total_num % 1000 == 0:
            pass
            libs.util.logger.Logger.getInstance().debugLog("Has handled %d device." % g_total_num)
        #Fixed me!!!!!!
        if g_total_num > max_count:
            break
        uid = cur[PFCollectionManager.final_getUidLabel()]
        uid_tag_map, uid_info_map =  calc_tags(manager, cur,  tagLst, tagObjectLst)
        tagMap[uid] = uid_tag_map[uid]
        userInfoMap[uid] = uid_info_map[uid]

    libs.util.logger.Logger.getInstance().debugLog("total num is: %s ." % g_total_num)
    libs.util.logger.Logger.getInstance().debugLog("recordTime time is: %s ." % recordTime.getEllapsedTime()) 
    libs.util.logger.Logger.getInstance().debugLog("total time of part1 is: %.3fs ." % part1_total_duration)
        
    return (tagMap, userInfoMap)
          
#Find the tag map,  which is in tagLst, but not in resultMap. compare with category and label, so must loop resultMap.
def __getCategoryMap(tagLst, compareMap):
    '''compareMap and resultMap:
    {category1:[tag1, tag2]
     category2:[tag2, tag3]
    }
    '''
    resultMap = {}
    for k in compareMap:
        resultMap[k] = compareMap[k]
    tagMangaer = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager
    for newTag in tagLst:
        category = newTag.get(tagMangaer.getCategoryLabel())
        name = newTag.get(tagMangaer.getUniqueNameLabel())
        if util.utils.isStrEmpty(category) or util.utils.isStrEmpty(name):
            continue
        if resultMap.get(category) is None:
            resultMap[category] = []
        # Same category and unique name, we only select first one(it is newest).
        shouldAppend = True
        for t in resultMap[category]:
            if t.get(tagMangaer.getUniqueNameLabel()) == newTag.get(tagMangaer.getUniqueNameLabel()):
                shouldAppend = False
                break 
        if  shouldAppend == True:
            resultMap[category].append(newTag) 
    return  resultMap  

def __pickTag(oldTagLst,  newTagLst):
    newTagMap = {}
    oldTagMap = {}
    tagMap = {}
    resultTagLst = []
    
    tagMangaer = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager
    
    newTagMap = __getCategoryMap(newTagLst, newTagMap)        
    oldTagMap = __getCategoryMap(oldTagLst, oldTagMap)
    tagMap = __getCategoryMap(oldTagLst, newTagMap)
    #tagMap is all unique tag(by category and name);
    #If category of tag appears in new tag list, new tags is front, old tags is back.
    #If category of tag appears only in old tag list, all in it is old tags; 

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
    for tagCategory in tagMap:
        tagLst = tagMap[tagCategory]
        count = tags.pf_tag_helpers.PFTagsHelper.final_returnTagCount(tagCategory)
        if count >= len(tagLst):
            count = len(tagLst)
        resultTagLst.extend(tagLst[:count])
    
    #resultTagLst doesn't contain category name which is only appears in old new tag list.
    oldOtherLst = []  
    oldUpdateLst = []
    newAddLst = []  
    for tg in resultTagLst:
        category = tg.get(tagMangaer.getCategoryLabel())
        if newTagMap.get(category) is None:
            #This tag only appears in old tag list.
            oldOtherLst.append(tg)
        elif tg in newTagLst:
            #Totally new tag list, which no one appears in old tag list.
            newAddLst.append(tg)
        else:
            oldUpdateLst.append(tg)

    return (newAddLst, oldUpdateLst, oldOtherLst) 

#return 
def findOverlapData(srcLst, desLst):
    overlapLst = []
    uselessLst = []
    updateLst = []
    manager = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager
    for s in srcLst:
        finded = False
        for d in desLst:
            if s.get(manager.getCategoryLabel()) == d.get(manager.getCategoryLabel()) and \
               s.get(manager.getUniqueNameLabel()) == d.get(manager.getUniqueNameLabel()):
                #Assume that d is the newest data than s, so we pick d instead of s.
                #Example: User just update the time of tags in tag collection, but dont't update it in the user profile collection.
                overlapLst.append(d)
                finded = True
                #Note the not fully equal tag.
                if s.get(manager.getTagIdLabel()) != d.get(manager.getTagIdLabel):
                    updateLst.append(s)
                break
        if finded is False:
            uselessLst.append(s)
    #updateLst is the tag list, which id of tag is changed.
    #uselessLst is the tag list, which tag is not valid anymore.  
    return (overlapLst, updateLst, uselessLst)  