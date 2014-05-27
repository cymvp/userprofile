import sys
import dbmanager.pf_metric_collection_manager
import dbmanager.pf_device_collection_manager
import dbmanager.pf_tags_collection_manager
import dbmanager.pf_taguid_collection_manager
import ubc.pf_metric_handle_general
import ubc.pf_metric_helper
import libs.util.utils
import util.pf_exception
import libs.util.logger
import tags.pf_tag_helpers
import libs.util.my_utils
import util.utils
import util.calc_tag
from config.const import *
from dbmanager.pf_metric_collection_manager import  PFMetricCollectionManager
from dbmanager.pf_user_collection import PFUserCollectionManager
from dbmanager.pf_device_collection_manager import PFDeviceCollectionManager
from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager


def consist_of_foreign_data(key_list, foreign_tuple, collection_name, old_linked_node_map):
    '''
    result is:
        {
          "device_1_4" : {
                'linked_collection': 
                'linkde_doc':['1', '2',...]
            } 
        }
    '''
    app_id = foreign_tuple[0]
    metric_id = foreign_tuple[1]
    metricHandler = ubc.pf_metric_helper.getMetricHandler('%x' % int(metric_id, 16),  str(app_id))
    foreign_label =  metricHandler.get_profile_metric_label()
    old_linked_node = old_linked_node_map.get(foreign_label)
    #Fix me. hard code about app collection key doc.
    if old_linked_node is not None and old_linked_node.get('linked_list') is not None:
        if app_id == '1' and metric_id == '0x4':
            key_list.extend(old_linked_node.get('linked_list'))
            key_list = list(set(key_list))
    l = len(key_list)
    if l > 20:
        l = 20
    return {foreign_label:{'linked_collection' : collection_name, 'linked_list' : key_list[0: l]}}

def __calc_profile(uidMap,  tops = 3): 
    '''
    uidMap is: 
    {
        uid1:{
            (appid, metricid): [
                {'timestamp':123456, 'strdesc': 10}, 
                {'timestamp':876543, 'strdesc': 11}]}, 
            (appid, metricid):[
            ], 
        uid2:{
        }
        ......
    }    
    '''        
    resultMap = {}
    #Get each uid from metrics_collection immediate struct.
    for uid in uidMap:
        #Get (appid, metricid) from user doc of metrics_collection immediate struct.
        for appIdMetricId in uidMap[uid]:
            #this is a tuple.
            appId,  metricId = appIdMetricId
            metricHandler = ubc.pf_metric_helper.getMetricHandler(metricId[2:],  appId)
        
            #uidMap[uid][appIdMetricId] is a list of certain metric data list.
            dataMap = metricHandler.calculateProfile(uidMap[uid][appIdMetricId])
            
            if dataMap is not None and len(dataMap) > 0:
                '''
                dataMap is :
                {
                    "baidu_cuiyang_bd":{
                         account_4097_7d2:[
                             {account_type:"tencent", account_name:"cuiyang"}
                             ...
                    },
                     "tencent_cuiyang":{
                         account_4097_7d2:[
                             {account_type:"tencent", account_name:"cuiyang"}
                             ...
                         ]
                     }
                }
                '''
                resultMap[uid] = dataMap
 
    return resultMap

#def __getUserInfo(manager, dicMap):
    '''
    resultMap is:
    {
        'cuid': xxxxxx,
        'imei': xxxxxx
    }
    '''
#    resultMap = {}
#    resultMap[dbmanager.pf_user_collection.PFUserCollectionManager.final_getLabelAccountType()] = dicMap.get(dbmanager.pf_user_collection.PFUserCollectionManager.final_getLabelAccountType())
#    resultMap[dbmanager.pf_user_collection.PFUserCollectionManager.final_getLabelAccountName()] = dicMap.get(dbmanager.pf_user_collection.PFUserCollectionManager.final_getLabelAccountName())
#    return resultMap

def __calc_tags(manager,  tagLst):
    '''result is:
    { com_baidu_cuiyang_bd:[  
               {'tagid1':123, 'name': n880e, 'category': model},  
               {'tagid2':456, 'name': n880e, 'category': model}  
        ] ,
     com_baidu_cuiyang_1:[
        ] , 
     ......
    }'''
    tagMap = {} 
    
    #Get all user doc of profile_collection
    accountProfileCursors = manager.getAllDoc()
    
    if accountProfileCursors is None:
        return tagMap
    
    #Because tag attribute in tag_uids collection is more than other collection, so we slim them.
    tmpLst = []
    for t in tagLst:
        slimTag =  PFTagsCollectionManager.final_SlimTag(t)
        tmpLst.append(slimTag)
    tagLst = tmpLst
    
    tagObjectLst = []
    for t in tagLst:
        tagObjectLst.append(PFTagsCollectionManager.final_convert2Tag(t)) 
    
    #for each user profile doc; cur is user doc.
    for cur in accountProfileCursors:
        #uid must not be None, and the key must existed.
        accountId = cur[dbmanager.pf_device_collection_manager.PFCollectionManager.final_getUidLabel()]
        libs.util.logger.Logger.getInstance().debugLog("In Calculating tags, accountId is %s" % accountId)
 
            
        newTagLst = []
        
        #userInfoMap[accountId] = __getUserInfo(manager, cur)  

        if tagMap.get(accountId) is None:
            tagMap[accountId] = []
         
        # For each metric data in user profile doc. item is name of column, which in user doc.
        for item in cur:
            appIdMetricId = ubc.pf_metric_handle_general.PFMetricHandler_Appid_Metricid.final_getAppIdMetricId(item)
            if appIdMetricId is None:
                #item name is not like: xxx_4096_1807:
                continue
            appId,  metricId = appIdMetricId  
            metricHandler = ubc.pf_metric_helper.getMetricHandler(metricId,  appId)
            #mapObjectLst is a list, which contains several tag object; it may include different category tags.
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
                   
        tagMap[accountId].extend(newTagLst)
        
    return tagMap

if __name__ == "__main__":  
    if len(sys.argv) != 4:
        print("Error params...... python pf_calc_profile.py nDays, mTop, metric_map_account.txt ")
        sys.exit(1)
    
    g_foreign_tuple_list = [('1', '0x4')]
    g_foreign_collection_map = {g_foreign_tuple_list[0]: PFDeviceCollectionManager.getCollectionName()}
    
    fMetricMap = libs.util.my_utils.openFile(sys.argv[3], 'r')
    
    metricMap = util.utils.loadMap(fMetricMap)
    
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    recordTime.startTime()
        
    metricManager = dbmanager.pf_metric_collection_manager.PFMetricCollectionManager()
    userManager = dbmanager.pf_user_collection.PFUserCollectionManager()
    tagManager = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager()
    deviceManager = PFDeviceCollectionManager()
    
    # For calculating tags of profile collection.
    tagCursors = tagManager.getAllDoc()
    tagLst = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager.final_convertCursor(tagCursors)
    
    tagObjectLst = []
    for t in tagLst:
        tagObjectLst.append(PFTagsCollectionManager.final_convert2Tag(t)) 
    
    total_device_count = 0
    total_uid_count = 0
    part1_total_duration = 0
    part2_total_duration = 0
    part3_total_duration = 0
    part4_total_duration = 0
    part5_total_duration = 0
    
    #userManager.drop_table()
    
    # For calculating metric data of profile collection.
    strToday = util.utils.getStrToday()
    monthLst = util.utils.calc_months(strToday,  int(sys.argv[1]))
    
    #Get all documents of indicated months; documents is user metrics.
    cursorLst = util.utils.getCursorLst(metricManager, monthLst)
    #Parse all user and info,and save to map.
    
    resultMap = {}
    
    for curs in cursorLst:
        libs.util.logger.Logger.getInstance().debugLog("Start calculating %s: %d items." % ("metrics_collection", curs.count()))  
        # curs contains cursor which located in same metric collections.
        max_count = get_actural_count(curs.count(), PERFORMANCE_DEVICE_COUNT)
        
        for cur in curs:
            total_device_count += 1
            if total_device_count % 1000 == 0:
                ellapsedTime = recordTime.getEllapsedTime()
                printProcess.printCurrentProcess(ellapsedTime, total_device_count) 
            #Fixed me!!!!!! temp py.
            if total_device_count >= max_count:
                break
            
            uid = cur[PFCollectionManager.final_getUidLabel()]
            
            uidMap, userInfoMap = util.utils.buildMetricData_new(metricManager, cur, metricMap, g_foreign_tuple_list)
            part1_total_duration += recordTime.getEllaspedTimeSinceLast()
            #sys.argv[2] is top.
            #Linked nodes don't join calc_profile(), and neither of calc_tags().
            #foreign keys don't join calc_profile(), but can join calc_tags().
            #both linked nodes and foreign keys are excluded from doc when calling buildMetricData();
            #Linked nodes will generate "profile_linked_node" sub doc; foreign keys will generate normal metric_xx_xx sub doc; before insert into collection.
            resultMap = __calc_profile(uidMap,  sys.argv[2])
            
            #resultMap's key is deviceid, and value is the map : {account_id:{metric_id:metriclist},...} 
            #One uid linked several foreign keys(_id of profile_device_collection), and one uid linked several internal keys(_id of profile_account_collection).
            #So, we join both sides of data using uid.
            if userInfoMap[uid].get('foreign_key_list') is None or len(userInfoMap[uid].get('foreign_key_list')) == 0:
                continue
            foreign_map = userInfoMap[uid]['foreign_key_list']
            #userInfoMap[uid]['foreign_key_list'] == foreign_map, is: {(1,4) : ['123456789', 'chunleiid'], (1,x): [xx,xx,xx]}
            part2_total_duration += recordTime.getEllaspedTimeSinceLast()
            for account_id in resultMap[uid]:
                #Fixed me!!!!! Should verify it, revise the final_get_linked_node(uid) to final_get_linked_node_by_cur(uidMap[account_id]). 20140522           
                
                user_data_map = {}
                
                user_data_map[account_id] = resultMap[uid][account_id]
                
                cur = userManager.isDocExist(account_id)
                if cur is None or cur.count() == 0:
                    old_linked_node_map = {}
                else:
                    cur = next(cur.__iter__())
                    old_linked_node_map = PFCollectionManager.final_get_linked_node_by_cur(cur)
                    
                part3_total_duration += recordTime.getEllaspedTimeSinceLast()
                
                for foreign_tuple in foreign_map:
                    if foreign_tuple not in g_foreign_tuple_list:
                        continue
                    foreign_collection_name = g_foreign_collection_map[foreign_tuple]
                    #The key of foreign_map is the list of foreign keys, which are the _id of referenced collection. 
                    key_list = foreign_map[foreign_tuple]
                    '''foreign_data_map is:
                    {
                      "device_1_4" : {
                            'linked_collection': 
                            'linkde_doc':['1', '2',...]
                        } 
                    }
                    '''
                    #consist of one linked node once time; it means handle one foreign metric_id once time.
                    foreign_data_map = consist_of_foreign_data(key_list, foreign_tuple, foreign_collection_name, old_linked_node_map)
                    if resultMap[uid][account_id].get(deviceManager.final_get_label_linkde_node()) is None:
                        resultMap[uid][account_id][deviceManager.final_get_label_linkde_node()] = {} 
                    resultMap[uid][account_id][deviceManager.final_get_label_linkde_node()][next(foreign_data_map.__iter__())] = next(foreign_data_map.values().__iter__())
                
                cur, is_insert = userManager.merge_new_data_map(cur, account_id, resultMap[uid][account_id])
                tagMap, userInfoMap = util.calc_tag.calc_tags(userManager, cur,  tagLst, tagObjectLst)
                #userManager.insertOrUpdateCollectionDevice(account_id, resultMap[uid][account_id])
                part4_total_duration += recordTime.getEllaspedTimeSinceLast()
                for accountId in tagMap:
                    profileMap = {userManager.__class__.final_getProfileTagLabel():tagMap[accountId]}
                    cur, temp_flag = userManager.merge_new_data_map(cur, accountId, profileMap)
                    #userManager.insertOrUpdateCollectionDevice(account_id, resultMap[uid][account_id])
                    total_uid_count += 1
                    if total_uid_count % 1000 == 0:
                        ellapsedTime = recordTime.getEllapsedTime()    
                        printProcess.printCurrentProcess(ellapsedTime, total_uid_count)
                    userManager.insertOrUpdateCollection(cur,  is_insert)
                part5_total_duration += recordTime.getEllaspedTimeSinceLast()
    #libs.util.logger.Logger.getInstance().debugLog("Start calculating tags.")
    #tagMap, userInfoMap = util.calc_tag.calc_tags(manager, cur,  tagLst, tagObjectLst)
    #tagMap = __calc_tags(userManager,  tagLst)

    '''profileMap is:
        "profile_tags":[  
               {'tagid1':123, 'name': n880e, 'category': model},  
               {'tagid2':456, 'name': n880e, 'category': model}  
        ] ,
        "profile_tags":[
                               
        ] , 
        ......
    }'''
    #libs.util.logger.Logger.getInstance().debugLog("Start inserting tags into user profile and tag uids: %d uids." % len(tagMap))
    #for accountId in tagMap:
    #    profileMap = {dbmanager.pf_user_collection.PFUserCollectionManager.final_getProfileTagLabel():tagMap[accountId]}
    #    userManager.updateCollectionTag(accountId, profileMap)

    libs.util.logger.Logger.getInstance().debugLog("total time of part1 is: %.3fs ." % part1_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part2 is: %.3fs ." % part2_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part3 is: %.3fs ." % part3_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part4 is: %.3fs ." % part4_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part5 is: %.3fs ." % part5_total_duration)
    
    libs.util.logger.Logger.getInstance().debugLog("total uid count is: %d ." % total_uid_count)
    ellapsedTime = recordTime.getEllapsedTime()
    printProcess.printFinalInfo(ellapsedTime)            

    
