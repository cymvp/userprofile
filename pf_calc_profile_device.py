import sys
import dbmanager.pf_metric_collection_manager
import dbmanager.pf_device_collection_manager
import dbmanager.pf_tags_collection_manager
import dbmanager.pf_taguid_collection_manager
import util.utils
import libs.util.logger
import libs.util.my_utils
import util.calc_tag
import ubc.pf_metric_helper
from config.const import *
from mythread.write_thread import write_thread
import time

from dbmanager.pf_hwv_collection import PFHWVCollectionManager
from dbmanager.pf_province_collection import PFProvinceCollectionManager
from dbmanager.pf_app_collection import PFAPPCollectionManager
from dbmanager.pf_collection_manager import PFCollectionManager


def consist_of_foreign_data(key_list, foreign_tuple, collection_name, old_linked_node_map):
    '''
    result is:
        {
          "deviceinfo_4096_7d3" : {
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
        if app_id == '1' and metric_id == '0x03':
            key_list.append(old_linked_node)
            key_list = list(set(key_list))
    l = len(key_list)
    if l > 20:
        l = 20
    return {foreign_label:{'linked_collection' : collection_name, 'linked_list' : key_list[0: l]}}

def func(i, j):
    time.sleep(5)
    print(i)
    print(j)

if __name__ == "__main__":  # nDays,  mTop
    i = 0
    write_thread_obj = write_thread()
    write_thread_obj.start()
    
    total_device_count = 0
    
    g_foreign_tuple_list = [('1', '0x1'), ('1', '0x2'), ('1', '0x3')]
    g_foreign_collection_map = {g_foreign_tuple_list[0]: PFHWVCollectionManager.getCollectionName(), 
                                g_foreign_tuple_list[1]: PFProvinceCollectionManager.getCollectionName(),
                                g_foreign_tuple_list[2]: PFAPPCollectionManager.getCollectionName()}
    '''
    if len(sys.argv) == 5:
        tagManager = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager()
        #tagUidsManager = dbmanager.pf_taguid_collection_manager.PFTagUidsCollectionManager()
        tagCursors = tagManager.getAllDoc()
        tagLst = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager.final_convertCursor(tagCursors)
        #for tagMap in tagLst:
        #    tagUidsManager.insertOrUpdateCollection(tagMap, '123456')
        sys.exit(1)
    '''
    if len(sys.argv) != 4:
        print("Error params...... python pf_calc__profile_device.py nDays, mTop ")
        sys.exit(1)
        
    fMetricMap = libs.util.my_utils.openFile(sys.argv[3], 'r')
    
    metricMap = util.utils.loadMap(fMetricMap)
    
    part1_total_duration = 0
    part2_total_duration = 0
    part3_total_duration = 0
    part4_total_duration = 0
    part5_total_duration = 0
    
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    recordTime.startTime()
        
    metricManager = dbmanager.pf_metric_collection_manager.PFMetricCollectionManager()
    userProfileManager = dbmanager.pf_device_collection_manager.PFDeviceCollectionManager()
    tagManager = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager()
    #tagUidsManager = dbmanager.pf_taguid_collection_manager.PFTagUidsCollectionManager()
    #Fixed me!!!!!!
    #userProfileManager.drop_table()
    
    #rdisConfigure = rdis.configure.Configure()
    #tagUidsRedis = rdis.tagredis.TagUidsRedis(rdisConfigure)

    # For calculating metric data of profile collection.
    strToday = util.utils.getStrToday()
    monthLst = util.utils.calc_months(strToday,  int(sys.argv[1]))
    
    #Get all documents of indicated months; documents is user metrics.
    cursorLst = util.utils.getCursorLst(metricManager, monthLst)
    #Parse all user and info,and save to map.
    
        # For calculating tags of profile collection.
    tagCursors = tagManager.getAllDoc()
    tagLst = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager.final_convertCursor(tagCursors)
    
    tagObjectLst = []
    for t in tagLst:
        tagObjectLst.append(dbmanager.pf_tags_collection_manager.PFTagsCollectionManager.final_convert2Tag(t)) 
    
    ellapsedTime = recordTime.getEllapsedTime()
    libs.util.logger.Logger.getInstance().debugLog("Call buildMetricData(): take %.3fs" % ellapsedTime)
    
    #cursorLst contains all cursor whch located in diffferent metric collections, seperated by month.
    for curs in cursorLst:
        # curs contains cursor which located in same metric collections.
        max_count = get_actural_count(curs.count(), PERFORMANCE_DEVICE_COUNT)
        for cur in curs:
            total_device_count += 1
            if total_device_count % 1000 == 0:
                libs.util.logger.Logger.getInstance().debugLog("Has handled %d device." % total_device_count)
                libs.util.logger.Logger.getInstance().debugLog("total time of part1 is: %.3fs ." % part1_total_duration)
                libs.util.logger.Logger.getInstance().debugLog("total time of part2 is: %.3fs ." % part2_total_duration)
                libs.util.logger.Logger.getInstance().debugLog("total time of part3 is: %.3fs ." % part3_total_duration)
                libs.util.logger.Logger.getInstance().debugLog("total time of part4 is: %.3fs ." % part4_total_duration)
            #Fixed me!!!!!! temp py.
            if total_device_count >= max_count:
                break
            
            uid = cur[dbmanager.pf_collection_manager.PFCollectionManager.final_getUidLabel()]
            '''
            if uid != '865854010357857Kr7Ri':
                continue
            else:
                print("Find it!")
            '''
            uidMap, userInfoMap = util.utils.buildMetricData_new(metricManager,  cur, metricMap, g_foreign_tuple_list)
            part1_total_duration += recordTime.getEllaspedTimeSinceLast()
            
            #sys.argv[2] is refered days.
            resultMap = util.utils.calc_profile(uidMap,  sys.argv[2])
            part2_total_duration += recordTime.getEllaspedTimeSinceLast()

            cuid = userInfoMap[uid].get(dbmanager.pf_device_collection_manager.PFCollectionManager.final_getCUidLabel())
            imei = userInfoMap[uid].get(dbmanager.pf_device_collection_manager.PFCollectionManager.final_getIMEILabel())

            #merge_new_data_map()
            cur = userProfileManager.isDocExist(uid)
            if cur is None or cur.count() == 0:
                old_linked_node_map = {}
            else:
                cur = next(cur.__iter__())
                old_linked_node_map = PFCollectionManager.final_get_linked_node_by_cur(cur)
            #till here, Performance: 4000/10s on PC.

            if userInfoMap[uid].get('foreign_key_list') is None or len(userInfoMap[uid].get('foreign_key_list')) == 0:
                foreign_map = {}
            else:
                foreign_map = userInfoMap[uid]['foreign_key_list']
            #userInfoMap[uid]['foreign_key_list'] is: {(1,1) : ['ZTE N807', 'ZTE N807'], (1,2):['beijing', 'shanghai']}
            
            for foreign_tuple in foreign_map:
                if foreign_tuple not in g_foreign_tuple_list:
                    continue
                foreign_collection_name = g_foreign_collection_map[foreign_tuple]
                key_list = foreign_map[foreign_tuple]
                
                foreign_data_map = consist_of_foreign_data(key_list, foreign_tuple, foreign_collection_name, old_linked_node_map)

                if resultMap[uid].get(userProfileManager.final_get_label_linkde_node()) is None:
                    resultMap[uid][userProfileManager.final_get_label_linkde_node()] = {} 
                resultMap[uid][userProfileManager.final_get_label_linkde_node()][next(foreign_data_map.__iter__())] = next(foreign_data_map.values().__iter__())
            part3_total_duration += recordTime.getEllaspedTimeSinceLast()
            
            cur, is_insert = userProfileManager.merge_new_data_map(cur, uid, cuid, imei, resultMap[uid])
            
            tagMap, userInfoMap = util.calc_tag.calc_tags(userProfileManager, cur,  tagLst, tagObjectLst)
            #part2_total_duration += recordTime.getEllaspedTimeSinceLast()
            '''profileMap is:
                "profile_tags":[  
                       {'tagid1':123, 'name': n880e, 'category': model},  
                       {'tagid2':456, 'name': n880e, 'category': model}  
                ] ,
                "profile_tags":[
                                       
                ] , 
                ......
            }'''
                
            for uid in tagMap:
                profileMap = {dbmanager.pf_device_collection_manager.PFDeviceCollectionManager.final_getProfileTagLabel():tagMap[uid]}
                cur, temp_flag = userProfileManager.merge_new_data_map(cur, uid, cuid, imei, profileMap)
                write_thread.write_to_queue(userProfileManager.getCollectionName(), cur, is_insert)
                #userProfileManager.insertOrUpdateUser_old(uid,  userInfoMap[uid][dbmanager.pf_device_collection_manager.PFCollectionManager.final_getCUidLabel()],  userInfoMap[uid][dbmanager.pf_device_collection_manager.PFCollectionManager.final_getIMEILabel()],  profileMap)
                #anandonTagLst = userInfoMap[uid].get('abondonTagList')
                #newTagLst = tagMap[uid]

            #userProfileManager.insertOrUpdateUser(cur, is_insert)
            
            #resultMap[uid][list(foreign_data_map.keys())[0]] = next(foreign_data_map.values().__iter__())
            #userProfileManager.insertOrUpdateUser_old(uid,  cuid, imei, resultMap[uid])
            
            #till here, Performance: 4000/5.5s on PC.
            part4_total_duration += recordTime.getEllaspedTimeSinceLast()
            
    libs.util.logger.Logger.getInstance().debugLog("start waiting for write thread finished.: %.3fs ." % part1_total_duration)
    write_thread_obj.set_interrupt()
    write_thread.get_queue().join()
    part5_total_duration += recordTime.getEllaspedTimeSinceLast()
    
    libs.util.logger.Logger.getInstance().debugLog("total time of part1 is: %.3fs ." % part1_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part2 is: %.3fs ." % part2_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part3 is: %.3fs ." % part3_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part4 is: %.3fs ." % part4_total_duration)
    libs.util.logger.Logger.getInstance().debugLog("total time of part5 is: %.3fs ." % part5_total_duration)
    
    ellapsedTime = recordTime.getEllapsedTime()
    printProcess.printCurrentProcess(ellapsedTime, total_device_count)
    
    
    '''
    Not call any more.
    '''
    sys.exit(1)
    
    libs.util.logger.Logger.getInstance().debugLog("Start calculating tags.")
    
    total_device_count = 0 
    part1_total_duration = 0
    part2_total_duration = 0
    part3_total_duration = 0
    part4_total_duration = 0
    
    #Get all user doc of profile_collection
    profileCursors = userProfileManager.getAllDoc()
    
    if profileCursors is None:
        libs.util.logger.Logger.getInstance().debugLog("device_collection is not existed, just quit.")
        sys.exit(1)

    max_count = get_actural_count(profileCursors.count(), PERFORMANCE_DEVICE_TAG_COUNT)
    
    #for each user profile doc; cur is user doc.
    for cur in profileCursors:
        #till here, Performance: 4000/0.54s on PC.
        part1_total_duration += recordTime.getEllaspedTimeSinceLast()
        total_device_count += 1
        if total_device_count % 1000 == 0:
            libs.util.logger.Logger.getInstance().debugLog("Has handled %d device." % total_device_count)
        #Fixed me!!!!!!
        if total_device_count > max_count:
            break
    
        tagMap, userInfoMap = util.calc_tag.calc_tags(userProfileManager, cur,  tagLst, tagObjectLst)
        part2_total_duration += recordTime.getEllaspedTimeSinceLast()
        '''profileMap is:
            "profile_tags":[  
                   {'tagid1':123, 'name': n880e, 'category': model},  
                   {'tagid2':456, 'name': n880e, 'category': model}  
            ] ,
            "profile_tags":[
                                   
            ] , 
            ......
        }'''
            
        for uid in tagMap:
            profileMap = {dbmanager.pf_device_collection_manager.PFDeviceCollectionManager.final_getProfileTagLabel():tagMap[uid]}
            userProfileManager.insertOrUpdateUser_old(uid,  userInfoMap[uid][dbmanager.pf_device_collection_manager.PFCollectionManager.final_getCUidLabel()],  userInfoMap[uid][dbmanager.pf_device_collection_manager.PFCollectionManager.final_getIMEILabel()],  profileMap)
            #anandonTagLst = userInfoMap[uid].get('abondonTagList')
            #newTagLst = tagMap[uid]
        part3_total_duration += recordTime.getEllaspedTimeSinceLast()
        
    ellapsedTime = recordTime.getEllapsedTime()
    printProcess.printFinalInfo(ellapsedTime)            

    
