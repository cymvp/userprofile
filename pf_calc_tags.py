#coding:utf-8
import sys
import util.utils
import libs.util.logger
import libs.util.my_utils
import util.calc_tag
import ubc.pf_metric_helper

from dbmanager.pf_tags_collection_manager import PFTagsCollectionManager
from dbmanager.pf_collection_manager import PFCollectionManager
from dbmanager.pf_device_collection_manager import PFDeviceCollectionManager
from dbmanager.pf_taguid_collection_manager import PFTagUidsCollectionManager

def __get_devices_by_tag(tag_manager, device_manager, tag_map):
    #tagManager = PFTagsCollectionManager()
    #Check whether the tag is existed.
    result_list = []
    #tag_id = tag_category + '_' + tag_unique_name        
    #c = tag_manager.isDocExist(tag_id)
    #if c is None:
    #    return (None, None)
    #target_tad_doc = next(c.__iter__())
    #hwv_data = next(c.__iter__())
    tag_id = tag_map[PFTagsCollectionManager.final_getUidLabel()]
    #Get device list from profile_device_collection, which marked as this tag.
    device_cursors = device_manager.getAllDoc()
    for cur in device_cursors:
        device_id = cur[PFCollectionManager.final_getUidLabel()]
        tag_lst = cur[PFDeviceCollectionManager.final_getProfileTagLabel()]
        if tag_lst == None:
            continue
        for tg_map in tag_lst:
            if tag_id == tg_map[PFTagsCollectionManager.final_getUidLabel()]:
                result_list.append(device_id)
                break
    return (tag_map, result_list)
    #Insert device list of tag into tag_uids_collection.

def __get_tag_device_list(tag_manager):
    cursors = tag_manager.getAllDoc()
    if cursors is None or cursors.count() == 0:
        return None
    return PFTagsCollectionManager.final_convertCursor(cursors)

if __name__ == "__main__": 
    tag_devicds_manager = PFTagUidsCollectionManager()
    tag_manager = PFTagsCollectionManager()
    device_manager = PFDeviceCollectionManager()
    tag_deviceid_manager = PFTagUidsCollectionManager()
    tag_list = __get_tag_device_list(tag_manager)
    if tag_list is not None:
        for tag_map in tag_list:
            tag_map, device_list = __get_devices_by_tag(tag_manager, device_manager, tag_map)
            if tag_map is not None and len(device_list) > 0:
                tag_deviceid_manager.insert_tag_devices_list(tag_map, device_list)
