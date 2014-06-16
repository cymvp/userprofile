import sys
from  dbmanager import pf_dbmanager

from dbmanager.pf_device_collection_manager import PFDeviceCollectionManager
from dbmanager.pf_collection_manager import PFCollectionManager


def get_app_list_by_category(dbManager, collection_name, category_name):
    result_list = []
    cursors = dbManager.find({}, dbManager.getCollection(collection_name))
    for cur in cursors:
        if cur.get('attributes_2_1') is None:
            continue
        if cur['attributes_2_1'][0]['category'].__contains__(category_name):
           result_list.append(cur['_id'])  
    return result_list

def get_app_list_by_id(dbManager, collection_name, name):
    result_list = []
    cursors = dbManager.find({}, dbManager.getCollection(collection_name))
    for cur in cursors:
        if cur['_id'].__contains__(name):
           result_list.append(cur['_id'])  
    return result_list
        
if __name__ == "__main__":  # nDays,  mTop
    deviceManager = PFDeviceCollectionManager()
    #tagUidsManager = dbmanager.pf_taguid_collection_manager.PFTagUidsCollectionManager()
    
    dbManager = pf_dbmanager.PFDBManager()
    dbManager.startDB()
    
    m = {}
    '''
    cursors = dbManager.find({},  dbManager.getCollection('apps_categories_collection'))
    
    for cur in cursors:
        m[cur['name']] = cur['count']
    
    sortedTuple = sorted(m.items(),  key=lambda d:d[1], reverse = True)
    
    print(sortedTuple)  
    '''
    
    #result_set = get_app_list_by_category(dbManager, 'profile_app_collection', '_id', '西游')
    result_set = get_app_list_by_id(dbManager, 'profile_app_collection',  '.')
    
    print(result_set)
