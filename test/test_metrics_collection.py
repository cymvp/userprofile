import sys
import os
import re
from dbmanager.pf_device_collection_manager import PFDeviceCollectionManager
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager
import libs.util.my_utils

sys.path.append('../')
        
if __name__ == "__main__":  # nDays,  mTop
    
    FILE_DIR = './testdata/'
    
    #collection_manager = PFMetricCollectionManager()
    #cursors = collection_manager.getDocWithCondition({'metrics.app_id':'36864', 'metrics.metric_data.channel':'2010000'})
    
    collection_manager = PFDeviceCollectionManager()
    cursors = collection_manager.getDocWithCondition({'profile_tags._id':"app_category_工具"})
    
    #rexExp = re.compile(tag_id, re.IGNORECASE)
    #cursors = tag_deviceid_manager.isDocExist(rexExp)
    #or:
    #spec_map = {'$regex': tag_id}
    
    #'operation_36864_1808.channel': '2000000'
    
    
    tag_uid_map = {}

    
    if cursors is None:
        #insert.
        print('No tag id doc.')
        sys.exit()
    
    print(cursors.count())
    
    '''
    for cursor in cursors:
        uid_list = tag_uid_map[PFCollectionManager.final_getUidLabel()]
        f_1 = libs.util.my_utils.openFile(os.path.join(FILE_DIR, 'result', 'w')
        print('%s has %d uids.' % (tag_uid_map[PFTagUidsCollectionManager.final_getUidLabel()], len(uid_list)))
        for uid in uid_list:
            f_1.write(uid + '\n')
        f_uidlist.close()
    '''
       
