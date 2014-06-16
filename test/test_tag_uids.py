import sys
import os
import re
from dbmanager.pf_taguid_collection_manager import PFTagUidsCollectionManager
import libs.util.my_utils

sys.path.append('../')
        
if __name__ == "__main__":  # nDays,  mTop
    
    FILE_DIR = './testdata/'
    
    tag_id = sys.argv[1]
    
    tag_id = tag_id.replace('/', '')
    
    #tag_id = 'operationchannel_2030000'
    
    tag_deviceid_manager = PFTagUidsCollectionManager()
    
    rexExp = re.compile(tag_id, re.IGNORECASE)
    cursors = tag_deviceid_manager.isDocExist(rexExp)
    #or:
    #spec_map = {'$regex': tag_id}
    #cursors = tag_deviceid_manager.isDocExist(spec_map)
    
    
    
    tag_uid_map = {}

    
    if cursors is None:
        #insert.
        print('No tag id doc.')
        sys.exit()
    
    for tag_uid_map in cursors:
        uid_list = tag_uid_map[PFTagUidsCollectionManager.final_getUidLstLabel()]
        f_uidlist = libs.util.my_utils.openFile(os.path.join(FILE_DIR, tag_uid_map[PFTagUidsCollectionManager.final_getUidLabel()]), 'w')
        print('%s has %d uids.' % (tag_uid_map[PFTagUidsCollectionManager.final_getUidLabel()], len(uid_list)))
        for uid in uid_list:
            f_uidlist.write(uid + '\n')
        f_uidlist.close()