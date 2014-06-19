import sys
import os
import re

sys.path.append(os.path.abspath('./'))
print(sys.path)

#import dbmanager
from dbmanager.pf_taguid_collection_manager import PFTagUidsCollectionManager
import libs.util.my_utils


#print(sys.modules)

#python3 test_tag_uids.py memory_1024
if __name__ == "__main__":  # nDays,  mTop
    
    FILE_DIR = './test/testdata/'
    
    tag_id = sys.argv[1]
    
    tag_id = tag_id.replace('/', '')
    
    #sys.exit(1)
    
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
        _id = tag_uid_map[PFTagUidsCollectionManager.final_getUidLabel()]
        if len(uid_list) != int(tag_uid_map['total_count']):
            _id = _id[0:_id.find('(')]
        f_uidlist = libs.util.my_utils.openFile(os.path.join(FILE_DIR, _id), 'a')
        print('%s has %d uids.' % (tag_uid_map[PFTagUidsCollectionManager.final_getUidLabel()], len(uid_list)))
        for uid in uid_list:
            f_uidlist.write(uid + '\n')
        f_uidlist.close()