import sys
import dbmanager.pf_tags_collection_manager
import tags.pf_tags
import libs.util.my_utils
    
if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        print("Error params...... python add_tags.py ./config/tag_list.txt ")
        sys.exit(1)
    
        
    tagManager = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager()
    
    #tagManager.drop_table()
    
    tag_list = []
    
    f_tag_config = libs.util.my_utils.openFile(sys.argv[1], 'r')
        
    for line in f_tag_config:
        arr = line.strip().split(',')
        t = tags.pf_tags.PFTags()
        t.setName(arr[2])
        t.setCategory(arr[0])
        t.setUniqueName(arr[1])
        tag_list.append(t)
        
    for t in tag_list:
        tagManager.insertOrUpdateCollection(t)
