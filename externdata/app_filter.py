from dbmanager.pf_app_collection import PFAPPCollectionManager
from dbmanager.pf_white_app_collection_manager import PFWhiteAPPCollectionManager

class AppFilter:
    
    app_data_map = None
    white_app_map = None
    
    def __init__(self):
        if AppFilter.app_data_map is None:
            AppFilter.app_data_map = {}
            app_collection_manager = PFAPPCollectionManager()
            app_cursors = app_collection_manager.getDocWithCondition({'xxx_2_1':{'$exists':True}})
            for app_cur in app_cursors:
                package_name = app_cur[PFCollectionManager.final_getUidLabel()]
                AppFilter.app_data_map[package_name] = 0
            app_cursors.close()
        if AppFilter.white_app_map is None:     
            white_app_collection_manager = PFWhiteAPPCollectionManager()
            white_app_cursors = white_app_collection_manager.getAllDoc()
            for app_cur in white_app_cursors:
                package_name = app_cur[PFCollectionManager.final_getUidLabel()]
                white_app_map[package_name] = 0
            white_app_cursors.close()
        
    def is_fit(self, package_name):
        if AppFilter.app_data_map.get(package_name) is not None:
            return true
        else:
            if package_name.find('baidu') or package_name.find('tencent') or package_name.find('qihoo'):
                return true
            else:
                return false
    
    def is_in_white_list(self, package_name):
        if AppFilter.white_app_map.get(package_name) is not None:
            return true
        else:
            return false
    
    g_white_app_map = {}
    white_app_cursors = g_white_app_collection_manager.getAllDoc()
    