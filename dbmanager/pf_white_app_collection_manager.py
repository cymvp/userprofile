from dbmanager.pf_collection_manager import PFCollectionManager
        
class PFWhiteAPPCollectionManager(PFCollectionManager):
    __PROFILE_COLLCETION_PREFIX = 'apps_manual_categories_collection'

                  
    def __getCollectionName__(self,   params = None):
        return PFWhiteAPPCollectionManager.__PROFILE_COLLCETION_PREFIX