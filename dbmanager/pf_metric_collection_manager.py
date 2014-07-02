import datetime
import traceback
import pymongo
import util.pf_exception
import libs.util.logger
from dbmanager.pf_collection_manager import PFCollectionManager
import ubc.pf_metric_helper

class PFMetricCollectionManager(PFCollectionManager):
    __UBC_METRIC_COLLCETION_PREFIX = 'test_metrics_collection_'
    cache_cursors = None
    cache_data = {}
    stat_doc = None
    
    @staticmethod
    def final_getMetricIdLabel():
        return 'metric_id'
    
    @staticmethod    
    def final_getAppIdLabel():
        return 'app_id'
    
    @staticmethod
    def final_getMetricsLabel():
        return 'metrics'
    
    @staticmethod
    def final_getMetricDataLabel():
        return 'metric_data'
        
    
    @staticmethod
    def __buildMetricId(metricId):
        return {PFMetricCollectionManager.final_getMetricIdLabel():  metricId}
    
    @staticmethod  
    def __buildAppId(appId):
        return {PFMetricCollectionManager.final_getAppIdLabel():  appId}
    
    #@override   
    def __getCollectionName__(self,  strDate = None):
        if strDate is None:
            strDate = datetime.datetime.now().strftime('%Y%m%d')
        if len(strDate) != 8:
            raise util.pf_exception.PFExceptionFormat
        return PFMetricCollectionManager.__UBC_METRIC_COLLCETION_PREFIX + strDate[0:6]

    #@override
    def __buildDocUser__(self,  chunleiId,  cuid,  imei = ''):
        userMap = super(PFMetricCollectionManager,  self).__buildDocUser__(chunleiId,  cuid,  imei)
        userMap[PFMetricCollectionManager.final_getMetricsLabel()] = []
        return userMap
    
    #def __buildDocUser(self, chunleiId,  cuid, model, imei = ''):
        #userMap = self.__buildDocUser__(self, chunleiId, cuid, imei)
        #userMap[PFMetricCollectionManager.final_getLabelModel()] = model
    
    def buildsubDocMetric(self,  metricId,  appId):
        metricMap = {}
        metricIdMap = PFMetricCollectionManager.__buildMetricId(metricId)
        appIdMap = PFMetricCollectionManager.__buildAppId(appId)
        metricMap[next(metricIdMap.__iter__())] = metricId
        metricMap[next(appIdMap.__iter__())] = appId
        metricMap[PFMetricCollectionManager.final_getMetricDataLabel()] = []
        return metricMap
    
    def isMetricExist(self,  metricList,  metricId,  appId): #userMap['metrics']
        metricIdMap = PFMetricCollectionManager.__buildMetricId(metricId)
        appIdMap = PFMetricCollectionManager.__buildAppId(appId)
        for metric in metricList:
            if metric[next(metricIdMap.__iter__())] == metricId and metric[next(appIdMap.__iter__())] == appId:
                return metric
        return None
    
    def __get_stat_doc(self):
        #初始时;
        _id = 'stat'
        if PFMetricCollectionManager.stat_doc is None:     
            c = self.isDocExist(_id)
            #不存在,则创建一个,并赋值给stat_doc
            if c is None or c.count() == 0:
                return None
            else:     
                PFMetricCollectionManager.stat_doc = next(c.__iter__())        
        return PFMetricCollectionManager.stat_doc 
    
    def set_stat_current_line(self, current_line_num):
        m = self.__get_stat_doc()
        if m is None:
            return None
        m['current_line'] = current_line_num
        self.__update_stat_doc(m)
    
    def get_stat_busy(self):
        m = self.__get_stat_doc()
        if m is None:
            return None
        return m['is_working']

    def set_stat_busy(self, is_working):
        m = self.__get_stat_doc()
        if m is None:
            return None
        m['is_working'] = is_working
        self.__update_stat_doc(m)
    
    def get_stat_update_date(self):
        m = self.__get_stat_doc()
        if m is None:
            return None
        return m['last_update_date']
    
    def set_stat_update_date(self, update_date):
        m = self.__get_stat_doc()
        if m is None:
            return None        
        m['last_update_date'] = update_date
        self.__update_stat_doc(m)
    
    def get_stat_first_date(self):
        m = self.__get_stat_doc()
        if m is None:
            return None
        return m['first_update_date']
    
    def set_stat_first_date(self, first_date):
        m = self.__get_stat_doc()
        if m is None:
            return None        
        m['first_update_date'] = first_date
        self.__update_stat_doc(m)
        
    def create_stat_doc(self, m):
        self.__update_stat_doc(m)
    
    def __update_stat_doc(self, m):
        try:
            collection = self.mDBManager.getCollection(self.__getCollectionName__())     
            self.mDBManager.update(self.__buildUid__(m.get('_id')), m, collection)
            PFMetricCollectionManager.stat_doc = m
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            libs.util.logger.Logger.getInstance().errorLog(traceback.format_exc())
            libs.util.logger.Logger.getInstance().errorLog('!!!! %s' % e)
            raise util.pf_exception.PFExceptionWrongStatus
        
    #final method    
    def final_getCollection(self,  strDate = None):
        return self.mDBManager.getCollection(self.__getCollectionName__(strDate))
    
    def insertOrUpdateUser(self,  chunleiId,  cuid,  imei, metrics_list, collection = None):
        try:
            if collection is None:
                collection = self.mDBManager.getCollection(self.__getCollectionName__())
            #check whether this document existed, checked by chunleiid.
            c = self.isDocExist(chunleiId)
            userMap = {}
            isInsert = 0
            if c is None:
                #insert.
                userMap = self.__buildDocUser__(chunleiId,  cuid, imei)
                isInsert = 1
            else:
                #update
                userMap = c.__getitem__(0)
                
            for metric_map in metrics_list:
                metric_id = metric_map[PFMetricCollectionManager.final_getMetricIdLabel()]
                app_id = metric_map[PFMetricCollectionManager.final_getAppIdLabel()]
                old_metric_map = self.isMetricExist(userMap[self.final_getMetricsLabel()],  metric_id,  app_id)
                if old_metric_map is None:
                    userMap[PFMetricCollectionManager.final_getMetricsLabel()].append(metric_map)
                else:
                    old_metric_map[PFMetricCollectionManager.final_getMetricDataLabel()] = metric_map[PFMetricCollectionManager.final_getMetricDataLabel()]

                #metricMap is sub element of metrics[] with certain appid and metricid, and metric_data[]
    
            if isInsert == 1:
                self.mDBManager.insert(userMap,  collection)
            else:
                self.mDBManager.update(self.__buildUid__(chunleiId),  userMap,  collection)
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            libs.util.logger.Logger.getInstance().errorLog(traceback.format_exc())
            libs.util.logger.Logger.getInstance().errorLog('!!!! %s' % e)
            raise util.pf_exception.PFExceptionWrongStatus
    
    '''   
    def insertOrUpdateUser(self,  chunleiId,  cuid,  imei, metricId_lst,  appId_lst,  valueMap_lst, collection = None):
        try:
            if collection is None:
                collection = self.mDBManager.getCollection(self.__getCollectionName__())
            #check whether this document existed, checked by chunleiid.
            c = self.isDocExist(chunleiId)
            userMap = {}
            isInsert = 0
            if c is None:
                #insert.
                userMap = self.__buildDocUser__(chunleiId,  cuid, imei)
                isInsert = 1
            else:
                #update
                userMap = c.__getitem__(0)
                
            i = 0
            while i < len(metricId_lst):
                metricId = metricId_lst[i]
                appId = appId_lst[i]
                valueMap = valueMap_lst[i]
    
                metricMap = self.isMetricExist(userMap[self.final_getMetricsLabel()],  metricId,  appId)
                if metricMap is None:
                    metricMap = self.buildsubDocMetric(metricId,  appId)
                    userMap[PFMetricCollectionManager.final_getMetricsLabel()].append(metricMap)
                
                #For metricMap is map for certain appid, metricid, and metric_data.
                #Fixed me. should revise to this. valueMap acturally is map list.
                metricMap[PFMetricCollectionManager.final_getMetricDataLabel()] = valueMap
                     
                #metricMap is sub element of metrics[] with certain appid and metricid, and metric_data[]
                i += 1
    
            if isInsert == 1:
                self.mDBManager.insert(userMap,  collection)
            else:
                self.mDBManager.update(self.__buildUid__(chunleiId),  userMap,  collection)
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            libs.util.logger.Logger.getInstance().errorLog(traceback.format_exc())
            libs.util.logger.Logger.getInstance().errorLog('!!!! %s' % e)
            raise util.pf_exception.PFExceptionWrongStatus
      '''      
       
    def get_metric_data_list(self, chunleiId, app_id, metric_id, collection = None):
        try:
            if collection is None:
                collection = self.mDBManager.getCollection(self.__getCollectionName__())
            #check whether this document existed, checked by deviceid.
            c = self.isDocExist(chunleiId)
            if c is None:
                return None
            userMap = c.__getitem__(0)
            metricMap = self.isMetricExist(userMap[self.final_getMetricsLabel()],  metric_id,  app_id)
            '''
            return metric map:
            {
                "metric_data" : [
                    {
                        "北京" : 30
                    }    
                ],
                "metric_id" : "0x2",
                "app_id" : "1"
            },
            '''
            return metricMap
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            libs.util.logger.Logger.getInstance().errorLog(traceback.format_exc())
            libs.util.logger.Logger.getInstance().errorLog('!!!! %s' % e)
            raise util.pf_exception.PFExceptionWrongStatus
                     