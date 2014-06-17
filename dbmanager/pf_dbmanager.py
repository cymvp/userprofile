import traceback
import pymongo
from  pymongo import MongoClient
from  pymongo.mongo_replica_set_client import MongoReplicaSetClient
import libs.util.logger
import util.pf_exception
from config.const import *
import bson

class PFDBManager:
    def __init__(self,  url = MONGODB_HOST,  port = MONGODB_PORT,  dbName = MONGODB_DBNAME):
        self.__mUrl = url
        self.__mPort = port
        self.__mDBName = dbName
        self.__mDB = None 
      
    def __checkStatus(self,  collcetion): 
        if self.__mDB is None or collcetion is None:
            raise util.pf_exception.PFExceptionWrongStatus
        
    def __reportExcept(self, e):
        libs.util.logger.Logger.getInstance().errorLog(traceback.format_exc())
        libs.util.logger.Logger.getInstance().errorLog('!!!! %s' % e)
    
    def __resetDBStatus(self):
        self.__mDB = None 
            
    def startDB(self):                 
        if self.__mDB is not None:
            return self.__mDB
        if platform.system() == 'Windows_xx':
            client = MongoClient(self.__mUrl, self.__mPort) 
            self.__mDB = client[self.__mDBName]
        else:
            client = MongoReplicaSetClient(self.__mUrl, port = self.__mPort, replicaSet = 'rsbaiyi') #,  replicaset='rs1'
            self.__mDB = client[self.__mDBName]
            self.__mDB.authenticate('cuiyang', 'ssniLmqNZS5DPN')
        return self.__mDB

    def getCollection(self, collectionName): 
        return self.__mDB[collectionName]
    
    def save(self, doc, collection):
        self.__checkStatus(collection)
        try:
            collection.save(doc)
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            self.__resetDBStatus()
            self.__reportExcept(e)
            raise util.pf_exception.PFExceptionWrongStatus
        
    def find(self,  dictQuery,  collection, include_fields = None):
        self.__checkStatus(collection)
        try:
            c = collection.find(spec = dictQuery, fields = include_fields)
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            self.__resetDBStatus()
            self.__reportExcept(e)
            raise util.pf_exception.PFExceptionWrongStatus
        return c
         
    def insert(self,  doc,  collection):
        self.__checkStatus(collection)
        try:
            collection.insert(doc)
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            self.__resetDBStatus()
            self.__reportExcept(e)
            raise util.pf_exception.PFExceptionWrongStatus
        except bson.errors.InvalidDocument as e:
            raise util.pf_exception.PFExceptionFormat
        
    def update(self,  doc,  newDoc,  collection):
        self.__checkStatus(collection)
        try:
            collection.update(doc,  newDoc)
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            self.__resetDBStatus()
            self.__reportExcept(e)
            raise util.pf_exception.PFExceptionWrongStatus
        
    def remove(self, doc, collection):
        self.__checkStatus(collection)
        try:
            collection.remove(doc)
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            self.__resetDBStatus()
            self.__reportExcept(e)
            raise util.pf_exception.PFExceptionWrongStatus
        
    def drop_collection(self, collection):
        self.__checkStatus(collection)
        try:
            self.__mDB.drop_collection(collection)
        except (pymongo.errors.TimeoutError,  pymongo.errors.AutoReconnect) as e:
            self.__resetDBStatus()
            self.__reportExcept(e)
            raise util.pf_exception.PFExceptionWrongStatus
