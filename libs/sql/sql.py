import mysql.connector
from mysql.connector import errorcode
import libs.util.logger
import time

class DBManager:
    def __init__(self, user, passwd, host, port, database):
        self.mDB = database
        self.mPort = port
        self.mUser = user
        self.mPasswd = passwd
        self.mHost = host
        self.mInsertStr = None
        
        self.mContext = self.__connect()
        
    def disconnect(self):
        if self.mContext is not None:
            self.mContext.close()
            self.mContext = None
    
    def is_connected(self):
        if self.mContext is not None:
            return self.mContext.is_connected() 
        return False

    def __connect(self):
        cntx = None
        try:
            cntx = mysql.connector.connect(host=self.mHost, port=self.mPort, user=self.mUser, database=self.mDB, password=self.mPasswd)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                libs.util.logger.Logger.getInstance().errorLog("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                libs.util.logger.Logger.getInstance().errorLog("Database does not exists")
            else:
                libs.util.logger.Logger.getInstance().errorLog(err)
        return cntx
    
    def buildInsertStr(self, columnNameLst, tableName):
        insertStr = "INSERT INTO %s " % tableName
        insertStr += str(tuple(columnNameLst)).replace("'", '')
        s = []
        for i in range(len(columnNameLst)):
            s.append(r'%s')
        insertStr = insertStr + " VALUES " + str(tuple(s)).replace("'", '')
        self.mInsertStr = insertStr
        
    
        
    def insert2DB(self, dataLst):
        libs.util.logger.Logger.getInstance().debugLog("Enter insert2DB")
        
        if self.mInsertStr is None:
            libs.util.logger.Logger.getInstance().errorLog("Don't call buildInsertData!")
            return None
        
        cur = self.mContext.cursor()
        
        try:
            cur.execute(self.mInsertStr, tuple(dataLst))
        except Exception as e:
            libs.util.logger.Logger.getInstance().errorLog(e)
            
        self.mContext.commit()
        cur.close()
        
    def createTable(self, strCreate):
        cur = self.mContext.cursor()
        
        cur.execute(strCreate)
        
        self.mContext.commit()
        cur.close()
        
    def isExistByDayChannel(self, dayStr, channelName):
        totalLine = 0
        q = "select event_day, task_type from yi_log_monitor  WHERE event_day = %s and task_type = %s"
        columnValue = (dayStr, channelName)
        
        cur = self.mContext.cursor()
        
        cur.execute(q, columnValue)
        for (eventDayColumn, litt) in cur:
            totalLine += 1
        if totalLine >= 1:
            return True
        else:
            return False
    
    def updateTable(self, tableName, columnNameLst, valueNameLst, whereNameLst, whereValueLst):

        #sentence = "UPDATE %s SET reason_describe = '2', operation_ctime = '3' WHERE event_day = '2014011'" % (tableName, )
        
        #sentence = "UPDATE %s SET reason_describe = 'xx',  operation_ctime = 0x0b WHERE event_day = 20140101" % (tableName, )
        #print(sentence)
        
        libs.util.logger.Logger.getInstance().debugLog(str(columnNameLst))
        libs.util.logger.Logger.getInstance().debugLog(str(valueNameLst))
        libs.util.logger.Logger.getInstance().debugLog(str(whereNameLst))
        libs.util.logger.Logger.getInstance().debugLog(str(whereValueLst))
        
        sentence = "update %s" % tableName
        sentence = sentence + " set "
        
        l = len(columnNameLst)
        if l > len(valueNameLst):
            l = len(valueNameLst)
        lst = []
        for i in range(l):
            lst.append('='.join((columnNameLst[i], r"'" + valueNameLst[i] + r"'")))
        sentence = sentence + ','.join(lst) + ' where '
        #print(sentence)
        
        l = len(whereNameLst)
        if l > len(whereValueLst):
            l = len(whereValueLst)
        lst = []
        for i in range(l):
            lst.append('='.join((whereNameLst[i], r"'" + whereValueLst[i] + r"'")))
        sentence = sentence + ' and '.join(lst)
        
        print(sentence)

        cur = self.mContext.cursor()
        
        cur.execute(sentence, None)
        
        self.mContext.commit()
        
        cur.close()

if __name__ == "__main__":
    
    libs.util.logger.Logger.getInstance().debugLog('123')
    
    
    #resultMap = {}
    #resultMap[('20130901', '官方', 'HUAWEI C8812', 'R33')] = [1, 0, 0, 0, 0]
    #columnNameLst = ['event_day', 'producename','modelname', 'swv', 'daily_active', 'daily_new', 'update_count', 'remain_1', 'update_remain_1', 'ctime']
    #dbManager = DBManager('cl_ubc_w', 'chunlei123', '10.23.243.56', '8806', 'ns_crm_service')
    #dbManager.isExistByDay('20131010')
    #dbManager.buildInsertStr(columnNameLst, 'rom_version_stat')
    #dbManager.insert2DB(resultMap, '20130901')
#x = buildInsertStr(['a', 'b', 'c'], 'py')
#print(x)
           
        
