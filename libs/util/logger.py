import os
import time
import libs.util.utils


class Logger:
    LOG_TYPE_E = 'E'
    LOG_TYPE_D = 'D'
    LOG_FILE_PATH = './python_log'
    class_Instance = None
    
    def __init__(self):
        self.__mDirectToFile = True
        self.__mLastDate = None
        self.__mLogFile = None
        self.__mLogFilePrefix = 'log_'
        self.__mLogFileSurfix = ''
        self.__tempBufferLine = 0
    
    def setLogFilePrefixName(self, str_prefix):
        self.__mLogFilePrefix = str_prefix
        
    def setLogFileSurfixName(self, str_surfix):
        self.__mLogFileSurfix = str_surfix
        
    def setDirectToFile(self, dirtectToFile = True):
        self.__mDirectToFile = dirtectToFile
    
    def debugLog(self, content):
        s = Logger.__getLogString(Logger.LOG_TYPE_D, content)
        self.__writeLog(s)
            
    def errorLog(self, content):
        s = Logger.__getLogString(Logger.LOG_TYPE_E, content)
        self.__writeLog(s)
        
    def __flushFile(self):
        if self.__mLogFile is not None:
            self.__mLogFile.flush()
            self.__tempBufferLine = 0

    def __writeLog(self, content):
        #Write to File;
        #return
        if self.__mDirectToFile == True:
            currentStrDate = libs.util.utils.getStrToday()
            if self.__mLastDate != currentStrDate:
                self.__closeFile()
                self.__openLogFile(currentStrDate)
            if self.__mLogFile is not None:
                self.__mLogFile.write(content + '\n')
                self.__tempBufferLine += 1
                if self.__tempBufferLine >= 1:
                    self.__flushFile()
        elif self.__mDirectToFile == False:
            print(content)
            
    def __openLogFile(self, strDate): 
        self.__closeFile()
        logStrFile = os.path.join(Logger.LOG_FILE_PATH, self.__mLogFilePrefix  + strDate + self.__mLogFileSurfix) 
        self.__mLastDate = strDate
        self.__mLogFile = open(logStrFile, 'a', 8192, "utf-8")
    
    def __closeFile(self):
        if self.__mLogFile is not None:
            self.__mLogFile.close()
            self.__mLogFile = None
            self.__mLastDate = None
            self.__tempBufferLine = 0
    
    @staticmethod
    def __getLogString(logType, content):
        return "[%s,%s] %s" % (time.strftime('%Y-%m-%d %H:%M:%S'),
                logType, content)
    
    @staticmethod
    def getInstance():
        if Logger.class_Instance is None:
            Logger.class_Instance = Logger()
        return Logger.class_Instance
    
#Logger.getInstance().setDirectToFile(False)    
Logger.getInstance().debugLog("Im am zhangyali!!!!!!")
#Logger.getInstance().setDirectToFile(False)
    
