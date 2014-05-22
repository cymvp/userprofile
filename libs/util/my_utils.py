import sys
import time
import os
import libs.util.logger

def openFile(srcFile, mode):
    if sys.hexversion > 0x3020000:
        f = open(srcFile, mode, 8192, "utf-8")
    else:
        f = open(srcFile, mode, 8192)    
    return f

def openWriteFile(srcFileDir, desFileName, desFilePath = None, m = 'w'):
    srcPath, srcName = os.path.split(srcFileDir)
    if desFilePath is None:
        desPath = srcPath
    else:
        desPath = desFilePath
        
    fout = openFile(os.path.join(desPath, desFileName), m)
    
    return fout

class RecordTime:
    def __init__(self):
        self.mStartTime = 0
        self.mLastTime = 0
    def startTime(self):
        self.mStartTime = time.time()
        self.mLastTime = self.mStartTime
    def getEllapsedTime(self):
        self.mLastTime = time.time()
        return self.mLastTime - self.mStartTime
    def getEllaspedTimeSinceLast(self):
        last_time = self.mLastTime
        self.mLastTime = time.time()
        return self.mLastTime - last_time
    def stopTime(self):
        self.mStartTime = 0

class PrintProcess:
    def __init__(self, fileName):
        self.mProcessId = os.getpid()
        self.mFileName = fileName
    def printCurrentProcess(self, ellaspedTime, lines):
        libs.util.logger.Logger.getInstance().debugLog("[child %d]: processed %d lines of '%s', takes %.3fs ..." % (self.mProcessId, lines, self.mFileName, ellaspedTime))
        #print("  [child %d]: processed %d lines of '%s', takes %.3fs ..." % (self.mProcessId, lines, self.mFileName, ellaspedTime))
    def printFinalInfo(self, ellaspedTime):
        libs.util.logger.Logger.getInstance().debugLog("It taks %.3fs to complete all tasks." % (ellaspedTime))
        #print("It taks %.3fs to complete all tasks." % (ellaspedTime))