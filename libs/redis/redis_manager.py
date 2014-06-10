import platform
import redis
from libs.util.logger import Logger
import libs.util.my_utils
from threading import Timer, RLock
import time


    
part1_total_duration = 0
part2_total_duration = 0
part3_total_duration = 0
part4_total_duration = 0
part5_total_duration = 0

recordTime = libs.util.my_utils.RecordTime()
printProcess = libs.util.my_utils.PrintProcess('')

recordTime.startTime()

class redis_manager:
    #__server = ['10.209.70.17:6379',  '10.23.243.56:6379']
    #__queueName = 'mk_queue'
    
    def __init__(self, serverLst, queueNameLst, isDemon = True):
        self.mServer = serverLst
        self.mQueueNameLst = queueNameLst
        self.mRdsDict = None
        self.mUpdateRdsTimer = None
        self.mSelectRdsTimer = None
        self.mCurrentRds = None
        self.mCurrentQueue = None
        self.mRdsLock = RLock()
        self.__connectRedis(isDemon)
    
    def stopDemon(self):
        self.mSelectRdsTimer.stopTimer()
        self.mUpdateRdsTimer.stopTimer()
    
    def __connectRedis(self, isDemon = True):
        rds_dict = {}
        for s in self.mServer:
            if ':' in s:
                server, port = s.split(':')
                port = int(port, 10)
            else:
                server = s
                port = 6379
            rds = redis.StrictRedis(server, port, socket_timeout=2)
            rds_dict[rds] = [s, 1]

        self.mRdsDict = rds_dict
        
        if isDemon == True:
            self.mSelectRdsTimer = TimerWorker(self.__selectRds)
            self.mSelectRdsTimer.startTimer()
            
            self.mUpdateRdsTimer = TimerWorker(self.__updateRdsStatus)
            self.mUpdateRdsTimer.startTimer()
            
        self.__updateRdsStatus()
        self.__selectRds()
        
        Logger.getInstance().debugLog("Start connecting redis Server.")
    
    def sadd(self, k, v_list):
        res = False
        i = 5
        with self.mRdsLock:
            if self.mCurrentRds is not None:
                try:
                    while i > 0:
                        i -= 1
                        #Logger.getInstance().debugLog("push to rds: %s" % self.mRdsDict[self.mCurrentRds][0])
                        res = self.mCurrentRds.sadd(k, v_list)
                        if res != None:
                            res = True
                            break;
                        time.sleep(1)         
                except redis.exceptions.ConnectionError:
                    Logger.getInstance().debugLog("error happened when sadd value, so we forced select other redis.")
                    res = False
        return res

    
    def push(self, k, b):
        #Logger.getInstance().debugLog("Enter push line method.")
        res = False
        i = 5
        with self.mRdsLock:
            if self.mCurrentRds is not None:
                try:
                    while i > 0:
                        i -= 1
                        #Logger.getInstance().debugLog("push to rds: %s" % self.mRdsDict[self.mCurrentRds][0])
                        res = self.mCurrentRds.lpush(k, b)
                        if res != None:
                            res = True
                            break;
                        time.sleep(1)         
                except redis.exceptions.ConnectionError:
                    Logger.getInstance().debugLog("error happened when push line, so we forced select other redis.")
                    res = False
        return res

    def pop(self, isBlock = False):
        
        global part1_total_duration
        global part2_total_duration
        global part3_total_duration
        global part4_total_duration
        global part5_total_duration
        
        line = None
        i = 10
        #Logger.getInstance().debugLog("Enter pop line method.")
        with self.mRdsLock:
            if self.mCurrentRds is not None and self.mCurrentQueue is not None:
                try:
                    while i > 0:
                        i -= 1
                        part1_total_duration += recordTime.getEllaspedTimeSinceLast()
                        #Logger.getInstance().debugLog("pop from rds: %s" % self.mRdsDict[self.mCurrentRds][0])      
                        if isBlock is False:
                            line = self.mCurrentRds.lpop(self.mCurrentQueue)
                        else:
                            line = self.mCurrentRds.blpop(self.mCurrentQueue)
                        part2_total_duration += recordTime.getEllaspedTimeSinceLast()
                        try: 
                            if line is None:
                                Logger.getInstance().debugLog("queue is empty.") 
                                break
                            #b = b'\x01\x33\xfa' => not utf-8.                     
                            line = line.decode('utf-8')
                        except :
                            Logger.getInstance().debugLog("abandon data:" + str(line))
                            line = None
                            Logger.getInstance().errorLog("line is not utf-8, abandon it and re pop again.")
                        if line is not None:
                            #Logger.getInstance().debugLog("pop correct line:")
                            #Logger.getInstance().debugLog("data:" + line)
                            break
                except redis.exceptions.ConnectionError:
                    Logger.getInstance().debugLog("error happened when pop line, so we forced select other redis.")
                    line = None 
                    self.__selectRds()   
            #Logger.getInstance().debugLog("exit pop()!")
            part3_total_duration += recordTime.getEllaspedTimeSinceLast()
            return line
    
    def printTime(self, isBlock = False):
        libs.util.logger.Logger.getInstance().debugLog("ztotal time of part1 is: %.3fs ." % part1_total_duration)
        libs.util.logger.Logger.getInstance().debugLog("ztotal time of part2 is: %.3fs ." % part2_total_duration)
        libs.util.logger.Logger.getInstance().debugLog("ztotal time of part3 is: %.3fs ." % part3_total_duration)
    
    #Should add Lock.
    def __selectRds(self):
        with self.mRdsLock:
            maxQueueLength = -1
            for rds, value in self.mRdsDict.items():
                # skip offline redis server
                if value[1] == 0: 
                    continue
                queueName, queueLength = self.__selectQueue(rds)
                if queueName is None:
                    Logger.getInstance().debugLog("redis server: %s is down." % value[0])
                    self.__setRdsStatus(rds, 0)
                else:
                    if maxQueueLength < queueLength:
                        maxQueueLength = queueLength
                        self.mCurrentRds = rds
                        self.mCurrentQueue = queueName
                Logger.getInstance().debugLog("select one redis server and queue: %s-%s-%d" % (value[0], queueName, queueLength))

    def __selectQueue(self, rds):
        maxQueueLength = -1
        selectedQueueName = None
        if rds is None:
            return (selectedQueueName, maxQueueLength)
        for name in self.mQueueNameLst:
            try:
                queueLength = rds.llen(name)
                if queueLength > maxQueueLength:
                    maxQueueLength = queueLength 
                    selectedQueueName = name
            except redis.exceptions.ConnectionError:
                Logger.getInstance().debugLog("error happened when select queue")
                maxQueueLength = 0
                selectedQueueName = None
                break
        return (selectedQueueName, maxQueueLength)
   
    def __setRdsStatus(self, rds, status):
        """ set rds's status. 0 - offline, 1 - online """
        with self.mRdsLock:
            self.mRdsDict[rds][1] = status  
                          
    def __updateRdsStatus(self):
        with self.mRdsLock:
            for rds in self.mRdsDict:
                try:
                    if rds.ping():
                        Logger.getInstance().debugLog("server %s is OK" % self.mRdsDict[rds][0])
                        self.mRdsDict[rds][1] = 1
                    else:
                        Logger.getInstance().debugLog("server %s is not OK" % self.mRdsDict[rds][0])
                        self.mRdsDict[rds][1] = 0
                except redis.exceptions.ConnectionError:
                    Logger.getInstance().debugLog("In update redis status: redis server: %s is down." % self.mRdsDict[rds][0])
                    self.mRdsDict[rds][1] = 0     

class TimerWorker:
    def __init__(self, workObject, interval = 10):
        self.mWorkObj = workObject
        self.mInterval = interval
        self.mTimer = None
        self.mIsRunning = False
        self.mStop = False
    def startTimer(self):
        #if self.mTimer and self.mTimer.is_alive():
        if self.mIsRunning == True:
            return
        self.mTimer = Timer(self.mInterval,  self.__repeatTimer, args=["123"])
        self.mTimer.start()
    def __repeatTimer(self, args):
        self.mIsRunning = False
        self.mWorkObj()
        if self.mStop == False:     
            self.startTimer()
    def stopTimer(self):
        self.mStop = True
     
#redisManager = redis_manager(__server, __queueName)            
    
