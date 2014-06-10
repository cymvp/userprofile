#!D:\WorkSpace\Python33\python.exe
import platform
import time
import libs.redis.redis_manager
import libs.util.logger
import libs.util.my_utils
import sys
import config.const

def lpush(name, *values):
    "Push ``values`` onto the head of the list ``name``"
    return execute_command('LPUSH', name, *values)

#### COMMAND EXECUTION AND PROTOCOL PARSING ####
def execute_command(self, *args, **options):
    command_name = args[0]

if __name__ == "__main__":
    
    #lpush("sadd", 'big', (1,2,3))
    
    lineNumber = 0
    errorNumber = 0
    
    libs.util.logger.Logger.getInstance().setLogFilePrefixName('')
    libs.util.logger.Logger.getInstance().setLogFileSurfixName('_importodmdata')
    
    if len(sys.argv) < 2:
        sys.exit()
    
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    recordTime.startTime()
    
    f_odm_file = libs.util.my_utils.openFile(sys.argv[1], 'r')
    #f_odm_file = [1]   
    redisManager = libs.redis.redis_manager.redis_manager(config.const.CONST_SERVER_ADDR, config.const.CONST_QUEUE_NAME, False)
    
    for line in f_odm_file:
        #line = 'o123\tabcde'
        arr = line.split('\t')
        if len(arr) < 2:
            errorNumber += 1
            continue
        
        if errorNumber > 100:
            libs.util.logger.Logger.getInstance().debugLog("error count is to much! so I quit.")
            break;
        
        lineNumber += 1
        if lineNumber % 100000 == 0:
            ellapsedTime = recordTime.getEllapsedTime()
            printProcess.printCurrentProcess(ellapsedTime, lineNumber)
        
        res = redisManager.sadd(arr[0].strip(), arr[1].strip())
        
        
        #res = False
        if res == False:
            errorNumber += 1    
            continue
