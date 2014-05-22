#coding:utf-8
import sys
import config.const
import libs.redis.redis_manager
import util.utils
import libs.util.my_utils
from libs.util.logger import Logger
import gzip

def handle_data(line,  metricMap):
    '''
    metricCollector is : 
    {
        8: ('timestamp',), 
        17: ('collect_date',), 
        18: ('launch_count',), 
        19: ('duration',), 
        12: ('packagename',)
    }
    '''
    arr = util.utils.isValidLine(line) 
    
    if arr == None:
        return None
    
    # '0x511' -> 0x511
    metricId = int(arr[0], 16)
    #4096
    appId = int(arr[1])
    metricCollector = metricMap.get((appId,metricId))
    #This metricId is not the candidate.
    if metricCollector is None:
        return None
    
    return ((arr[1], arr[0]))

#python import_raw_data.py D:\mongodb\p_data\ubc_100 D:\Cloud\WorkSpace\profile\config\MetricMap_part.txt
if __name__ == "__main__":  # python3 import_raw_data.py chunlei_ubc_data_20140430 metricmap.txt
    
    #Logger.getInstance().setDirectToFile(False)

    if len(sys.argv) != 3:
        print(r'Error params...... python import_raw_data.py D:\mongodb\p_data\ubc_100 ./config/metric_map_metrics.txt')
        sys.exit(1)
        
    fMetricMap = libs.util.my_utils.openFile(sys.argv[2], 'r')
    
    #sys.argv[1] = 'd:/chunlei_ubc_20140512.gz'
    fInputFile = gzip.open(sys.argv[1], 'rt', encoding = 'utf-8')
    #fInputFile = gzip.open(sys.argv[1], 'rb')
    #fInputFile = libs.util.my_utils.openFile(sys.argv[1], 'r')
    
    metricMap = util.utils.loadMap(fMetricMap)
    
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    redisManager = libs.redis.redis_manager.redis_manager(config.const.CONST_SERVER_ADDR, config.const.CONST_QUEUE_NAME, False)
    
    recordTime.startTime()
    
    #lines = fInputFile.readlines()
    
    lineNumber = 0
    errorNumber = 0
    try:
        fInputFile.readline()
        fInputFile.readline()
        fInputFile.readline()
    except  Exception as e:
        print(e)
    for line in fInputFile:
        lineNumber += 1
        if lineNumber % 100000 == 0:
            ellapsedTime = recordTime.getEllapsedTime()
            printProcess.printCurrentProcess(ellapsedTime, lineNumber)
        if errorNumber > 100:
            break;
        tupl = handle_data(line,  metricMap)
        if tupl is None:
            continue
        res = redisManager.push(config.const.CONST_QUEUE_NAME[0], line)
        #res = False
        if res == False:
            errorNumber += 1    
            continue
    #except Exception as e:
    ellapsedTime = recordTime.getEllapsedTime()
    printProcess.printFinalInfo(ellapsedTime)            

    
