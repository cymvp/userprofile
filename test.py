import sys
import dbmanager.pf_metric_collection_manager
import dbmanager.pf_device_collection_manager
import dbmanager.pf_tags_collection_manager
import dbmanager.pf_taguid_collection_manager
import util.utils
import libs.util.logger
import libs.util.my_utils
import util.calc_tag
import ubc.pf_metric_helper

from dbmanager.pf_device_collection_manager import PFDeviceCollectionManager
from dbmanager.pf_hwv_collection import PFHWVCollectionManager
from dbmanager.pf_province_collection import PFProvinceCollectionManager
from dbmanager.pf_app_collection import PFAPPCollectionManager
from dbmanager.pf_metric_collection_manager import PFMetricCollectionManager

class Singleton(object):  
    def __new__(cls, *args, **kw):  
        if not hasattr(cls, '_instance'):  
            orig = super(Singleton, cls)  
            cls._instance = orig.__new__(cls, *args, **kw)  
        return cls._instance  
  
class MyClass(Singleton):
    a = 1  
    
    def __init__(self):
        print('11')
  
#one = MyClass()  
#two = MyClass()  

#print(one.a)
#print(two.a)

if __name__ == "__main__":  # nDays,  mTop
    recordTime = libs.util.my_utils.RecordTime()
    printProcess = libs.util.my_utils.PrintProcess('')
    
    recordTime.startTime()
    '''
    i = 0
    while i < 10000:
        i += 1
        #print(i)
        metricHandler = ubc.pf_metric_helper.getMetricHandler('%x' % 1807,  '4096')
    
    ellapsedTime = recordTime.getEllaspedTimeSinceLast()
    print("part1 time is: %.3fx" % ellapsedTime)
    '''
    
    #sys.exit(1)
    
    tagManager = dbmanager.pf_tags_collection_manager.PFTagsCollectionManager()
    deviceManager = PFDeviceCollectionManager()
    #tagUidsManager = dbmanager.pf_taguid_collection_manager.PFTagUidsCollectionManager()
    
    max_num = 4000
        
    cursors = deviceManager.getAllDoc()
    curLst = []
    mx = max_num
    if cursors is not None:
        print(cursors.count())
        for cur in cursors:
            mx -= 1
            if mx < 0:
                break
            continue
            if mx == 1000:
                uid = cur['_id']
                print(uid)

            curLst.append(cur)
    else:
        print('..')
    ellapsedTime = recordTime.getEllaspedTimeSinceLast()
    print("part1 time is: %.3fx" % ellapsedTime)
    
    sys.exit(1)
    
    mx = max_num
    while mx > 0:
        c = deviceManager.getDocWithCondition({'_id':'00000000ztd96_xx'})
        #c = deviceManager.getDocWithCondition({'_id':'868232009378514'})
        if c is None or c.count() <= 0:
            pass
        #print('error')
        #deviceManager.get_linked_tag("A0000037D844D38")
        mx -= 1
    
    ellapsedTime = recordTime.getEllaspedTimeSinceLast()
    
    print("part2 time is: %.3fx" % ellapsedTime)
      

    
