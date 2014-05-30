import platform

IS_TEST = True

if platform.system() == 'Windows__xx':
    CONST_SERVER_ADDR = ['127.0.0.1:6379',]
else:
    if IS_TEST != True:
        CONST_SERVER_ADDR = ['10.209.70.17:6379',  '10.23.243.56:6379']
    else:
        CONST_SERVER_ADDR = ['redis02.baiyi.com:6379',]
        #CONST_SERVER_ADDR = ['10.209.70.17:6379',]
    
CONST_QUEUE_NAME = ['key_userprofile_l',]

if platform.system() == 'Windows_xxx':
    MONGODB_HOST = 'localhost'
    MONGODB_PORT = 27017
    MONGODB_DBNAME = 'ubc_database'
else:
    MONGODB_HOST = 'bj-ci-01.baiyi.com'
    MONGODB_PORT = 27017
    MONGODB_DBNAME = 'user_profile'
    
    #MONGODB_HOST = 'hz01-wise-store05107.hz01.baidu.com'
    #MONGODB_PORT = 27100
    #MONGODB_DBNAME = 'ubc_database'

    #MONGODB_HOST = 'localhost'
    #MONGODB_PORT = 27017
    #MONGODB_DBNAME = 'user_profile'

PERFORMANCE_TEST = True#False
if PERFORMANCE_TEST == True:    
    PERFORMANCE_DEVICE_COUNT = 10000
    PERFORMANCE_DEVICE_TAG_COUNT = 10000
else:
    PERFORMANCE_DEVICE_COUNT = -1
    PERFORMANCE_DEVICE_TAG_COUNT = -1
    
def get_actural_count(count, test_count):
    if test_count < 0:
        return count
    else:
        if count > test_count:
            return test_count
        else:
            return count


