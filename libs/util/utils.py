import datetime
import time

def getNextDate(strSrcDate, intStep, forward):
    ''' Get the target day by strSrcDate, intStep is diff days, forward is direction.'''
    dateTarget = None 
    dateSrc = datetime.date(int(strSrcDate[0:4]), int(strSrcDate[4:6]), int(strSrcDate[6:8]))
    durationDelta = datetime.timedelta(days = intStep)
    if forward > 0:
        dateTarget = dateSrc + durationDelta
    else:
        dateTarget = dateSrc - durationDelta
    return dateTarget.strftime('%Y%m%d') 
 
def getStrToday(): 
    return datetime.datetime.now().strftime('%Y%m%d')

def getStrCurrentTime():
    stamp = int(time.time())
    #datetime.datetime.fromtimestamp(stamp)
    return str(stamp)

def getUid(line):
    return line.strip().split(',')[2]

def getAppId(line):
    return line.strip().split(',')[1]

def getIMEI(line):
    return line.strip().split(',')[3]

def isStrEmpty(s):
    if s is None or len(s) == 0:
        return True
    else:
        return False
    
    
