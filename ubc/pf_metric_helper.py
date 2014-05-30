from ubc.pf_metric_handle_general import *

g_handler_map = {}


def getMetricHandler(metricId,  appId):
    a = g_handler_map.get((metricId, appId))
    #a = None
    if a is None:
        try:
            a = eval(CONST_METRIC_HANDLER_PREFIX + str(appId) + '_' + str(metricId) + '()')
        except NameError:
            a = eval('PFMetricHandler_Appid_Metricid' +  '()')
        finally:
            g_handler_map[(metricId, appId)] = a
    return a


        
        
        
