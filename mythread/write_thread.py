import threading
from threading import Thread
from queue import Queue
from dbmanager.pf_device_collection_manager import PFDeviceCollectionManager
import queue

class write_thread(threading.Thread):
    __queue = Queue()
    
    def __init__(self):
        threading.Thread.__init__(self) 
        self.interrupt_flag = False
        pass
    
    def set_interrupt(self):
        self.interrupt_flag = True
        
    @staticmethod
    def get_queue():
        return write_thread.__queue
    
    @staticmethod
    def write_to_queue(collection_name, cur, is_insert):
        write_thread.__queue.put((collection_name, cur, is_insert))
    
    @staticmethod
    def read_from_queue():
        return write_thread.__queue.get(timeout = 60)
    
    def run(self):
        
        deviceProfileManager = PFDeviceCollectionManager()
        
        while True:
            try:
                if self.interrupt_flag == True and write_thread.get_queue().empty():
                    break
                collection_name, cur, is_insert = write_thread.read_from_queue()
                deviceProfileManager.insertOrUpdateUser(cur, is_insert)
                write_thread.__queue.task_done()
            except queue.Empty as e:
                if self.interrupt_flag == True:
                    break

if __name__ == "__main__": 
    x = {1:2}
    y = x
    #a = write_thread(target=func, args=(x, y))
    #a.start()

    #print(x)
    #print(y)