import threading
from threading import Thread

class write_thread(threading.Thread):
    def init(self, queue):
        pass
    

def func(i, j):
    print(i)
    print(j)

if __name__ == "__main__": 
    x = {1:2}
    y = x
    a = write_thread(target=func, args=(x, y))
    a.start()

    #print(x)
    #print(y)