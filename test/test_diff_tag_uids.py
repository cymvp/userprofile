#coding:utf-8
import sys
import os
import getopt
sys.path.append(os.path.abspath('./'))

print(sys.path)

import libs.util.my_utils

class CompareObject:
    def __init__(self, columnArr, f):
        self.mColumnArr = columnArr
        self.mF = f
        
    def convertSomthing(self, arr):
        temp_list = []
        for c in self.mColumnArr:
            try:
                t = arr[int(c)].strip()
                temp_list.append(t)   #有些情况下应该严格比较，应该区分大小写
            except:
                print(str(arr)) 
        return temp_list
        
    def make_map(self):
        result_map = {}
        for li in self.mF:
            li = li.rstrip()
            arr = li.split('\t')
            temp_list = self.convertSomthing(arr)
            result_map[tuple(temp_list)] = li
        return result_map
    
def find_diff(f1_map, f2_map): # 在f1_map中查找f2_map的每个一元素，不在f1_map中的会列出来
    result_map = {}
    for l2 in f2_map:
        if f1_map.get(l2) is None:
            result_map[l2] =  f2_map[l2]
    return result_map

def find_diff_with_file(f1_map, f, columnIndexArr):
    def make_map(arr, columnIndexArr):
        temp_list = []
        for c in columnIndexArr:
            t = arr[int(c)]
            #print(arr)
            temp_list.append(t)
        return temp_list
    resultLst = []
    for li in f:
        li = li.rstrip()
        arr = li.split('\t')
        temp_list = make_map(arr, columnIndexArr)
        if f1_map.get(tuple(temp_list)) is None:
            resultLst.append(li)
    return resultLst

def find_same_with_file(f1_map, f, columnIndexArr):
    def make_map(arr, columnIndexArr):
        temp_list = []
        for c in columnIndexArr:
            t = arr[int(c)]
            #print(arr)
            temp_list.append(t)
        return temp_list
    resultLst = []
    for li in f:
        li = li.rstrip()
        arr = li.split('\t')
        temp_list = make_map(arr, columnIndexArr)
        if f1_map.get(tuple(temp_list)) is not None:
            resultLst.append(f1_map[tuple(temp_list)])
    return resultLst    

def find_same(f1_map, f2_map): # 在f2_map中查找f1_map的每个一元素，在f2_map中的会列出来
    result_map = {}
    for l1 in f1_map:
        if f2_map.get(l1) is not None:
            result_map[l1] =  f1_map[l1]
    return result_map

def print_map(fw, f_map):
    for li in f_map:
        fw.write(f_map[li] + '\n')

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Error params......")
        sys.exit(1)
        
    try:
        opts, args = getopt.getopt(sys.argv[3:], "t:", [])
    except getopt.GetoptError:
        print("error! wrong input parameters")
        print(__doc__)
        sys.exit(1)
    
    g_type = 0
    for o, a in opts:
        if o in ('-t', ):
            g_type = a

    
    strPath, strName = os.path.split(sys.argv[1])    
    f1 = libs.util.my_utils.openFile(sys.argv[1], 'r')
    f2 = libs.util.my_utils.openFile(sys.argv[2], 'r')
    fw = libs.util.my_utils.openWriteFile(sys.argv[1], strName + "result")
    f1_column = [0]
    f2_column = [0]
    if len(f1_column) != len(f2_column):
        print("Error params......")
        sys.exit(1)

    o1 = CompareObject(f1_column,f1)
    o2 = CompareObject(f2_column,f2)
    
    f1_map = o1.make_map()
    f2_map = o2.make_map()
    
    print("First map size is(unique): " + str(len(f1_map)))
    print("Second map size is(unique): " + str(len(f2_map)))
    
    #0: union; 1: interset
    if g_type == 'or':
        r1_list = find_diff(f1_map, f2_map)
        r2_list = find_diff(f2_map, f1_map)
        same_list = find_same(f1_map, f2_map)
        print("Union of two map size is(unique): %d" % (len(r1_list) + len(r2_list) + len(same_list), ))
        print_map(fw, r1_list) #将diff结果写入文件
        print_map(fw, r2_list) #将diff结果写入文件
        print_map(fw, same_list) #将diff结果写入文件
    
    else:
        r1_list = find_same(f1_map, f2_map)
        print("Same of two map size is(unique): %d" % len(r1_list))
        print_map(fw, r1_list) #将diff结果写入文件

    #print("Difference of first map  size is(unique)" + str(len(r1_map)))
    #print("Same of first map  size is(unique)" + str(len(r1_map)))



