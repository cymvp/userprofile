import sys
import libs.util.my_utils
import os

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
        
    def make_map(self, f, f1_column):
        result_map = {}
        for li in f:
            li = li.rstrip()
            arr = li.split('\t')
            temp_list = self.convertSomthing(arr)
            result_map[tuple(temp_list)] = li
        return result_map
	
def find_diff(f1_map, f2_map): # 在f2_map中查找f1_map的每个一元素，不在f2_map中的会列出来
    result_map = {}
    for l1 in f1_map:
        if f2_map.get(l1) is None:
            result_map[l1] =  f1_map[l1]
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
        fw.write(li + '\n')

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Error params......")
        sys.exit(1)
    
    strPath, strName = os.path.split(sys.argv[1])    
    f1 = libs.util.my_utils.openFile(sys.argv[1], 'r')
    f2 = libs.util.my_utils.openFile(sys.argv[2], 'r')
    fw = libs.util.my_utils.openWriteFile(sys.argv[1], strName + "result")
    f1_column = sys.argv[2].split('\t')
    f2_column = sys.argv[4].split('\t')
    if len(f1_column) != len(f2_column):
        print("Error params......")
        sys.exit(1)

    o1 = CompareObject(f1_column,f1)
    
    f1_map = o1.make_map(f1, f1_column)
    print("First map size is(unique): " + str(len(f1_map)))
    
    r1_map = find_diff_with_file(f1_map, f2, f2_column) 

    #print(r1_map)
    print_map(fw, r1_map) #将diff结果写入文件

    print("Difference of first map  size is(unique)" + str(len(r1_map)))



