#!/usr/bin/env python3
from ctypes import *
import os
import os.path
import pandas as pd
import numpy as np
import threading

dll = CDLL(r'D:\svinsight\util\EI_FileProc.dll')

def __ei2csv(filename):
    dll.ConvertFile(create_string_buffer(
        bytes(filename, encoding='utf8')), dll.InitEnbMonitorThreadHandle())

def ei2csv(directory, eifiles):
    max_thread = 8
    complete = 0
    for i in np.arange(0, len(eifiles), max_thread):
        threadlist = []
        for eifile in eifiles[i:i+max_thread]:
            thread = threading.Thread(target=__ei2csv, args=(directory + '\\' + eifile, ))
            threadlist.append(thread)
            thread.start()
                
        for thread in threadlist:
            thread.join()
            complete = complete + 1
            print('processed %d of total %d files' %(complete, len(eifiles)))

    print("!!Done!!")


if __name__ == '__main__':
    directory = r"D:\svlog\slice"
    files = pd.Series(os.listdir(directory))
    eifiles = list(files[files.apply(lambda x: x.endswith(r'.ei') and x.rsplit('.')[0]+r'.csv' not in files)])  
    ei2csv(directory, eifiles)  
