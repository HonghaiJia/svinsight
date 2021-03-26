#!/usr/bin/env python3
from ctypes import *
import os
import os.path
import pandas as pd
import threading

dll = CDLL(r'D:\svinsight\util\EI_FileProc.dll')

def ei2csv(filename):
    print("ei2csv start of file: %s " %(filename))
    dll.ConvertFile(create_string_buffer(bytes(filename, encoding='utf8')), dll.InitEnbMonitorThreadHandle())
    print("ei2csv end of file: %s " %(filename))

if __name__ == '__main__' :
    directory = r"D:\sv\20210322"
    eifiles = pd.Series(os.listdir(directory))
    eifiles = eifiles[eifiles.apply(lambda x: x.endswith(r'.ei'))]

    for name in eifiles:   
        csvname = name.rsplit('.')[0] + r'.csv'
        if csvname in os.listdir(directory):
            continue
        thread = threading.Thread(target=ei2csv, args = (directory + '\\' + name, ))
        thread.start()