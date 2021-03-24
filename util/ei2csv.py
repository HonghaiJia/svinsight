#!/usr/bin/env python3
from ctypes import *
import os
import os.path

dll = CDLL(r'D:\svinsight\util\EI_FileProc.dll')
#handle = dll.InitEnbMonitorThreadHandle()

def ei2csv(*filename):
    dll.ConvertFile(create_string_buffer(bytes(filename[0], encoding='utf8')), dll.InitEnbMonitorThreadHandle())