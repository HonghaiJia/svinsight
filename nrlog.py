# coding=utf-8
import numpy as np
import pandas as pd
import datetime
import os
import const
from ue import Ue
from cell import Cell

class NrLog(object):
    ''' NR调度模块Log分析接口类

        主要提供如下3类功能：
        a) 信息呈现
        b）问题发现
        c）问题定位
        要求所有文件命名符合EI命名格式：子系统_时间.csv
    '''

    def __init__(self, directory, time_interval=None):
        '''初始化Log实例,把所有Log按照类型分类

           Args:
               directory: Log所在目录
               time_interval: 时间范围[start, end],格式为yyyymmddhhmmss
        '''
        if time_interval:
            assert(len(time_interval)==2)
            assert(pd.to_datetime(time_interval[0]) <= pd.to_datetime(time_interval[1]))
            
        self._directory = directory
        self._logfiles={}
        self._time_interval = pd.to_datetime(time_interval) if time_interval else None
        self._cells = {}
        self._cellids = set()
        self._ues = {}
        self._cell_and_ue_ids = pd.DataFrame()
        for filetype in const.NR_FILE_TYPES:
            filenames = self._filenames_of_type(filetype)
            if filenames:
                logfile = NrFile(filetype, directory, filenames)
                if logfile.lines == 0:
                    continue
                self._logfiles[filetype] = logfile
                self._cellids = set.union(self._cellids, logfile.cellids)
                self._cell_and_ue_ids = pd.concat([logfile._cell_and_ue_ids, self._cell_and_ue_ids]).drop_duplicates()

    @property
    def directory(self):
        return self._directory

    def _filenames_of_type(self, filetype):
        '''获取指定文件类型的所有文件名
            Args：
                filetype：文件类型
                time_interval: 时间范围[start, end],格式为yyyymmddhhmmss
            Returns:
                文件名列表
        '''
        names_of_filetype = []
        for name in np.sort(os.listdir(self._directory)):
            if not name.endswith(r'.csv'):
                continue
            if -1 == name.find(filetype):
                continue
            if self._time_interval is not None:
                time = pd.to_datetime(name.rsplit('.')[0].rsplit('_')[-1])
                if time < self._time_interval[0] or time > self._time_interval[1]:
                    continue
            names_of_filetype.append(name)
        return names_of_filetype

    def describle(self):
        '''当前目录下相关Log文件总体描述，每类Log文件合并为一个文件

           输出文件名，大小，行数，时间范围，airtime范围等，每个Log文件一列
        '''
        df = pd.DataFrame()
        for type, logfile in self._logfiles.items():
            df.at[type, 'size'] = logfile.size
            df.at[type, 'num_of_files'] = len(logfile.files)
            df.at[type, 'num_of_lines'] = logfile.lines
            df.at[type, 'starttime'] = logfile.times[0]
            df.at[type, 'endtime'] = logfile.times[1]
        df.index.name = 'filename'
        return df

    def get_cell(self, cellid):
        '''获取小区实例
            Args：
                cellid：小区id
            Returns:
                对应的小区实例
        '''
        
        if cellid in self._cellids:
            if cellid not in self._cells.keys():
                self._cells[cellid] = Cell(cellid, self)
            return self._cells[cellid]
        else:
            return '非法CellId值，此小区不存在'
        
    def get_ue(self, uegid, cellid=None):
        '''获取小区实例
            Args：
                uegid：uegid
            Returns:
                对应的UE实例
        '''
        
        if cellid:
            if (cellid == self._cell_and_ue_ids['CellId']).any():
                self._cells[cellid] = Cell(cellid, self)
                return self._cells[cellid].get_ue(uegid)
            else:
                return '非法cellid, uegId值，此小区或ue不存在'
        else:
            if (uegid == self._cell_and_ue_ids['UEGID']).any():
                dllog = self._get_dlschd_logfile(uegid=uegid)
                ullog = self._get_ulschd_logfile(uegid=uegid)
                self._ues[uegid] = Ue(ullog, dllog, None, uegid)
                return self._ues[uegid]
            else:
                return '非法ueId值，此小区不存在'
        
    def get_cell_and_ue_ids(self):
        '''获取小区实例
            Args：
                None
            Returns:
                Log中的所有CellId,UeId
        '''

        return self._cell_and_ue_ids


    def _get_schd_logfile(self, filetype, cellid=None, uegid=None):
        '''获取Log文件实例'''

        if filetype not in self._logfiles:
            return None
        
        id_filter = {}     
        if cellid:
            id_filter.update({'CellId': [cellid]})     
        if uegid:
            id_filter.update({'UEGID': [uegid]})
         
        return NrFile(filetype, self._directory, self._filenames_of_type(filetype), id_filter=id_filter)

    def get_dlschd_logfile(self, cellid=None, uegid=None):
        '''获取Log文件实例'''
        
        filetype = const.NR_FILE_DLSCHD
        assert(filetype in self._logfiles)         
        id_filter = {}     
        if cellid:
            id_filter.update({'CellId': [cellid]})     
        if uegid:
            id_filter.update({'UEGID': [uegid]})
         
        return NrFile(filetype, self._directory, self._filenames_of_type(filetype), id_filter=id_filter)

    def get_ulschd_logfile(self, cellid=None, uegid=None):
        '''获取Log文件实例'''
        
        filetype = const.NR_FILE_ULSCHD
        assert(filetype in self._logfiles)         
        id_filter = {}     
        if cellid:
            id_filter.update({'CellId': [cellid]})     
        if uegid:
            id_filter.update({'UEGID': [uegid]})
         
        return NrFile(filetype, self._directory, self._filenames_of_type(filetype), id_filter=id_filter)
        
class NrFile(object):
    '''Log文件接口类'''

    def __init__(self, filetype, directory, files, id_filter=None):
        '''初始化Log实例,把所有Log按照类型分类

           Args:
               file: 文件名
               filetype: log类型
        '''
        self._files = files
        self._type = type
        self._directory = directory
        self._id_filter = id_filter
        self._time_filter = None
        self._size = sum([os.path.getsize(os.path.join(directory, file)) for file in files])
        self._times = [-1, -1]
        self._lines = 0
        self._cellids = set()
        self._uegids = set()
        self._cell_and_ue_ids = pd.DataFrame()

        cols = ['LocalTime']
        for data in self.gen_of_cols(cols, format_time=True):
            if len(data.index) == 0:
                self._lines += 0
                continue
            self._lines = self._lines + len(data.index)
            if self._times[0] == -1:
                self._times[0] = data.iat[0, 0]
            self._times[1] = data.iat[-1, 0]

        cols = ['CellId', 'UEGID']
        for data in self.gen_of_cols(cols):
            if len(data.index) == 0:
                continue
            self._cellids = set.union(self._cellids, set(data[cols[0]]))
            self._uegids = set.union(self._uegids, set(data[cols[1]]))
            self._cell_and_ue_ids = pd.concat([data[cols].drop_duplicates(), self._cell_and_ue_ids]).drop_duplicates()

    @property
    def cellids(self):
        '''获取所有小区ID'''
        return self._cellids

    @property
    def uegids(self):
        '''获取所有UEGID'''
        return self._uegids
    
    @property
    def cell_and_ue_ids(self):
        '''获取所有UEGID,CellGid'''
        return self._cell_and_ue_ids

    @property
    def type(self):
        return self._type

    @property
    def files(self):
        return self._files

    @property
    def size(self):
        return self._size

    @property
    def id_filter(self):
        return self._id_filter

    @property
    def lines(self):
        '''获取文件总行数'''
        return self._lines

    @property
    def times(self):
        '''PC时间范围'''
        return tuple(self._times)


    def _format_time(self, data, datestr):
        col = 'LocalTime'
        def __retime(timestr):
            y, m ,d = datestr[:4], datestr[4:6], datestr[6:8]
            h, mi, s, ms = timestr.replace(' ', '').split(":")
            return datetime.datetime(int(y), int(m), int(d), int(h), int(mi), int(s), int(ms)*1000)
        data[col] = data[col].apply(__retime)
        return data

    def gen_of_cols(self, cols=None, val_filter=None, format_time=False):
        '''获取指定列的生成器
            Args：
                cols: 列名列表，如果为None，表示获取全部列
                col_val_filter: 过滤条件，字典格式{'colname': [val1,]}
            Yields:
                生成器格式
        '''

        if format_time:
            assert('LocalTime' in cols)

        filters = {}
        if val_filter:
            filters.update(val_filter)
        if self._id_filter:
            filters.update(self._id_filter)

        if cols is not None:
            cols = list(set.union(set(filters), set(cols)))

        for file in self._files:
            filename = os.path.join(self._directory, file)
            data = pd.read_csv(filename, na_values='-', usecols=cols)
            if format_time:
                datestr = file.rsplit('.')[0].rsplit('_')[-1]
                self._format_time(data, datestr)

            if not filters:
                yield data
                continue

            mask = data[list(filters.keys())].isin(filters).all(1)
            if cols is not None:
                yield data[mask][cols]
            else:
                yield data[mask]
            
    def get_data_of_cols(self, cols, val_filter=None):
        '''获取指定cols的数据
            Args：
                cols: 列名列表
                col_val_filter: 过滤条件，字典格式{'colname': [val1,]}
            Returns:
                数据，DataFrame格式
        '''
        rlt = pd.DataFrame()
        for data in self.gen_of_cols(cols=cols, val_filter=val_filter):
            rlt = pd.concat([rlt, data])
        return rlt

    def mean_of_cols(self, cols, time_bin=1, filters=None):
        '''按照时间粒度计算指定列的平均值

            Args:
                cols:待汇总的列名
                time_bin：时间粒度（s)
                filters：滤波条件，字典格式{‘列名0’：值， ‘列名1’：值...}
        '''
        
        rlt = pd.DataFrame()
        time_col = 'LocalTime'
        if time_col not in cols:
            cols.append(time_col) 
        for data in self.gen_of_cols(cols, val_filter=filters, format_time=True):
            rlt = pd.concat([rlt, data])

        rlt = rlt.set_index(data[time_col]).drop(time_col)
        rlt = rlt.resample(str(time_bin)+'S').apply('mean')  
        return rlt

    def sum_of_cols(self, cols, time_bin=1, filters=None):
        '''按照时间粒度计算指定列的总和

            Args:
                cols:待汇总的列名
                time_bin：时间粒度（s)
                filters：滤波条件，字典格式{‘列名0’：值， ‘列名1’：值...}
        '''
        time_col = 'LocalTime'
        if time_col not in cols:
            cols.append(time_col) 
        rlt = pd.DataFrame()
        for data in self.gen_of_cols(cols, val_filter=filters, format_time=True):
            rlt = pd.concat([rlt, data])

        rlt = rlt.set_index(time_col)
        rlt = rlt.resample(str(time_bin)+'S').apply('sum') 
        return rlt
        
    def min_of_cols(self, cols, time_bin=1, filters=None):
        '''按照时间粒度计算指定列的最小值

            Args:
                cols:待汇总的列名
                time_bin：时间粒度（s)
                filters：滤波条件，字典格式{‘列名0’：值， ‘列名1’：值...}
                time_col: 聚合的时间列名，默认‘AirTime’
        '''
        time_col = 'LocalTime'
        if time_col not in cols:
            cols.append(time_col) 
        rlt = pd.DataFrame()
        for data in self.gen_of_cols(cols, val_filter=filters, format_time=True):
            rlt = pd.concat([rlt, data])
        rlt = rlt.set_index(time_col)
        rlt = rlt.resample(str(time_bin)+'S').apply('min') 
        return rlt
        
    def max_of_cols(self, cols, time_bin=1, filters=None):
        '''按照时间粒度计算指定列的最大值

            Args:
                cols:待汇总的列名
                time_bin：时间粒度（s)
                filters：滤波条件，字典格式{‘列名0’：值， ‘列名1’：值...}
                time_col: 聚合的时间列名，默认‘AirTime’
        '''
        time_col = 'LocalTime'
        if time_col not in cols:
            cols.append(time_col) 
        rlt = pd.DataFrame()
        for data in self.gen_of_cols(cols, val_filter=filters, format_time=True):
            rlt = pd.concat([rlt, data])
        rlt = rlt.set_index(time_col)
        rlt = rlt.resample(str(time_bin)+'S').apply('max') 
        return rlt

    def cnt_of_cols(self, cols, time_bin, filters=None):
        '''按照时间粒度计算指定列的次数

            Args:
                time_bin：时间粒度（s), 0表示不区分时间粒度
                filters：滤波条件，字典格式{‘列名0’：值， ‘列名1’：值...}
        '''
        time_col = 'LocalTime'
        if time_col not in cols:
            cols.append(time_col) 
        rlt = pd.DataFrame()
        for data in self.gen_of_cols(cols, val_filter=filters, format_time=True):
            rlt = pd.concat([rlt, data])
        rlt = rlt.set_index(time_col)
        rlt = rlt.resample(str(time_bin)+'S').apply('cnt') 
        return rlt

    def hist_of_col(self, col, time_bin=1, ratio=False, filters=None):
        '''按照时间粒度计算指定列的直方图数据

            ratio: 是否计算比例, 默认Ratio为True
            time_bin：时间粒度（s)
            filters：滤波条件，字典格式{‘列名0’：值， ‘列名1’：值...}
        '''
        time_col = 'LocalTime'
        col_name = col[0]
        if time_col not in col:
            col.append(time_col) 
        rlt = pd.DataFrame()
        for data in self.gen_of_cols(col, val_filter=filters, format_time=True):
            rlt = pd.concat([rlt, data])

        rlt = rlt.set_index(time_col)
        rlt = rlt[col_name]
        rlt = rlt.resample(str(time_bin)+'S').apply(lambda x: x.value_counts()).unstack()
        if ratio:
            rlt = rlt.apply(lambda x: x/x.sum(), axis=1)
        rlt.columns.name = col_name
        return rlt

if __name__ == '__main__' :
    svlog = NrLog(r"D:\问题分析\FAPI+EI+200UE")
    cell = svlog.get_cell(1)
    dl = cell.dl
    dl.schdfail_reasons()