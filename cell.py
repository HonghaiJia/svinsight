# coding=utf-8
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import const
from dlschd import DlSchdCell
from ue import Ue
from ulschd import UlSchdCell


class Cell(object):
    '''小区实例'''

    def __init__(self, cellid, log):
        '''初始化小区实例
            根据输入的信息，完成如下事情：
                a) 尝试推断上下行配比,有可能需要用户输入

            Args:
                cellid: 小区Id
        '''
        self._cellid = cellid
        self._log = log

        dl_log = log.get_dlschd_logfile(cellid=cellid)
        if dl_log is not None:
            self._dl = DlSchdCell(dl_log, self)

        ul_log = log.get_ulschd_logfile(cellid=cellid)
        if ul_log is not None:
            self._ul = UlSchdCell(ul_log, self)

        self._ues = {}
        self._uegids = self._get_uegids()

    @property
    def cellid(self):
        return self._cellid

    @property
    def log(self):
        return self._log

    @property
    def dl(self):
        return getattr(self, '_dl', None)

    @property
    def ul(self):
        return getattr(self, '_ul', None)

    @property
    def ni(self):
        return getattr(self, '_ni', None)

    def _get_uegids(self):
        '''获取本小区下的所有UEGID'''
        dluegid = self._dl.log.uegids if hasattr(self, '_dl') else set()
        uluegid = self._ul.log.uegids if hasattr(self, '_ul') else set()
        return set.union(dluegid, uluegid)

    def get_ue(self, uegid=None):
        '''获取小区实例
            Args：
                uegid：如果想查看Log中的所有uegid，那么不用赋值
            Returns:
                如果uegid不为None，返回对应的UE实例，否则返回Log中的所有uegid
        '''
        if uegid is None:
            return '所有UEGID：{uegids}. 请使用get_ue(uegid)获取对应的UE实例'.format(uegids=self._uegids)

        if uegid in self._uegids:
            dllog = self.log.get_dlschd_logfile(self.cellid, uegid)
            ullog = self.log.get_ulschd_logfile(self.cellid, uegid)
            self._ues[uegid] = Ue(ullog, dllog, self, uegid)
            return self._ues[uegid]
        else:
            return '非法uegid值，此ue不存在'

    def show_ue_livetime(self):
        cols = ['LocalTime', 'UEGID']

        logs = []
        if hasattr(self, '_dl'):
            logs.append(self.dl.log)
        if hasattr(self, '_ul'):
            logs.append(self.ul.log)

        if 0 == len(logs):
            return

        rlt = pd.DataFrame()
        for log in logs:
            for data in self.dl.log.gen_of_cols(cols):
                data = data.drop_duplicates().pivot(cols[0], cols[1], cols[1])
                rlt = rlt.combine_first(data)
                
        for idx, col in enumerate(rlt.columns):
            rlt[col] = idx + 1
            
        rlt.columns.name = 'UEGID'
        rlt.index.name = 'Time'
        fig, ax = plt.subplots(1, 1)
        rlt.plot(ax=ax, kind='line', title='Ue_Alive_time')

    def describle(self):
        '''小区整体信息描述'''
        rlt = pd.Series(name='小区整体信息描述')
        rlt['TotUeNum'] = len(self._uegids)
        rlt['dlschd_log_lines'] = self._dl.log.lines if hasattr(self, '_dl') else 0
        rlt['ulschd_log_lines'] = self._ul.log.lines if hasattr(self, '_ul') else 0
        return rlt.astype(np.uint32)
