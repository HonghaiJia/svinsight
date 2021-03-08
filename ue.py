# coding=utf-8
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import const
from dlschd import DlSchdUe
from ulschd import UlSchdUe


class Ue(object):
    def __init__(self, ullog, dllog, cell, uegid):
        self._uegid = uegid
        self._cell = cell
        if dllog:
            self._dllog = dllog
            self._dl = DlSchdUe(dllog, cell, uegid)
        if ullog:
            self._ullog = ullog
            self._ul = UlSchdUe(ullog, cell, uegid)

    @property
    def uegid(self):
        return self._uegid

    @property
    def dl(self):
        return getattr(self, '_dl', None)

    @property
    def ul(self):
        return getattr(self, '_ul', None)
    
    def show_livetime(self):
        '''
            画出UE在各小区存在的时间
        '''
        logs = []
        if hasattr(self, '_dl'):
            logs.append(self.dl.log)
        if hasattr(self, '_ul'):
            logs.append(self.ul.log)

        if 0 == len(logs):
            return

        rlt = pd.DataFrame()
        cols = ['LocalTime', 'CellId']
        for log in logs:
            for data in self.dl.log.gen_of_cols(cols):
                group_data = data[cols[1]].groupby(data[cols[0]]).apply(lambda x: x.value_counts())
                if 0 == group_data.size:
                    continue
                group_data = group_data.unstack(level=1, fill_value=0)
                rlt = rlt.add(group_data, fill_value=0)
        
        rlt[rlt==0]=None
        rlt.columns.name = 'CellId'
        rlt.index.name = 'Time'
        fig, ax = plt.subplots(1, 1)
        rlt.plot(ax=ax, kind='line', title='Ue_Alive_time')
        
        return