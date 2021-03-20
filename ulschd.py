# coding=utf-8
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import const


class UlSchd():
    '''上行调度Log分析类'''

    def __init__(self, log, cell=None, uegid=None):
        self._type = const.NR_FILE_ULSCHD
        self._log = log
        self._id_filter = {}
        self._cell = cell
        
        if cell:
            self._id_filter['CellId'] = [cell.cellid]
        if uegid:
            self._id_filter['UEGID'] = [uegid]

    @property
    def type(self):
        return self._type

    @property
    def log(self):
        return self._log

    def show_bler(self, airtime_bin_size=1, ax=None):
        ack_cols = ['CRCI.u32DemTime', 'CRCI.u8AckInfo']
        rlt = pd.DataFrame()
        for data in self._log.gen_of_cols(ack_cols):
            data = data.dropna(how='any')
            if 0 == data.size:
                continue
            cnt = data[ack_cols[1]].groupby(data[ack_cols[0]]// (airtime_bin_size*25600))\
                .apply(lambda x: x.value_counts()).unstack(level=1)
            rlt = rlt.add(cnt, fill_value=0, level=0)

        def func(data):
            return (data[0] + data[2]) / ((data[0]+data[1]+data[2])+1)
        bler_data = rlt.reindex(columns=[0, 1, 2], fill_value=0).apply(func, axis=1)
        bler_data.index.astype(int)

        if ax is None:
            ax = plt.subplots(1, 1)[1]
        ax.set_ylabel('Bler')
        ax.set_ylim([0, 1])
        xlabel = 'Airtime/{bin}s'.format(bin=airtime_bin_size)
        bler_data.index.name = xlabel
        bler_data.plot(ax=ax, kind='line', style='o--')
        
    def show_dci0lost(self, airtime_bin_size=1, ax=None):
        ack_cols = ['CRCI.u32DemTime', 'CRCI.u8AckInfo']
        rlt = pd.DataFrame()
        for data in self._log.gen_of_cols(ack_cols):
            data = data.dropna(how='any')
            if 0 == data.size:
                continue
            cnt = data[ack_cols[1]].groupby(data[ack_cols[0]]// (airtime_bin_size*25600))\
                .apply(lambda x: x.value_counts()).unstack(level=1)
            rlt = rlt.add(cnt, fill_value=0, level=0)

        def func(data):
            return (data[2]) / ((data[0]+data[1]+data[2])+1)
        bler_data = rlt.reindex(columns=[0, 1, 2], fill_value=0).apply(func, axis=1)
        bler_data.index.astype(int)

        if ax is None:
            ax = plt.subplots(1, 1)[1]
        ax.set_ylabel('dci0lost_ratio')
        ax.set_ylim([0, 1])
        xlabel = 'Airtime/{bin}s'.format(bin=airtime_bin_size)
        bler_data.index.name = xlabel
        bler_data.plot(ax=ax, kind='line', style='o--')
        
    def show_dci0lost_cnt(self, airtime_bin_size=1):
        ack_cols = ['CRCI.u32DemTime','CRCI.u8AckInfo']
        rlt = pd.Series()
        for data in self._log.gen_of_cols(ack_cols):
            data[ack_cols[1]][data[ack_cols[1]]!=2]=0
            if 0 == data.size:
                continue
            cnt = data[ack_cols[1]].groupby(data[ack_cols[0]]// (airtime_bin_size*25600)).sum()
            rlt = rlt.add(cnt, fill_value=0)

        ax = plt.subplots(1, 1)[1]
        ax.set_ylabel('dci0lost_ratio')
        ax.set_ylim([0, 1])
        xlabel = 'Airtime/{bin}s'.format(bin=airtime_bin_size)
        rlt.index.name = xlabel
        if rlt.size:
            rlt.plot(ax=ax, kind='line', style='o--')
        
    def show_harqfail_cnt(self, airtime_bin_size=1):
        '''画图描述指定粒度下的harqfail次数

            Args:
                col_name: 待分析字段
                airtime_bin_size：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为调度UE次数
        '''
        col_name = r'CRCI.b8IsHarqFail'
        self._log.show_trend(col_name, const.AGG_FUNC_SUM, airtime_bin_size)
        return
        
    def show_pathloss(self, airtime_bin_size=1, ax=None):
        col_name = r'PHR.u16PathLoss'
        self._log.show_trend(col_name, const.AGG_FUNC_MEAN, airtime_bin_size)
        return
    
    def show_1rbsinr(self, airtime_bin_size=1, ax=None):
        col_name = r'PUSCH_SINR.s16SingleRbSINR'
        self._log.show_trend(col_name, const.AGG_FUNC_MEAN, airtime_bin_size)
        return

    def show_amc(self, airtime_bin_size=1):
        '''画图描述指定粒度下的调度RB数

            Args:
                airtime_bin_size：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为平均1rb_sinr, pl, delta, bler
        '''
        cols = ['PUSCH_SINR.s16SingleRbSINR', 'AMC.s16DeltaMcs', 'AMC.u8StdMcs']
        mean_data = self._log.mean_of_cols(cols, airtime_bin_size=airtime_bin_size, by='cnt', time_col='AirTime')
        fig, ax = plt.subplots(4, 1, sharex=True)

        delta = mean_data[cols[1]] / 100
        ax[0].set_ylabel('Delta')
        ax[0].set_ylim([-28, 28])
        xlabel = 'Airtime/{bin}s'.format(bin=airtime_bin_size)
        ax[0].set_xlabel(xlabel)
        delta.plot(ax=ax[0], kind='line', style='o--')

        stdmcs = mean_data[cols[2]]
        ax[1].set_ylabel('Std_mcs')
        ax[1].set_ylim([0, 29])
        xlabel = 'Airtime/{bin}s'.format(bin=airtime_bin_size)
        ax[1].set_xlabel(xlabel)
        stdmcs.plot(ax=ax[1], kind='line', style='o--')

        singlerb_sinr = mean_data[cols[0]]
        ax[2].set_ylabel('1Rb_Sinr')
        xlabel = 'Airtime/{bin}s'.format(bin=airtime_bin_size)
        ax[2].set_xlabel(xlabel)
        singlerb_sinr.plot(ax=ax[2], kind='line', style='o--')

        self.show_bler(airtime_bin_size=airtime_bin_size, ax=ax[3])
        return

    def show_schd_uecnt(self, airtime_bin_size=1):
        '''画图描述指定粒度下的调度UE次数

            Args:
                col_name: 待分析字段
                airtime_bin_size：统计粒度，默认为1s
            Returns：
                直方图（kde）：x轴调度UE数，y轴为比例
                趋势图：x轴为时间粒度，y轴为调度UE次数
        '''
        col_name = r'GRANT.u8HarqId'
        self._log.show_trend(col_name, const.AGG_FUNC_CNT, airtime_bin_size)
        return

    def show_schd_rbnum(self, airtime_bin_size=1):
        '''画图描述指定粒度下的调度RB数

            Args:
                col_name: 待分析字段
                airtime_bin_size：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为平均RB数
        '''
        col_name = r'GRANT.u16RbNum'
        self._log.show_trend(col_name, const.AGG_FUNC_MEAN, airtime_bin_size, mean_by='time')
        #self._log.show_hist(col_name, xlim=[0, 100])
        return
    
    def show_schd_mcs(self, airtime_bin_size=1):
        '''画图描述指定粒度下的调度mcs

            Args:
                col_name: 待分析字段
                airtime_bin_size：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为平均RB数
        '''
        col_name = r'TB.u8Mcs'
        self._log.show_trend(col_name, const.AGG_FUNC_MEAN, airtime_bin_size, mean_by='time')
        return
        
    def show_rpt_minbsr(self, lchgrp=3, airtime_bin_size=1):
        '''画图描述指定粒度下的report bsr

            Args:
                lchgrp: 待分析字段
                airtime_bin_size：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为平均RB数
        '''
        col_name = r'BSR.u32LchGrpBsr'
        filters = {r'BSR.u32LchGrpId': [lchgrp]}
        self._log.show_trend(col_name, const.AGG_FUNC_MIN, airtime_bin_size, filters=filters)
        return
        
    def show_rpt_maxbsr(self, lchgrp=3, airtime_bin_size=1):
        '''画图描述指定粒度下的report bsr

            Args:
                lchgrp: 待分析lchgrpId
                airtime_bin_size：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为平均RB数
        '''
        col_name = r'BSR.u32LchGrpBsr'
        filters = {r'BSR.u32LchGrpId': [lchgrp]}
        self._log.show_trend(col_name, const.AGG_FUNC_MAX, airtime_bin_size, filters=filters)
        return
        
    def show_rpt_bsr(self, lchgrp=3):
        '''画图描述report bsr

            Args:
                lchgrp: 待分析lchgrpId
            Returns：
                趋势图：x轴为时间粒度，y轴为平均RB数
        '''
        col = ['AirTime', r'BSR.u32LchGrpId', r'BSR.u32LchGrpBsr']
        filters = {r'BSR.u32LchGrpId': [lchgrp]}
        rpt_bsr = self._log.get_data_of_cols(cols=col, val_filter=filters)
        rpt_bsr = rpt_bsr.set_index(col[0])[col[2]]
        rpt_bsr.plot()
        return

    def schdfail_reasons(self):
        '''汇总调度失败原因
        
            Args：
                无
            Returns：
                所有失败原因的总数以及所占比例
        '''
        col = ['SCHD_FAIL_RSN.u32UeSchdFailRsn']
        rlt = pd.Series(name='SchdFail_Cnt')
        for data in self._log.gen_of_cols(col):
            data = data[col[0]].dropna().astype(np.int32).value_counts()
            rlt = rlt.add(data, fill_value=0) if rlt is not None else data
        rlt.index = [const.NR_SCHD_FAIL_RSNS[int(idx)] for idx in rlt.index]
        rlt.index.name = 'Fail_Rsn'
        return rlt
            
    def show_throuput(self, airtime_bin_size=1):
        ''' 输出流量图

            根据时间粒度，输出流量图
            Args：
                airtime_bin_size: 时间粒度s
            Return:
                流量图
        '''
        assert(airtime_bin_size>=1)
        cols = [r'TB.u16TbSize', 'AirTime']
        rlt = pd.Series()
        for data in self._log.gen_of_cols(cols):
            airtime = data[cols[1]] // (airtime_bin_size*25600)
            group_data = data[cols[0]].groupby(airtime)
            rlt = rlt.add(group_data.sum(), fill_value=0)
            
        ax = plt.subplots(1, 1)[1]
        xlabel = 'Airtime/{bin}s'.format(bin=airtime_bin_size)
        ax.set_ylabel('Throuput/Kbits')
        rlt.index.name = xlabel
        rlt = rlt * 8 / 1000
        rlt.plot(ax=ax, kind='line', style='ko--')
        return
        
    def bler_of_slot(self, time_bin=1, slot = 255):
        '''计算指定时间粒度下特定子帧的bler,按照传输方案分别计算

            Args:
                time_bin：统计粒度，默认为1s
                slot: 255(不区分子帧), <20(特定slot)
            Returns：
                bler， DataFrame格式，列为传输方案，行为时间
        '''
        
        ack_cols = ['LocalTime', 'CRCI.u8AckInfo', 'CRCI.u32DemTime']
        rlt = pd.DataFrame()
        for data in self._log.gen_of_cols(ack_cols, format_time=True):
            data = data.dropna(how='any')
            if 0 == data.size:
                continue
            rlt = pd.concat([rlt, data])

        rlt = rlt.set_index(ack_cols[0]).astype(int)
        rlt = rlt[rlt[ack_cols[2]]%256 == slot] if slot < 20 else rlt
        rlt = rlt[ack_cols[1]]
        rlt.name = 'bler_of_slot'

        def bler(data):
            vc = data.value_counts().reindex([0,1,2],fill_value=0)
            total = vc[0] + vc[1] + vc[2]
            return (vc[0] + vc[2]) * 100 / total if total != 0 else (vc[0] + vc[2]) * 100 / (total + 1)

        return rlt.resample(str(time_bin)+'S').apply(bler)

    def find_selfmaintain(self):
        '''查找是否存在自维护, 并输出相关信息'''

        cols = ['UEGID', 'CRCI.u32DemTime', 'CRCI.u8HarqId', 'CRCI.b8IsSelfMainTain']
        rlt = pd.DataFrame(columns=cols)
        for data in self._log.gen_of_cols(cols):
            rlt = rlt.append(data[data[cols[3]] == 1])
        return rlt

    def find_harqfail(self):
        '''查找是否存harqfail, 并输出相关信息'''

        cols = ['UEGID', 'CRCI.u32DemTime', 'CRCI.u8HarqId', 'CRCI.b8IsHarqFail']
        rlt = pd.DataFrame(columns=cols)
        for data in self._log.gen_of_cols(cols):
            rlt = rlt.append(data[data[cols[3]] == 1])
        return rlt

    def find_dci0lost(self):
        '''查找是否存dci0lost, 并输出相关信息'''

        cols = ['UEGID', 'CRCI.u32DemTime', 'CRCI.u8HarqId', 'CRCI.u8DciLostFlag']
        rlt = pd.DataFrame(columns=cols)
        for data in self._log.gen_of_cols(cols):
            data = data[data[cols[3]] == 1]
            rlt = rlt.append(data)
        return rlt

class UlSchdCell(UlSchd):
    '''上行调度Log分析类'''

    def __init__(self, log, cell):
        super(UlSchdCell, self).__init__(log, cell)

class UlSchdUe(UlSchd):
    '''上行调度UE分析类'''

    def __init__(self, log, cell, uegid):
        super(UlSchdUe, self).__init__(log, cell, uegid)
        self._uegid = uegid
