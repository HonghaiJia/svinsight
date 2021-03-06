# coding=utf-8
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import const

class DlSchd():
    '''下行调度分析类'''

    def __init__(self, log, cell, uegid=None):
        self._type = const.NR_FILE_DLSCHD
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

    def bler_of_slot(self, time_bin=1, slot = 255):
        '''计算指定时间粒度下特定子帧的bler,按照传输方案分别计算

            Args:
                time_bin：统计粒度，默认为1s
                slot: 255(不区分子帧), <20(特定slot)
            Returns：
                bler， DataFrame格式，列为传输方案，行为时间
        '''
        
        cols = ['LocalTime', 'ACK.u8AckInfo', 'ACK.u32DemTime']
        rlt = self._log.get_data_of_cols(cols, format_time=True).dropna(how='any')
        rlt = rlt.set_index(cols[0]).astype(int)
        rlt = rlt[rlt[cols[2]]%256 == slot] if slot < 20 else rlt
        rlt = rlt[cols[1]]
        rlt.name = 'bler_of_slot'

        def bler(data):
            vc = data.value_counts().reindex([0,1,2],fill_value=0)
            total = vc[0] + vc[1] + vc[2]
            return (vc[0] + vc[2]) * 100 / total if total != 0 else (vc[0] + vc[2]) * 100 / (total + 1)

        return rlt.resample(str(time_bin)+'S').apply(bler)

    def mimo_layers(self, time_bin=1, ratio=True):
        '''MIMO自适应layer统计

            Args:
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为各传输方案比例
        '''

        cols = ['LocalTime','SCHD.u8Layers']
        rlt = self._log.get_data_of_cols(cols, format_time=True).dropna(how='any')
        rlt = rlt.set_index(cols[0]).astype(int)
        rlt = rlt[cols[1]]
        rlt = rlt.resample(str(time_bin)+'S').apply(lambda x: x.value_counts()).unstack()
        if ratio:
            rlt = rlt.apply(lambda x: x/x.sum(), axis=1)
        rlt.index.name = 'mimo_layers'
        return rlt

    def rpt_csi(self, ri=1, time_bin=1):
        '''MIMO自适应layer统计

            Args:
                ri: 上报的RI
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为各传输方案比例
        '''
        cols = ['LocalTime','CSI.u8RptRI', 'CSI.u8RptWideCqi']
        val_filter = {cols[1]: [ri]}
        rlt = self._log.get_data_of_cols(cols, val_filter=val_filter, format_time=True).dropna(how='any')
        rlt = rlt.set_index(cols[0])
        rlt = rlt[cols[1:]]
        return rlt.resample(str(time_bin)+'S').apply('count')

    def amc(self, layer=1, time_bin=1):
        '''amc 统计

            Args:
                layer: 调度RI
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为各传输方案比例
        '''
        cols = ['LocalTime', 'SCHD.u8Layers', 'AMC.s16InnerSinr', 'AMC.s16DeltaSinr', 'AMC.u8SchdMcs']
        val_filter = {cols[1]: [layer]}
        rlt = self._log.get_data_of_cols(cols, val_filter=val_filter, format_time=True).dropna(how='any')
        rlt = rlt.set_index(cols[0])
        rlt = rlt[cols[2:]]
        return rlt.resample(str(time_bin)+'S').apply('mean')
         
    def schd_ue_cnt(self, time_bin=1):
        '''画图描述指定粒度下的调度UE次数

            Args:
                col_name: 待分析字段
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为调度UE次数
        '''
        col_name = ['SCHD.u8HarqId']
        return self._log.cnt_of_cols(col_name, time_bin=time_bin)
        

    def schd_rbnum(self, time_bin=1):
        '''画图描述指定粒度下的调度RB数

            Args:
                col_name: 待分析字段
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为平均RB数
        '''
        col_name = ['SCHD.u16RbNum']
        return self._log.sum_of_cols(col_name, time_bin=time_bin)
        

    def schdfail_reasons(self, time_bin=1):
        '''汇总调度失败原因

            Args：
                无
            Returns：
                所有失败原因的总数以及所占比例
        '''
        col_name = ['SCHD_FAIL_RSN.u32UeSchdFailRsn']
        return self._log.value_count_of_col(col_name, time_bin=time_bin)

    def throuput(self, time_bin=1):
        ''' 输出流量图

            根据时间粒度，输出流量图
            Args：
                time_bin: 时间粒度s
            Return:
                流量图
        '''
        col_name = ['SCHD.u32TbSize']
        return self._log.sum_of_cols(col_name, time_bin=time_bin)
    
    def dtx_cnt(self, time_bin=1):
        '''画图描述指定粒度下的dtx次数

            Args:
                col_name: 待分析字段
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为调度UE次数
        '''
        col_name = ['ACK.u8AckInfo']
        filters = {col_name[0]: [2]}
        return self._log.cnt_of_cols(col_name, time_bin=time_bin, filters=filters)
        
    def harqfail_cnt(self, time_bin=1):
        '''画图描述指定粒度下的harqfail次数

            Args:
                col_name: 待分析字段
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为调度UE次数
        '''
        col_name = ['ACK.u8IsHarqFail']
        filters = {col_name[0]: [1]}
        return self._log.cnt_of_cols(col_name, time_bin=time_bin, filters=filters)

    def selfmaintain_cnt(self, time_bin=1):
        '''画图描述指定粒度下的自维护次数

            Args:
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为调度UE次数
        '''

        col_name = ['ACK.u8IsSelfMainTain']
        filters = {col_name[0]: [1]}
        return self._log.cnt_of_cols(col_name, time_bin=time_bin, filters=filters)

        
    
class DlSchdCell(DlSchd):
    '''下行调度分析类'''

    def __init__(self, log, cell):
        super(DlSchdCell, self).__init__(log, cell)

    def describle_dtx(self):
        '''DTX时隙的UE调度组合
        '''
        
        cols = ['UEGID', 'ACK.u8AckInfo', 'ACK.u32DemTime']
        rlt = self._log.get_data_of_cols(cols).dropna(how='any')
        rlt = rlt[rlt[cols[1]] == 2].astype('uint')
        rlt = rlt.groupby(cols[2])[cols[0]].apply(lambda x: x.value_counts()).unstack().fillna(-1)
        rlt.reset_index(inplace=True)
        for col in rlt.columns[1:]:
            rlt.plot.scatter(x=cols[2], y=col,figsize=(15,5))
        return rlt

class DlSchdUe(DlSchd):
    '''下行调度UE分析类'''

    def __init__(self, log, cell, uegid):
        super(DlSchdUe, self).__init__(log, cell, uegid)
        self._uegid = uegid

    def show_ta(self):
        '''画图显示TA变化以及TAC调整命令

            Args：
                无
            Yields：
                每个TTI文件画一张趋势图，通过next()来获取下一张趋势图
        '''
        cols = ['AirTime', 'SCHD.u8Tac', 'TA.as16RptTa']
        rlt = self._log.get_data_of_cols(cols)
        rlt[cols[0]] = rlt[cols[0]].map(self._log.dectime)
        rlt[cols[1]] = rlt[cols[1]].map(lambda x: (x-31)*16)
        rlt = rlt.set_index(cols[0]) 
        ax = plt.subplots(2, 1, sharex=True)[1]
        rlt.index.name = 'Airtime/s'
        ax[0].set_ylabel('tac/ts')
        rlt[cols[1]].plot(ax=ax[0], kind='line', style='o--')
        
        ax[1].set_ylabel('report ta/ts')
        rlt[cols[2]].plot(ax=ax[1], kind='line', style='o--')
                
        return

    def bsr(self):
        '''判断UE的bsr是否充足,schdbsr <= rlcbsr'''
        cols = ['LocalTime','LCH_SCHD.u32RlcRptBsr', 'LCH_SCHD.u32SchdBsr']
        data = self._log.get_data_of_cols(cols,format_time=True)
        data = data.set_index(cols[0])
        data.plot()
        return data

    def get_idx_of_lastschd(self, curtime):
        '''距离当前时间往前的最近一次调度索引'''
        return self._log.get_idx_of_last_cols(curtime, ['SCHD.u8HarqId'], how='all')

    def pucch_sinr_blow(self, fmt, thresh=-5, time_bin=1):
        cols = ['LocalTime', 'PUCCH_PC.format', 'PUCCH_PC.rptSinr']
        data = self._log.get_data_of_cols(cols, format_time=True)
        data = data[(data[cols[1]]==fmt) & (data[cols[2]] <= thresh)]
        data = data.set_index(cols[0])
        return data[cols[2]].resample(str(time_bin)+'S').apply('count') 
    
    def pucch_pc(self, fmt=255):
        cols = [
        'AirTime',
        'PUCCH_PC.Format', 
        #'SCHD.u8Tpc',
        #'PUCCH_PC.RptSinr', 
        'PUCCH_PC.RptAdjSinr', 
        #'PUCCH_PC.FilterSinr', 
        'PUCCH_PC.ni', 
        'PUCCH_PC.ant0Pwr', 
        #'PUCCH_PC.ant1Pwr'
        ]

        #tpc_table = [-1, 0, 1, 3]
        data = self._log.get_data_of_cols(cols)
        if fmt != 255:
            data = data[data[cols[1]]==fmt]
        #data[cols[2]] = data[cols[2]].apply(lambda x: tpc_table[int(x)] if x <=3 else 0)
        #data.reset_index(inplace=True)
        for col in cols[2:]:
            data.plot.scatter(x=cols[0], y=col,figsize=(15,5))
        return data[cols[2:]]

