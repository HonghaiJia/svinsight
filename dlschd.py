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
        
        ack_cols = ['LocalTime', 'ACK.u8AckInfo', 'ACK.u32DemTime']
        rlt = pd.DataFrame()
        for data in self._log.gen_of_cols(ack_cols, format_time=True):
            data = data.dropna(how='any')
            if 0 == data.size:
                continue
            rlt = pd.concat([rlt, data])

        rlt = rlt.set_index(ack_cols[0])
        rlt = rlt[rlt[ack_cols[2]]%256 == slot] if slot < 20 else rlt
        rlt = rlt[ack_cols[1]]
        rlt.name = 'bler'

        def bler(data):
            vc = data.value_counts().reindex([0,1,2],fill_value=0)
            total = vc[0] + vc[1] + vc[2]
            return (vc[0] + vc[2]) * 100 / total if total != 0 else (vc[0] + vc[2]) * 100 / (total + 1)

        return rlt.resample(str(time_bin)+'S').apply(bler)

    def show_mimo(self, airtime_bin_size=1):
        '''图形化输出MIMO自适应信息

            Args:
                airtime_bin_size：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为各传输方案比例
        '''

        cols = ['SCHD.u8Layers']
        ratio = self._log.hist_of_col(cols[0], airtime_bin_size=airtime_bin_size, ratio=True)
        ratio = ratio.reindex(columns=[1, 2, 3, 4], fill_value=0)
        idxs = ['1_Layer', '2_Layers', '3_Layers', '4_Layers']
        ratio.columns = ratio.columns.map(lambda x: idxs[x])

        ax = plt.subplots(1, 1)[1]
        xlabel = 'Airtime/{bin}s'.format(bin=airtime_bin_size)
        ax.set_xlabel(xlabel)
        ax.set_ylabel('Ratio')
        ratio.plot(ax=ax, kind='line', style='o--')
 
    def show_bler_of_slot(self, airtime_bin_size=1, slot = 255, ax=None):
        '''画图描述指定粒度下的子帧级bler

            Args:
                airtime_bin_size：统计粒度，默认为1s
                slot: 255(不区分slot), <20(特定slot)
            Returns：
                趋势图：x轴为时间粒度，y轴为bler(两种传输方案分别统计）
        '''
        if ax is None:
            fig, ax = plt.subplots(1, 1)
        ax.set_ylabel('Bler')
        ax.set_ylim([0, 1])
        xlabel = 'Airtime/{bin}s'.format(bin=airtime_bin_size)
        bler_data = self.bler_of_slot(airtime_bin_size=airtime_bin_size, slot = slot)
        bler_data.index.name = xlabel
        bler_data.plot(ax=ax, kind='line', style='o--')

    def show_schd_uecnt(self, airtime_bin_size=1):
        '''画图描述指定粒度下的调度UE次数

            Args:
                col_name: 待分析字段
                airtime_bin_size：统计粒度，默认为1s
            Returns：
                直方图（kde）：x轴调度UE数，y轴为比例
                趋势图：x轴为时间粒度，y轴为调度UE次数
        '''
        col_name = r'SCHD.u8HarqId'
        self._log.show_trend(col_name, const.AGG_FUNC_CNT, airtime_bin_size)
        return

    def show_schd_rbnum(self, airtime_bin_size=1):
        '''画图描述指定粒度下的调度RB数

            Args:
                col_name: 待分析字段
                airtime_bin_size：统计粒度，默认为1s
            Returns：
                直方图（kde）：x轴调度RB数，y轴为调度次数
                趋势图：x轴为时间粒度，y轴为平均RB数
        '''
        col_name = r'SCHD.u16RbNum'
        self._log.show_trend(col_name, const.AGG_FUNC_MEAN, airtime_bin_size, mean_by='time')
        self._log.show_hist(col_name, xlim=[0, 300])
        return

    def schdfail_reasons(self):
        '''汇总调度失败原因

            Args：
                无
            Returns：
                所有失败原因的总数以及所占比例
        '''
        col = [r'SCHD_FAIL_RSN.u32UeSchdFailRsn']
        rlt = pd.Series(name='SchdFail_Cnt')
        for data in self._log.gen_of_cols(col):
            data = data[col[0]].dropna().astype(np.int32).value_counts()
            rlt = rlt.add(data, fill_value=0) if rlt is not None else data
        rlt.index = [const.NR_SCHD_FAIL_RSNS[int(idx)] for idx in rlt.index]
        rlt.index.name='Fail_Rsn'
        return rlt

    def is_valid_airtime(self, airtime):
        '''给定的airtime是否在当前log的时间范围'''
        return airtime in self._log.airtimes()

    def find_selfmaintain(self):
        '''查找是否存在自维护, 并输出相关信息'''

        cols = ['ACK.u32DemTime', 'UEGID', 'ACK.u8HarqId', 'ACK.u8IsSelfMainTain']
        rlt = pd.DataFrame(columns=cols)
        for data in self._log.gen_of_cols(cols):
            data = data[data[cols[3]] == 1]
            rlt = rlt.append(data)
        return rlt

    def find_harqfail(self):
        '''查找是否存harqfail, 并输出相关信息'''

        cols = ['ACK.u32DemTime', 'UEGID', 'ACK.u8HarqId', 'ACK.u8IsHarqFail']
        rlt = pd.DataFrame(columns=cols)
        for data in self._log.gen_of_cols(cols):
            data = data[data[cols[3]] == 1]
            rlt = rlt.append(data)
        return rlt

    def find_dtx(self):
        '''查找是否存dtx, 并输出相关信息'''

        cols = ['ACK.u32DemTime', 'UEGID', 'ACK.u8HarqId', 'ACK.u8AckInfo']
        rlt = pd.DataFrame(columns=cols)
        for data in self._log.gen_of_cols(cols):
            data = data[data[cols[3]] == 2]
            rlt = rlt.append(data)
        return rlt

    def find_bler_over(self, thresh, airtime_bin_size=1):
        '''查找bler超过一定门限的时间,时间粒度可以指定
            Args：
                thresh：门限，（0,1）之间
                airtime_bin_size：时间粒度，默认1s
            Returns：
                Series格式，{airtime：bler}
        '''
        bler_data = self.bler(airtime_bin_size=airtime_bin_size)
        bler_data = bler_data[bler_data > thresh].dropna().reindex(lambda x: x*airtime_bin_size for x in bler_data.index)
        bler_data.index.name = 'AirTime'
        return bler_data

    def show_throuput(self, airtime_bin_size=1):
        ''' 输出流量图

            根据时间粒度，输出流量图
            Args：
                airtime_bin_size: 时间粒度s
            Return:
                流量图
        '''
        assert(airtime_bin_size>=1)
        cols = [r'SCHD.u32TbSize', 'AirTime']
        rlt = pd.Series()
        for data in self._log.gen_of_cols(cols):
            airtime = data[cols[1]] // (airtime_bin_size*25600)
            group_data = data[cols[0]].groupby(airtime)
            rlt = rlt.add(group_data.sum(), fill_value=0)
            
        ax = plt.subplots(1, 1)[1]
        xlabel = 'Airtime/{bin}s'.format(bin=airtime_bin_size)
        ax.set_ylabel('Throuput/Kbits')
        rlt = rlt * 8 / 1000
        rlt.index.name = xlabel
        rlt.plot(ax=ax, kind='line', style='ko--')
        return
    
    def show_dtx_cnt(self, airtime_bin_size=1):
        '''画图描述指定粒度下的dtx次数

            Args:
                col_name: 待分析字段
                airtime_bin_size：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为调度UE次数
        '''
        assert(airtime_bin_size>=1)
        cols = ['ACK.u32DemTime','ACK.u8AckInfo']
        rlt = pd.DataFrame()
        for data in self._log.gen_of_cols(cols):
            data[cols[1]][data[cols[1]] != 2] = 0      
            airtime = data[cols[0]] // (airtime_bin_size*25600)
            group_data = data[cols[1]].groupby(airtime).sum()
            rlt = rlt.add(group_data, fill_value=0)
        
        ax = plt.subplots(1, 1)[1]
        xlabel = 'Airtime/{bin}s'.format(bin=airtime_bin_size)
        ax.set_ylabel('DtxCnt')
        rlt.index.name = xlabel
        if rlt.size:
            rlt.plot(ax=ax, kind='line', style='ko--')
        return
        
    def show_harqfail_cnt(self, airtime_bin_size=1):
        '''画图描述指定粒度下的harqfail次数

            Args:
                col_name: 待分析字段
                airtime_bin_size：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为调度UE次数
        '''
        assert(airtime_bin_size>=1)
        cols = ['ACK.u32DemTime','ACK.u8IsHarqFail']
        rlt = pd.DataFrame()
        for data in self._log.gen_of_cols(cols):
            data[cols[1]][data[cols[1]] != 1] = 0        
            airtime = data[cols[0]] // (airtime_bin_size*25600)
            group_data = data[cols[1]].groupby(airtime).sum()
            rlt = rlt.add(group_data,fill_value=0)
        
        ax = plt.subplots(1, 1)[1]
        xlabel = 'Airtime/{bin}s'.format(bin=airtime_bin_size)
        ax.set_ylabel('HarqFailCnt')
        rlt.index.name = xlabel
        rlt.plot(ax=ax, kind='line', style='ko--')
        return
    
class DlSchdCell(DlSchd):
    '''下行调度分析类'''

    def __init__(self, log, cell):
        super(DlSchdCell, self).__init__(log, cell)

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

    def is_bsr_enough(self):
        '''判断UE的bsr是否充足,schdbsr <= rlcbsr'''
        cols = ['LCH_SCHD.u32RlcRptBsr', 'LCH_SCHD.u32SchdBsr']
        for data in self._log.gen_of_cols(cols):
            if data(data[cols[1]] < data[cols[2]]).any():
                return False
        return True

    def get_idx_of_lastschd(self, curtime):
        '''距离当前时间往前的最近一次调度索引'''
        return self._log.get_idx_of_last_cols(curtime, ['SCHD.u8HarqId'], how='all')
