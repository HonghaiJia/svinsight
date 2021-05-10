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
                        
    def harqfail_cnt(self, time_bin=1):
        '''画图描述指定粒度下的harqfail次数

            Args:
                col_name: 待分析字段
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为调度UE次数
        '''
        col_name = ['CRCI.u8IsHarqFail']  
        return self._log.cnt_of_cols(col_name, time_bin=time_bin)
        
    def pathloss(self, time_bin=1, ax=None):
        col_name = ['PHR.u16PathLoss']
        return self._log.mean_of_cols(col_name, time_bin=time_bin)
    
    def singlerbsinr(self, time_bin=1):
        col_name = ['PUSCH_SINR.s16SingleRbSINR']
        return self._log.mean_of_cols(col_name, time_bin=time_bin)


    def show_amc(self, time_bin=1):
        '''画图描述指定粒度下的调度RB数

            Args:
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为平均1rb_sinr, pl, delta, bler
        '''
        col_name = ['PUSCH_SINR.s16SingleRbSINR', 'AMC.s16DeltaMcs', 'AMC.u8StdMcs']
        return self._log.mean_of_cols(col_name, time_bin=time_bin)

    def schd_uecnt(self, time_bin=1):
        '''画图描述指定粒度下的调度UE次数

            Args:
                col_name: 待分析字段
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为调度UE次数
        '''
        col_name = ['GRANT.u8HarqId']
        return self._log.cnt_of_cols(col_name, time_bin=time_bin)

    def schd_rbnum(self, time_bin=1):
        '''画图描述指定粒度下的调度RB数

            Args:
                col_name: 待分析字段
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为平均RB数
        '''
        col_name = ['GRANT.u16RbNum']
        return self._log.mean_of_cols(col_name, time_bin=time_bin)
    
    def schd_mcs(self, time_bin=1):
        '''画图描述指定粒度下的调度mcs

            Args:
                col_name: 待分析字段
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为平均RB数
        '''
        col_name = ['TB.u8Mcs']
        return self._log.mean_of_cols(col_name, time_bin=time_bin)
        
    def rpt_minbsr(self, lchgrp=7, time_bin=1):
        '''画图描述指定粒度下的report bsr

            Args:
                lchgrp: 待分析字段
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为平均RB数
        '''
        col_name = ['BSR.u32LchGrpBsr']
        filters = {'BSR.u32LchGrpId': [lchgrp]}
        return self._log.min_of_cols(col_name, time_bin=time_bin, filters=filters)
        
    def rpt_maxbsr(self, lchgrp=7, time_bin=1):
        '''画图描述指定粒度下的report bsr

            Args:
                lchgrp: 待分析lchgrpId
                time_bin：统计粒度，默认为1s
            Returns：
                趋势图：x轴为时间粒度，y轴为平均RB数
        '''
        col_name = ['BSR.u32LchGrpBsr']
        filters = {'BSR.u32LchGrpId': [lchgrp]}
        return self._log.max_of_cols(col_name, time_bin=time_bin, filters=filters)
        
    def show_rpt_bsr(self, lchgrp=3):
        '''画图描述report bsr

            Args:
                lchgrp: 待分析lchgrpId
            Returns：
                趋势图：x轴为时间粒度，y轴为平均RB数
        '''
        col = ['AirTime', 'BSR.u32LchGrpId', 'BSR.u32LchGrpBsr']
        filters = {'BSR.u32LchGrpId': [lchgrp]}
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
        data = self._log.get_data_of_cols(col)
        data = data[col[0]].dropna().astype(np.int32).value_counts()
        rlt = rlt.add(data, fill_value=0) if rlt is not None else data
        rlt.index = [const.NR_SCHD_FAIL_RSNS[int(idx)] for idx in rlt.index]
        rlt.index.name = 'Fail_Rsn'
        return rlt
            
    def throuput(self, time_bin=1):
        ''' 输出流量图

            根据时间粒度，输出流量图
            Args：
                airtime_bin_size: 时间粒度s
            Return:
                流量图
        '''
        col_name = ['TB.u16TbSize']
        return self._log.sum_of_cols(col_name, time_bin=time_bin)
        
    def bler_of_slot(self, time_bin=1, slot=255):
        '''计算指定时间粒度下特定子帧的bler,按照传输方案分别计算

            Args:
                time_bin：统计粒度，默认为1s
                slot: 255(不区分子帧), <20(特定slot)
            Returns：
                bler， DataFrame格式，列为传输方案，行为时间
        '''
        
        ack_cols = ['LocalTime', 'CRCI.u8AckInfo', 'CRCI.u32DemTime']
        rlt = pd.DataFrame()
        data = self._log.get_data_of_cols(ack_cols, format_time=True)
        data = data.dropna(how='any')
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

        cols = ['UEGID', 'CRCI.u32DemTime', 'CRCI.u8HarqId', 'CRCI.u8IsSelfMainTain']
        data = self._log.get_data_of_cols(cols)
        return data[data[cols[3]] == 1]

    def find_harqfail(self):
        '''查找是否存harqfail, 并输出相关信息'''

        cols = ['UEGID', 'CRCI.u32DemTime', 'CRCI.u8HarqId', 'CRCI.u8IsHarqFail']
        data = self._log.get_data_of_cols(cols)
        return data[data[cols[3]] == 1]

class UlSchdCell(UlSchd):
    '''上行调度Log分析类'''

    def __init__(self, log, cell):
        super(UlSchdCell, self).__init__(log, cell)

class UlSchdUe(UlSchd):
    '''上行调度UE分析类'''

    def __init__(self, log, cell, uegid):
        super(UlSchdUe, self).__init__(log, cell, uegid)
        self._uegid = uegid
