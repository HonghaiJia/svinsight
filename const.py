# coding=utf-8

# 聚合函数
AGG_FUNC = ('sum', 'mean', 'max', 'min', 'count')
AGG_FUNC_SUM = AGG_FUNC[0]
AGG_FUNC_MEAN = AGG_FUNC[1]
AGG_FUNC_CNT = AGG_FUNC[4]
AGG_FUNC_MIN = AGG_FUNC[3]
AGG_FUNC_MAX = AGG_FUNC[2]

# NR调度失败原因
NR_SCHD_FAIL_RSNS = ('cce_input_param', 'cce_not_enough', 'cce_position_collision', 'cce_pwr_limit',
                      'alloc_harqproc_fail', 'alloc_harqid_fail', 'alloc_rb_fail', 'retx_retx_rbnum_different',
                      'gap', 'dmrs_conflict', 'rbest_fail', 'has_schded', 'drx', 'pttsps_schded', 'sps_schded',
                      'retx_coderate_over', 'sr_request_time_over', 'sr_schded', 'err_state', 'schd_ue_num_over',
                      'pre_cnt_over', 'ccch_schd_timeover', 'fbd_schd', 'msg0_num_over', 'preamble_id_err',
                      'msg0_alloc_cce_fail', 'cce_alloc_fail', 'retx_occupy_rb_fail', 'pttgap', 'other')

# NR文件类型
NR_FILE_TYPES = ('CMAC_ulUeTtiInfo', 'CMAC_dlUeTtiInfo')
NR_FILE_ULSCHD = NR_FILE_TYPES[0]
NR_FILE_DLSCHD = NR_FILE_TYPES[1]
