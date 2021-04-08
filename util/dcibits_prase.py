import bitstring
import pandas as pd

def riv2startandlen(riv, bwp_rbnum=273):
    N = bwp_rbnum
    a = int(riv / N)+1
    b = riv % N

    if a + b > N:
        l = N + 2 - a
        s = N - 1 - b
    else:
        l = a
        s = b
    return s, l

def parse_dci_1_1(dci_hex_bytes):
    dci = pd.Series()
    bit_stream = bitstring.BitStream(dci_hex_bytes)
    dci['dl_fmt'] = bit_stream.read('uint:1')
    dci['dl_bwpid'] = bit_stream.read('uint:1')
    #dci['rballoc_type'] = bit_stream.read('uint:1')
    #dci['riv'] = bit_stream.read('uint:16')
    dci['dl_startPrb'], dci['dl_Prbnum'] =  riv2startandlen(bit_stream.read('uint:16'))
    dci['dl_timealloc'] = bit_stream.read('uint:3')
    dci['dl_mcs'] = bit_stream.read('uint:5')
    dci['dl_ndi'] = bit_stream.read('uint:1')
    dci['dl_rv'] = bit_stream.read('uint:2')
    dci['dl_harqid'] = bit_stream.read('uint:4')
    dci['dl_dai'] = bit_stream.read('uint:2')
    dci['dl_tpc'] = bit_stream.read('uint:2')
    dci['dl_pucch_res'] = bit_stream.read('uint:3')
    dci['dl_K1'] = bit_stream.read('uint:3')
    dci['dl_antenna_port'] = bit_stream.read('uint:4')
    dci['dl_srs'] = bit_stream.read('uint:2')
    dci['dl_dmrs'] = bit_stream.read('uint:1')
    return dci

def parse_dci_0_1(dci_hex_bytes):
    dci = pd.Series()
    bit_stream = bitstring.BitStream(dci_hex_bytes)
    dci['ul_fmt'] = bit_stream.read('uint:1')
    dci['ul_bwpid'] = bit_stream.read('uint:1')
    #dci['rballoc_type'] = bit_stream.read('uint:1')
    #dci['riv'] = bit_stream.read('uint:16')
    dci['ul_startPrb'], dci['ul_Prbnum'] =  riv2startandlen(bit_stream.read('uint:16'))
    dci['ul_timealloc'] = bit_stream.read('uint:2')
    dci['ul_mcs'] = bit_stream.read('uint:5')
    dci['ul_ndi'] = bit_stream.read('uint:1')
    dci['ul_rv'] = bit_stream.read('uint:2')
    dci['ul_harqid'] = bit_stream.read('uint:4')
    dci['ul_dai'] = bit_stream.read('uint:2')
    dci['ul_tpc'] = bit_stream.read('uint:2')
    dci['ul_antenna_port'] = bit_stream.read('uint:3')
    dci['ul_srs'] = bit_stream.read('uint:2')
    dci['ul_aprd_csi'] = bit_stream.read('uint:1')
    dci['ul_beta_uci'] = bit_stream.read('uint:2')
    dci['ul_dmrs'] = bit_stream.read('uint:1')
    dci['ul_data'] = bit_stream.read('uint:1')
    return dci

def parse_dci_0_0(dci_hex_bytes):
    dci = pd.Series()
    bit_stream = bitstring.BitStream(dci_hex_bytes)
    dci['ul_fmt'] = bit_stream.read('uint:1')
    #dci['rballoc_type'] = bit_stream.read('uint:1')
    #dci['riv'] = bit_stream.read('uint:16')
    dci['ul_startPrb'], dci['ul_Prbnum'] =  riv2startandlen(bit_stream.read('uint:16'), bwp_rbnum=51)
    dci['ul_timealloc'] = bit_stream.read('uint:2')
    dci['ul_hop'] = bit_stream.read('uint:1')
    dci['ul_mcs'] = bit_stream.read('uint:5')
    dci['ul_ndi'] = bit_stream.read('uint:1')
    dci['ul_rv'] = bit_stream.read('uint:2')
    dci['ul_harqid'] = bit_stream.read('uint:4')
    dci['ul_tpc'] = bit_stream.read('uint:2')
    return dci


if __name__ == '__main__':

    s, l = riv2startandlen(4095)
    
    dldci ={ '183/12':'0xc0884680e80e00', 
             '183/16':'0xc0884672c82e00', 
             '184/17':'0xc0884641680e00', 
             '184/18':'0xc0886660888e00', 
             '185/0' :'0xc0884640284e00', 
             '185/1' :'0xc0884674c82e00', 
             '185/2' :'0xc0884642600e00', 
             '187/2' :'0xc08845a7680e00', 
             '187/14':'0xc08845a5680e00', 
           }

    uldci ={ '183/12':'0x41de6985d42000', 
             '187/2' :'0x41de69c6d42000', 
             '187/14':'0x41de69e6d42000', 
           }

    dldata = pd.DataFrame()
    for time in dldci.keys():
        dldata[time] = parse_dci_1_1(dldci[time])

    uldata = pd.DataFrame()
    for time in uldci.keys():
        uldata[time] = parse_dci_0_0(uldci[time])
    
    data = pd.concat([dldata, uldata])
    data.to_csv('D:\dci.csv', na_rep='-')