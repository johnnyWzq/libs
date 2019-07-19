#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 16:31:48 2019

@author: wuzhiqiang
"""
import pandas as pd
import numpy as np
import re
import os 
import time
from dateutil import parser
import io_operation as ioo

def deal_1_argv(argv):
    """
    第一位代表运行模式，debug/run
    """
    if argv[1] == 'debug':#运行模式
        run_mode = 'debug'
    elif argv[1] == 'run':
        run_mode = 'run'
    else:
        run_mode = 'debug'
    print('setting %s mode...'%run_mode)

    return run_mode

def transfer_data(tick, cur_df, keywords='stime'):
    """
    将2维的df转换为1维
    返回第tick行
    """
    df = pd.DataFrame(columns=['start_tick', 'end_tick',
                               'data_num'])
    df.loc[tick, 'start_tick'] = str(cur_df[keywords].iloc[0])
    df.loc[tick, 'end_tick'] = str(cur_df[keywords].iloc[-1])
    df.loc[tick, 'data_num'] = len(cur_df)
    df.loc[tick, 'c'] = np.abs(cur_df['c'].iloc[-1])
    
    for col_name in cur_df.columns:
        for fix in ['voltage', 'current', 'dqdv']:
            if fix in col_name:
                df = cal_stat_row(tick, cur_df[col_name], col_name, df)
                df = cal_stat_row(tick, cur_df[col_name].diff(), col_name + '_diff', df)
                df = cal_stat_row(tick, cur_df[col_name].diff().diff(), col_name + '_diff2', df)
                df = cal_stat_row(tick, cur_df[col_name].diff() / cur_df[col_name], col_name + '_diffrate', df)
        if 'temperature' in col_name:
            df = cal_stat_row(tick, cur_df[col_name], col_name, df)
    return df

def cal_stat_row(cnt, ser, col_name, df0):
    """
    求统计值
    """
    df = df0.copy()
    func = lambda x: x.fillna(method='ffill').fillna(method='bfill').dropna()
    ser = ser.replace(np.inf, np.nan)
    ser = ser.replace(-np.inf, np.nan)
    ser = func(ser)
    df.loc[cnt, col_name + '_mean'] = ser.mean(skipna=True)
    df.loc[cnt, col_name + '_min'] = ser.min(skipna=True)
    df.loc[cnt, col_name + '_max'] = ser.max(skipna=True)
    df.loc[cnt, col_name + '_median'] = ser.median(skipna=True)
    df.loc[cnt, col_name + '_std'] = ser.std(skipna=True)
    return df

def get_filename_regx(regx, **kwg):
    """
    获得查找文件所需的正则式,未指定则全部
    文件命名：regx_(bat_model)_(bat_type)_structure_year
    """
    if 'year' in kwg:
        year = kwg['year']
        mask_year = '_%s'%year
    else:
        year = '\d+'
        mask_year = ''
    if 'bat_type' in kwg:
        bat_type = kwg['bat_type']
        mask_type = '_%s'%bat_type
    else:
        bat_type = '[0-9a-zA-Z]+'
        mask_type = ''
    if 'bat_model' in kwg:
        bat_model =  kwg['bat_model']
        mask_model = '_%s'%bat_model
    else:
        bat_model = '[0-9a-zA-Z]+'
        mask_model = ''
    if 'structure' in kwg:
        structure = kwg['structure']
        mask_structure = '_%s'%structure
    else:
        structure = '[0-9a-zA-Z]+'
        mask_structure = ''
    regxs = r'_%s_%s_%s_%s[_0-9]{0,4}'%(bat_model, bat_type, structure, year)
    mask_filename =  mask_model + mask_type + mask_structure + mask_year
    if mask_filename == '':
        mask_filename = regx + '_data'
    else:
        mask_filename = mask_filename[1:] + '_' + regx + '_data'
    if kwg == {}:
        regxs = ''
    regx += regxs
    
    print(regx)
    print(mask_filename)

    return regx, mask_filename

def get_columns_list(orgin_column, keyword):
    """
    从原有的列表中选出匹配kwyword的列表
    """
    target_list = []
    for stg in orgin_column:
        if re.match(r'%s\d+'%keyword, stg):
            target_list.append(stg)
    return target_list

def get_regx_data(regx, raw_data_dir):
    temp = []
    start = time.time()
    for filename in os.listdir(raw_data_dir):#获取文件夹内所有文件名
        if re.match(regx, filename, re.I):#大小写不敏感
            print('---------------------------------------')
            print(filename)
            temp.append(pd.read_csv(os.path.join(raw_data_dir, filename),
                                    encoding='gb18030'))
    if len(temp) == 0:
        print("there is any file's name included %s."%regx)
        return False, temp
    temp = pd.concat(tuple(temp), ignore_index=True)
    print('data shape: ' + temp.shape.__str__())
    end = time.time()
    print('Done, it took %d seconds to read the data.'%(end-start))
    return True, temp

def get_recent_time(time_list):
    """
    从输入的time_list中，找到最近的时间
    返回对应的位置
    时间均是time.struct_time
    """
    s_time = max(time_list)
    index = time_list.index(s_time)
    return index

def find_sequence(table_name, showbar=None, total=100, t_keywords='stime', cur_keywords='current', 
                  time_gap=300, charge_time=100, discharge_time=100, valid_cnt=100,
                  start_index=0, **kwg):
    """
    #找到给定对数据中所有满足条件对充电1、静置0或放电2过程对应的数据起始与终止字段信息
    #通用的处理是根据电流值来进行判断
    #放电2：电流值<-1；充电1：>1:
    #充电charge_time和放电discharge_time过程小于设定值则认为过程无效，直接丢弃，并且不影响前后过程的判断,默认值300秒
    #充放电过程数据valid_cnt<100,无效，静置数据默认10
    #两个数据间格time_gap大于设定值，也认为是新的一个过程,默认值300秒
    #默认时间字段stime
    #start_index代表pro的起始编号
    """
    dis_current = -1
    cha_current = 1
    mode = kwg['run_mode']
    config = kwg['config'][mode]
    df = ioo.read_sql_data(config, table_name, limit=kwg['data_limit'][mode])
    print(df.shape.__str__())
    #filt samples on rows, if a row has too few none-nan value, drop it
    DROPNA_THRESH = df.shape[1]//5
    df = df.dropna(thresh=DROPNA_THRESH)
    df = df.rename(columns={t_keywords: 'stime'})
    df['stime'] = df['stime'].apply(str)
    df['stime'] = df['stime'].apply(lambda x: parser.parse(x))
    df = df.sort_values('stime')
    #增加每条数据的充放电状态，并将不符合条件的数据丢弃
    df['state'] = np.nan
    df['state'][df['current'] > cha_current] = 1
    df['state'][df['current'] < dis_current] = 2
    df['state'][df['current'] == 0] = 0
    #开始划分
    pro_df = []
    j_last = 0
    start_index = start_index
    df_len = len(df)
    per = df_len // total + 1
    if showbar != None:
        next(showbar)
    for j in range(1, df_len):
        state_cur = df['state'].iloc[j]
        state_last = df['state'].iloc[j - 1]
        if j >= (df_len - 1) or (df.iloc[j]['stime'] - df.iloc[j - 1]['stime']).seconds > time_gap or state_last != state_cur:
            cur_df = df.iloc[j_last:j]
            valid_flag = True
            cur_mean = cur_df[cur_keywords].mean(skipna=True)
            if cur_mean < dis_current: #放电
                cad_time = discharge_time
                state = 2 #'discharge'
            elif cur_mean > cha_current: #充电
                cad_time = charge_time
                state = 1 #'charge'
            else:
                valid_cnt = 1 #改写有效计数要求
                cad_time = valid_cnt
                state = 0 #'rest'
            if state_last != state_cur and (j - cad_time) < j_last:#电流状态改变持续时间不够条件
                valid_flag = False
            if valid_flag:
                pro_df.append(get_process_info(cur_df, start_index, state))
                start_index += 1   #只有满足条件了才改变     
                j_last = j
        valid_flag = False
        if showbar != None:
            r = showbar.send(j//per)
    pro_df = pd.concat(tuple(pro_df), axis=0)
    return pro_df

def get_process_info(df, start_index, state):
    df_pro = pd.DataFrame(index=range(1), columns=('process_no','state','start_index','end_index','start_time','end_time','data_num'))
    df_pro['process_no'] = start_index
    df_pro['state'] = state
    df_pro['start_index'] = df.index[0]
    df_pro['end_index'] = df.index[-1]
    df_pro['start_time'] = df.iloc[0]['stime']
    df_pro['end_time'] = df.iloc[-1]['stime']
    df_pro['data_num'] = len(df)
    df_pro['sample_time'] = int((df_pro['end_time'].iloc[0] - df_pro['start_time'].iloc[0]).seconds / (df_pro['data_num'].iloc[0] - 1))
    return df_pro

def filter_sequence(pro_info, r_filter=100, c_filter=100, d_filter=100, p_keywords='process_no', s_keywords='state', d_keywords='data_num'):
    """
    基于pro_info找到符合s_filter的充电及放电序列
    返回三种状态process_no信息的list的dict
    """
    if pro_info is None or len(pro_info) < 3:
        print('there is not enough data to sequenced.')
        return None
    process_no_dict = {}
    state_dict = {0: r_filter, 1: c_filter, 2: d_filter}
    temp_dict = {0: 'rest', 1: 'charge', 2: 'discharge'}
    data_gp = pro_info.groupby(s_keywords)
    for key in data_gp.groups.keys():
        df = data_gp.get_group(key)
        df = df[df[d_keywords] >= state_dict[key]]#筛选大于设定条件的过程
        process_no_dict[temp_dict[key]] = df.loc[:, p_keywords].tolist()
   
    return process_no_dict
##--------------------------------------------------------------------------------
def test():
    test=(
          'pro_at_lfp_cell_2012', 'pro_at_lfp_cell_2012_2', 'pro_at_lfp_cell_2013_3.csv', 'pro_at_lfp_pack_2013_4',
          'pro_bt_lfp_cell_2012_1', 'pro_bt_lfp_cell_2012_2', 'pro_bt_lfp_cell_2013_3', 'pro_bt_lfp_cell_2013_4',
          'pro_bt_nmc_cell_2015_1', 'pro_bt_nmc_cell_2015_2', 'pro_bt_lfp_cell_2013_3', 'pro_bt_nmc_pack_2013_4',
          'pro_at_lfp', 'pro_at'
          )
    r = {
         'year': 2013,
         'bat_model': 'at',
         'bat_type': 'lfp',
         'structure': 'cell'
         }
    regx, mask = get_filename_regx('pro', **r)
    print(regx)
    print(mask)
    tt = []
    for t in test:
        if re.match(regx, t):
            tt.append(t)
    print(tt)
        
if __name__ == '__main__':
    test()