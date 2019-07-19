#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 15:51:37 2019

@author: wuzhiqiang
"""

import os, re
import pandas as pd
import io_operation as ioo

def get_cell_rate_para(config, cell_info, fuzzy=False):
    """
    #通过电池基础信息表获得电池基础参数
    """
    bat_config = {'C_RATE': 210, 'V_RATE': 3.2, 'T_REFER': 25}
    
    df = ioo.read_sql_data(config, '电池信息表')
    cell_info_list = set(df['电池型号'].tolist())
    if not fuzzy:
        if cell_info in cell_info_list: #暂时先按每个电芯做匹配
            bat_config['C_RATE'] = df[df['电池型号']==cell_info]['额定容量'].iloc[0]
            bat_config['V_RATE'] = df[df['电池型号']==cell_info]['额定电压'].iloc[0]
            bat_config['bat_type'] = df[df['电池型号']==cell_info]['bat_type'].iloc[0]
            bat_config['parallel'] = df[df['电池型号']==cell_info]['并联数'].iloc[0]
            bat_config['series'] = df[df['电池型号']==cell_info]['串联数'].iloc[0]
    else:
        for cell_reg in cell_info_list:
            if re.match(cell_reg, cell_info):
                bat_config['C_RATE'] = df[df['电池型号']==cell_reg]['额定容量'].iloc[0]
                bat_config['V_RATE'] = df[df['电池型号']==cell_reg]['额定电压'].iloc[0]
                bat_config['bat_type'] = df[df['电池型号']==cell_reg]['bat_type'].iloc[0]
                bat_config['parallel'] = df[df['电池型号']==cell_reg]['并联数'].iloc[0]
                bat_config['series'] = df[df['电池型号']==cell_reg]['串联数'].iloc[0]
                break
    return bat_config

def get_feature_columns(file_dir, file_name):
    df = pd.read_csv(os.path.join(file_dir, file_name), encoding='gb18030')
    return df.columns

def columns_sequence(data, aim_columns):
    data = data[aim_columns]
    return data

def calc_score(data, score_key):
    """
    """
    data['score'] = data[score_key]

    return data

def transfer_feature(data, score_key, C_RATE=38.0, V_RATE=3.6, T_REFER=20):
    """
    curr-c_rate,voltage-v_rate,T-T_refer
    """
    print('normalizating the data... ')
    data['current_mean'] = data['current_mean'] / C_RATE
    data['current_min'] = data['current_min'] / C_RATE
    data['current_max'] = data['current_max'] / C_RATE
    data['current_median'] = data['current_median'] / C_RATE
    data['current_std'] = data['current_std'] / C_RATE
    
    data['current_diff_mean'] = data['current_diff_mean'] / C_RATE
    data['current_diff_min'] = data['current_diff_min'] / C_RATE
    data['current_diff_max'] = data['current_diff_max'] / C_RATE
    data['current_diff_median'] = data['current_diff_median'] / C_RATE
    data['current_diff_std'] = data['current_diff_std'] / C_RATE
    
    data['current_diff2_mean'] = data['current_diff2_mean'] / C_RATE
    data['current_diff2_min'] = data['current_diff2_min'] / C_RATE
    data['current_diff2_max'] = data['current_diff2_max'] / C_RATE
    data['current_diff2_median'] = data['current_diff2_median'] / C_RATE
    data['current_diff2_std'] = data['current_diff2_std'] / C_RATE
    
    data['current_diffrate_mean'] = data['current_diffrate_mean'] / C_RATE
    data['current_diffrate_min'] = data['current_diffrate_min'] / C_RATE
    data['current_diffrate_max'] = data['current_diffrate_max'] / C_RATE
    data['current_diffrate_median'] = data['current_diffrate_median'] / C_RATE
    data['current_diffrate_std'] = data['current_diffrate_std'] / C_RATE
    
    data['voltage_mean'] = data['voltage_mean'] / V_RATE
    data['voltage_min'] = data['voltage_min'] / V_RATE
    data['voltage_max'] = data['voltage_max'] / V_RATE
    data['voltage_median'] = data['voltage_median'] / V_RATE
    data['voltage_std'] = data['voltage_std'] / V_RATE
    
    data['voltage_diff_mean'] = data['voltage_diff_mean'] / V_RATE
    data['voltage_diff_min'] = data['voltage_diff_min'] / V_RATE
    data['voltage_diff_max'] = data['voltage_diff_max'] / V_RATE
    data['voltage_diff_median'] = data['voltage_diff_median'] / V_RATE
    data['voltage_diff_std'] = data['voltage_diff_std'] / V_RATE
    
    data['voltage_diff2_mean'] = data['voltage_diff2_mean'] / V_RATE
    data['voltage_diff2_min'] = data['voltage_diff2_min'] / V_RATE
    data['voltage_diff2_max'] = data['voltage_diff2_max'] / V_RATE
    data['voltage_diff2_median'] = data['voltage_diff2_median'] / V_RATE
    data['voltage_diff2_std'] = data['voltage_diff2_std'] / V_RATE
    
    data['voltage_diffrate_mean'] = data['voltage_diffrate_mean'] / V_RATE
    data['voltage_diffrate_min'] = data['voltage_diffrate_min'] / V_RATE
    data['voltage_diffrate_max'] = data['voltage_diffrate_max'] / V_RATE
    data['voltage_diffrate_median'] = data['voltage_diffrate_median'] / V_RATE
    data['voltage_diffrate_std'] = data['voltage_diffrate_std'] / V_RATE
    
    
    data['temperature_mean'] = data['temperature_mean'] / T_REFER
    data['temperature_min'] = data['temperature_min'] / T_REFER
    data['temperature_max'] = data['temperature_max'] / T_REFER
    data['temperature_median'] = data['temperature_median'] / T_REFER
    data['temperature_std'] = data['temperature_std'] / T_REFER
    
    data['dqdv_mean'] = data['dqdv_mean'].abs() / C_RATE * V_RATE
    data['dqdv_min'] = data['dqdv_min'].abs() / C_RATE * V_RATE
    data['dqdv_max'] = data['dqdv_max'].abs() / C_RATE * V_RATE
    data['dqdv_median'] = data['dqdv_median'].abs() / C_RATE * V_RATE
    data['dqdv_std'] = data['dqdv_std'].abs() / C_RATE * V_RATE
    
    data['dqdv_diff_mean'] = data['dqdv_diff_mean'].abs() / C_RATE * V_RATE
    data['dqdv_diff_min'] = data['dqdv_diff_min'].abs() / C_RATE * V_RATE
    data['dqdv_diff_max'] = data['dqdv_diff_max'].abs() / C_RATE * V_RATE
    data['dqdv_diff_median'] = data['dqdv_diff_median'].abs() / C_RATE * V_RATE
    data['dqdv_diff_std'] = data['dqdv_diff_std'].abs() / C_RATE * V_RATE
    
    data['dqdv_diff2_mean'] = data['dqdv_diff2_mean'].abs() / C_RATE * V_RATE
    data['dqdv_diff2_min'] = data['dqdv_diff2_min'].abs() / C_RATE * V_RATE
    data['dqdv_diff2_max'] = data['dqdv_diff2_max'].abs() / C_RATE * V_RATE
    data['dqdv_diff2_median'] = data['dqdv_diff2_median'].abs() / C_RATE * V_RATE
    data['dqdv_diff2_std'] = data['dqdv_diff2_std'].abs() / C_RATE * V_RATE
    
    data['dqdv_diffrate_mean'] = data['dqdv_diffrate_mean'].abs() / C_RATE * V_RATE
    data['dqdv_diffrate_min'] = data['dqdv_diffrate_min'].abs() / C_RATE * V_RATE
    data['dqdv_diffrate_max'] = data['dqdv_diffrate_max'].abs() / C_RATE * V_RATE
    data['dqdv_diffrate_median'] = data['dqdv_diffrate_median'].abs() / C_RATE * V_RATE
    data['dqdv_diffrate_std'] = data['dqdv_diffrate_std'].abs() / C_RATE * V_RATE
    
    data[score_key] = data[score_key] / C_RATE
    
    return data

def drop_feature(data, is_current=True, is_dqdv=True):
    if is_current:
        data = data.drop([i for i in data.columns if '_current_' in i], axis=1)
    if is_dqdv:
        columns = [i for i in data.columns if '_dqdv_' in i]
        column1 = [i for i in columns if '_max' in i]
        column1.extend([i for i in columns if '_min' in i])
        data = data.drop(column1, axis=1)
    return data
