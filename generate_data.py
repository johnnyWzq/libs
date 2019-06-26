#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 14:02:13 2019

@author: wuzhiqiang
"""

print(__doc__)
import pandas as pd

import io_operation as ioo

def get_time(ser, start_kwd, end_kwd):
    start_time = ser[start_kwd]
    end_time = ser[end_kwd]
    return start_time, end_time

def get_processed_data(file_name):
    processed_data = pd.read_csv(file_name+'.csv', encoding='gb18030')
    return processed_data
    
def get_info(ser, score_key, start_kwd, end_kwd):
    score = ser[score_key]
    start_time, end_time = get_time(ser, start_kwd, end_kwd)
    print(score, start_time, end_time)
    return score, start_time, end_time

def scale_data(ser, config, cell_no, score_key, start_kwd, end_kwd, nclip=10, no_scale=False):
    score, start_time, end_time = get_info(ser, score_key, start_kwd, end_kwd)
    cell_info = str(ser[cell_no])
    data_dict = ioo.read_sql_data(config, cell_info, start_time=start_time, end_time=end_time, no_scale=no_scale, nclip=nclip)
    return data_dict, score, cell_info