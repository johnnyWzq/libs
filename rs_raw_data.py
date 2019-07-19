#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 26 11:41:20 2019

@author: wuzhiqiang
将原始文件存入数据库
"""
from dateutil import parser
import io_operation as ioo

def clean_data(data):
    """
    对读取对数据进行初步对清洗
    """
    
    #对每一张表进行清洗
    #删除无用的列
    if data is None:
        print('The data is None.')
        return None
    print('cleaning cell data...')
    data = data.rename(columns = {'Test_Time(s)': 'timestamp', 'Date_Time': 'stime',
                                  'Step_Index': 'step_no', 'Cycle_Index': 'cycle_no',
                                  'Current(A)': 'current', 'Voltage(V)': 'voltage',
                                  'Charge_Capacity(Ah)': 'charge_c',
                                  'Discharge_Capacity(Ah)': 'discharge_c',
                                  'Charge_Energy(Wh)': 'charge_e', 'Discharge_Energy(Wh)': 'discharge_e',
                                  'dV/dt(V/s)': 'dv/dt', 'Temperature (C)_1':'temperature'})
    data = data[['timestamp', 'stime', 'cycle_no', 'step_no', 'current', 'voltage',
                 'charge_c', 'discharge_c',
                 'charge_e', 'discharge_e', 'dv/dt', 'temperature']]
    data = data.dropna()
    data['stime'] = data['stime'].apply(str)
    data['stime'] = data['stime'].apply(lambda x: parser.parse(x))
    data = data.sort_values('stime')
    data['stime'] = data['stime'].apply(str)
    return data

def rs_jt_data():
    config =  {'s': '192.168.1.105', 'u': 'data', 'p': 'For2019&tomorrow', 'db': 'test_bat', 'port': 3306}
    table_name = 'LS37_NCM_cell_2018_19'
    data_dir = ioo.input_dir()
    print(data_dir)
    regx1 = r'LS37Ah.*'#r'MGL35_[0-9a-zA-Z\_]+_\d_Cycle+\w+'
    regx = r'Channel_\d+\-\d+\_\d+'
    data = ioo.read_excel_files(data_dir, regx0=regx1, regx=regx, sheet_name=None)
    data = clean_data(data)
    ioo.save_data_sql(data, config, table_name)
    
def rs_bat_config_data():
    config =  {'s': '192.168.1.105', 'u': 'data', 'p': 'For2019&tomorrow', 'db': 'bat_config', 'port': 3306}
    table_name = '电池信息表'
    data_dir = ioo.input_dir()
    print(data_dir)
    regx = r'电池信息表.xlsx'
    data = ioo.read_excel_files(data_dir, regx)
    ioo.save_data_sql(data, config, table_name)
    
def add_sample_time(sample_time=1):
    #对已有对pro_info增加sample——time列
    config =  {'s': '192.168.1.105', 'u': 'data', 'p': 'For2019&tomorrow', 'db': 'test_bat', 'port': 3306}
    bat_list = ioo.input_table_name(config)
    for bat in bat_list:
        ioo.insert_col_sql(config, bat, 'sample_time')
        ioo.modify_data_sql(config, bat, 'sample_time', sample_time)
        
def main():
    #rs_jt_data()
    #rs_bat_config_data()
    add_sample_time(10)
if __name__ == '__main__':
    main()