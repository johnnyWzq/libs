#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  1 14:09:28 2018

@author: wuzhiqiang
生成系统内自定义的电池pack id：sys_id。
采用hash中的md5算法，确保id唯一。
"""

import hashlib
import datetime
import numpy as np
import pandas as pd
import io_operation as ioo


def create_bat_id(seeds):
    """
    #创建数据库系统里唯一电池编号
    x1~x4:w+
    x5~x36:md5(seed)
    
    """
    df = pd.DataFrame(columns=['sys_id'])
    seeds = seeds.apply(str)
    i = 0
    for seed in seeds:
        md5 = hashlib.md5()
        md5.update(seed.encode('utf-8'))
        x = 'w0' + str(datetime.datetime.now().microsecond)[-2:]
        df.loc[i, ['sys_id']] = x + md5.hexdigest()
        i = i + 1
    return df

def save_bat_id(df, config, table_name='bat_info'):
    """
    存入指定数据库，bat_config, table:bat_info
    存之查看是否有相同的vin，有则放弃
    """
    bat_info = df[['sys_id', 'vin']]
    ioo.insert_data_sql(bat_info, config, table_name, *['sys_id','vin'])
            
    
def main():
    """
    c_id = pd.DataFrame(np.random.randint(low=20, high=50, size=(5, 1)),
                         columns=['vin_code'])
    df = create_bat_id(c_id['vin_code'])
    df = df.join(c_id)
    """
    config = {'s': '192.168.1.105', 'u': 'data', 'p': 'For2019&tomorrow', 'db': 'bsa', 'port': 3306}
    df = ioo.read_sql_data(config, 'vehicle_info')
    df = df[['vin']]
    df1 = create_bat_id(df['vin'])
    df = df.join(df1)
    config['db'] = 'bat_config'
    save_bat_id(df, config)
    config['db'] = 'evb_batterybase'
    df = df = ioo.read_sql_data(config, 'bat_info')
    df = df[['sys_id', 'vin_code']]
    df = df.rename(columns={'vin_code': 'vin'})
    config['db'] = 'bat_config'
    save_bat_id(df, config)
if __name__ == '__main__':
    main()