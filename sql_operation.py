#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  6 14:14:27 2018

@author: wuzhiqiang
"""
import re
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Float, Integer

def create_connection(config):
    
    print('conneting the database...')
    try:
        
        conn = pymysql.connect(host=config['s'], user=config['u'],
                               password=config['p'], db=config['db'], 
                               port=config['port'], charset='utf8')
    
    except pymysql.OperationalError:
        print ("error: Could not Connection SQL Server!please check your dblink configure!")
        return None
    else:
        print('the connetion is sucessful.')
        return conn
    
def close_connection(conn):
    """
    close the connection
    """
    conn.close()
    print('the connetion is closed.')
    
def table_exists(conn,table_name):
    """
    #用来判断表是否存在
    """
    cursor = conn.cursor()
    sql_cmd = "show tables;"
    cursor.execute(sql_cmd)
    tables = [cursor.fetchall()]
    table_list = re.findall('(\'.*?\')',str(tables))
    table_list = [re.sub("'",'',each) for each in table_list]
    if table_name in table_list:
        return 1        #存在返回1
    else:
        return 0        #不存在返回0
    
def get_table_list(conn):
    """
    #找到所有表名
    """
    cursor = conn.cursor()
    sql_cmd = "show tables;"
    cursor.execute(sql_cmd)
    tables = [cursor.fetchall()]
    table_list = re.findall('(\'.*?\')',str(tables))
    table_list = [re.sub("'",'',each) for each in table_list]
    return table_list

def get_data_count(conn, sql_cmd):
    sql_cmd_c = sql_cmd.replace('*', 'count(*)')
    cursor = conn.cursor()
    cursor.execute(sql_cmd_c)
    rows = cursor.fetchall()
    return rows[0][0]

def read_sql_bar(conn, sql_cmd, data_len, showbar=None, max_time=100):
    """
    分批读数据，并且显示进度，默认分100次读
    data_len总条数
    """
    if data_len == 0:
        print('there is no data matched!')
        return []
    per = data_len // max_time
    remainder = data_len % max_time
    
    rows = []
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql_cmd_bk = sql_cmd
    sql_cmd = sql_cmd_bk + ' limit %d, %d'%(0, remainder)
    cursor.execute(sql_cmd)
    cur_rows = cursor.fetchall()
    rows.extend(cur_rows)
    start = remainder
    if per > 0:
        if showbar != None:
            next(showbar)
        cnt = 0
        while start < data_len:
            sql_cmd = sql_cmd_bk + ' limit %d, %d'%(start, per)
            cursor.execute(sql_cmd)
            cur_rows = cursor.fetchall()
            rows.extend(cur_rows)
            if showbar != None:
                r = showbar.send(cnt)
            start += per
            cnt += 1
        if showbar != None:
            r = showbar.send(max_time-1)
            showbar.close()
    return rows
        

def create_table(conn, sql_cmd, table_name):
    """
    """
    if(table_exists(conn, table_name) != 1):
        print("it can be create a table named bat_info.")
        cursor = conn.cursor()
        cursor.execute(sql_cmd)
        cursor.close()
        
def create_sql_engine(basename, user, password, server, port):
    return create_engine('mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8'
                  %(user, password, server, port, basename))
    
def exe_update(conn, sql_cmd):#更新语句，可执行update,insert语句
    cursor = conn.cursor()
    try:
       # 执行sql语句
       cursor.execute(sql_cmd)
       # 提交到数据库执行
       conn.commit()
       print('the execution is successful')
    except:
       # 如果发生错误则回滚
       conn.rollback()
       print('the execution is failed')
    
def mapping_df_types(df):
    dtypedict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            dtypedict.update({i: NVARCHAR(length=255)})
        if "float" in str(j):
            dtypedict.update({i: Float(precision=8, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: Integer()})
    return dtypedict