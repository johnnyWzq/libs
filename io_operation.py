#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  8 10:18:01 2019

@author: wuzhiqiang

常用的文件的操作库
"""

print(__doc__)
import os
import re
import time
import pandas as pd
import sql_operation as sql
#import pymysql

def input_dir():
    """
    通过键盘输入获得文件的目录
    """
    for i in range(3):
        data_dir = input("输入文件所在目录：")
        if os.path.exists(data_dir):
            return data_dir
        else:
            print("此目录不存在")
    return None

def input_table_name(config):
    """
    通过键盘获得要检索的数据库的表名
    可以是表名前面的关键字
    """
    conn = sql.create_connection(config)
    exist_table_list = sql.get_table_list(conn)
    table_name_list = input("please input the keyword of selected tables:")
    table_name_list = table_name_list.split()
    match_table_list = []
    print('get the match table:\n')
    for table_name in table_name_list:
        for exist_table in exist_table_list:
            if re.match(table_name, exist_table, re.I):
                match_table_list.append(exist_table)
    match_table_list = list(set(match_table_list))
    if len(match_table_list) == 0:
        print('no match table.')
        return None
    for table_name in match_table_list:
        print(':%s\n'%table_name)
    return match_table_list

def get_file_name():
    """
    通过键盘获得要检索的文件名
    """
    for i in range(3):
        regx = input("please input the keyword of selected files:")#.encode('utf8')
        if regx:
            return regx
    return None

def get_all_files_name(data_dir, regx):
    """
    一次性读取目录下所有的符合正则式的文件名，并返回文件名列表
    """
    temp = []
    if os.path.exists(data_dir):
        for filename in os.listdir(data_dir):#获取文件夹内所有文件名
            if re.match(regx, filename, re.I):#大小写不敏感
                print('---------------------------------------')
                print(filename)
                temp.append(filename)
        return temp
    else:
        print("There isn't such a path.")
        return None
    
def read_excel_files(data_dir, regx0, **kwds):
    """
    读取目录下符合正则式的所有excel文件，并拼合起来返回dataframe
    """
    temp = []
    for filename in os.listdir(data_dir):#获取文件夹内所有文件名
        if re.match(regx0, filename):
            print('---------------------------------------')
            print(filename)
            temp.append(read_excel(os.path.join(data_dir, filename), **kwds))
    temp = pd.concat(tuple(temp), ignore_index=True)
    print('data shape: ' + temp.shape.__str__())
    return temp

def read_excel(filename, **kwds):
    """
    读取一个excel文件，默认读取所有sheet
    """
    regx = r'.*'
    sheet_name = None
    skiprows = None
    if 'regx' in kwds:
       regx = kwds['regx']
    if 'sheet_name' in kwds:
       sheet_name = kwds['sheet_name']
    if 'skiprows' in kwds:
        skiprows = kwds['skiprows']
    print('reading a excel file...')
    temp = pd.DataFrame()
    start = time.time()
    data_dict = pd.read_excel(filename, sheet_name=sheet_name, skiprows=skiprows, encoding='gb18030')
    for key, value in data_dict.items():
        if re.match(regx, key) and value is not None:
            temp = temp.append(value, ignore_index=True)
            print('adding %s in the cache...'%key)
    end = time.time()
    print('Done, it took %d seconds to read the data.'%(end-start))
    print('data shape: ' + temp.shape.__str__())
    return temp
    
def read_csv(filename, **kwds):
    """
    读取一个csv文件
    最大读取nrows行，如果超过，则需要进行分别读
    
    """
    print('reading a csv file...')
    start = time.time()
    header = 'infer'
    skiprows = None
    nrows = None
    usecols = None
    names = None
    chunksize = None
    sep = ','
    encoding='gb18030'
    parse_dates = False
    date_parser = None
    index_col = None
    prefix = None
    for key, value in kwds.items():
        if key == 'header':
            header = value
        elif key == 'skiprows':
            skiprows = value
        elif key == 'nrows':
            nrows = value
        elif key == 'usecols':
            usecols = value
        elif key == 'names':
            names = value
        elif key == 'chunksize':
            chunksize = value
        elif key == 'sep':
            sep = value
        elif key == 'encoding':
            encoding = value
        elif key == 'parse_dates':
            parse_dates = value
        elif key == 'date_parser':
            date_parser = value
        elif key == 'index_col':
            index_col = value
        elif key == 'prefix':
            prefix = value
    if chunksize is not None:
        f = open(filename)
        reader = pd.read_csv(f, iterator=True, sep=sep, header=header, skiprows=skiprows,
                           nrows=nrows, usecols=usecols, names=names, encoding=encoding,
                           parse_dates=parse_dates, date_parser=date_parser, index_col=index_col,
                           engine='python', error_bad_lines=False, prefix=prefix)
        loop = True
        chunks = []
        while loop:
            try:
                chunk = reader.get_chunk(chunksize)
                chunks.append(chunk)
            except StopIteration:
                loop = False
                print('Interation is stopped.')
        data = pd.concat(chunks, ignore_index=True)
    else:      
        data = pd.read_csv(filename, sep=sep, header=header, skiprows=skiprows,
                           nrows=nrows, usecols=usecols, names=names, encoding=encoding,
                           parse_dates=parse_dates, date_parser=date_parser,
                           index_col=index_col,engine='python', prefix=prefix)
    end = time.time()
    print('Done, it took %d seconds to read the data.'%(end-start))
    print('data shape: ' + data.shape.__str__())
    return data

def match_sql_data(config, table_name, condition, str_value, limit=None):
    """
    获得指定条件的数据,类型为字符串
    """
    print('reading a table named %s...'%table_name)
    conn = sql.create_connection(config)
    if (sql.table_exists(conn, table_name)) == 1:
        print('the table is existe and reading...')
        sql_cmd = "select * from %s where %s = '%s'"\
                        %(table_name, condition, str_value)
        len_limit = sql.get_data_count(conn, sql_cmd)
        if limit != None:
            #sql_cmd += ' limit %d'%limit
            len_limit = min(limit, len_limit) #获得查询条数限制值
        rows = sql.read_sql_bar(conn, sql_cmd, len_limit)
        df = pd.DataFrame(rows)
        if len(df) > 0:
            return df
        else:
            print('there is no data match the condition.')
            return None
    else:
        print('the table is not exist.')
        return None
    
def read_sql_data(config, table_name, **kwg):
    """
    读取sql数据库中的一个表
    参数可以传递start_time,end_time
    no_sclae是一个数据扩展标志位，如果为1则表示数据需要扩展
    扩展方式为讲读取的数据等分nclip份，形成nclip！份数据
    """
    keywords = 'stime'
    no_scale = True
    nclip = 10
    is_bar = False
    if 'keywords' in kwg:
        keywords = kwg['keywords']
    if 'no_scale' in kwg:
        no_scale = kwg['no_scale']
    if 'nclip' in kwg:
        nclip = kwg['nclip']
    if 'is_bar' in kwg:
        is_bar = kwg['is_bar']
    print('reading a table named %s...'%table_name)
    conn = sql.create_connection(config)
    if (sql.table_exists(conn, table_name)) == 1:
        print('the table is existe and reading...')
        #cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        start = time.time()
        if 'start_time' in kwg and 'end_time' in kwg:
            start_time = kwg['start_time']
            end_time = kwg['end_time']
            sql_cmd = "select * from %s where %s between '%s' and '%s'"\
                        %(table_name, keywords, start_time, end_time)
        else:
            sql_cmd = "select * from %s"%table_name
            
        len_limit = sql.get_data_count(conn, sql_cmd)
        if 'limit' in kwg:
            limit = kwg['limit']
            if limit != None:
                #sql_cmd += ' limit %d'%limit
                len_limit = min(limit, len_limit) #获得查询条数限制值
        print("sql_cmd: %s"%sql_cmd)
        #创建进度条对象
        total = 10#默认10
        if is_bar:
            import processbar as pbar
            bar = pbar.Processbar(total)
            showbar = pbar.showbar(bar)
        else:
            showbar = None
        rows = sql.read_sql_bar(conn, sql_cmd, len_limit, showbar, total)
        if is_bar:
            bar.finish()
        #cursor.execute(sql_cmd)#获取数据行数
        #rows = cursor.fetchall()
        end = time.time()
        print('Finished reading the data from the database which took %d seconds.'%(end-start))
        print('the length of data is : %d'%len(rows))
        if len(rows) > 0:
            df = pd.DataFrame(rows)
            if no_scale:
                return df
            else:
                scale = len(df)
                interval = scale // nclip
                data_dict = {}
                for i in range(0, scale, interval):
                    for j in range(interval, scale, interval):
                        if (i+j) <= scale:
                            data_dict[str(i)+'_'+str(j)] = df[i:(i+j)].copy(deep=True)
                print('the first step of scaling data has been done.')
                return data_dict
        else:
            print('there is no data in the table.')
            return None
    else:
        print('the table is not exist.')
        return None
     
def save_data_xlsx(data, max_lens, filename, output_dir=None):
    """
    保存数据，如果数据行较大，进行分割分别放入sheet
    """
    if data is None:
        print('The data is None.')
    elif output_dir:
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        writer = pd.ExcelWriter(os.path.join(output_dir, '%s.xlsx'%filename))
        lens = data.shape[0]
        i = 0
        j = lens
        while j > 0:
            #切片，并保存至sheet
            k = i + min(j, max_lens)
            df = data[i:k]
            #保存处理后的数据
            df.to_excel(writer, '%d-%d'%(i,k))
            print('Saved the slip data in a sheet which named %d-%d.'%(i,k))
            i = i + max_lens
            j = j - max_lens
        print('The whole data has been saved.')
        
def save_dict_xlsx(data_dict,  filename, output_dir):
    if data_dict is None:
        print('The data is None.')
    elif output_dir:
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        writer = pd.ExcelWriter(os.path.join(output_dir, '%s.xlsx'%filename))
        for key, data in data_dict.items():
            data.to_excel(writer, key)
        print('The dict has been saved.')
        
def save_data_csv(data, filename, output_dir=None, chunksize=None):
    """
    保存数据
    """
    if data is None or len(data) == 0:
        print('The data is None.')
    elif output_dir:
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        start = time.time()
        data.to_csv(os.path.join(output_dir, '%s.csv'%filename), chunksize=chunksize,
                    index=False, index_label=False, encoding='gb18030')
        #data[:70000].to_csv(os.path.join(output_dir, 'test_%s.csv'%filename), index=False, index_label=False, encoding='gb18030')
        print('The whole data has been saved.')
        end = time.time()
        print('Done, it took %d seconds to save the data.'%(end-start))
        
def save_data_sql(data, config, table_name, if_exists='replace', chunksize=None):
    """
    将dataframe一次性保存进sql中的表
    如果表存在，默认替换
    """
    if data.empty != True:
        print('Saving the data into the sql...')
        host, user, password, db, port =config['s'], config['u'],\
                                    config['p'], config['db'], config['port']
        engine = sql.create_sql_engine(db, user, password, host, port)
        dtypedict = sql.mapping_df_types(data)
        data.to_sql(table_name, engine, if_exists=if_exists, chunksize=chunksize, dtype=dtypedict)
        print('The whole data has been put into the sql.')
        
def insert_data_sql(data, config, table_name, *col):
    if data.empty != True:
        print('Inserting the data into the sql...')
        col = list(col)
        conn = sql.create_connection(config)
        if(sql.table_exists(conn, table_name) != 1):
            sql_cmd = "CREATE TABLE IF NOT EXISTS `%s`(\
                       `sys_id` VARCHAR(100) NOT NULL,\
                       `vin` VARCHAR(40) NOT NULL,\
                       PRIMARY KEY ( `sys_id` )\
                       )"%(table_name)
            sql.create_table(conn, sql_cmd, table_name)
            
        value = data[col]
        cols = ''
        for c in col:
            cols += c
            cols += ', '
        cols = cols[:-2]
        for i in range(len(value)):
            ds = value.iloc[i].tolist()
            vs = ''
            for d in ds:
                vs += "'"+ str(d) + "'"
                vs += ', '
            vs = vs[:-2]
            sql_cmd = "INSERT INTO %s (%s) VALUES (%s)"%(table_name, cols, vs)
            print('sql_cmd:' + sql_cmd)
            sql.exe_update(conn, sql_cmd)
        
def save_model_result(res, model_list, result_dir, prefix):
    if not os.path.exists(result_dir):
            os.mkdir(result_dir)
    writer = pd.ExcelWriter(os.path.join(result_dir, '%s_result.xlsx'%prefix))
    eva = pd.DataFrame()
    for s in res:
        res[s]['train'].to_excel(writer, model_list[s])
        res[s]['test'].to_excel(writer, model_list[s], startcol=3)
        eva = eva.append(res[s]['eva'])
    eva = eva[['type', 'EVS', 'MAE', 'MSE', 'R2']]
    eva.to_excel(writer, model_list['eva'])
    
def main():
    data_dir = '/Volumes/Elements/data/evb/apps.csv'
    nrows = 5000
    #df0 = read_csv(data_dir, nrows = 1)
    #df1 = read_csv(data_dir, skiprows=10, names=df0.columns)
    #chunks = pd.read_csv(data_dir,iterator = True,  sep='[,\t]', skiprows=2, header=None)
    #chunk = chunks.get_chunk(5)
    df2 = read_csv(data_dir, sep='[,\t]', skiprows=4184270, header=None, nrows=nrows, chunsize=100)
                   #parse_dates=[1], date_parser=lambda col: pd.to_datetime(col, utc=True)) #由于文件里含制表符，需要分割
    print (len(df2))
    print(df2.columns)
    save_data_csv(df2, 'test_data', './')
    
if __name__ == '__main__':
    main()