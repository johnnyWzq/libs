#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 10 10:09:55 2019

@author: wuzhiqiang
#将单体的soh按照一定的算法得到pack的soh
#先简单按单体最低作为packsoh
"""

def single_pack_soh(data, algorithm='Min_func'):
    if algorithm == 'Min_func':
        data['pack'] = data.min(axis=1)
    return data