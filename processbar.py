#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 10:42:01 2019

@author: wuzhiqiang
"""
import time
from progressbar import *
class Processbar():
    def __init__(self, total=100):
        widgets = ['Progress: ',Percentage(), ' ', Bar('#'),' ', Timer(),
           ' ', ETA(), ' ', FileTransferSpeed()]
        self.pbar = ProgressBar(widgets=widgets, maxval=total).start()

    def update(self, i):
        self.pbar.update(i+1)
        
    def finish(self):
        self.pbar.finish()

def showbar(bar):
    r = 0
    while True:
        n = yield r%2
        bar.update(n)
        r += 1
    
def create_bar(total):
    bar = pbar.Processbar(total)
    c = pbar.showbar(bar)
    
def create_bar1():
    N = 1000
    st = time.clock()
    for i in range(N):
        p = round((i + 1) * 100 / N)
        duration = round(time.clock() - st, 2)
        remaining = round(duration * 100 / (0.01 + p) - duration, 2)
        print("进度:{0}%，已耗时:{1}s，预计剩余时间:{2}s".format(p, duration, remaining), end="\r")
    time.sleep(0.01)
    
 
 
def dosomework():
    time.sleep(0.01)

def create_bar2(total):
    pbar = ProgressBar().start()
    for i in range(total):
        pbar.update(int((i / (total - 1)) * 100))
        dosomework()
    pbar.finish()

def create_bar3(total):
 
    widgets = ['Progress: ',Percentage(), ' ', Bar('#'),' ', Timer(),
               ' ', ETA(), ' ', FileTransferSpeed()]
    pbar = ProgressBar(widgets=widgets, maxval=10*total).start()
    for i in range(total):
        # do something
        pbar.update(10 * i + 1)
        dosomework()
    pbar.finish()