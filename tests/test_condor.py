#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from runlib.condor import CondorPool
from time import sleep

def f(x):
    sleep(x)
    return x**2

def test_condor_map():
    print('\ntest 1: map')
    pool = CondorPool()
    print('->', sum(pool.map(f, list(range(10)))))



if __name__ == '__main__':
    test_condor_map()