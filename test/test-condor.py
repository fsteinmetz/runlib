#!/usr/bin/env python
# vim:fileencoding=utf-8



import sys
from os.path import abspath, dirname
sys.path.append(dirname(dirname(abspath(__file__)))) # add parent directory to the path

from condor import CondorPool as Pool
# from multiprocessing import Pool

from time import sleep
from numpy import eye

def f(x):
    sleep(x)
    return x**2

def g(x, y):
    sleep(x)
    return x**2 + y**2

def test1():
    print('\ntest 1: map')
    pool = Pool()
    print('->', sum(pool.map(f, list(range(10)))))

def test2():
    print('\ntest 2: imap_unordered')
    pool = Pool()
    it = pool.imap_unordered(f, list(range(10)))
    print('->', sum(it))

def test3():
    print('\ntest 3: map with 2 arguments')
    pool = Pool()
    print('->', sum(pool.map(g, list(range(10)), list(range(20, 30)))))

def test4():
    print('\ntest 3: imap_unordered with 2 arguments')
    pool = Pool()
    it = pool.imap_unordered(g, list(range(10)), list(range(20, 30)))
    print('->',  sum(it))

if __name__ == '__main__':

    test1()
    test2()
    test3()
    test4()
