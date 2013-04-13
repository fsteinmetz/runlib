#!/usr/bin/env python
# vim:fileencoding=utf-8



import sys
from os.path import abspath, dirname
sys.path.append(dirname(dirname(abspath(__file__)))) # add parent directory to the path

from condor import CondorPool as Pool
# from multiprocessing import Pool

from time import sleep

def f(x):
    print 'apply f', x
    sleep(x)
    return x**2

def red(x, y):
    print 'reduce', x, y
    return x+y

if __name__ == '__main__':

    pool = Pool()

    it = pool.imap_unordered(f, range(5))
    # it = pool.map(f, range(5))

    print 'result is ', reduce(red, it)

