#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''
Run several condor tests

Note: does not work through pytest. Should be run as a standard script.
'''

from runlib.condor import CondorPool
from time import sleep
import getpass

def f(x):
    return x**2

def getuser(x):
    return getpass.getuser()


def _test_condor_map():
    pool = CondorPool()
    assert sum(pool.map(f, range(10))) == sum(map(f, range(10)))

def _test_condor_user():
    """
    Check that job runs as the same user as  main script
    """
    res = CondorPool().map(getuser, range(10))
    assert res[0] == getuser(0), f'{res[0]} != {getuser(0)}'



if __name__ == '__main__':
    _test_condor_map()
    _test_condor_user()