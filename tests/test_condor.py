#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''
Run several condor tests

Can be run as a standard script (tests work differently then, regarding imports):
    python -m tests.test_condor

Can watch condor progress with:
    watch condor_q
'''

import pytest
from runlib.condor import CondorPool
from tests.sample_function import sample_function
import getpass

def f(x):
    return x**2

def getuser(x):
    return getpass.getuser()


@pytest.mark.parametrize('func', [f, sample_function])
def test_condor_map(func):
    pool = CondorPool()
    assert sum(pool.map(func, range(10))) == sum(map(f, range(10)))

def test_condor_user():
    """
    Check that job runs as the same user as  main script
    """
    res = CondorPool().map(getuser, range(10))
    assert res[0] == getuser(0), f'{res[0]} != {getuser(0)}'


if __name__ == '__main__':
    test_condor_map(f)
    test_condor_map(sample_function)