#!/usr/bin/env python2.7
# vim:fileencoding=utf-8



from sys import argv
from os import system, getcwd
from condor import CondorPool


def function(cmd, cwd, arg):
    CMD = 'cd {} ; {} {}'.format(cwd, cmd, arg)
    print 'executing', CMD
    system(CMD)

if __name__ == '__main__':

    if len(argv) < 2:
        print 'Syntax: dispatch.py cmd arg1 arg2...'
        print '        Execute cmd with various arguments on condor'
        print 'Exemple:'
        print "dispatch.py 'gunzip -v' *.gz"
        exit(1)

    cwd = getcwd()
    cmd = argv[1]
    args = argv[2:]
    N = len(args)

    if N == 0:
        print 'No argument provided'
        exit(0)

    print 'Execute command "{}"'.format(cmd)
    print 'Number of arguments is {}'.format(N)

    pool = CondorPool()
    pool.map(function, [cmd]*N, [cwd]*N, args)

