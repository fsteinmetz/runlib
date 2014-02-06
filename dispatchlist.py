#!/usr/bin/env python2.7
# vim:fileencoding=utf-8



from sys import argv
from os import system, getcwd
from condor import CondorPool


def function(cmd, cwd):
    CMD = 'cd %s ; %s' % (cwd, cmd)
    print 'executing', CMD
    system(CMD)

if __name__ == '__main__':

    if len(argv) < 1:
        print 'Syntax: dispatchlist.py cmdfile'
        print '        Execute each command in cmdfile (1 command per line) on condor'
        exit(1)

    cwd = getcwd()
    cmdfile = argv[1]
    cmds = open(cmdfile).readlines()
    N = len(cmds)

    print 'Execute %d commands from file %s' % (N, cmdfile)

    pool = CondorPool()
    pool.map(function, cmds, [cwd]*N)

