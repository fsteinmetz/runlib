#!/usr/bin/env python
# vim:fileencoding=utf-8:foldmethod=marker:foldlevel=99




from os import system
from sys import exit, path
try:
    import Pyro4
    pyro_available = True
    Pyro4.config.SERVERTYPE = "multiplex"
except:
    pyro_available = False


def get_pyro4():#{{{
    print 'Pyro4 has not been found'
    pkgz = 'Pyro4-4.14.tar.gz'
    pkg = pkgz.replace('.tar.gz', '')
    cmds = []
    cmds.append('wget http://pypi.python.org/packages/source/P/Pyro4/%s' % (pkgz))
    cmds.append('tar xzf %s' % (pkgz))
    cmds.append('rm -fv %s' % (pkgz))
    cmds.append('ln -sfn %s/src/Pyro4 Pyro4' % (pkg))
    print 'Download it in current directory (%s) using the following commands ?' % (path[0])
    for cmd in cmds:
        print '  ', cmd

    if raw_input('(y/N) ? ') in ['y', 'Y']:
        for cmd in cmds:
            system(cmd)
    else:
        print 'Exiting'
        exit(1)

    print 'Pyro4 has been downloaded in current directory'
    print 'Please restart this script'
    exit(1)
#}}}




if __name__ == '__main__':
    if not pyro_available:
        get_pyro4()
    else:
        print 'Pyro4 is already installed'
