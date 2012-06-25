#!/usr/bin/env python
# vim:fileencoding=utf-8:foldmethod=marker:foldlevel=99


'''
How to use this lib:
                       # local       # server            # worker
proc = Processor(...)  # do nothing  # write server uri  # connects to the server,
                       # except init #                   # run the jobs and exists
proc.submit(...)       # run the job # populate queue    # -
proc.wait()            # do nothing  # run server loop   # -
proc.results   # contains the results
'''

from select import select
from os.path import exists
import Queue as queue
from os import system
from sys import exit, stdin, path
from multiprocessing import Process
from time import sleep
from datetime import datetime
import socket
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


class Jobs(object):#{{{
    def __init__(self):
        self.jobs = queue.Queue()
        self.njobs = -1
        self.inprogress = queue.Queue()
        self.results = queue.Queue()
    def getJob(self):
        self.inprogress.put(0)
        return self.jobs.get()
    def putJob(self, x):
        self.jobs.put(x)
    def setNJobs(self):
        self.njobs = self.jobs.qsize()
    def jobsEmpty(self):
        return self.jobs.empty()
    def resultsEmpty(self):
        return self.results.empty()
    def getResult(self):
        return self.results.get()
    def isDone(self):
        if self.njobs == -1:
            print 'Warning, njobs is not initialized when calling "isDone"'
        return self.results.qsize() == self.njobs
    def nJobs(self):
        return self.jobs.qsize()
    def nip(self):
        return self.inprogress.qsize()
    def nDone(self):
        return self.results.qsize()
    def putResult(self, res):
        self.results.put(res)
        self.inprogress.get()
#}}}

class Processor(object):

    def __init__(self, server=False, server_file='server.status', function=None):#{{{

        self.function = function
        self.server_file = server_file
        self.results = []

        if server:

            if not pyro_available:
                get_pyro4()

            if not exists(server_file):

                #
                # create jobs list
                #
                self.jobs = Jobs()
                self.mode = 'server'

            else:

                #
                # starts worker
                #
                self.mode = 'worker'
                uri = open(server_file).read()
                print 'Trying to reach server at "%s"' % (uri)

                try:
                    self.jobs = Pyro4.Proxy(uri)
                    while not self.jobs.jobsEmpty():
                        self.execute()
                    print 'No job left.'
                except Pyro4.errors.CommunicationError:
                    print 'Cannot connect to %s' % (uri)
                    print 'You may want to remove %s if you are sure that the server is down' % (server_file)
                    exit(1)
                exit(0)

        else:
            self.mode = 'local'
        #}}}

    def run(self, args, kwargs):
        if self.function == None:
            res = system(args[0])
            if res:
                print 'Interrupted'
                exit(1)

        else:
            res = self.function(*args, **kwargs)
        return res


    def execute(self):#{{{
        '''
        execute one job (in client mode)
        '''
        (args, kwargs) = self.jobs.getJob()
        res = self.run(args, kwargs)
        self.jobs.putResult(res)
    #}}}

    def submit(self, *args, **kwargs):#{{{
        if self.mode == 'local':
            res = self.run(args, kwargs)
            self.results.append(res)
        elif self.mode == 'server':
            self.jobs.putJob((args, kwargs))
    #}}}

    def loop(self):
        ip = socket.gethostbyname(socket.gethostname())
        if ip == '127.0.0.1':
            print 'Error retrieving local ip, exiting...'
            exit(1)
        daemon = Pyro4.Daemon(host=ip)
        uri = daemon.register(self.jobs)

        print 'Server uri is', uri
        print 'Writing', self.server_file
        fd = open(self.server_file, 'w')
        fd.write(str(uri))
        fd.close()

        print 'Starting server loop...'
        daemon.requestLoop()

    def wait(self):#{{{

        if self.mode == 'local':
            return

        start = datetime.now()
        self.jobs.setNJobs()

        # start server loop in background
        p = Process(target=self.loop)
        p.start()

        # interactive process manager
        sleep(1)
        uri = open(self.server_file).read()
        jobs = Pyro4.Proxy(str(uri))
        print '(q) quit (other) info'
        while not jobs.isDone():
            rlist, _, _ = select([stdin], [], [], 2)
            if rlist:
                s = stdin.readline()
                if s == 'q\n':
                    break
                elapsed = datetime.now()-start
                njobs_left = jobs.nJobs()
                njobs_done = jobs.nDone()
                if njobs_done != 0:
                    eta = njobs_left * elapsed/njobs_done
                else:
                    eta = 'N/A'
                print '%d jobs left, %d in progress, %d done, elapsed=%s, ETA=%s' % (njobs_left, jobs.nip(), njobs_done, elapsed, eta)

        print 'Getting results...'
        while not jobs.resultsEmpty():
            self.results.append(jobs.getResult())

        print 'Done'

        p.terminate()
        system('rm -fv %s' % (self.server_file))
    #}}}

def main():
    def f(x):
        return x**2

    proc = Processor(function=f, server=True)
    for i in xrange(1000):
        proc.submit(i)
    proc.wait()

    print len(proc.results), proc.results[-5:]


if __name__ == '__main__':
    main()
