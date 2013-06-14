#!/usr/bin/env python
# vim:fileencoding=utf-8



'''
A python wrapper of HTCondor allowing to apply pure python functions

* The method map(...) is similar to the standard map function but spawns the
  function execution across condor

* the method imap_unordered is similar to the function itertools.imap
  it works like map(...) but returns an iterator immediately
  the returned data may be unordered


Example:
    def f(x):
        return x**2

    p = CondorPool()
    results = p.map(f, range(5))

If the function has several arguments:
    def g(x,y):
        return x+y

    p = CondorPool()
    results = p.map(g, range(5), range(5,10))


WARNING:
    The mapped function *must* be safely importable: if your main mapping code
    and the target function are contained in the same file, you should make
    sure to enclose your main code (in which the CondorPool class is used) in a
    "if __name__ == '__main__':" section.

HOW IT WORKS:
    A pyro4 server is started to share the inputs/outputs across the machines
    The N inputs are stored in the server, alons with the functions name and
    the file containing it.
    A condor script is written: this module (__main__ in condor.py) acts as the
    worker, and is executed N times by condor - the pyro server address is passed as an
    argument.
    Each worker then connects to the server, gets the inputs and the function
    to run, runs it and stores the results in the server.
    When all jobs are done, the main script returns the values.
'''


import os
import imp
import sys
import socket
import getpass
from multiprocessing import Queue
from time import sleep
from os.path import dirname, basename
from multiprocessing import Process
from os import system
from tmpfiles import Tmp
from sys import argv
import inspect
from string import join as sjoin
from bisect import bisect
import Pyro4
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='Pyro4') # ignore warning "HMAC_KEY not set, protocol data may not be secure"
Pyro4.config.SERVERTYPE = "multiplex"

try:
    from progressbar import ProgressBar, Percentage, Bar, ETA, Counter
    progressbar_available = True
except:
    progressbar_available = False


condor_header = '''
universe = vanilla
notification = Error
executable = {python_exec}
log = {dirlog}/$(Cluster).log
output = {dirlog}/$(Cluster).$(Process).out
error = {dirlog}/$(Cluster).$(Process).error
environment = "PYTHONPATH={pythonpath}"
requirements = (Memory >= {memory}) && (OpSys == "LINUX") && (LoadAvg < {loadavg})
'''

condor_job = '''
arguments = -m {worker} {pyro_uri} {job_id}
queue
'''


class Jobs(object):

    def __init__(self):
        self.inputs = []
        self.outputs = Queue() # (id, value) pairs
        self.ndone = 0    # number of finished jobs
        self.nq = 0  # number of queued results

    def putJob(self, job):
        self.inputs.append(job)

    def getJob(self, job_id):
        return self.inputs[job_id]

    def putResult(self, job_id, value):
        self.outputs.put((job_id, value))
        self.ndone += 1
        self.nq += 1

    def getResult(self):
        (k, v) = self.outputs.get()
        self.nq -= 1
        return v # return one value

    def getResults_sorted(self):

        results = []
        keys = []
        for _ in xrange(self.nq):
            (k, v) = self.outputs.get()
            index = bisect(keys, k)
            keys.insert(index, k)
            results.insert(index, v)
        self.nq = 0

        return results

    def left(self): # number of remaining jobs
        return len(self.inputs) - self.ndone

    def done(self): # number of finished jobs
        return self.ndone
    
    def total(self): # total number of jobs
        return len(self.inputs)
    
    def nqueued(self):
        return self.nq


def pyro_server(jobs, uri_q):

    # initialize the pyro4 daemon
    ip = socket.gethostbyname(socket.gethostname())
    if ip == '127.0.0.1':
        print 'Error retrieving local ip, exiting...'
        exit(1)
    daemon = Pyro4.Daemon(host=ip)

    uri = daemon.register(jobs)
    uri_q.put(uri)

    print 'starting pyro4 daemon at', uri
    daemon.requestLoop()


class CondorPool(object):

    def __init__(self, log='/tmp/condor-log-{}'.format(getpass.getuser()), loadavg = 0.8, memory = 2000):

        self.__log = log
        self.__loadavg = loadavg
        self.__memory = memory
        self.__server = None

    def map(self, function, *iterables):

        if len(iterables) == 0:
            return []

        jobs = self._condor_map_async(function, *iterables)

        #
        # wait for the jobs to finish
        #
        try:
            if progressbar_available:
                pbar = ProgressBar(widgets=[Percentage(),Counter(',%d/'+str(jobs.total())),Bar(),' ',ETA()],
                        maxval=jobs.total())
                pbar.start()
            while jobs.left() != 0:
                if progressbar_available:
                    pbar.update(jobs.done())
                else:
                    print '{}/{} results have been received'.format(
                            jobs.done(),
                            jobs.total())
                sleep(2)
            if progressbar_available:
                pbar.finish()
        except KeyboardInterrupt:
            self.__server.terminate()
            print 'interrupted!'
            raise



        #
        # store the results
        #
        results = jobs.getResults_sorted()

        #
        # terminate the pyro daemon
        #
        self.__server.terminate()

        # return only the results
        return results


    def imap_unordered(self, function, *iterables):

        if len(iterables) == 0:
            return
            yield

        jobs = self._condor_map_async(function, *iterables)

        while (jobs.left() != 0):
            if jobs.nqueued() > 0:
                yield jobs.getResult()
            else:
                sleep(2)

        #
        # terminate the pyro daemon
        #
        self.__server.terminate()

    def _condor_map_async(self, function, *iterables):

        if function.func_name == '<lambda>':
            raise Exception('Can not run lambda functions using condor.py')


        #
        # start the pyro daemon in a thread
        #
        uri_q = Queue()
        self.__server = Process(target=pyro_server, args=(Jobs(), uri_q))
        self.__server.start()
        sleep(1)
        uri = uri_q.get()
        uri_q.close()
        jobs = Pyro4.Proxy(uri)

        #
        # initializations
        #

        filename = os.path.abspath(inspect.getfile(function))
        if filename.endswith('.pyc'):
            filename = filename[:-1]
            print 'DEBUG PYC'
        function_str = function.__name__
        print 'Condor map function "{}" in "{}" with executable "{}"'.format(function_str, filename, sys.executable)
        print 'Log directory is "{}"'.format(self.__log)

        for args in zip(*iterables):
            jobs.putJob([filename, function_str, args])


        #
        # create log directory if necessary
        #
        if not os.path.exists(self.__log):
            os.mkdir(self.__log)


        #
        # create the condor script
        #
        condor_script = Tmp('condor.run')
        fp = open(condor_script, 'w')
        fp.write(condor_header.format(
            python_exec = sys.executable,
            pythonpath = sjoin(sys.path,':'),
            dirlog = self.__log,
            memory = self.__memory,
            loadavg = self.__loadavg))
        for i in xrange(jobs.total()):
            fp.write(condor_job.format(
                worker=__name__,
                pyro_uri=uri,
                job_id=i,
                ))
        fp.close()


        #
        # submit the jobs to condor
        #
        command = 'condor_submit {}'.format(condor_script)
        system(command)
        sleep(3)
        condor_script.clean()

        return jobs



def worker(argv):

    pyro_uri = argv[1]
    job_id = int(argv[2])

    # write a small header at the start of stdout and stderr logs
    msg = '### {} log on {} ###\n'
    sys.stdout.write(msg.format('Output', socket.gethostname()))
    sys.stderr.write(msg.format('Error', socket.gethostname()))

    #
    # connect to the daemon
    #
    jobs = Pyro4.Proxy(pyro_uri)
    filename, function, args = jobs.getJob(job_id)

    #
    # for safety,"cd" to the directory containing the target function
    #
    os.chdir(os.path.dirname(filename))

    #
    # load the target function
    #
    modname = basename(filename)
    if modname.endswith('.py'):
        modname = modname[:-3]

    module = imp.find_module(modname, [dirname(filename)])

    mod = imp.load_module(modname, *module)
    f = getattr(mod, function)

    try:
        result = f(*args)
    except:
        jobs.putResult(job_id, None)
        raise

    jobs.putResult(job_id, result)


if __name__ == '__main__':

    worker(argv)


