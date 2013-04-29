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
from multiprocessing import Queue
from time import sleep
from os.path import dirname, basename
from multiprocessing import Process
from os import system
from tmpfiles import Tmp
from sys import argv
import inspect
from string import join as sjoin
import Pyro4
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='Pyro4') # ignore warning "HMAC_KEY not set, protocol data may not be secure"
Pyro4.config.SERVERTYPE = "multiplex"

try:
    from progressbar import ProgressBar
    progressbar_available = True
except:
    progressbar_available = False


condor_header = '''
universe = vanilla
notification = Error
executable = {python_exec}
log = {dirlog}/log
output = {dirlog}/$(Process).out
error = {dirlog}/$(Process).error
environment = "PYTHONPATH={pythonpath}"
requirements = (Memory >= {memory}) && (OpSys == "LINUX") && (LoadAvg < {loadavg})
'''

condor_job = '''
arguments = -m {worker} {pyro_uri}
queue
'''


class Jobs(object):

    def __init__(self):
        self.inputs = Queue()
        self.outputs = Queue()
        self.jobsleft = Queue()

    def putJob(self, job):
        self.jobsleft.put(0)
        self.inputs.put(job)


    def getJob(self):
        return self.inputs.get()

    def putResult(self, value):
        self.jobsleft.get()
        self.outputs.put(value)

    def getResult(self):
        return self.outputs.get()

    def resultsLeft(self):
        return self.outputs.qsize()

    def jobsLeft(self):
        return self.jobsleft.qsize()

    def totalLeft(self):
        return self.jobsLeft() + self.resultsLeft()

    def flush(self):
        while not self.inputs.empty():
            self.inputs.get()
        while not self.outputs.empty():
            self.outputs.get()
        while not self.jobsleft.empty():
            self.jobsleft.get()




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

    def __init__(self, log='condor-log', loadavg = 0.8, memory = 2000):

        self.__log = log
        self.__loadavg = loadavg
        self.__memory = memory
        self.__server = None

    def map(self, function, *iterables):

        jobs, njobs = self._condor_map_async(function, *iterables)

        #
        # wait for the jobs to finish
        #
        try:
            if progressbar_available:
                pbar = ProgressBar(maxval = njobs)
                pbar.start()
            while jobs.jobsLeft() != 0:
                if progressbar_available:
                    pbar.update(njobs - jobs.jobsLeft())
                else:
                    print '{}/{} results have been received'.format(
                            njobs - jobs.jobsLeft(),
                            njobs)
                sleep(1)
            if progressbar_available:
                pbar.finish()
        except KeyboardInterrupt:
            jobs.flush()
            self.__server.terminate()
            print 'interrupted!'
            raise



        #
        # store the results
        #
        results = []
        while jobs.resultsLeft() != 0:
            # get (count, result)
            results.append(jobs.getResult())

        # sort by count
        results.sort(key=lambda x: x[0])


        #
        # terminate the pyro daemon
        #
        self.__server.terminate()

        # return only the results
        return map(lambda x: x[1], results)


    def imap_unordered(self, function, *iterables):

        jobs, njobs = self._condor_map_async(function, *iterables)

        while (jobs.totalLeft() != 0):
            result = jobs.getResult()
            yield result[1]

        #
        # terminate the pyro daemon
        #
        self.__server.terminate()

    def _condor_map_async(self, function, *iterables):

        #
        # init jobs object
        #
        jobs = Jobs()

        #
        # start the pyro daemon in a thread
        #
        uri_q = Queue()
        self.__server = Process(target=pyro_server, args=(jobs, uri_q))
        self.__server.start()
        sleep(1)
        uri = uri_q.get()
        uri_q.close()

        #
        # initializations
        #

        filename = os.path.abspath(inspect.getfile(function))
        if filename.endswith('.pyc'):
            filename = filename[:-1]
            print 'DEBUG PYC'
        function_str = function.__name__
        print 'Condor map function "{}"'.format(function_str), 'in', filename, 'with executable', sys.executable

        count = 0
        for args in zip(*iterables):
            jobs.putJob([count, filename, function_str, args])
            count += 1

        njobs = len(iterables[0])


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
        for i in xrange(njobs):
            fp.write(condor_job.format(
                worker=__name__,
                pyro_uri=uri,
                ))
        fp.close()


        #
        # submit the jobs to condor
        #
        command = 'condor_submit {}'.format(condor_script)
        system(command)
        sleep(3)
        condor_script.clean()

        return jobs, njobs



def worker(argv):

    pyro_uri = argv[1]

    #
    # connect to the daemon
    #
    jobs = Pyro4.Proxy(pyro_uri)
    count, filename, function, args = jobs.getJob()

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
        jobs.putResult((count, None))
        raise

    jobs.putResult((count, result))


if __name__ == '__main__':

    worker(argv)


