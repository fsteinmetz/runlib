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
from datetime import datetime, timedelta
import Pyro4
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='Pyro4') # ignore warning "HMAC_KEY not set, protocol data may not be secure"

Pyro4.config.SERVERTYPE = "multiplex"

# the pickle serializer is less safe, but works for all objects
# the default and safe serializer for Pyro4 is 'serpent'
Pyro4.config.SERIALIZER = "pickle"
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')

#
# Progress bar
#
try:
    from progressbar import ProgressBar, Percentage, Bar, ETA, Counter, Widget
except:
    #
    # define a basic ProgressBar (text progressbar)
    # http://code.google.com/p/python-progressbar/
    # if the module is not available
    #
    from datetime import datetime
    class ProgressBar(object):
        def __init__(self, *args, **kwargs):
            self.__total = kwargs['maxval']
            self.__previous = None
        def start(self): pass
        def update(self, N):
            if N != self.__previous:
                print '[{}] {}/{} results have been received'.format(
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        N, self.__total)
                self.__previous = N
            pass
        def finish(self):
            self.update(self.__total)
    def Percentage(): pass
    def Bar(): pass
    def ETA(): pass
    def Counter(x): pass
    class Widget(): pass

class Custom(Widget):
    '''
    A custom ProgressBar widget to display arbitrary text
    '''
    def update(self, bar):
        try: return self.__text
        except: return ''
    def set(self, text):
        self.__text = text




condor_header = '''
universe = vanilla
notification = Error
executable = /usr/bin/env
log = {dirlog}/$(Cluster).log
output = {dirlog}/$(Cluster).$(Process).out
error = {dirlog}/$(Cluster).$(Process).error
environment = "LD_LIBRARY_PATH={ld_library_path} PYTHONPATH={pythonpath} PATH={path}"
requirements = (OpSys == "LINUX") && (LoadAvg < {loadavg})
request_memory = {memory}
'''

condor_job = '''
arguments = "sh -c '{python_exec} -m {worker} {pyro_uri} {job_id}'"
queue
'''




class Jobs(object):
    '''
    A class to manage the jobs inputs/outputs
    '''

    # status of jobs
    status_waiting = 0     # waiting  - job has not started
    status_running = 1     # running  - job is running
    status_stored  = 2     # stored   - job has been stored in results queue
    status_fetched  = 3    # fetched  - job has been dequeued

    def __init__(self):
        self.inputs = []
        self.outputs = Queue() # (id, value) pairs
        self.__totaltime = timedelta(0)
        self.__status = []

    def putJob(self, job):

        # the job starts with 'waiting'
        self.__status.append(self.status_waiting)

        self.inputs.append(job)

    def getJob(self, job_id):

        # the job becomes 'running'
        self.__status[job_id] = self.status_running

        return self.inputs[job_id]

    def putResult(self, TUPLE):

        # the job becomes 'stored'
        job_id = TUPLE[0]
        self.__status[job_id] = self.status_stored

        self.outputs.put(TUPLE)

    def getResult(self):

        (job_id, v, t) = self.outputs.get()

        # the job becomes 'fetched'
        self.__status[job_id] = self.status_fetched

        self.__totaltime += t
        return v # return one value

    def getResults_sorted(self):

        results = []
        keys = []
        for _ in xrange(self.nstored()):
            (job_id, v, t) = self.outputs.get()
            self.__status[job_id] = self.status_fetched
            index = bisect(keys, job_id)
            keys.insert(index, job_id)
            results.insert(index, v)
            self.__totaltime += t

        return results

    def nstored(self):
        return self.__status.count(self.status_stored)

    def nfetched(self):
        return self.__status.count(self.status_fetched)

    def nrunning(self):
        return self.__status.count(self.status_running)

    def ndone(self):
        ''' number of finished jobs (stored or fetched) '''
        return self.nstored() + self.nfetched()

    def finished(self, mode):
        '''
        returns whether all the jobs are finished
        'map' mode:  all jobs must be 'stored'
                     (they will be fetched afterwards)
        'imap' mode: all jobs must be 'fetched'
        '''
        if mode == 'map':
            return self.nstored() == self.total()

        elif mode == 'imap':
            # imap: all jobs must be 'fetched'
            return self.nfetched() == self.total()
        else:
            raise Exception('jobs.finished: mode should be either "map" or "imap"')

    def total(self):
        ''' total number of jobs '''
        return len(self.inputs)

    def totaltime(self):
        return self.__totaltime



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

    def __init__(self, log='/tmp/condor-log-{}'.format(getpass.getuser()),
            loadavg = 0.8,
            memory = 2000,
            progressbar = True):

        self.__log = log
        self.__loadavg = loadavg
        self.__memory = memory
        self.__server = None
        self.__progressbar = progressbar

    def map(self, function, *iterables):

        if len(iterables[0]) == 0:
            return []

        jobs = self._condor_map_async(function, *iterables)

        #
        # wait for the jobs to finish
        #
        t0 = datetime.now()
        try:
            if self.__progressbar:
                custom = Custom()
                pbar = ProgressBar(widgets=[custom,Percentage(),Counter(',%d/'+str(jobs.total())),Bar(),' ',ETA()],
                        maxval=jobs.total())
                pbar.start()
            while not jobs.finished('map'):
                if self.__progressbar:
                    custom.set('[%d running] ' % (jobs.nrunning()))
                    pbar.update(jobs.ndone())
                sleep(2)
            if self.__progressbar:
                custom.set('')
                pbar.finish()
        except KeyboardInterrupt:
            self.__server.terminate()
            print 'interrupted!'
            raise



        #
        # store the results
        #
        results = jobs.getResults_sorted()

        # display total time
        if self.__progressbar:
            totaltime = datetime.now() - t0
            print 'Total time:', totaltime
            print 'Total CPU time:', jobs.totaltime()
            print 'Ratio is %.2f' % (jobs.totaltime().total_seconds()/totaltime.total_seconds())

        #
        # terminate the pyro daemon
        #
        self.__server.terminate()

        # return only the results
        return results


    def imap_unordered(self, function, *iterables):

        if len(iterables[0]) == 0:
            return
            yield

        jobs = self._condor_map_async(function, *iterables)

        if self.__progressbar:
            custom = Custom()
            pbar = ProgressBar(widgets=[custom,Percentage(),Counter(',%d/'+str(jobs.total())),Bar(),' ',ETA()],
                    maxval=jobs.total())
            pbar.start()

        t0 = datetime.now()
        while not jobs.finished('imap'):

            if self.__progressbar:
                custom.set('[%d running] ' % (jobs.nrunning()))
                pbar.update(jobs.ndone())

            if jobs.nstored() > 0:
                yield jobs.getResult()
            else:
                sleep(2)

        # display total time
        if self.__progressbar:
            custom.set('')
            pbar.finish()
            totaltime = datetime.now() - t0
            print 'Total time:', totaltime
            print 'Total CPU time:', jobs.totaltime()
            print 'Ratio is %.2f' % (jobs.totaltime().total_seconds()/totaltime.total_seconds())


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
            pythonpath = sjoin(sys.path,':'),
            path = os.environ.get("PATH", failobj=""),
            ld_library_path = os.environ.get("LD_LIBRARY_PATH", failobj=""),
            dirlog = self.__log,
            memory = self.__memory,
            loadavg = self.__loadavg))
        for i in xrange(jobs.total()):
            fp.write(condor_job.format(
                worker=__name__,
                python_exec = sys.executable,
                pyro_uri=uri,
                job_id=i,
                ))
        fp.close()


        #
        # submit the jobs to condor
        #
        command = 'condor_submit {}'.format(condor_script)
        ret = system(command)
        if ret != 0:
            self.__server.terminate()
            condor_script.clean()
            raise Exception('Could not run %s' % (command))

        sleep(3)
        condor_script.clean()

        return jobs



def worker(argv):

    pyro_uri = argv[1]
    job_id = int(argv[2])

    t0 = datetime.now()

    # write a small header at the start of stdout and stderr logs
    msg = '### {} log on {} ###\n'
    sys.stdout.write(msg.format('Output', socket.gethostname()))
    sys.stdout.flush()
    sys.stderr.write(msg.format('Error', socket.gethostname()))
    sys.stderr.flush()

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
        jobs.putResult((job_id, None, datetime.now() - t0))
        raise

    jobs.putResult((job_id, result, datetime.now() - t0))


if __name__ == '__main__':

    worker(argv)


