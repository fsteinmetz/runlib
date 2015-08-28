#!/usr/bin/env python
# vim:fileencoding=utf-8



'''
A python interface to massively parallel computing frameworks (HTCondor and Sun Grid Engine)

* The method map(...) is similar to the standard map function but spawns the
  function execution using the processing framework.

* the method imap_unordered is similar to the function itertools.imap
  it works like map(...) but returns an iterator immediately
  the returned data may be unordered


Example: using HTCondor framework
    def f(x):
        return x**2

    if __name__ == '__main__':
        p = CondorPool()
        results = p.map(f, range(5))

Example: using SGE framework (Sun Grid Engine)
    def f(x):
        return x**2

    if __name__ == '__main__':
        p = QsubPool()
        results = p.map(f, range(5))

Example: if the function has several arguments
    def g(x,y):
        return x+y

    if __name__ == '__main__':
        p = CondorPool()
        results = p.map(g, range(5), range(5,10))

Example: using imap instead of map.
    The method imap_unordered is equivalent to multiprocessing.Pool's
    imap_unordered. It returns an iterator (imediately) instead of the list of
    results. This allows to start using the results while processing is still
    on-going.

    def f(x):
        return x**2

    if __name__ == '__main__':
        p = CondorPool()
        for result in p.imap_unordered(f, range(5)):
            print result # result will be available as soon as possible


WARNING:
    The mapped function *must* be safely importable: if your main mapping code
    and the target function are contained in the same file, you should make
    sure to enclose your main code (in which the CondorPool class is used) in a
    "if __name__ == '__main__':" section.

HOW IT WORKS:
    A pyro4 server is started to share the inputs/outputs across the machines
    The N inputs are stored in the server, along with the functions name and
    the file containing it.
    The worker (contained in the __main__ of this file) is called N times using
    the processing framework SGE or HTCondor. The address of the pyro server is
    passed to the worker, which connects to it and receives the function name
    and file, along with the arguments to use. The function is executed and the
    results are stored on the pyro server.
'''


from __future__ import print_function
import importlib
import os
import sys
import socket
import getpass
from math import ceil
from multiprocessing import Queue
from time import sleep
from multiprocessing import Process
from os import system, makedirs
from tmpfiles import Tmp
from sys import argv
import inspect
from bisect import bisect
from datetime import datetime, timedelta
import Pyro4
import textwrap
import warnings
from collections import Counter as CCounter
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
    class ProgressBar(object):
        def __init__(self, *args, **kwargs):
            self.__total = kwargs['maxval']
            self.__previous = None
        def start(self): pass
        def update(self, N):
            if N != self.__previous:
                print('[{}] {}/{} results have been received'.format(
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        N, self.__total))
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

def chunks(l, n):
    '''
    Yield successive n-sized chunks from l.
    '''
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

class Jobs(object):
    '''
    A class to manage the jobs inputs/outputs
    '''

    # status of jobs
    status_waiting = 0     # waiting  - job has not started
    status_sending = 1     # sending  - job is being sent to the worker
    status_running = 2     # running  - job is running
    status_storing = 3     # storing  - job is being stored in results queue
    status_stored  = 4     # stored   - job has been stored in results queue
    status_done    = 5     # done     - job has been dequeued

    max_counter = 20

    def __init__(self, filename, function_str, custom=[], nqueue=-1):
        self.inputs = []
        self.outputs = Queue()  # (id, value) pairs
        self.__totaltime = timedelta(0)
        self.__status = []
        self.__filename = filename            # name of the module containing the function to execute
                                              # (absolute file name)
        self.__function_str = function_str    # name of the function to execute
        self.__custom = custom                # additional attributes of module necessary
                                              # to pass the arguments
                                              # (for example, custom classes)
        self.__stopping = False  # a flag to stop the server
        self.__nqueue = nqueue    # maximum number of elements in output queue
                                  # (avoid memory overflow in some cases)

        self.__results = []   # initialize the list of resuts (filled by map())
        self.__job_ids = []   # job ids to store the results by job id

        self.__counter = {}   # number of occurence of each job result (string or int only)
        self.__endtimes = {}  # last occurence of each result

    def nqueue(self):
        return self.__nqueue

    def filename(self):
        return self.__filename

    def function_str(self):
        return self.__function_str

    def custom(self):
        return self.__custom

    def putJob(self, job):

        # the job starts with 'waiting'
        self.__status.append(self.status_waiting)

        self.inputs.append(job)

    def getJob(self, job_id):

        # starting to send the job
        self.__status[job_id] = self.status_sending

        args = self.inputs[job_id]

        # job becomes 'running'
        self.__status[job_id] = self.status_running

        return args

    def putResult(self, TUPLE):

        if self.__stopping: return

        # the job becomes 'stored'
        job_id = TUPLE[0]
        self.__status[job_id] = self.status_storing

        self.outputs.put(TUPLE)

        # done storing the job
        self.__status[job_id] = self.status_stored

    def getResult(self):
        '''
        get one result, returns (job_id, result, time)
        '''

        (job_id, v, t) = self.outputs.get()

        # the job becomes 'fetched'
        self.__status[job_id] = self.status_done

        self.__totaltime += t

        # count the occurences of each result
        # (only for int and string, with a limit of max_counter)
        # for other classes, count only the classes
        if isinstance(v, int) or isinstance(v, str):
            if len(self.__counter) < self.max_counter:
                if v in self.__counter:
                    self.__counter[v] += 1
                else:
                    self.__counter[v] = 1
                self.__endtimes[v] = datetime.now()
        else:
            vclass = v.__class__
            if vclass in self.__counter:
                self.__counter[vclass] += 1
            else:
                self.__counter[vclass] = 1
            self.__endtimes[vclass] = datetime.now()

        return (job_id, v, t)

    def resultCounter(self):
        return self.__counter, self.__endtimes

    def stop(self):
        self.__stopping = True

        # wait until nothing is 'storing' or 'sending'
        while True:
            nsending = self.__status.count(self.status_sending)
            nstoring = self.__status.count(self.status_sending)
            if (nsending + nstoring == 0): break
            print('{} elements are being sent, {} are being stored, waiting...'.format(nsending, nstoring))
            sleep(2)

    def nstored(self):
        return self.__status.count(self.status_stored)

    def nfetched(self):
        return self.__status.count(self.status_done)

    def counter(self):
        return CCounter(self.__status)

    def status(self):
        '''
        returns a string describing the status,
        and the number of jobs done (stored or fetched)
        '''

        count = self.counter()
        S = []

        for (stat, desc) in [
                (self.status_waiting, 'waiting'),
                (self.status_sending, 'sending'),
                (self.status_running, 'running'),
                (self.status_storing, 'storing'),
                (self.status_storing, 'storing'),
                (self.status_stored, 'stored'),
                (self.status_done, 'done'),
                ]:

            if count[stat] > 0:
                S.append('{} {}'.format(count[stat], desc))

        return '[{}] '.format('|'.join(S)), count[self.status_stored] + count[self.status_done]

    def finished(self, mode):
        '''
        returns whether all the jobs are done
        'map' mode:  first dequeue all stored results
        'imap' mode: no need to dequeue the jobs, they are
                     dequeued in imap_unordered
        '''
        if mode == 'map':

            # dequeue all stored jobs
            for _ in range(self.nstored()):
                (job_id, v, _) = self.getResult()
                self.__status[job_id] = self.status_done
                index = bisect(self.__job_ids, job_id)
                self.__job_ids.insert(index, job_id)
                self.__results.insert(index, v)

        return self.nfetched() == self.total()

    def results(self):
        return self.__results

    def endtimes(self):
        return self.__endtimes

    def total(self):
        ''' total number of jobs '''
        return len(self.inputs)

    def totaltime(self):
        return self.__totaltime



def pyro_server(jobs, uri_q):

    # initialize the pyro4 daemon
    ip = socket.gethostbyname(socket.gethostname())
    if ip == '127.0.0.1':
        print('Error retrieving local ip, exiting...')
        exit(1)
    daemon = Pyro4.Daemon(host=ip)

    uri = daemon.register(jobs)
    uri_q.put(uri)

    print('starting pyro4 daemon at', uri)
    daemon.requestLoop()



class Pool(object):
    '''
    This is a generic base class for a pool of jobs

    Arguments:
        - progressbar (bool, default True)
        - nqueue: maximum number of results in queue. Used with imap_unordered
          to avoid potential overflow of the results queue
        - custom: list of attributes necessary to pass the arguments
                  (custom classes)
    '''

    def __init__(self, progressbar=True, nqueue=-1, custom=[]):
        self.__progressbar = progressbar
        self.__server = None
        self.__nqueue = nqueue
        self.__custom = custom

    def map(self, function, *iterables):

        if (len(iterables) == 0) or (len(iterables[0]) == 0):
            return []

        jobs = self._map_async(function, *iterables)

        if jobs.nqueue() > 0:
            raise Exception('map is incompatible with the use of nqueue = {}'.format(jobs.nqueue()))

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
                status, ndone = jobs.status()
                if self.__progressbar:
                    custom.set(status)
                    pbar.update(ndone)
                sleep(2)
            if self.__progressbar:
                custom.set('')
                pbar.finish()
        except KeyboardInterrupt:
            jobs.stop()
            self.__server.terminate()
            print('interrupted!')
            raise


        #
        # store the results
        #
        results = jobs.results()

        # display total time
        if self.__progressbar:
            totaltime = datetime.now() - t0
            print('Elapsed time:', totaltime)
            print('Total time:', jobs.totaltime())
            print('Ratio is %.2f' % (jobs.totaltime().total_seconds()/totaltime.total_seconds()))

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

        jobs = self._map_async(function, *iterables)

        if self.__progressbar:
            custom = Custom()
            pbar = ProgressBar(widgets=[custom,Percentage(),Counter(',%d/'+str(jobs.total())),Bar(),' ',ETA()],
                    maxval=jobs.total())
            pbar.start()

        t0 = datetime.now()
        try:
            while not jobs.finished('imap'):

                status, ndone = jobs.status()
                if self.__progressbar:
                    custom.set(status)
                    pbar.update(ndone)

                if jobs.nstored() > 0:
                    yield jobs.getResult()[1]
                else:
                    sleep(2)
        except KeyboardInterrupt:
            jobs.stop()
            self.__server.terminate()
            raise

        # display total time
        if self.__progressbar:
            custom.set('')
            pbar.finish()
            totaltime = datetime.now() - t0
            print('Total time:', totaltime)
            print('Total CPU time:', jobs.totaltime())
            print('Ratio is %.2f' % (jobs.totaltime().total_seconds()/totaltime.total_seconds()))


        #
        # terminate the pyro daemon
        #
        self.__server.terminate()


    def _map_async(self, function, *iterables):

        if function.__name__ == '<lambda>':
            raise Exception('Can not run lambda functions using condor.py')

        #
        # initializations
        #

        filename = os.path.abspath(inspect.getfile(function))
        if filename.endswith('.pyc'):
            filename = filename[:-1]
            print('DEBUG PYC')
        function_str = function.__name__
        print('Map function "{}" in "{}" with executable "{}"'.format(function_str, filename, sys.executable))

        #
        # start the pyro daemon in a thread
        #
        uri_q = Queue()
        self.__server = Process(target=pyro_server, args=(Jobs(filename, function_str,
            nqueue=self.__nqueue, custom=self.__custom), uri_q))
        self.__server.start()
        sleep(1)
        uri = uri_q.get()
        uri_q.close()
        jobs = Pyro4.Proxy(uri)

        #
        # Add jobs to server
        #
        for args in zip(*iterables):
            jobs.putJob(args)

        #
        # submit the jobs
        #
        self.submit(jobs, uri)

        return jobs

    def terminate_server(self):
        self.__server.terminate()

    def submit(self, jobs, uri):
        self.terminate_server()
        raise Exception('The Pool class is a base class, not intended to run jobs. Please use a subclass. ')



class CondorPool(Pool):
    '''
    This is a pool using HTCondor system

    Arguments:
        - log: location for storing the log files
        - loadavg: average load requirement passed to Qsub
        - memory requirement passed to Qsub
        - groupsize: launch jobs by groups of size groupsize
        - ngroups: adjust groupsize to use a maximum of ngroups
          this option supercedes groupsize
          default None: use groupsize
        - **kwargs: other keyword arguments passed to Pool
    '''

    def __init__(self, log='/tmp/condor-log-{}'.format(getpass.getuser()),
            loadavg = 2.,
            memory = 2000,
            groupsize = 1,
            ngroups = None,
            **kwargs):

        Pool.__init__(self, **kwargs)

        self.__log = log
        self.__loadavg = loadavg
        self.__memory = memory
        assert isinstance(groupsize, int) and (groupsize > 0)
        self.__groupsize = groupsize
        self.__ngroups = ngroups

    def submit(self, jobs, uri):

        print('Using condor')
        print('Log directory is "{}"'.format(self.__log))

        #
        # create log directory if necessary
        #
        if not os.path.exists(self.__log):
            makedirs(self.__log)

        # adjust groupsize to ngroups
        if self.__ngroups is not None:
            self.__groupsize = int(jobs.total()/float(self.__ngroups))+1

        condor_header = textwrap.dedent('''
        universe = vanilla
        notification = Error
        executable = /usr/bin/env
        log = {dirlog}/$(Cluster).log
        output = {dirlog}/$(Cluster).$(Process).out
        error = {dirlog}/$(Cluster).$(Process).error
        environment = "LD_LIBRARY_PATH={ld_library_path} PYTHONPATH={pythonpath} PATH={path}"
        requirements = (OpSys == "LINUX") && (LoadAvg < {loadavg})
        request_memory = {memory}
        request_cpus = 1
        ''')

        condor_job = textwrap.dedent('''
        arguments = "sh -c '{python_exec} -m {worker} {pyro_uri} C {job_ids}'"
        queue
        ''')
        with Tmp('condor.run') as condor_script:

            #
            # create the condor script
            #
            fp = open(condor_script, 'w')
            fp.write(condor_header.format(
                pythonpath = ':'.join(sys.path),
                path = os.environ.get("PATH", ""),
                ld_library_path = os.environ.get("LD_LIBRARY_PATH", ""),
                dirlog = self.__log,
                memory = self.__memory,
                loadavg = self.__loadavg))
            for grp in chunks(range(jobs.total()), self.__groupsize):
                job_ids = ' '.join(map(str, grp))  # a string containing all the jobs in this group
                fp.write(condor_job.format(
                    worker=__name__,
                    python_exec = sys.executable,
                    pyro_uri=uri,
                    job_ids=job_ids,
                    ))
            fp.close()


            #
            # submit the jobs to condor
            #
            command = 'condor_submit -terse {}'.format(condor_script)
            ret = system(command)
            if ret != 0:
                self.terminate_server()
                raise Exception('Could not run %s' % (command))

            sleep(3)



class QsubPool(Pool):
    '''
    This is a pool using QSUB (SGE) system

    Arguments:
        - log: location for storing the log files
        - loadavg: average load requirement passed to Qsub
        - memory requirement passed to Qsub
        - ngroups: use a maximum of ngroups
          default None: use 1 group per job
        - **kwargs: other keyword arguments passed to Pool
    '''

    def __init__(self, log='/tmp/qsub-log-{}'.format(getpass.getuser()),
                 loadavg = 2.,
                 memory = 2000,
                 ngroups = None,
                 **kwargs):

        Pool.__init__(self, **kwargs)

        self.__log = log
        self.__loadavg = loadavg
        self.__memory = memory
        self.__ngroups = ngroups

    def submit(self, jobs, uri):

        print('Using QSUB')
        print('Log directory is "{}"'.format(self.__log))

        #
        # create log directory if necessary
        #
        if not os.path.exists(self.__log):
            os.mkdir(self.__log)

        if self.__ngroups is None:
            self.__ngroups = jobs.total()
        groupsize = int(ceil(jobs.total()/float(self.__ngroups)))

        qsub_header = textwrap.dedent('''
        #PBS -S /bin/bash
        #PBS -o {dirlog}/out.$PBS_JOBID
        #PBS -e {dirlog}/err.$PBS_JOBID
        #PBS -t 0-{ngroups}
        export LD_LIBRARY_PATH={ld_library_path} 
        export PYTHONPATH={pythonpath} 
        export PATH={path}
        ''')

        qsub_job = textwrap.dedent('''
        sh -c '{python_exec} -m {worker} {pyro_uri} Q $PBS_ARRAYID {groupsize} {njobs}'
        ''')

        with Tmp('qsub.pbs') as qsub_script:
            #
            # create the QSUB script
            #
            fp = open(qsub_script, 'w')
            fp.write(qsub_header.format(
                pythonpath = ':'.join(sys.path),
                path = os.environ.get("PATH", ""),
                ld_library_path = os.environ.get("LD_LIBRARY_PATH", ""),
                dirlog = self.__log,
                memory = self.__memory,
                loadavg = self.__loadavg,
                ngroups = self.__ngroups-1))
            fp.write(qsub_job.format(
                    worker=__name__,
                    python_exec = sys.executable,
                    pyro_uri=uri,
                    groupsize=groupsize,
                    njobs=jobs.total(),
                    ))
            fp.close()

            #
            # submit the jobs to QSUB
            #
            command = 'qsub {}'.format(qsub_script)
            ret = system(command)
            if ret != 0:
                self.terminate_server()
                raise Exception('Could not run %s' % (command))

            sleep(3)


def monitor(pyro_uri):

    import curses
    import operator

    try:
        jobs = Pyro4.Proxy(pyro_uri)
    except:
        print('Cannot connect to {}'.format(pyro_uri))
        exit(1)

    # initialize curses
    stdscr = curses.initscr()
    curses.noecho()
    curses.cbreak()
    stdscr.keypad(1)
    stdscr.nodelay(1)

    # display header
    line = 0
    stdscr.addstr(line, 0, 'Monitoring pyro server {}...'.format(pyro_uri)) ; line += 1
    stdscr.addstr(line, 0, 'Press q to quit') ; line += 1
    line += 1
    stdscr.addstr(line, 0, 'Last results obtained:') ; line += 1

    cnt = 0
    while True:

        c = stdscr.getch()
        if c == ord('q'):
            break
        try:
            sleep(0.25)
        except KeyboardInterrupt:
            break

        if cnt == 0:

            try:
                count, endtimes = jobs.resultCounter()
            except:
                print('Server has been terminated.')
                break

            # sort results by increasing endtime
            endtimes_srt = sorted(endtimes.items(), key=operator.itemgetter(1))

            if len(count) == 0:
                stdscr.addstr(line, 0, '(None)')

            for i in xrange(len(count)):
                k, t = endtimes_srt[i]
                c = count[k]
                message = ' {} ({} {}, latest {} ago)'.format(k, c,
                        {True: 'time', False: 'times'}[c == 1],
                        timedelta(seconds=int((datetime.now() - t).total_seconds())))
                stdscr.move(line+i, 0)
                stdscr.clrtoeol()
                stdscr.addstr(line+i, 0, message)


        cnt += 1
        if cnt > 10: cnt = 0


    # uninitialize curses
    curses.nocbreak()
    stdscr.keypad(0)
    curses.echo()
    curses.endwin()


def worker(argv):

    pyro_uri = argv[1]
    method = argv[2]  # method to specify the jobs ids
    if method == 'C':
        # condor method, specify all job ids
        job_ids = map(int, argv[3:])  # list of job ids to process
    elif method == 'Q':
        # qsub method, specify group id, group size and last job id
        group_id = int(argv[3])
        groupsize = int(argv[4])
        njobs = int(argv[5])
        job_ids = range(group_id*groupsize, (group_id+1)*groupsize)
        job_ids = filter(lambda x: x<njobs, job_ids)

    exceptions = []

    #
    # connect to the daemon
    #
    jobs = Pyro4.Proxy(pyro_uri)
    filename = jobs.filename()
    function_str = jobs.function_str()
    custom = jobs.custom()

    #
    # for safety,"cd" to the directory containing the target function
    #
    os.chdir(os.path.dirname(filename))

    #
    # import the target module
    #
    modname = os.path.basename(filename)
    if modname.endswith('.py'):
        modname = modname[:-3]

    mod = importlib.import_module(modname)

    # load the function to execute
    f = getattr(mod, function_str)

    # load the additional attributes from module mod
    for a in custom:
        globals().update({a: getattr(mod, a)})

    # loop over the job(s)
    for job_id in job_ids:

        # write a small header at the start of stdout and stderr logs
        msg = '### {} log on {} (job {}) ###\n'
        sys.stdout.write(msg.format('Output', socket.gethostname(), job_id))
        sys.stdout.flush()
        sys.stderr.write(msg.format('Error', socket.gethostname(), job_id))
        sys.stderr.flush()

        t0 = datetime.now()

        # fetch the arguments
        try:
            args = jobs.getJob(job_id)
        except AttributeError as ex:
            jobs.putResult((job_id, ex, datetime.now() - t0))
            raise


        # wait until the output queue has less than N values
        while True:
            if jobs.nqueue() <= 0: break
            if jobs.nstored() < jobs.nqueue():
                break
            # sys.stderr.write('condor.py: results queue has {} items, waiting until it has less than {}...'.format(
            #     jobs.nstored(), jobs.nqueue()))
            # sys.stderr.flush()
            sleep(2)

        try:
            result = f(*args)
        except Exception as ex:
            jobs.putResult((job_id, ex, datetime.now() - t0))
            exceptions.append(ex)
        else:
            jobs.putResult((job_id, result, datetime.now() - t0))

    # raise an exception, if any
    if len(exceptions) > 0:
        raise exceptions[0]


if __name__ == '__main__':

    if len(argv) == 2:
        monitor(argv[1])
    else:
        worker(argv)


