#!/usr/bin/python


'''
Usage: condorstats.py <condor log file>

Display a histogram of run times and memory usage.
'''



from sys import argv
import htcondor
import datetime
import numpy


nbins = 50

def get_total_seconds(td): # total_seconds is not available in python 2.6
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6

def histo(data, bins=50, width=100, fmt=lambda x: str(x)):
    H, edges = numpy.histogram(data, bins=bins)
    W = max(map(lambda x: len(str(fmt(x))), edges))

    for i in xrange(len(H)):
        print str(fmt(0.5*(edges[i] + edges[i+1]))).rjust(W, ' '), '#'*int(width*H[i]/float(max(H)))

def main():

    logfile = argv[1]
    evts = htcondor.read_events(open(logfile))


    times = []
    mem = {}
    for i in evts:

        if i['MyType'] == 'JobImageSizeEvent':
            # parse memory usage
            mem[i['Proc']] = int(i['MemoryUsage'])

        if i['MyType'] == 'JobTerminatedEvent':

            # parse total time
            assert i['TotalRemoteUsage'].startswith('Usr ')
            _, days, time, _, _, _ =  i['TotalRemoteUsage'].split(' ')
            H, M, S = time.replace(',','').split(':')
            times.append(get_total_seconds(datetime.timedelta(days=int(days),
                    hours=int(H),
                    minutes=int(M),
                    seconds=int(S))))

    print
    print 'Histogram of run times (%d jobs)' % (len(times))
    histo(times, bins=30, fmt=lambda x: datetime.timedelta(seconds=int(x)))

    print
    print 'Histogram of memory usage (Mb, %d jobs)' % (len(mem))
    histo(mem.values(), bins=30, fmt=int)




if __name__ == '__main__':
    main()
