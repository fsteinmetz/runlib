#!/usr/bin/env python
# vim:fileencoding=utf-8


'''
Displays condor's list of current clusters, number of jobs and owner
'''


import subprocess


if __name__ == '__main__':

    out, err = subprocess.Popen('condor_q', stdout=subprocess.PIPE).communicate()

    clusters = {}
    for line in out.split('\n'):

        try:
            id, owner, _, _, _, st, prio = line.split()[:7]
        except ValueError:
            continue
        if not '.' in id: continue

        cl = id.split('.')[0]

        # clusters = [owner, total, running, sum_prio]
        try:
            clusters[cl][1] += 1
        except KeyError:
            clusters[cl] = [owner, 1, 0, 0.]
        if st == 'R':
            clusters[cl][2] += 1
        clusters[cl][3] += float(prio)

    w1, w2, w3 = 10, 12, 10
    print 'CLUSTER'.ljust(w1), 'OWNER'.ljust(w2), 'RUNNING'.ljust(w3), 'PRIORITY'
    for cl in clusters.keys():
        print cl.ljust(w1), \
                clusters[cl][0].ljust(w2), \
                ('%d/%d' % (clusters[cl][2], clusters[cl][1])).ljust(w3), \
                clusters[cl][3]/clusters[cl][1]

