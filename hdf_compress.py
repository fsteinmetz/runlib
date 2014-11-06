#!/usr/bin/env python
# vim:fileencoding=UTF-8

'''
hdf compression tool
François Steinmetz, started sept. 2006
'''

import pyhdf.SD
from runlib.tmpfiles import TmpOutput
import multiprocessing
from optparse import OptionParser
try:
    from progressbar import ProgressBar
except ImportError:
    ProgressBar = lambda : lambda x:x # disactivate progressbar if not available



COMPRESS = (pyhdf.SD.SDC.COMP_DEFLATE, 9)


def compress(filein):

    #
    # initialization
    #


    try:
        f1 = pyhdf.SD.SD(filein)
    except:
        print 'Could not open file "%s"' % (filein)
        return 1 # Error

    compress = ()
    try:
        compress = f1.select(0).getcompress()
    except: pass

    if compress == COMPRESS:
        return 2 # already compressed

    #
    # start compression
    #
    tmpout = TmpOutput(filein, overwrite=True)
    print 'Compressing', filein, '...'
    print '   (temporary file is {})'.format(tmpout)
    f2 = pyhdf.SD.SD(tmpout, pyhdf.SD.SDC.WRITE|pyhdf.SD.SDC.CREATE)

    # vérification présence attributs
    if (f1.attributes() != {}):
        for a in f1.attributes().keys():
            setattr(f2, a, f1.attributes()[a])

    ndatasets = f1.datasets().__len__()
    for i in range(ndatasets):
        f1sds = f1.select(i)

        dname = f1sds.info()[0]
        type  = f1sds.info()[3]
        shape = f1sds.info()[2]
        # print ' -> %d/%d : %s' % (i+1, ndatasets, dname)
        f2sds = f2.create(dname, type, shape)

        # set cal
        try:
            cal, cal_error, offset, offset_err, data_type = f1sds.getcal()
            f2sds.setcal(cal, cal_error, offset, offset_err, data_type)
        except: pass

        # set compress
        f2sds.setcompress(COMPRESS[0], COMPRESS[1])
#        ecriture donnees
        f2sds[:] = f1sds[:]

        # attributs de sds
        for a in f1sds.attributes().keys():
            if not(a in f2sds.attributes().keys()):
                # probleme pour affecter l'attribut '_FillValue' avec setattr
                # on utilise alors setfillvalue à la place
                if a == '_FillValue':
                    f2sds.setfillvalue(f1sds.attributes()[a])
                else:
                    setattr(f2sds, a, f1sds.attributes()[a])

        # fermeture
        f2sds.endaccess()
        f1sds.endaccess()


    f1.end()
    f2.end()

    tmpout.move()

    return 0



def main():

    #
    # parse arguments
    #
    parser = OptionParser()
    parser.add_option('-n', '--nthreads', dest='nthreads', type='int',
          default=1,
          help='number of threads ; default = 1 ; if 0, the number of CPUs is used')

    (options, args) = parser.parse_args()

    print 'Number of files to compress: {}'.format(len(args))


    if options.nthreads == 1:
        # sequential process
        res = []
        for i in ProgressBar()(args):
            res.append(compress(i))

    else:
        # start the pool and process the files
        if options.nthreads == 0:
            N = None
        else:
            N = options.nthreads
        pool = multiprocessing.Pool(N)

        # NOTE
        # see http://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool
        res = pool.map_async(compress, args).get(1e10)

    print 'done.'
    print '{} existing files'.format(res.count(2))
    print '{} errors'.format(res.count(1))
    print '{} files compressed'.format(res.count(0))


if __name__ == '__main__':
    main()

