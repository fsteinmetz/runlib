runlib
======

Some python utility tools for massively parallel processing and
data handling.


1. condor.py
------------

A simple python interface to massively parallel computing frameworks.
Initially developped for [HTCondor](http://research.cs.wisc.edu/htcondor/) ; it
has been extended to support Sun Grid Engine (qsub).

_Example:_

    from condor import CondorPool  # or QsubPool

    def f(x):
        return x**2

    if __name__ == '__main__':
        p = CondorPool()
        results = p.map(f, range(5))


2. tmpfiles.py
--------------

Management of temporary files: inputs to a processing (TmpManager().input),
outputs of a processing (TmpManager().output), pure temporary files
(TmpManager().file) and temporary directories (TmpManager().directory).
Includes several features: cleanup after use, automatic uncompress of input
files (gz, bz, tar, zip), check disck space, unique paths, etc.

_Example:_

        with TmpManager('/tmp/') as tm:  # instantiate the tmp manager on directory '/tmp/'

            # decompress a file to tmp directory and return the name
            # of the decompressed file
            input1 = tm.input('/data/file.gz')

            # if the input is an archive, returns a list of all the files in
            # the archive
            file_list = tm.input('/data/file.tar.gz')

            # returns a temporary file that will be cleaned up
            tmp = tm.file('filename.txt')

            # returns a temporary directory
            dir = tm.directory()

            # returns a filename in tmp directory
            # this file will be created afterwards, and moved to destination
            # upon commit()
            out = tm.output('/data/result.dat') 


            # move all output files to their destination
            # (otherwise they are cleared)
            tm.commit()

        # NOTE: all temporary files are cleared up when leaving the 'with' context
        # even in case of error in the python code.

