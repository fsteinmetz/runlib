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

        with TmpInput('/path/to/source.data') as f:
            # the source file is copied to a unique temporary directory
            # and f contains the file name of the temporary file
            # <use f as an input to a processor>
        # f in cleaned up at this point


        with TmpOutput('/path/to/target.data') as f:
            # at this point, f is the temporary file name (non-existing yet)
            # and f.target() is the target file
            #
            # <create file f>
            #
            if <f created successfully>:
                f.move() # move f to target

