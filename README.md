runlib
======

Several utility tools written in python, for massively parallel processing and
data handling.


1. condor.py
------------

A clean and simple python interface to massively parallel computing frameworks.
Initially developped for [HTCondor](http://research.cs.wisc.edu/htcondor/) ; it
has been extended to support [Oracle Grid Engine (previously Sun Grid Engine)](http://en.wikipedia.org/wiki/Oracle_Grid_Engine).  

_Example:_

    from condor import CondorPool

    def f(x):
        return x**2

    if __name__ == '__main__':
        p = CondorPool()
        results = p.map(f, range(5))


2. tmpfiles.py
--------------

Management of temporary files: inputs to a processing (TmpInput), outputs of a processing (TmpOutput), and pure temporary files (Tmp).
Includes several useful features, such as freedisk space verification, 

_Example:_

        f = TmpInput('/path/to/source.data')
        # the source file is copied to a unique temporary directory
        # and f contains the file name of the temporary file
        # <use f as an input to a processor>
        f.clean() # or Tmp.cleanAll()

        f = TmpOutput('/path/to/target.data')
        # at this point, f is the temporary file (non-existing yet)
        # and f.target() is the target file
        #
        # <create file f>
        #
        if <f created successfully>:
            f.move() # move f to target
        else:
            f.clean()

3. archive.py
-------------

Archive data to external hard drives.
