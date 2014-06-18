#!/usr/bin/env python
# vim:fileencoding=utf-8



'''
A module to easily manage temporary files

    1) Temporary input files (TmpInput)
       These files are copied locally, used as an input for the processing,
       then removed

    2) Temporary output files (TmpOutput)
       These temporary files are created, and copied upon success to their
       final destination

    3) Pure temporary files (Tmp)
       Pure temporary files are created from scratch, and removed after use

    These classes contain the following features:
        - Ensure temporary files unicity by using unique temporary directories
        - Check free disk space on start
        - Automatic cleanup of all temporary files via static method
          Tmp.cleanAll() (call this at the end of your script)

'''




from os.path import exists, basename, join, dirname
from os import system, rmdir, statvfs
import tempfile
import warnings
from shutil import rmtree


TMPLIST = [] # a list of all 'dirty' tmpfiles



def df(path):
    '''
    get free disk space in MB
    '''
    res = statvfs(path)
    available = int(res.f_frsize*res.f_bavail/(1024.**2))
    return available


def remove(filename, verbose=False):
    ''' remove a file '''
    if verbose:
        system('rm -fv {}'.format(filename))
    else:
        system('rm -f {}'.format(filename))


class Tmp(str):
    '''
    A simple temporary file created by the user, removed afterwards

    Parameters:
        * tmpfile
          The temporary file name
          - If it is a single file name (without directory), it will be
            initialized in the temporary directory
          - If it is a full file name, it will remain as-is
        * tmpdir
          In previous 1st case, this determines the temporary directory to use
        * verbose
        * freespace: minimum free space

    '''
    def __new__(cls,
            tmpfile,
            tmpdir='/tmp/',
            verbose=False,
            freespace=1000):

        if dirname(tmpfile) == '':
            # check free disk space
            if (freespace > 0) and (df(tmpdir) < freespace):
                raise IOError('Not enough free space in {} ({} MB remaining, {} MB required)'.format(
                    tmpdir, df(tmpdir), freespace))

            tmpd = tempfile.mkdtemp(dir=tmpdir, prefix='tmpfiles_')
            tmpfile = join(tmpd, tmpfile)
        else:
            tmpd = None


        self = str.__new__(cls, tmpfile)
        self.__clean = False
        self.__tmpdir = tmpd
        self.__verbose = verbose
        TMPLIST.append(self)
        return self

    def clean(self):

        if self.__clean:
            warnings.warn('Warning, {} has already been cleaned'.format(self))
            return

        if exists(self):
            # remove temporary file
            remove(self, verbose=self.__verbose)

        if self.__tmpdir != None:
            rmdir(self.__tmpdir)
        self.__clean = True
        TMPLIST.remove(self)


    def __del__(self):
        # raise an exception if the object is deleted before clean is called
        if not self.__clean:
            print('Warning: clean has not been called for file "{}"'.format(self))

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if not self.__clean:
            self.clean()

    @staticmethod
    def cleanAll():
        while len(TMPLIST) != 0:
            TMPLIST[0].clean()


class TmpInput(str):
    '''
    A class for simplifying the usage of temporary input files.

    Parameters
       * filename: the original file which will be copied locally and
         uncompressed if necessary
       * copy:
           - None
             => automatic mode, detect gz or bz2, otherwise just cp
           - a string, like "cp {} {}"
             => will be formatted with .format(source, target)
       * rename: a function applied to the basename
         (example: lambda x: x[:-3] to remove extension)
       * tmpdir: the temporary directory
       * verbose

    Example:
        f = TmpInput('/path/to/source.data')
        # the source file is copied to a unique temporary directory
        # and f contains the file name of the temporary file
        # <use f as an input to a processor>
        f.clean() # or Tmp.cleanAll()

    '''

    # NOTE: subclassing an immutable object requires to use the __new__ method

    def __new__(cls,
            filename,
            copy = None,
            rename = lambda x:x,
            tmpdir='/tmp/',
            verbose=False,
            freespace=1000):

        assert exists(tmpdir)

        # check free disk space
        if (freespace > 0) and (df(tmpdir) < freespace):
            raise IOError('Not enough free space in {} ({} MB remaining, {} MB required)'.format(
                tmpdir, df(tmpdir), freespace))

        if copy == None:

            if filename.endswith('.gz'):
                if verbose:
                    copy = 'gunzip -vc {} > {}'
                else:
                    copy = 'gunzip -c {} > {}'
                rename = lambda x: x[:-3]

            if filename.endswith('.Z'):
                if verbose:
                    copy = 'gunzip -vc {} > {}'
                else:
                    copy = 'gunzip -c {} > {}'
                rename = lambda x: x[:-2]

            elif filename.endswith('.bz2'):
                if verbose:
                    copy = 'bunzip2 -vc {} > {}'
                else:
                    copy = 'bunzip2 -c {} > {}'
                rename = lambda x: x[:-4]

            elif verbose:
                copy = 'cp -v {} {}'
            else:
                copy = 'cp {} {}'


        # check that input file exists
        if not exists(filename):
            raise IOError('File "{}" does not exist'.format(filename))

        # determine temporary file name
        base = rename(basename(filename))
        tmpd = tempfile.mkdtemp(dir=tmpdir, prefix='tmpfiles_')
        tmpfile = join(tmpd, base)

        assert not exists(tmpfile)

        # does the copy
        cmd = copy.format(filename, tmpfile)
        if system(cmd):
            remove(tmpfile, verbose=verbose)
            raise IOError('Error executing "{}"'.format(cmd))

        # create the object and sets its attributes
        self = str.__new__(cls, tmpfile)
        self.__tmpfile = tmpfile
        self.__tmpdir = tmpd
        self.__filename = filename
        self.__verbose = verbose
        self.__clean = False

        TMPLIST.append(self)

        return self

    def clean(self):

        if self.__clean:
            warnings.warn('Warning, {} has already been cleaned'.format(self))
            return

        if not exists(self.__tmpfile):
            raise IOError('file {} does not exist'.format(self.__tmpfile))

        # remove temporary file
        remove(self.__tmpfile, verbose=self.__verbose)
        rmdir(self.__tmpdir)
        self.__clean = True
        TMPLIST.remove(self)

    def source(self):
        return self.__filename

    def __del__(self):
        # raise an exception if the object is deleted before clean is called
        if not self.__clean:
            print('Warning: clean has not been called for file "{}" - temporary file "{}" may remain.'.format(self.__filename, self))

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if not self.__clean:
            self.clean()

    @staticmethod
    def cleanAll():
        while len(TMPLIST) != 0:
            TMPLIST[0].clean()

class TmpOutput(str):
    '''
    A class intended to simplify the usage of temporary output files

    Parameters:
       * filename: the target file which will be first written locally, then
         copied to the destination
       * copy: a custom command for copying the files
       * uniq: whether a unique temporary filename should be used
       * tmpdir: the temporary firectory
       * overwrite: whether the target should be overwritten
       * verbose

    Example:
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
    '''

    # NOTE: subclassing an immutable object requires to use the __new__ method

    def __new__(cls,
            filename,
            copy = None,
            tmpdir='/tmp/',
            overwrite=False,
            verbose=False,
            freespace=1000):

        assert exists(tmpdir)

        # check free disk space
        if (freespace > 0) and (df(tmpdir) < freespace):
            raise IOError('Not enough free space in {} ({} MB remaining, {} MB required)'.format(
                tmpdir, df(tmpdir), freespace))


        if copy == None:
            if verbose:
                copy = 'cp -v {} {}'
            else:
                copy = 'cp {} {}'

        # check that output file does not exist
        if not overwrite and exists(filename):
            raise IOError('File "{}" exists'.format(filename))

        # create output directory if necessary
        if (not exists(dirname(filename))) and (dirname(filename) != ''):
            system('mkdir -p {}'.format(dirname(filename)))

        # determine temporary file name
        base = basename(filename)
        tmpd = tempfile.mkdtemp(dir=tmpdir, prefix='tmpfiles_')
        tmpfile = join(tmpd, base)

        assert not exists(tmpfile)

        # create the object and sets its attributes
        self = str.__new__(cls, tmpfile)
        self.__tmpfile = tmpfile
        self.__tmpdir = tmpd
        self.__filename = filename
        self.__cp = copy
        self.__verbose = verbose
        self.__clean = False

        TMPLIST.append(self)

        return self

    def clean(self):

        if self.__clean:
            warnings.warn('Warning, {} has already been cleaned'.format(self))
            return

        # remove temporary file (may not exist)
        if exists(self.__tmpfile):
            remove(self.__tmpfile, verbose=self.__verbose)

        rmdir(self.__tmpdir)
        self.__clean = True
        TMPLIST.remove(self)

    def target(self):
        return self.__filename

    def move(self):
        print 'move', self, 'to', self.target()

        if not exists(self.__tmpfile):
            raise IOError('file {} does not exist'.format(self.__tmpfile))

        # output file: copy to destination
        cmd = self.__cp.format(self.__tmpfile, self.__filename)
        if system(cmd):
            raise IOError('Error executing "{}"'.format(cmd))

        self.clean()

    def __del__(self):
        # raise an exception if the object is deleted before clean is called
        if not self.__clean:
            print('Warning: clean has not been called for file "{}" - temporary file "{}" may remain.'.format(self.__filename, self))

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if not self.__clean:
            self.clean()

    @staticmethod
    def cleanAll():
        while len(TMPLIST) != 0:
            TMPLIST[0].clean()

class TmpDir(str):
    '''
    Create a temporary directory

    Example:
        d = TmpDir()   # create a temporary directory such as /tmp/tmpfiles_9aamr0/
        # <use this directory as string d>
        d.clean() # remove the temporary directory
    '''

    def __new__(cls,
            tmpdir='/tmp/',
            verbose=False,
            freespace=1000):

        assert exists(tmpdir)

        # check free disk space
        if (freespace > 0) and (df(tmpdir) < freespace):
            raise IOError('Not enough free space in {} ({} MB remaining, {} MB required)'.format(
                tmpdir, df(tmpdir), freespace))

        # create the temporary directory
        tmpd = tempfile.mkdtemp(dir=tmpdir, prefix='tmpdir_')
        ret = system('mkdir -p {}'.format(tmpd))
        if ret:
            raise 'Error creating directory {}'.format(tmpd)

        # create the object and sets its attributes
        self = str.__new__(cls, tmpd)
        self.__verbose = verbose
        self.__clean = False

        TMPLIST.append(self)

        if self.__verbose:
            print 'Creating temporary directory "{}"'.format(self)

        return self
    
    def clean(self):

        if self.__verbose:
            print 'Clean temporary directory "{}"'.format(self)
        rmtree(self)
        self.__clean = True
        TMPLIST.remove(self)

    def __del__(self):
        # raise an exception if the object is deleted before clean is called
        if not self.__clean:
            print('Warning: clean has not been called for file "{}"'.format(self))

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if not self.__clean:
            self.clean()

    @staticmethod
    def cleanAll():
        while len(TMPLIST) != 0:
            TMPLIST[0].clean()


#
# tests
#
def test_tmp():
    f = Tmp('myfile.tmp', verbose=True, freespace=1)
    open(f, 'w').write('test')
    f.clean()

def test_input():
    # create temporary file
    tmp = Tmp('myfile.tmp', verbose=True)
    open(tmp, 'w').write('test')

    #... and use it as a temporary input
    TmpInput(tmp, verbose=True)

    # clean all
    Tmp.cleanAll()

def test_output():

    f = Tmp('myfile.tmp') # the target is also a temporary file
    tmp = TmpOutput(f, verbose=True)

    open(tmp, 'w').write('test')

    tmp.move()
    Tmp.cleanAll()

def test_dir():
    
    d = TmpDir(verbose=True)
    filename = join(d, 'test')
    open(filename, 'w').write('test')
    d.clean()


if __name__ == '__main__':
    test_tmp()
    test_input()
    test_output()
    test_dir()
