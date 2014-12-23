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




from __future__ import print_function
from os.path import exists, basename, join, dirname
from os import system, rmdir, statvfs, walk
import tempfile
import warnings
from shutil import rmtree
import fnmatch




# module-wide parameters
class Cfg:
    ''' store module-wise parameters
    to be used as: from tmpfiles import cfg
                   cfg.tmpdir = <some temporary directory>
                   cfg.verbose = True
                   etc.
    '''
    def __init__(self):
        # default values
        self.tmpdir = '/tmp/'  # location of the temporary directory
        self.verbose = False   # verbosity
        self.freespace = 1000  # free disk space required, in MB

        # global parameter
        self.TMPLIST = []    # a list of all 'dirty' tmpfiles

    def check_free_space(self):

        assert exists(self.tmpdir)

        if (self.freespace > 0) and (df(self.tmpdir) < self.freespace):
            raise IOError('Not enough free space in {} ({} MB remaining, {} MB required)'.format(
                self.tmpdir, df(self.tmpdir), self.freespace))
cfg = Cfg()

def df(path):
    '''
    get free disk space in MB
    '''
    res = statvfs(path)
    available = int(res.f_frsize*res.f_bavail/(1024.**2))
    return available

def findfiles(path, pat='*', split=False):
    '''
    recursively finds files starting from path and using a pattern
    if split, returns (root, filename), otherwise the full path
    '''
    if isinstance(path, list):
        paths = path
    else:
        paths = [path]
    for path in paths:
        for root, dirnames, filenames in walk(path):
            dirnames.sort()
            filenames.sort()
            for filename in fnmatch.filter(filenames, pat):
                if split:
                    yield (root, filename)
                else:
                    fname = join(root, filename)
                    yield fname


def remove(filename):
    ''' remove a file '''
    if cfg.verbose:
        system('rm -fv "{}"'.format(filename))
    else:
        system('rm -f "{}"'.format(filename))


class Tmp(str):
    '''
    A simple temporary file created by the user, removed afterwards

    Parameters:
        * tmpfile: The temporary file name (default: 'tmpfile')
          Should not contain a directory, the directory is provided module-wise
    '''
    def __new__(cls, tmpfile='tmpfile'):

        assert dirname(tmpfile) == ''

        # check free disk space
        cfg.check_free_space()

        tmpd = tempfile.mkdtemp(dir=cfg.tmpdir, prefix='tmpfiles_')
        tmpfile = join(tmpd, tmpfile)

        self = str.__new__(cls, tmpfile)
        self.__clean = False
        self.__tmpdir = tmpd
        self.__verbose = cfg.verbose
        cfg.TMPLIST.append(self)
        return self

    def clean(self):

        if self.__clean:
            warnings.warn('Warning, {} has already been cleaned'.format(self))
            return

        if exists(self):
            # remove temporary file
            remove(self)

        if self.__tmpdir != None:
            rmtree(self.__tmpdir)
        self.__clean = True
        cfg.TMPLIST.remove(self)


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
        while len(cfg.TMPLIST) != 0:
            cfg.TMPLIST[0].clean()


class TmpInput(str):
    '''
    A class for simplifying the usage of temporary input files.

    Parameters
       * filename: the original file which will be copied locally and
         uncompressed if necessary
       * pattern: if the TmpInput contains multiple files, this pattern
         determines which file to use as the reference file name. If there are
         multiple files, take the first one in alphabetical order.
         default value: '*'
       * tmpdir: the base temporary directory
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
            pattern = '*'):

        if cfg.verbose:
            v = 'v'
        else:
            v = ''

        # check free disk space
        cfg.check_free_space()

        #
        # determine how to deal with input file
        #
        if (filename.endswith('.tgz')
                or filename.endswith('.tar.gz')):
            copy = 'tar xz{v}f "{input_file}" -C "{output_dir}"'
            base = None

        elif filename.endswith('.gz'):
            copy = 'gunzip -{v}c "{input_file}" > "{output_file}"'
            base = basename(filename)[:-3]

        elif filename.endswith('.Z'):
            copy = 'gunzip -{v}c "{input_file}" > "{output_file}"'
            base = basename(filename)[:-2]

        elif filename.endswith('.tar.bz2') or filename.endswith('.tbz2'):
            copy = 'tar xj{v}f "{input_file}" -C "{output_dir}"'
            base = None

        elif filename.endswith('.tar'):
            copy = 'tar x{v}f "{input_file}" -C "{output_dir}"'
            base = None

        elif filename.endswith('.bz2'):
            copy = 'bunzip2 -{v}c "{input_file}" > "{output_file}"'
            base = basename(filename)[:-3]

        elif cfg.verbose:
            copy = 'cp -v "{input_file}" "{output_file}"'
            base = basename(filename)
        else:
            copy = 'cp "{input_file}" "{output_file}"'
            base = basename(filename)

        # check that input file exists
        if not exists(filename):
            raise IOError('File "{}" does not exist'.format(filename))

        # determine temporary file name
        tmpd = tempfile.mkdtemp(dir=cfg.tmpdir, prefix='tmpfiles_')

        # format the copy command
        if base is None:
            tmpfile = None
            cmd = copy.format(input_file=filename, output_dir=tmpd, v=v)
        else:
            tmpfile = join(tmpd, base)
            cmd = copy.format(input_file=filename, output_file=tmpfile, v=v)

        # does the copy
        if system(cmd):
            remove(tmpfile)
            raise IOError('Error executing "{}"'.format(cmd))

        # determine the reference file name if not done already
        if tmpfile is None:
            tmpfile = list(findfiles(tmpd, pattern))[0]

        # create the object and sets its attributes
        self = str.__new__(cls, tmpfile)
        self.__tmpfile = tmpfile
        self.__tmpdir = tmpd
        self.__filename = filename
        self.__clean = False

        cfg.TMPLIST.append(self)

        return self

    def clean(self):

        if self.__clean:
            warnings.warn('Warning, {} has already been cleaned'.format(self))
            return

        # remove temporary directory
        rmtree(self.__tmpdir)
        self.__clean = True
        cfg.TMPLIST.remove(self)

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
        while len(cfg.TMPLIST) != 0:
            cfg.TMPLIST[0].clean()

class TmpOutput(str):
    '''
    A class intended to simplify the usage of temporary output files

    Parameters:
       * filename: the target file which will be first written locally, then
         copied to the destination
       * overwrite: whether the target should be overwritten

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
            overwrite=False):

        # check free disk space
        cfg.check_free_space()

        if cfg.verbose:
            copy = 'cp -v "{}" "{}"'
        else:
            copy = 'cp "{}" "{}"'

        # check that output file does not exist
        if not overwrite and exists(filename):
            raise IOError('File "{}" exists'.format(filename))

        # create output directory if necessary
        if (not exists(dirname(filename))) and (dirname(filename) != ''):
            system('mkdir -p {}'.format(dirname(filename)))

        # determine temporary file name
        base = basename(filename)
        tmpd = tempfile.mkdtemp(dir=cfg.tmpdir, prefix='tmpfiles_')
        tmpfile = join(tmpd, base)

        assert not exists(tmpfile)

        # create the object and sets its attributes
        self = str.__new__(cls, tmpfile)
        self.__tmpfile = tmpfile
        self.__tmpdir = tmpd
        self.__filename = filename
        self.__cp = copy
        self.__clean = False

        cfg.TMPLIST.append(self)

        return self

    def clean(self):

        if self.__clean:
            warnings.warn('Warning, {} has already been cleaned'.format(self))
            return

        # remove whole temporary directory
        rmtree(self.__tmpdir)
        self.__clean = True
        cfg.TMPLIST.remove(self)

    def target(self):
        return self.__filename

    def move(self):
        print('move', self, 'to', self.target())

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
        while len(cfg.TMPLIST) != 0:
            cfg.TMPLIST[0].clean()

class TmpDir(str):
    '''
    Create a temporary directory

    Example:
        d = TmpDir()   # create a temporary directory such as /tmp/tmpfiles_9aamr0/
        # <use this directory as string d>
        d.clean() # remove the temporary directory
    '''

    def __new__(cls):

        # check free disk space
        cfg.check_free_space()

        # create the temporary directory
        tmpd = tempfile.mkdtemp(dir=cfg.tmpdir, prefix='tmpdir_')
        ret = system('mkdir -p {}'.format(tmpd))
        if ret:
            raise 'Error creating directory {}'.format(tmpd)

        # create the object and sets its attributes
        self = str.__new__(cls, tmpd)
        self.__clean = False

        cfg.TMPLIST.append(self)

        if cfg.verbose:
            print('Creating temporary directory "{}"'.format(self))

        return self

    def clean(self):

        if cfg.verbose:
            print('Clean temporary directory "{}"'.format(self))
        rmtree(self)
        self.__clean = True
        cfg.TMPLIST.remove(self)

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
        while len(cfg.TMPLIST) != 0:
            cfg.TMPLIST[0].clean()


#
# tests
#
def test_tmp():
    cfg.verbose=True
    cfg.freespace = 1
    f = Tmp('myfile.tmp')
    open(f, 'w').write('test')
    f.clean()

def test_tmp2():
    cfg.verbose=True
    cfg.freespace = 1
    with Tmp('myfile.tmp') as f:
        open(f, 'w').write('test')

def test_input():
    cfg.verbose=True
    cfg.freespace = 1

    # create temporary file
    tmp = Tmp('myfile.tmp')
    open(tmp, 'w').write('test')
    #... and use it as a temporary input
    TmpInput(tmp)
    # clean all
    Tmp.cleanAll()

def test_output():
    cfg.verbose=True
    cfg.freespace = 10

    f = Tmp() # the target is also a temporary file
    tmp = TmpOutput(f)

    open(tmp, 'w').write('test')
    tmp.move()
    Tmp.cleanAll()

def test_dir():
    cfg.verbose=True
    cfg.freespace = 10

    d = TmpDir()
    filename = join(d, 'test')
    open(filename, 'w').write('test')
    d.clean()

if __name__ == '__main__':
    test_tmp()
    test_tmp2()
    test_input()
    test_output()
    test_dir()
