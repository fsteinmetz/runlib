#!/usr/bin/env python
# vim:fileencoding=utf-8



from os.path import exists, basename, join, dirname
from os import system
import tempfile


def remove(filename, verbose=False):
    ''' remove a file '''
    if verbose:
        system('rm -fv {}'.format(filename))
    else:
        system('rm -f {}'.format(filename))

class TmpInput(str):
    '''
    A class for simplifying the usage of temporary input files.
       * filename: the original file which will be copied locally and
         uncompressed if necessary
       * copy:
           - None
             => automatic mode, detect gz or bz2, otherwise just cp
           - a string, like "cp {} {}"
             => will be formatted with .format(source, target)
       * uniq: whether a unique temporary filename should be used
       * rename: a function applied to the basename
         (example: lambda x: x[:-3] to remove extension)
       * tmpdir: the temporary firectory
       * overwrite_tmp: whether existing temporary files should be overwritten
    '''

    # NOTE: subclassing an immutable object requires to use the __new__ method

    def __new__(cls,
            filename,
            copy = None,
            uniq = False,
            rename = lambda x:x,
            tmpdir='/tmp/',
            overwrite_tmp=True,
            verbose=False):

        assert exists(tmpdir)

        if copy == None:

            if filename.endswith('.gz'):
                if verbose:
                    copy = 'gunzip -vc {} > {}'
                else:
                    copy = 'gunzip -c {} > {}'
                rename = lambda x: x[:-3]

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
            raise Exception('File "{}" does not exist'.format(filename))

        # determine temporary file name
        base = rename(basename(filename))
        if uniq:
            tmpfile = tempfile.mktemp(suffix='__'+base, dir=tmpdir)
        else:
            tmpfile = join(tmpdir, base)

        if exists(tmpfile):
            if overwrite_tmp:
                remove(tmpfile, verbose=verbose)
            else:
                raise Exception('Temporary file "{}" exists.'.format(tmpfile))

        # does the copy
        cmd = copy.format(filename, tmpfile)
        system(cmd)

        # create the object and sets its attributes
        self = str.__new__(cls, tmpfile)
        self.__tmpfile = tmpfile
        self.__filename = filename
        self.__verbose = verbose
        self.__finished = False
        return self

    def finish(self):
        print 'finish', self

        if not exists(self.__tmpfile):
            raise Exception('file {} does not exist'.format(self.__tmpfile))

        # remove temporary file
        remove(self.__tmpfile, verbose=self.__verbose)
        self.__finished = True

    def file(self):
        return self.__filename

    def __del__(self):
        # raise an exception if the object is deledted before finish is called
        if not self.__finished:
            print('\nERROR: finish has not been called for file "{}"\ntemporary file "{}" may remain.\n'.format(self.__filename, self))


class TmpOutput(str):
    '''
    A class for simplifying the usage of temporary output files.
       * filename: the target file which will be first written locally, then
         copied to the destination
       * copy: a custom command for copying the files
       * uniq: whether a unique temporary filename should be used
       * tmpdir: the temporary firectory
       * overwrite_tmp: whether existing temporary files should be overwritten
    '''

    # NOTE: subclassing an immutable object requires to use the __new__ method

    def __new__(cls,
            filename,
            copy = None,
            uniq = False,
            tmpdir='/tmp/',
            overwrite_tmp=True,
            verbose=False):

        assert exists(tmpdir)

        if copy == None:
            if verbose:
                copy = 'cp -v {} {}'
            else:
                copy = 'cp {} {}'

        # check that output file does not exist
        if exists(filename):
            raise Exception('File "{}" exists'.format(filename))

        # create output directory if necessary
        if not exists(dirname(filename)):
            raise Exception('Output directory "{}" does not exist'.format(dirname(filename)))


        # determine temporary file name
        base = basename(filename)
        if uniq:
            tmpfile = tempfile.mktemp(suffix='__'+base, dir=tmpdir)
        else:
            tmpfile = join(tmpdir, base)

        if exists(tmpfile):
            if overwrite_tmp:
                remove(tmpfile, verbose=verbose)
            else:
                raise Exception('Temporary file "{}" exists.'.format(tmpfile))


        # create the object and sets its attributes
        self = str.__new__(cls, tmpfile)
        self.__tmpfile = tmpfile
        self.__filename = filename
        self.__cp = copy
        self.__verbose = verbose
        self.__finished = False
        return self

    def finish(self):
        print 'finish', self

        if not exists(self.__tmpfile):
            raise Exception('file {} does not exist'.format(self.__tmpfile))

        # output file: copy to destination
        cmd = self.__cp.format(self.__tmpfile, self.__filename)
        system(cmd)

        # remove temporary file
        remove(self.__tmpfile, verbose=self.__verbose)
        self.__finished = True

    def file(self):
        return self.__filename

    def __del__(self):
        # raise an exception if the object of selected before finish is called
        if not self.__finished:
            print('\nERROR: finish has not been called for file "{}"\ntemporary file "{}" may remain.\n'.format(self.__filename, self))

