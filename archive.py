#!/usr/bin/env python
# encoding: utf-8


'''
a simple script to archive data to multiple hard drives

usage:
    archive.py archive <path>
    archive.py unarchive <path>
    archive.py trash

archive configuration is stored in 'archive.cfg'.
contains the list of remotes, such as:
ARCHIVE1 /run/media/user/3a817f8a-9a77-45af-966d/archive/
ARCHIVE2 /run/media/user/9a77-45af-966d/archive/

after a file has been archived, it is replaced by a (broken) symlink of the form:
    file -> REMOTE/SHASUM

the original file is moved to a folder 'trash'.
the content of this folder can be safely removed by using the command
"archive.py trash", which also checks that the content of each file can be read
from the remote.
'''


config = 'archive.cfg'
trash_dir = 'trash'

margin = 10 # do not fill drive to more than this value (in Mb)

from os import statvfs
from sys import argv
from os.path import isdir
import os
import shutil
from os.path import exists, join, dirname
import hashlib
from string import split
from datetime import datetime

def clean_dir(directory):
    '''
    remove the directory if empty
    and its parents if they are also empty
    '''

    assert isdir(directory)
    d = directory
    while isdir(d) and len(os.listdir(d)) == 0:
        os.rmdir(d)
        d = dirname(d)



def get_shasum(filename):
    return hashlib.sha256(open(filename, 'rb').read()).hexdigest()

def get_free_space_mb(path):
    '''
    Return folder/drive free space (in Mb)
    return None if path does not exist
    '''

    if not exists(path): return None

    st = statvfs(path)
    return st.f_bavail * st.f_frsize/(1024*1024)

def file_size(filename):
    '''
    return file size in Mb
    '''
    statinfo = os.stat(filename)

    return statinfo.st_size/(1024*1024)


def loop(paths):
    for path in paths:
        for (dirpath, dirnames, filenames) in os.walk(path):
            dirnames.sort()
            for filename in filenames:
                yield join(dirpath, filename)


def archive(remotes, paths):

    print 'archive', paths
    if paths == []:
        print 'please give a path to archive'

    count = 0
    for filename in loop(paths):

        if filename == config: continue

        # check that filename is not a symlink
        if os.path.islink(filename): continue

        # check that we are not trying to archive the trash
        if filename.startswith(trash_dir): continue

        print filename

        # check file attributes:
        # need to have write authorization before archiving
        if not (os.access(filename, os.W_OK) and os.access(filename, os.R_OK)):
            print '   Does not have rw attributes, skipping...'
            continue

        # check that filename does not contain a space
        if ' ' in filename:
            print '   Filename shall not contain a whitespace, skipping...'
            continue


        #
        # select the first remote with enough free space
        #
        allfull = False
        for (rem, rempath) in remotes:

            free = get_free_space_mb(rempath)
            if free == None: continue

            fs = file_size(filename)
            if fs + margin > free:
                continue
            else:
                break

            allfull = True

        if allfull:
            print '   All remotes are full or offline'
            continue


        #
        # archive to selected remote
        #
        print '   Copy to {}... (at {})'.format(rem, datetime.now())

        target = join(rempath, filename)
        targettmp = target + '.tmp'

        # create remote directory if necessary
        if not exists(dirname(target)):
            os.makedirs(dirname(target))

        shutil.copyfile(filename, targettmp)
        shutil.move(targettmp, target)

        # get sha256sum
        print '   get shasum ...'
        shasum = get_shasum(filename)

        # move the original file to the trash_dir
        if not exists(dirname(join(trash_dir, filename))):
            os.makedirs(dirname(join(trash_dir, filename)))
        shutil.move(filename, join(trash_dir, filename))

        # create a broken symlink in place of the original file
        # file -> REMOTE/SHASUM
        os.symlink('{}/{}'.format(rem, shasum), filename)


        count += 1

        print '   OK'

    print paths, 'have been archived ({} files)'.format(count)
    print 'You may want to call the trash command now to remove the trash safely'
    print 'Or just remove the trash folder if you are confident ;)'



def trash(remotes):
    '''
    for each file in the trash: verify that it can be read from the remote
    and remove it definitely if so
    '''

    print 'trash'

    for trashfile in loop([trash_dir]):

        original = trashfile[len(trash_dir):]
        if original.startswith('/'):
            original = original[1:]

        (rem, shasum) = split(os.readlink(original), '/')

        rempath = dict(remotes)[rem]

        archived = join(rempath, original)

        print '{} (in {})'.format(original, rem)

        if not exists(rempath):
            print 'Please make {} available'.format(rem)
            continue

        if not exists(archived):
            print 'Error, {} is not available in {}'.format(archived, rem)
            continue

        # checking shasum on remote
        shasum1 = get_shasum(archived)

        if shasum == shasum1:
            os.remove(trashfile)
            clean_dir(dirname(trashfile))
            print '   removed from trash'
        else:
            print '   error, this file failed verification'



def unarchive(remotes, paths):

    print 'unarchive', paths

    for filename in loop(paths):

        if not os.path.islink(filename): continue

        (rem, shasum) = split(os.readlink(filename), '/')
        print filename

        rempath = dict(remotes)[rem]
        target = join(rempath, filename)

        if not exists(rempath):
            print 'Please make {} available'.format(rem)
            continue

        if not exists(target):
            print 'Error, {} is not available in {}'.format(target, rem)
            continue

        # unarchive
        print '   unarchiving...'
        shutil.copy(target, filename+'.tmp')

        # verify checksum
        print '   checksum...'
        if get_shasum(filename+'.tmp') != shasum:
            print 'Error in checksum'
            os.remove(filename+'.tmp')
            continue

        # remove from remote
        os.remove(target)

        clean_dir(dirname(target))

        # replace symlink by the file
        os.remove(filename)
        shutil.move(filename+'.tmp', filename)

        print '   done.'



def usage():
    print 'usage:'
    print '    archive.py archive <path>'
    print '          archive <path> to the remotes, move the original file to trash'
    print '    archive.py trash'
    print '          verify that each file in the trash can be read from remote: if so, remove it'
    print '    archive.py unarchive <path>'
    print '          unarchive <path>'




def main():

    remotes = []

    # read configuration in 'archive.cfg'
    try:
        lines = open(config).readlines()
    except:
        raise Exception('Error, could not open {}'.format(config))

    print 'List of remotes:'
    for line in lines:
        if line.startswith('#'): pass
        name, path = line.split()
        remotes.append((name, path))

        # create path if it does not exist
        offline = False
        if not isdir(path):
            try:
                os.makedirs(path)
            except OSError:
                offline = True
        if offline:
            print name, 'OFFLINE', path
        else:
            print name, get_free_space_mb(path), 'Mb', path

        # create 'archive.cfg' on remote
        # just to write the remote name
        desc = join(path, config)
        if not exists(desc):
            with open(desc, 'w') as fp:
                fp.write(name+'\n')




    if len(argv) == 1:
        usage()
    elif argv[1] == 'archive':
        archive(remotes, argv[2:])
    elif argv[1] == 'trash':
        trash(remotes)
    elif argv[1] == 'unarchive':
        unarchive(remotes, argv[2:])
    else:
        usage()




if __name__ == '__main__':
    main()
