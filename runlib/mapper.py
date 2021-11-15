from pathlib import Path
import typing
from tempfile import TemporaryDirectory
from functools import wraps
import shutil

def check_f(func):
    '''
    A wrapper to check that func can be properly mapped

    (in particular, that it returns the generated file)
    '''
    @wraps(func)
    def wrapped(f_in, f_out, *args):
        ret = func(f_in, f_out, *args)
        assert ret == f_out
        assert ret.exists()
        return ret

    # because __globals__['__file__'] is used by condor.py to determine the function path
    wrapped.__globals__['__file__'] = func.__globals__['__file__']
    return wrapped


def mapper(
        f: callable,
        list_input: typing.Iterable,
        list_output: typing.Union[typing.Iterable, callable],
        dir_out: Path,
        f_args: typing.Optional[list]=None,
        create_out_dir=False,
        if_exists='skip',
        map_function=map,
    ):
    '''
    Wraps a function of type `file_in` -> `file_out` over multiple files

    Adds the following features:
        - returns the list of output files
        - skips existing files, does nothing if all files exist
        - runs `f` in either local or over Condor
        - make sure `dir_out` exists
        - `f` returns temporary files, moved by the mapper
          (to allow clean stopping the main script)

    Arguments:
        * `f`: callable of type f(file_in, file_out, *args) -> file_out
        * `list_input`: list of input files to be processed (list of str or Path)
           Note: not necessarily files, can be simple strings
        * `list_output`:
            - list of output files (without directory)
            - or, a function applied to input, to generate output files.
                Example: lambda x: x.name+'.nc'
        * `dir_out`: output directory
        * `f_args`: list of further positional arguments to be passed to `f`
        * `create_out_dir`: whether to create output directory if it does not exist yet
        * `map_function`: the mapping function. Can be:
            - `map` for local mapping
            - `CondorPool().imap_unordered` for Condor mapping
    '''
    if create_out_dir:
        dir_out.mkdir(exist_ok=True, parents=True)
    else:
        assert dir_out.exists()

    with TemporaryDirectory(dir=dir_out, prefix='tmp_mapper_') as tmpdir:

        # Generate list of arguments
        list_out = []
        list_args = []
        n_skipped = 0
        last_skipped = None
        for i in range(len(list_input)):
            f_in = list_input[i]
            if callable(list_output):
                f_out = dir_out/list_output(f_in)
            else:
                f_out = dir_out/list_output[i]
            f_out_tmp = Path(tmpdir)/f_out.name
            list_out.append(f_out)

            if f_out.exists():
                if if_exists == 'skip':
                    n_skipped += 1
                    last_skipped = f_out
                elif if_exists == 'error':
                    raise IOError(f'Error, output file {f_out} exists.')
                else:
                    raise Exception(f'Invalid argument if_exists={if_exists}')
            else:
                list_args.append((f_in, f_out_tmp, *(f_args if f_args else [])))

        # Apply `f` over all args and move output upon success
        print(f'Mapping {f.__name__} over {len(list_out)} items...')
        if list_args:
            for res in map_function(check_f(f), *zip(*list_args)):
                f_out = dir_out/res.name
                shutil.move(res, f_out)

    print(f'Processed {len(list_out) - n_skipped} files with {f.__name__}')

    if n_skipped:
        print(f'Skipped {n_skipped} existing files such as {last_skipped}')

    if Path(tmpdir).exists():
        print(f'Warning, {tmpdir} has not been removed.')

    return list_out
