runlib
======

A simple python module for parallel/cluster code execution.

How it works: you write a single script using runlib. The first time it is run,
a server is created, containing the jobs list. When you run it again (possibly
serveral times, and on different computers connected in the same local
network), clients are connected to the server and process the jobs as required.

Example
-------

    from runlib.runlib import Processor

    p = Processor(server=True)                # (a)
    for i in range(10):
        p.submit('process %s' % (files[i]))   # (b)
    p.wait()                                  # (c)

### Server mode

When running this script for the first time:

(a) The server is created. A file is written in current directory, and contains
its ip address and port. By default, this file is "server.status".

(b) The server's jobs queue is populated.

(c) The server starts waiting for connections.
    At this point, you will run this same script on the same computer, or on
another computer in the local network. See section 1.2.
    You will be able to monitor the remaining jobs by pressing enter, or stop
    the server by pressing 'q'.


### Client mode

When running the same script as a server already exists, the following happens:

(a) The file "server.status" is detected. The ip/port of the server is read
from this file. A connection is then made with the server. The client receives
the jobs from the server and executed them until the jobs queue is empty. When
done, the script exits (so steps (b) and (c) are not done).

NOTE: You can run this script in client mode many times


### Local mode

If you do not want to use server mode, use `Processor(server=False)`, or simply
`Processor()`.


### Processing python functions

To process a python function, pass the argument `function=f` to `Processor()`

    from runlib.runlib import Processor

    def f(x):
        return x**2

    p = Processor(server=True; function=f)    # (a)
    for i in range(10):
        p.submit(i)                           # (b)
    p.wait()                                  # (c)
    print p.results    # contains the function's results

The results of the function execution are stored in `p.results`.
    

How to use this module
----------------------

The python module [Pyro4] (http://packages.python.org/Pyro4/) is required.

However, if this module is not found, you will be guided to install it locally.


