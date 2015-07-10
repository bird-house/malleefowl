.. _installation:

Installation
************
Check out code from the malleefowl github repo and start the installation::
 
   $ git clone https://github.com/bird-house/malleefowl.git
   $ cd malleefowl
   $ make

For other install options run ``make help`` and read the documention for the `Makefile <https://github.com/bird-house/birdhousebuilder.bootstrap/blob/master/README.rst>`_.

After successful installation you need to start the
services. Malleefowl is using `Anaconda <http://www.continuum.io/>`_
Python distribution system. All installed files (config etc ...) are
below the Anaconda root folder which is by default in your home
directory ``~/anaconda``. Now, start the services::

   $ make start   # starts supervisor services
   $ make status  # show supervisor status

The depolyed WPS service is available on: 

http://localhost:8091/wps?service=WPS&version=1.0.0&request=GetCapabilities.

Check the log files for errors::

   $ tail -f  ~/anaconda/var/log/pywps/malleefowl.log
   $ tail -f  ~/anaconda/var/log/pywps/malleefowl_trace.log

Anaconda package
================

Malleefowl is also available as Anaconda package if you want to use it as a library::

  $ conda install -c birdhouse malleefowl

