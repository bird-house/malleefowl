.. _installation:

Installation
************

Check out code from the malleefowl github repo and start the installation:

.. code-block:: sh

   $ git clone https://github.com/bird-house/malleefowl.git
   $ cd malleefowl
   $ make clean install

For other install options run ``make help`` and read the documention of
the `Makefile <https://github.com/bird-house/birdhousebuilder.bootstrap/blob/master/README.rst>`_.
All installation files are going by default into the folder ``~/birdhouse``.

After successful installation you need to start the
services:

.. code-block:: sh

   $ make start   # starts supervisor services
   $ make status  # show supervisor status

The depolyed WPS service is available at:

http://localhost:8091/wps?service=WPS&version=1.0.0&request=GetCapabilities.

Check the log files for errors:

.. code-block:: sh

   $ tail -f  ~/birdhouse/var/log/pywps/malleefowl.log
   $ tail -f  ~/birdhouse/var/log/supervisor/malleefowl.log
