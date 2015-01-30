**********
Malleefowl
**********

.. contents::

Introduction
************

Malleefowl (the bird)
   *Malleefowl are shy, wary, solitary birds that usually fly only to escape danger or reach a tree to roost in. Although very active, they are seldom seen [..]* (`Wikipedia <https://en.wikipedia.org/wiki/Malleefowl>`_).


Malleefowl is a Python package to simplify the usage of Web Processing Services (WPS). Currently it is using the `PyWPS <https://github.com/geopython/PyWPS>`_ server. It comes with some WPS processes which are used by the ``Phoenix`` WPS web-application. These processes are used to support climate data processing with WPS.

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

The depolyed WPS service is available on http://localhost:8091/wps?service=WPS&version=1.0.0&request=GetCapabilities.

Check the log files for errors::

   $ tail -f  ~/anaconda/var/log/pywps/malleefowl.log
   $ tail -f  ~/anaconda/var/log/pywps/malleefowl_trace.log

Configuration
=============

If you want to run on a different hostname or port then change the default values in ``custom.cfg``::

   $ cd malleefowl
   $ vim custom.cfg
   $ cat custom.cfg
   [settings]
   hostname = localhost
   http-port = 8091

After any change to your ``custom.cfg`` you **need** to run ``make install`` again and restart the ``supervisor`` service::

   $ make install
   $ make restart
   $ make status

Running unit tests with testdata
================================

To run all unit tests one needs to fetch tests with an esgf openid and start the malleefowl service::

    $ bin/wpsfetch -u username
    $ make start
    $ make test

Testdata is collected in ``testdata.json``::

    $ vim testdata.json

Anaconda package
================

Malleefowl is also available as Anaconda package if you want to use it as a library::

  $ conda install -c https://conda.binstar.org/birdhouse malleefowl


Using Docker image
==================

To run Malleefowl Web Processing Service you can also use the `Docker <https://registry.hub.docker.com/u/birdhouse/malleefowl/>`_ image::

  $ docker run -i -d -p 9001:9001 -p 8090:8090 -p 8091:8091 --name=malleefowl birdhouse/malleefowl

Check the docker logs::

  $ docker logs malleefowl

Show running docker containers::

  $ docker ps

Open your browser and enter the url of the supervisor service:

  http://localhost:9001

Run a GetCapabilites WPS request:

  http://localhost:8091/wps?service=WPS&version=1.0.0&request=getcapabilities
