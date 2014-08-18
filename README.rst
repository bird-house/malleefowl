**********
Malleefowl
**********

.. contents::

Introduction
************

Malleefowl (the bird)
  *Malleefowl are shy, wary, solitary birds that usually fly only to escape danger or reach a tree to roost in. Although very active, they are seldom seen [..].* (`Wikipedia https://en.wikipedia.org/wiki/Malleefowl`_).


Malleefowl is a Python package to simplify the usage of Web Processing Services (WPS). Currently it is using the `PyWPS https://github.com/geopython/PyWPS`_ server. It comes with some WPS processes which are used by the ``Phoenix`` WPS web-application. These processes are used to support climate data processing with WPS.

Installation
************

Check out code from the malleefowl git repo (will be available on github). Then do the following::

   $ cd malleefowl
   $ ./requirements.sh
   $ ./install.sh


After successful installation you need to start the services. Malleefowl is using `Anaconda http://www.continuum.io/`_ Python distribution system. All installed files (config etc ...) are below the Anaconda root folder which is by default in your home directory ``~/anaconda``. Now, start the services::

   $ cd ~/anaconda
   $ etc/init.d/supervisor start
   $ etc/init.d/nginx start

The depolyed WPS service is available on http://localhost:8091/wps?service=WPS&version=1.0.0&request=GetCapabilities.

Check the log files for errors::

   $ tail -f  ~/anaconda/var/log/pywps/malleefowl.log
   $ tail -f  ~/anaconda/var/log/pywps/malleefowl_trace.log

Configuration
*************

If you want to run on a different hostname or port then change the default values in ``custom.cfg``::

   $ cd malleefowl
   $ vim custom.cfg
   $ cat custom.cfg
   [settings]
   hostname = localhost
   http-port = 8091

After any change to your ``custom.cfg`` you **need** to run ``install.sh`` again and restart the ``supervisor`` service::

  $ ./install.sh
  $  ~/anaconda/etc/init.d/supervisor restart

Update
******

When updating your installation you may run ``clean.sh`` to remove outdated Python dependencies::

   $ cd malleefowl
   $ git pull
   $ ./clean.sh
   $ ./requirement.sh
   $ ./install.sh

And then restart the ``supervisor`` and ``nginx`` service.
