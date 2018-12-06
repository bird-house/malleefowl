.. _configuration:

Configuration
=============

.. warning::

  Please read the PyWPS documentation_ to find details about possible configuration options.

Command-line options
--------------------

You can overwrite the default `PyWPS`_ configuration by using command-line options.
See the Malleefowl help which options are available::

    $ malleefowl start --help
    --hostname HOSTNAME        hostname in PyWPS configuration.
    --port PORT                port in PyWPS configuration.

Start service with different hostname and port::

    $ malleefowl start --hostname localhost --port 5001

Use a custom configuration file
-------------------------------

You can overwrite the default `PyWPS`_ configuration by providing your own
PyWPS configuration file (just modifiy the options you want to change).
Use one of the existing ``sample-*.cfg`` files as example and copy them to ``etc/custom.cfg``.

For example change the hostname (*demo.org*) and logging level:

.. code-block:: sh

   $ cd malleefowl
   $ vim etc/custom.cfg
   $ cat etc/custom.cfg
   [server]
   url = http://demo.org:5000/wps
   outputurl = http://demo.org:5000/outputs

   [logging]
   level = DEBUG

Start the service with your custom configuration:

.. code-block:: sh

   # start the service with this configuration
   $ malleefowl start -c etc/custom.cfg

Read the PyWPS documentation_ for futher options and details.

Configure path to data archive
------------------------------

Malleefowl extends the configuration of PyWPS with a *data* section.

[data]
~~~~~~

:archive_root:
    path to a *read-only* ESGF data archive which is used by the download process to make use of a local ESGF archive.
    You can configure several archives paths by using a colon ``:`` as seperator. Default: `/tmp/archive`.

:cache_path:
    path to a *writeable* cache folder which is used by the download process to store files.
    Default: `PYWPS_OUTPUTPATH/cache`.

:archive_node:
    an option to specify an ESGF data provider for site specfic settings.
    Possible values: `default`, `dkrz`, `ipsl`.
    Default: `default`.

Example
~~~~~~~

.. code-block:: ini

  [server]
  url = http://demo.org:5000/wps
  outputurl = http://demo.org:5000/outputs
  outputpath = /data/pywps/outputs

  [data]
  archive_root = /data/archive/cmip5:/data/archive/cordex
  cache_path = /data/cache
  archive_node = default

.. _PyWPS: http://pywps.org/
.. _documentation: https://pywps.readthedocs.io/en/master/configuration.html
