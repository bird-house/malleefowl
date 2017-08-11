.. _malleefowl:

Malleefowl WPS Processes
========================

Malleefowl is the server part of Birdhouse for WPS web processing
processes. Birdhouse uses PyWPS as the default web processing server. The
``Wizard`` of Phoenix builds a workflow script with a ``source`` process
(to retrieve input files for `worker` processes) and a ``worker`` process
(processing one or more input files).

Web Processing Services
-----------------------

I don't give an introdution to Web Processing Services here. *Please*
read the documentation of `PyWPS Tutorial`_, `OWSLib WPS`_ and `OGC WPS`_.

One remark. The WPS standard defines literal data types like int,
float, string, boolean, ... and a complex data type which we use for
file input and output. Read the docs for details :)

Creating a WPS Process
======================

You can derive your WPS process directly from the PyWPS class
pywps.Process.WPSProcess.

To see an example of how to implement a WPS process check the already
available processes in the folder::

        $ cd malleefowl/processes

Creating a Source WPS Process
-----------------------------

The Wizard component of Phoenix (Birdhouse web-interface) generates a
workflow script with an Source process and an Worker process. The
Source process provides data files (for example netcdf files from
ESGF). The Worker process works on these data files (one or more) and
comes up with one or more result.

See the esgf source process as an example in `processes/esgf.py`.

Creating a Worker WPS Process
-----------------------------

These processes do the hard work on provided input data files. They
are used in the Wizard component of Phoenix for a chained workflow
process with a source and a worker process.

Worker processes are derived from the Python class malleefowl.WorkerProcess.

See the cdo process as an example of a worker process in `processes/cdo.py`.

.. _`OGC WPS`: http://www.opengeospatial.org/standards/wps/
.. _`PyWPS`: http://pywps.org/
.. _`PyWPS Tutorial`: http://pywps.org/docs/
.. _`OWSLib WPS`: http://geopython.github.io/OWSLib/#wps
