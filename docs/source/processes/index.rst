.. _processes:
WPS Processes
*************

We describe here the WPS processes available in Malleefowl. These are supporting processes to provide access to climate data (currently from ESGF data nodes).

Logon with ESGF OpenID
======================

The `ESGF <https://github.com/ESGF>`_ logon process returns a X509 proxy certificate for your ESGF OpenID account. This certificate is used to download files from ESGF data nodes.


ESGF Logon: WPS process description
-----------------------------------

Using the default Malleefowl installation the ``DescribeProcess`` request is as follows:

http://localhost:8091/wps?service=WPS&version=1.0.0&request=DescribeProcess&identifier=esgf_logon

The XML response of the WPS service is the following document:

.. literalinclude:: wps_esgf_logon.xml
    :language: xml
    :emphasize-lines: 10,19,30
    :linenos:

The WPS Parameters are:

*openid*
    Is an input parameter for your OpenID (https://esgf-data-node/esgf-idp/openid/myopenid). It is a WPS `LiteralData <http://pywps.wald.intevation.org/documentation/course/ogc-wps/index.html#literaldata>`_ string type.

*password*
     Is an input parameter to provide the password for the openid. It is a WPS LiteralData string type.

*output*
     Is the output parameter to provide the X509 proxy certificate. 
     It is a WPS `ComplexData <http://pywps.wald.intevation.org/documentation/course/ogc-wps/index.html#complexdata>`_ type with MIME-type ``application/x-pkcs7-mime``.

ESGF Logon: WPS process execution
---------------------------------

An example execution of the ESGF logon process:

http://localhost:8091/wps?service=WPS&version=1.0.0&request=Execute&identifier=esgf_logon&DataInputs=openid=https://esgf-data-node/esgf-idp/openid/myopenid;password=wonttellyou&RawDataOutput=output

The process is called with key/value parameters, synchronously and with direct output (``RawDataOutput``).

The resulting document is a X509 certificate.


ESGF Search
===========

The ESGF search process runs a ESGF search request with constraints (project, experiment, ...) to get a list of matching files on ESGF data nodes. It is using `esgf-pyclient <https://github.com/stephenpascoe/esgf-pyclient>`_ Python client for the ESGF search API. 

In addition to the esgf-pyclient the process checks if local replicas are available and would return the replica files instead of the original one.

The result is a JSON document with a list of ``http://`` URLs to files on ESGF data nodes.
 

ESGF Search: WPS process description
------------------------------------

Using the default Malleefowl installation the ``DescribeProcess`` request is as follows:

http://localhost:8091/wps?service=WPS&version=1.0.0&request=DescribeProcess&identifier=esgsearch

The XML response of the WPS service is the following document:

.. literalinclude:: wps_esgsearch.xml
    :language: xml
    :emphasize-lines: 9,74,160
..    :lines: 1-17,58-109,159-175
    :linenos:

The main WPS Parameters are (see the WPS description for a complete list):

*url*
    Is an input parameter for the URL of the ESGF index node which is used for search queries. 
    Is is a LiteralData string type. 

*constraints*
    Is an input parameter with a list of comma seperated key/value pairs. 
    Example: ``project:CORDEX, time_frequency:mon, variable:tas``. 
    It is a LiteralData string type.

*output*
    Is an output parameter to provide a JSON document with a list of ``http://`` URLs to files on ESGF nodes. 
    Is is a ComplexData type with MIME-type ``text/json``.

A possible search result JSON document could look like this::

    [
    "http://carbon.dkrz.de/thredds/fileServer/cordex/output/AFR-44/CLMcom/ECMWF-ERAINT/evaluation/r1i1p1/CLMcom-CCLM4-8-17/v1/mon/tas/v20140401/tas_AFR-44_ECMWF-ERAINT_evaluation_r1i1p1_CLMcom-CCLM4-8-17_v1_mon_200101-200812.nc", 
    "http://carbon.dkrz.de/thredds/fileServer/cordex/output/AFR-44/CLMcom/ECMWF-ERAINT/evaluation/r1i1p1/CLMcom-CCLM4-8-17/v1/mon/tas/v20140401/tas_AFR-44_ECMWF-ERAINT_evaluation_r1i1p1_CLMcom-CCLM4-8-17_v1_mon_199101-200012.nc"
    ]


Download files
==============

The download process gets one or more URLs (``http://``, ``file://``) of NetCDF files (could also be other formats). The downloader checks if the file is available in the local ESGF archive. If not then the file will be downloaded and stored in a local cache. As a result it provides a list of local ``file://`` paths to the requested files. 

The downloader does not download files if they are already in the ESGF archive or in the local cache. 

An X509 proxy certificate is needed to access ESGF files from external ESGF data nodes.

Download: WPS process description
---------------------------------

Using the default Malleefowl installation the ``DescribeProcess`` request is as follows:

http://localhost:8091/wps?service=WPS&version=1.0.0&request=DescribeProcess&identifier=wget

The XML response of the WPS service is the following document:

.. literalinclude:: wps_wget.xml
    :language: xml
    :emphasize-lines: 9,18,37
    :linenos:

The WPS Parameters are:

*resource*
     Is an input parameter to provide one or more URLs for resources to download (usually NetCDF files from ESGF data nodes).
     It is a WPS LiteralData string type.

*credentials*
     Is an optional input parameter to provide an X509 proxy certificate to access files on ESGF data nodes. Is is a WPS ComplexData type with MIME-type ``application/x-pkcs7-mime``.

*output*
     Is an output parameter to provide a json document with a list of local ``file://`` URLs to the downloaded files. It is a WPS ComplexData type with MIME-type ``test/json``.

An example result JSON document could look like this::

    [
    "file:///gpfs_750/projects/CMIP5/data/cmip5/output1/MPI-M/MPI-ESM-LR/historical/mon/atmos/Amon/r1i1p1/v20120315/tas/tas_Amon_MPI-ESM-LR_historical_r1i1p1_185001-200512.nc", 
    "file:///gpfs_750/projects/CMIP5/data/cmip5/output1/MPI-M/MPI-ESM-LR/historical/mon/atmos/Amon/r1i1p1/v20111119/tas/tas_Amon_MPI-ESM-LR_historical_r1i1p1_185001-200512.nc", 
    "file:///gpfs_750/projects/CMIP5/data/cmip5/output1/MPI-M/MPI-ESM-LR/historical/mon/atmos/Amon/r1i1p1/v20111006/tas/tas_Amon_MPI-ESM-LR_historical_r1i1p1_185001-200512.nc"
    ]

Run Dispel Workflow
===================

The workflow process is usually called by the `Phoenix <http://pyramid-phoenix.readthedocs.org>`_ WPS web client to run WPS process for climate data (like cfchecker, climate indices with ocgis, ...) with a given selection of input data (currently NetCDF files from ESGF data nodes). Currently the `Dispel4Py <http://www2.epcc.ed.ac.uk/~amrey/VERCE/Dispel4Py/index.html>`_ workflow engine is used.

The Workflow for ESGF input data is as follows::

Search ESGF files -> Download ESGF files -> Run choosen process on local (downloaded) ESGF files.

to be continued ...

Publish
=======

to be continued ...













