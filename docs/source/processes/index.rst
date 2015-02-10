.. _processes:
WPS Processes
*************

We describe here the WPS processes available in Malleefowl.

Logon with ESGF OpenID
======================

The `ESGF <https://github.com/ESGF>`_ logon process returns a X509 proxy certificate for your ESGF OpenID account. This certificate is used to download files from ESGF data nodes.


WPS process description
-----------------------

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

WPS process execution
---------------------

An example execution of the ESGF logon process:

http://localhost:8091/wps?service=WPS&version=1.0.0&request=Execute&identifier=esgf_logon&DataInputs=openid=https://esgf-data-node/esgf-idp/openid/myopenid;password=wonttellyou&RawDataOutput=output

The process is called with key/value parameters, synchronously and with direct output (``RawDataOutput``).

The resulting document is a X509 certificate.


ESGF Search
===========

to be continued ...

Download files
==============

The download process gets one or more URLs (``http://``, ``file://``) of NetCDF files (could also be other formats). The downloader checks if the file is available in the local ESGF archive. If not then the file will be downloaded and stored in a local cache. As a result it provides a list of local ``file://`` paths to the requested files. 

The downloader does not download files if they are already in the ESGF archive or in the local cache. 

An X509 proxy certificate is needed to access ESGF files from external ESGF data nodes.

WPS process description
-----------------------

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

to be continued ...

Publish
=======

to be continued ...













