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

to be continued ...

Run Dispel Workflow
===================

to be continued ...

Publish
=======

to be continued ...













