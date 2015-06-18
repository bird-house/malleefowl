Todo list
*********

* setup esgf-archive for tests (test_dispel_dummy etc won't work otherwise) 
* fix python search path: buildout malleefowl should overwrite anaconda malleefowl
* add opendap access
* check opendap urls
* esgserach: filter files by start/end also when temporal search is not active
* esgsearch: fix case when esgsearch has no resuls
* fix esgsearch process datetime parameters
* handle multiple wget downloads (mabe same file):
  maybe wget does this already or use python lockfile
  https://pypi.python.org/pypi/lockfile
* add literal input with value range[(0,100)] ... see pywps doc
* token service ... sets simple username/password for wps https access
* check WebSockets and wps
https://pypi.python.org/pypi/gevent-websocket
* test suite with nose


Bugs
====

* search with replica=True returns nothing:
constraints: [('project', 'CMIP5'), ('model', 'MPI-ESM-LR'), ('variable', 'ta'), ('cmor_table', 'Amon'), ('experiment', 'historical'), ('ensemble', 'r1i1p1')]
* dispel workflow: exception after esgsearch with many result files:
Failed to execute WPS process [dispel]: HTTP Error 413: Request Entity Too Large
* owslib can not handle some exception reports: for example:
<Exception exceptionCode="NoApplicableCode">
                <ExceptionText>'Failed to execute WPS process [visualisation]: (returncode:1) cdo showdate: Open failed on &gt;./pywpsInput5A4aF4&lt;\nNo such file or directory\n'</ExceptionText>
        </Exception>

* install of project emu etc will fail if malleefowl conda dependencies are not already installed. Check projects depending on malleefowl.
* wget on debian does not work for esgf downloads
https://bugs.launchpad.net/linuxmint/+bug/1335174
* pywps key-value request does not work if data-input has an @ sign
* set egg_cache:
python-eggs is writable by group/others ... (set PYTHON_EGG_CACHE environment variable)


Research
========

* check couchdb, sqark, ElasticSearch
  http://couchdb.apache.org/
  pypi/buildout_couchdb
  http://www.elasticsearch.org/
  pypi/django-simple-elasticsearch
  http://spark.apache.org/
* check cache for downloads
conda caching
buildout caching
https://code.google.com/p/python-cache/


OWSLib
======

* add support for bounding box
* patch: encode complexinput parameters which are inline of the wps request
* patch: sync wps request

