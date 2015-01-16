Todo list
*********

* setup esgf-archive for tests (test_dispel_dummy etc won't work otherwise) 
* add sphinx documenation
* fix pyesgf search to use q=*:* and facets for file-search
* handle wget download failures in dispel workflow
* fix python search path: buildout malleefowl should overwrite anaconda malleefowl
* add opendap access
* check opendap urls
* esgserach: filter files by start/end also when temporal search is not active
* esgsearch: fix case when esgsearch has no resuls
* fix temporal contrain in esgf pyclient
* fix esgsearch process datetime parameters
* handle multiple wget downloads (mabe same file) ...
* add literal input with value range[(0,100)] ... see pywps doc
* change tomcat config
* check dispel4py as workflow engine
  http://www2.epcc.ed.ac.uk/~amrey/VERCE/Dispel4Py/index.html
  https://github.com/akrause2014/dispel4py
* token service ... sets simple username/password for wps https access
* user stats/jobs/... proxy before wps services  
* check WebSockets and wps
https://pypi.python.org/pypi/gevent-websocket
* build docker images
* use salt to deploy docker images
* use common birdhouse bootstrap for starting buildout installer
* maybe use metadata wps attribute to provide esgf filter etc ... could be local/pseudo links with json response.
* concurrent logging or logging per process ...
* test suite with nose
* cleanup data on server: wpsoutput, ...
* configure datasets scan for thredds
http://fe4.sic.rm.cnr.it:8080/thredds/docs/datasetScan/index.html
* configure ncwms for more climate variables


Bugs
====

* dispel workflow: exception after esgsearch with many result files:
Failed to execute WPS process [dispel]: HTTP Error 413: Request Entity Too Large
* init supervisor log dir failed:
Starting supervisor service ...
/home/pingu/anaconda/etc/init.d/supervisord start
Starting supervisord:
Error: The directory named as part of the path /home/pingu/anaconda/var/log/supervisor/supervisord.log does not exist.
For help, use /home/pingu/anaconda/bin/supervisord -h
make: *** [start] Error 2

* fix logging:
Error in atexit._run_exitfuncs:
Traceback (most recent call last):
  File "/home/pingu/anaconda/lib/python2.7/atexit.py", line 24, in _run_exitfuncs
    func(*targs, **kargs)
  File "/home/pingu/anaconda/lib/python2.7/logging/__init__.py", line 1662, in shutdown
    h.close()
  File "build/bdist.linux-x86_64/egg/cloghandler.py", line 230, in close
    if not self.stream_lock.closed:
AttributeError: 'NoneType' object has no attribute 'closed'
Error in sys.exitfunc:
Traceback (most recent call last):
  File "/home/pingu/anaconda/lib/python2.7/atexit.py", line 24, in _run_exitfuncs
    func(*targs, **kargs)
  File "/home/pingu/anaconda/lib/python2.7/logging/__init__.py", line 1662, in shutdown
    h.close()
  File "build/bdist.linux-x86_64/egg/cloghandler.py", line 230, in close
AttributeError: 'NoneType' object has no attribute 'closed'


* owslib can not handle some exception reports: for example:
<Exception exceptionCode="NoApplicableCode">
                <ExceptionText>'Failed to execute WPS process [visualisation]: (returncode:1) cdo showdate: Open failed on &gt;./pywpsInput5A4aF4&lt;\nNo such file or directory\n'</ExceptionText>
        </Exception>

* install of project emu etc will fail if malleefowl conda dependencies are not already installed. Check projects depending on malleefowl.
* wget on debian does not work for esgf downloads
https://bugs.launchpad.net/linuxmint/+bug/1335174
* fix capabilities of isometa process
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
https://code.google.com/p/python-cache/
memcache:
https://cloud.google.com/appengine/docs/python/memcache/clientclass?csw=1
https://pypi.python.org/pypi/datacache/0.4.2


wget
====

* should accept file:// urls

OWSLib
======

* add support for bounding box
* patch: encode complexinput parameters which are inline of the wps request
* patch: sync wps request

NcWMS
=====

* patch: supporting more calendars

Restflow
========

* restflow does not work with anaconda python







