Todo list
*********

* check dispel4py as workflow engine
  http://www2.epcc.ed.ac.uk/~amrey/VERCE/Dispel4Py/index.html
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







