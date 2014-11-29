Todo list
*********

* token service ... sets simple username/password for wps https access
* user stats/jobs/... proxy before wps services  
* add wps commandline tool similar to miklip:
https://code.zmaw.de/projects/miklip-d-integration/wiki

progress_bar, more work on input, output, get wps list form csw,
refactor wps processes (e.a. wps), unit test
choose bird (wps) by setting env variable (set as default)
chaining example
add wps caps caching
store output refs in variable?

* esgf search on console (files, opendap, local files)
* csw search on console
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

* install of project emu etc will fail if malleefowl conda dependencies are not already installed. Check projects depending on malleefowl.
* wget on debian does not work for esgf downloads
https://bugs.launchpad.net/linuxmint/+bug/1335174
* fix capabilities of isometa process
* pywps key-value request does not work if data-input has an @ sign
* set egg_cache:
python-eggs is writable by group/others ... (set PYTHON_EGG_CACHE environment variable)


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







