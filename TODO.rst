TODO list  ....
===============


Common
------

* refactor tokenmgr access methods
* download multiple files (wget, opendap, ...), using python threads ...
* need a tough little helpful bird module:
  http://toughlittlebirds.com/support-research/
* need common buildout modules for nginx, supervisor, conda, ...
  http://www.buildout.org/en/latest/docs/recipe.html
  http://www.buildout.org/en/latest/docs/recipelist.html
  https://github.com/hexagonit/hexagonit.recipe.download
  https://en.wikipedia.org/wiki/Hummingbird
* malleefowl: fix publish process (token handling)
* malleefowl: use esgf logon process to get cert and cookie
* configure service etc for unit tests ...
* data: include example with opendap
  http://earthpy.org/smos_sea_ice_thickness.html
* need common python modules used by client and server
* malleefowl: guess source and worker processes
* malleefowl: process search local files: common storage + user storage
* integrate sos service
* malleefowl: maybe separate unit und functional tests
* buildout: integrate nosetests in install:
  https://nose.readthedocs.org/en/latest/setuptools_integration.html
* pywps: get file name/url of complex input (input.inputs['value'])
* malleefowl: rename file_identifier to source
* try kepler
* pywps, phoenix: upload function for files
* buildout: run processes with unpriviledged user
* buildout: configure esgf index node
* malleefowl: use dkrz cloud as publishing site
* pywps: maybe link input files from destination to temp folder instead of copying
* module: install openclimategis
  https://earthsystemcog.org/projects/openclimategis/
* anaconda: integrate cdo stuff to main conda recipes
* buildout: configure download folder, can also be a shared one
* module: install icclim
  http://icclim.readthedocs.org/en/latest/index.html
  https://github.com/tatarinova/icclim
* gunicorn: use debian gunicorn installation
  http://docs.gunicorn.org/en/latest/install.html
* anaconda: set env only for system user (might brake other things like ubuntuone)
* buildout: write recipe for debian, macosx, anaconda install ...
* supervisor: maybe user nginx supervisor module
  http://labs.frickle.com/nginx_ngx_supervisord/
* restflow: maybe use geotools wps module in restflow
* nginx: using nginx with https
  https://www.mare-system.de/guide-to-nginx-ssl-spdy-hsts/
* update docs, todos, ...
* supervisor: install superlance plugins
* supervisor: show logs in monitor
* update to buildout 2.x
* use buildout config for bootstrap
* DONE: write process to generate workflow script for restflow with template
* add https://earthsystemcog.org/projects/openclimategis/
* DONE: write restflow wps-actor
* need testing environment
* configure ncwms for more climate variables
* add publisher to workflow (optional)
* using download cache: maybe ehcache?
  * http://beaker.readthedocs.org/en/latest/
  * https://bitbucket.org/zzzeek/dogpile.cache/
* p2p network for climdaps instances
* cache WPS requests (getcapabilities, describeprocess)
* maybe integrate also geoserver
* malleefowl: provide paster template/scaffold for new processes
* use cdo python
* use python mimetypes: http://docs.python.org/2/library/mimetypes.html
* setup jenkins test suite
* use debug mode in buildout (enable debugtoolbar) 


qc workflow
-----------

* wps pid generate process with caching (mongodb)
* irods: simple copy of kit folder to dkrz
* swift: copy swift folder to lokal folder (pywps)
* DONE: wps: token generator process for user access, folder names, ...
* separate gui from services
* install climdaps on centos/carbon

helsinki tutorial
-----------------

* find some tutorials for pywps, geoserver, zoo, ...
* prepare tutorials with ipython notebook
* maybe prepare vm for users to create own processes

OWSLib
------

* add support for bounding box
* handle binary complex input data

Coding ...
----------

* need common module header
* documentation style

Low Priority
------------

* maybe coords transformation wps ...
* make process configurable with yaml


Nice to have ...
----------------

* integrated shell (python or javascript, see mongodb shell)
* integrate ipython notebook (shell)
* integrate cera staging and iso meta search




