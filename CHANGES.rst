changes ....
------------

* test account for esgf data access can be configured in test section in custom.cfg

Tasks:

* DONE: remove pycsw.cfg from supervisor (not used)
* DONE: malleefowl: configure default timeout of restflow process
* DONE: malleefowl: need better logging
* DONE: malleefowl: unit tests for workflow tests
* DONE: pywps: configure path to restflow
* DONE: buildout: use prefix for install
* DONE: use unpriviledged user for services: pywps, pycsw, phoenix
  https://github.com/collective/collective.recipe.grp

Bugs:

* DONE: supervisor does not kill pywps processes
* DONE: at least on ubuntu 12.04 python-netcdf4 ubuntu package does not work
  using anaconda for netcdf deps


release 22.12.2013
------------------

* using anaconda for several dependencies (cdo, netcdf4, ...)
* refactored buildout and prepared config for frontend and backend
* using ubuntu supervisor 
* installs now into system (/var/lib/pywps, /var/log/pywps.log, ...)

Tasks:

* DONE: buildout: using buildout for upgrades
* SKIP: configure nginx for frontend, backend in seperate configs
  http://wiki.nginx.org/Configuration
  http://wiki.nginx.org/HttpUwsgiModuleMultipleDynamicApplications
  https://www.packtpub.com/article/nginx-web-services-configuration-implementation
* DONE: supervisor: configure monitor
  http://supervisord.org/configuration.html?highlight=web
* DONE: move anaconda to /opt/
* DONE: configure PATH for pywps processes
* DONE: maybe use anaconda for install of scientific packages
* DONE: clean up buildouts
* DONE: set path to cdo (compiled with buildout)
* DONE: rewrite start scripts for gunicorn


release 31.12.2013
------------------

Tasks:

* DONE: set default java7 with alternatives
* DONE: maybe use boostrap formview:
  * https://github.com/nandoflorestan/deform_bootstrap_extra/blob/master/deform_bootstrap_extra/pyramid/views.py
* DONE: add notes and tags to jobs ... like delicious
  * http://stackoverflow.com/questions/3731756/how-to-design-the-schema-for-something-like-stackoverflow-questions-tags
* DONE: rework buildout (nginx+gunicorn, system packages)
* DONE: need handling of user rights (whitelist, ...)
* DONE: integration of openid (maybe configurable auth)
* DONE: rotate log files with logrotate (at least for pywps)
* DONE: move logs to var
* SKIPPED: maybe hide thredds for normal users?
* DONE: dont show passwords in logs
* DONE: compile cdo with netcdf4 support
* DONE: publish process visible for normal user
* DONE: thredds with user dir
* DONE: set wps service title
* DONE: check admin users
* DONE: use local users (admin, demo1, demo2)
* DONE: configure catalog service
* DONE: fix wps switching
* DONE: dont display internal processes for normal users
* DONE: show 2d map
* SKIPPED: show 3d map
* DONE: google earth
* DONE: notes with simple input
* DONE: geotiff/gdal
* DONE: move files and www to var
* DONE: move downloads to top level
* DONE: process with land sea mask fails
* DONE: csc process: select 9 datasets, just 5 are shown
* DONE: ReferenceError: tinyMCE is not defined tinyMCE.init({
  occurs when loading process gui
* DONE: fix hostname of thredds and monitor
* DONE: keep order of wps input/ouput fields
* DONE: pywps returns before file copy has finished (esgf.wget)
* DONE: fix dirty hack of wps input late binding
* DONE: rendering of search view fails after submit
* SKIP: buildout: taverna_security part fails sometimes
* DONE: ncks fails on opendap access at least on ubuntu 12.04
* DONE: false statusLocation url in db when using several wps services
* DONE: handle error when statusLocation/wps is not accessible
* DONE: fix hardcoded path in Mallowfee init.py
* DONE: clear user jobs (all or selected)
* DONE: add notes and tags to submitted jobs (like delicious, ...)
* DONE: intergrate threads with wcs, wms, opendap, ...
* DONE: integrate local pywps
* DONE: use supervisor for process control
* DONE: use mongodb
* DONE: integrate esgf staging and seach
* DONE: need to access opendap/thredds
* DONE: integrate metadata tool
* DONE: enable forms/wizzard for cascaded wps calls
* DONE: integrate catalog service
* DONE: integrate workflow enging (restflow)
* DONE: create base classes for wps processes
* DONE: check alchemy colander: http://colanderalchemy.readthedocs.org/en/latest/
* DONE: see schemaker.py in bootstrap_extra as example for wps schema
* DONE: show error message form wps
* DONE: use ctrl-key to select multiple values (variables) in esg-search
* DONE: wsgi script for gunicorn
* DONE: filter opendap urls with variables etc ...
* DONE: filter wget files with time str (variable etc ...)
* DONE: choose better name for climdaps_wps
* DONE: handle multiple outputs
* DONE: collect values of output data
* DONE: ComplexData Input inline or as url
* DONE: check InputFormChoice/OutputFormChoice  (additional infos ...)
  * input can be reference to external resource (e.a. wps ...)
* DONE: handle and display exceptions
* DONE: display all processing states
* DONE: show percentage of progress
* SKIP: maybe use pydap instead of tds?
* DONE: install tomcat with thredds
* DONE: use more external python packages, numpy, lxml, ...
* DONE: use download script with caching
* DONE: use cmmi recipe
* DONE: set hostname
* DONE: maybe use local buildout cfg? using tracy.cfg
* DONE: restart nginx after update
* DONE: start documentation with sphinx
* DONE: use system nginx
* REJECTED: write climdaps start script for all processes
* DONE: finish input param form
