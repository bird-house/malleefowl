Notes
======


wpsclient call from groovy
--------------------------

* http://bip.weizmann.ac.il/course/python/PyMOTW/PyMOTW/docs/optparse/index.html#module-optparse
* http://pymotw.com/2/json/
* http://groovy.codehaus.org/Executing+External+Processes+From+Groovy


python web frameworks
---------------------

* http://bottlepy.org/docs/dev/index.html


buildout
--------

* check recipes (supervisor, download, cmmi):
http://www.buildout.org/en/latest/docs/recipelist.html
* buildout with user interaction?
  * use paster template to generate buildout configs
    http://www.sixfeetup.com/technologies/plone-content-management/quick-reference-cards/refcard.pdf
* check buildout example with netcdf, numpy:
https://svn.apache.org/repos/asf/incubator/climate/trunk/easy-rcmet/buildout.cfg
http://svn.simo-project.org/tools/trunk/spatialdb/buildout.cfg


map integration
---------------

* integrate map
  * http://mapquery.org/
  * http://workshops.boundlessgeo.com/openlayers-intro/integration/jqui-dialog.html

esg search widget
-----------------

* esg search widget
  * http://evolvingweb.github.io/ajax-solr/examples/reuters/index.html
  * https://github.com/evolvingweb/ajax-solr/wiki
  * http://docs.pylonsproject.org/projects/deform/en/latest/widget.html#writing-your-own-widget
* temporal and spatial search:
http://esg-datanode.jpl.nasa.gov/esg-search/search?start=2013-06-01T00:00:00Z&end=2013-07-01T00:00:00Z&bbox=[-10,-10,+10,+10]
* use tag cloud:
  * http://welldonethings.com/tags/manager
  * http://stackoverflow.com/questions/15171314/how-to-use-tags-manager
  * http://jsfiddle.net/vt2z4/
  * http://xoxco.com/projects/code/tagsinput/
  * http://stackapps.com/questions/2062/stack-overflow-tag-manager
  * http://ioncache.github.io/Tag-Handler/
  * https://github.com/insin/greasemonkey
* using esg search with javascript ajax
  * http://developer.yahoo.com/javascript/howto-proxy.html
  * http://evolvingweb.github.io/ajax-solr/examples/reuters/index.html
  * https://github.com/evolvingweb/ajax-solr/wiki
  * http://stackoverflow.com/questions/7957998/nginx-caching-ajax
* rest api:
  * www.esgf.org/wiki/ESGF_Search_REST_API

PyWPS
-----

* complete handling of data types (e.a date, dateTime, time, ...). See list at:
http://www.w3.org/TR/xmlschema-2/#time
* fix input dataType (Literal, complex, boundingbox)

climate process
---------------

* malleefowl (austalian hard working bird)
* http://theonethingps274.wordpress.com/2008/03/27/what-a-hard-working-bird/

WPS Protocol ...
----------------

* use ows:metadata for input/output values
* fix boundingbox on wps client
* evaluate mime-type, formats, ...
* use units (uom)
* check ValuesReferences for literal input data
* tutorial:
http://wiki.ieee-earth.org/Documents/GEOSS_Tutorials/GEOSS_Provider_Tutorials/Web_Processing_Service_Tutorial_for_GEOSS_Providers/Section_2%3A_Introduction_to_WPS
* wps chaining:
http://docs.geoserver.org/stable/en/user/extensions/wps/processes.html

Thredds
-------


* docs: 
* http://blog.marinexplore.com/pydap-open-source-python-opendap-library-turns-ten/
http://www.unidata.ucar.edu/projects/THREDDS/tech/tds4.2/tutorial/GettingStarted.html
http://www.unidata.ucar.edu/projects/THREDDS/tech/tds4.3/tutorial/GettingStarted.html

* deploy test data
* try opendap, wcs, wms, http, nciso
http://www.unidata.ucar.edu/projects/THREDDS/tech/tds4.2/tutorial/BasicConfig.html
http://www.unidata.ucar.edu/projects/THREDDS/tech/reference/WCS.html

WMS
---

* http://www.resc.rdg.ac.uk/trac/ncWMS/
* http://www.unidata.ucar.edu/software/thredds/current/tds/TDS.html
* http://www.unidata.ucar.edu/software/thredds/current/tds/tds4.2/UpgradingTo4.2.html#wmsConfigFile
* http://www.unidata.ucar.edu/software/thredds/current/tds/tds4.3/reference/WMS.html
* http://www.resc.reading.ac.uk/trac/myocean-tools/wiki/WmsDetailedConfiguration

WPS
---

* http://www.zoo-project.org/
* http://www.zoo-project.org/docs/workshop/2010/building_wps_client_using_ol.html


opendap
-------

* avail clients
  * http://www.opendap.org/whatClients
  * http://www.pydap.org/client.html
  * http://code.google.com/p/netcdf4-python/issues/detail?id=154


Netcdf/grib
-----------

there are different python libs available:

* http://www-pord.ucsd.edu/~cjiang/python.html
* http://www.scipy.org/
* http://netcdf4-python.googlecode.com/svn/trunk/docs/netCDF4-module.html
* https://pypi.python.org/pypi/pupynere/
* http://pygrib.googlecode.com/svn/trunk/docs/index.html

Troubleshootig:

* http://stackoverflow.com/questions/9449309/how-to-correctly-install-python-numpy-in-ubuntu-11-10-oneiric

iris: scientific tools for meteo data using netcdf

* https://github.com/SciTools/iris


Imaging
-------

* http://www.lcdf.org/gifsicle/
* https://github.com/python-imaging/Pillow/blob/master/Scripts/gifmaker.py
* http://python-imaging.github.io/
  
Docs
----

* DONE: use buildout theme for sphinx
  * https://github.com/ryan-roemer/sphinx-bootstrap-theme


install nco
-----------

binaries:

* http://nco.sourceforge.net/#Binaries
* http://nco.cvs.sf.net/nco/nco/doc/beta.txt

source:

* http://nco.sourceforge.net/#bld



Visualisation
-------------

* climate data visualisation with python, examples:
  * http://earthpy.org/near_realtime_data_from_arctic_ice_mass_balance_buoys.html
  * http://ocefpaf.github.io/blog/2013/11/25/waves/
* try shiny for data visualisation with R:
  * http://www.rstudio.com/shiny/showcase/
* use matplotlib basemap with wms
  * http://matplotlib.org/basemap/
  * https://github.com/matplotlib/basemap/blob/master/examples/testwmsimage.py
* try pyngl for visualisation
* ncl, grads -> ask joerg
* ferret
  * http://ferret.pmel.noaa.gov/Ferret/documentation/pyferret/build-install/

openlayers
----------

* seperate 2d and 3d map

* http://openlayers.org/
* http://dev.openlayers.org/
* http://trac.osgeo.org/openlayers/wiki/Documentation
* http://dev.openlayers.org/docs/files/OpenLayers-js.html
* http://svn.openlayers.org/addins/
* https://github.com/mpriour
* http://openlayers.org/dev/examples/wmst.html
* http://openlayers.org/dev/examples/wps.html
* http://openlayers.org/dev/examples/wps-client.html
* http://dev.openlayers.org/sandbox/mpriour/temporal_map/openlayers/examples/time-control.html

openid
------

* https://github.com/cd34/apex
* http://thesoftwarestudio.com/apex/
* http://pieceofpy.com/blog/2011/07/24/pyramid-and-velruse-for-google-authentication/
* https://pypi.python.org/pypi/velruse
* https://pypi.python.org/pypi/pyramid-openid
* https://github.com/openid/python-openid
* http://stackoverflow.com/questions/16251231/how-to-fix-https-openid-error
* https://dashboard.janrain.com

persona
-------

* https://pyramid_persona.readthedocs.org/en/latest/

Other
-----

* check python for geosciences
  * http://pyaos.johnny-lin.com/?p=1271
  * https://github.com/koldunovn/python_for_geosciences
  * http://pyaos.johnny-lin.com/?p=1282
  * http://earthpy.org/
* check glues:
  * http://geoportal.glues.geo.tu-dresden.de/geoportal/index.php
* check earthsystem cog (django + esgf search):
  * https://github.com/EarthSystemCoG
  * http://earthsystemcog.org/
* check geonode: http://elogeo.nottingham.ac.uk/xmlui/handle/url/285
* check 0install: http://0install.net/user-guide-first-launch.html
* integrate local opendap source
* DONE: integrate local files source
* check python Nio
* set view permisson for most views
* maybe use anaconda python installation
  * http://www.continuum.io/downloads
* check apache climate workbench
http://climate.incubator.apache.org/
* check: 
  * http://pydoc.net/search?q=pyramid&qfor=package
  * http://pydoc.net/Python/pyramid_mongodb/1.3/
  * http://pydoc.net/Python/pyramid_mongoengine/0.1/
* check http://pydoc.net/Python/pyramid_deform/0.2a5/
* check http://dirac.cnrs-orleans.fr/plone/software/scientificpython/
* check enthougt packages for linux https://www.enthought.com/products/epd/
* check https://pypi.python.org/pypi/python-openid/ 
* check cookies with urllib:
http://stackoverflow.com/questions/3334809/python-urllib2-how-to-send-cookie-with-urlopen-request
* check ogr/gdal access to opendap: http://gis-lab.info/docs/gdal/gdal_ogr_user_docs.html#dods
* check https://github.com/SciTools
* check pydap
* check geonode http://geonode.org/
* check salt(security, renderer, ...) and zeromq (p2p communication)
* maybe use salt for configuration, install etc ...
* maybe deploy with virtualenv
* try accessing wcs server
* try accessing with openlayers
* try with udig
* evalute security concepts (check also salt)
* check gunicorn async:
    * http://docs.gunicorn.org/en/latest/install.html#async-workers
* handle external git repos (pywps, owslib)
    * DONE: fork both on github 
* check 52 north wps
  * http://52north.org/communities/geoprocessing/wps/index.html
* check modules on nextgis: https://github.com/nextgis
* get into catalogs service, usage of iso metadata, rdf schema, query, ...
        * http://de.wikipedia.org/wiki/Web_Catalogue_Service
        * http://de.wikipedia.org/wiki/GeoNetwork
* DONE: check demo app:
  https://github.com/Pylons/shootout
