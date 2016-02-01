import nose.tools
from nose import SkipTest

import os
import pywps
from xml.dom import minidom

caps_request = "service=wps&request=getcapabilities"
wpsns = "http://www.opengis.net/wps/1.0.0"

import logging

def test_caps():
    pywps_path = os.path.dirname(pywps.__file__)
    os.environ['PYWPS_CFG'] = os.path.abspath(os.path.join(pywps_path, '..', '..', '..', '..', 'etc', 'pywps', 'malleefowl.cfg'))
    os.environ['REQUEST_METHOD'] = pywps.METHOD_GET
  
    wps = pywps.Pywps(os.environ)
    logging.info(os.environ)
    inputs = wps.parseRequest(caps_request)
    wps.performRequest(inputs)
    xmldom = minidom.parseString(wps.response)
    nose.tools.ok_(len(xmldom.getElementsByTagNameNS(wpsns,"Process")) == 9)

