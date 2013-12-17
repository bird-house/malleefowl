import nose.tools
from nose import SkipTest

import pywps
from xml.dom import minidom

getcapabilitiesrequest = "service=wps&request=getcapabilities"
wpsns = "http://www.opengis.net/wps/1.0.0"

def test_caps():
    mypywps = pywps.Pywps(pywps.METHOD_GET)
    inputs = mypywps.parseRequest(getcapabilitiesrequest)
    mypywps.performRequest(inputs)
    xmldom = minidom.parseString(mypywps.response)
    nose.tools.ok_(len(xmldom.getElementsByTagNameNS(wpsns,"Process")) > 0)

