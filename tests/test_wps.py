import nose.tools
from nose import SkipTest

import pywps
from xml.dom import minidom

caps_request = "service=wps&request=getcapabilities"
wpsns = "http://www.opengis.net/wps/1.0.0"

def test_caps():
    raise SkipTest

    mypywps = pywps.Pywps(pywps.METHOD_GET)
    inputs = mypywps.parseRequest(caps_request)
    mypywps.performRequest(inputs)
    xmldom = minidom.parseString(mypywps.response)
    nose.tools.ok_(len(xmldom.getElementsByTagNameNS(wpsns,"Process")) > 0)

