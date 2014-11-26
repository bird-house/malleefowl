import nose.tools
from nose import SkipTest

import pywps
from lxml import etree
from StringIO import StringIO

execute_request = "service=wps&request=execute&version=1.0.0&identifier=dummyprocess&datainputs=[input1=20;input2=10]"
wpsns = "http://www.opengis.net/wps/1.0.0"
ns = {'wps': wpsns}

def test_dummy():
    raise SkipTest
    mypywps = pywps.Pywps(pywps.METHOD_GET)
    inputs = mypywps.parseRequest(execute_request)
    mypywps.performRequest(inputs)
    doc = etree.parse(StringIO(mypywps.response))
    r = doc.xpath("//*/wps:Output", namespaces=ns)
    nose.tools.ok_(len(r) == 2, r)

