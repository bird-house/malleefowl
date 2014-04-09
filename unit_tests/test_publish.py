import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import publish

def test_publish_local():
    result = publish.publish_local()
    nose.tools.ok_(result == [])
