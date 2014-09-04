import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import publish

def test_to_local_store():
    result = publish.to_local_store()
    nose.tools.ok_(result == [])
