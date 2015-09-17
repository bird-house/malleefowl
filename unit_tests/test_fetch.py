import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from __init__ import TESTDATA

def test_testdata():
    raise SkipTest
    nose.tools.ok_( len(TESTDATA) > 0, TESTDATA )
    nose.tools.ok_( 'file:///' in TESTDATA.values()[0], TESTDATA)
    #nose.tools.ok_(False, TESTDATA)
