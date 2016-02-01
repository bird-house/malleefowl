import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from tests.common import WpsTestClient, TESTDATA

@attr('online')
def test_wps_thredds_download():
    wps = WpsTestClient()
    datainputs = "[url={0}]".format(TESTDATA['noaa_catalog_1'])
    resp = wps.get(service='wps', request='execute', version='1.0.0', identifier='thredds_download',
                   datainputs=datainputs)
