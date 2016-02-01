import nose.tools
from nose.plugins.attrib import attr

from tests.common import WpsTestClient, TESTDATA, assert_response_success

import tempfile
import yaml

@attr('online')
def test_wps_dummy():
    wps = WpsTestClient()
    datainputs = "[dataset={0}]".format(TESTDATA['noaa_nc_1'])
    resp = wps.get(service='wps', request='execute', version='1.0.0', identifier='dummy',
                   datainputs=datainputs)
    assert_response_success(resp)

@attr('online')
def test_wps_thredds_workflow():
    doc = """
    workflow:
      name: test_thredds_workflow
    source:
      thredds:
        catalog_url: {0}
    worker:
      identifier: dummy
      url: http://localhost:8091/wps
      resource: dataset
    """.format(TESTDATA['noaa_catalog_1'])
    fp = tempfile.NamedTemporaryFile(suffix=".txt")
    yaml.dump(yaml.load(doc), fp)
    
    wps = WpsTestClient()
    datainputs = "[workflow=file://{0}]".format(fp.name)
    resp = wps.get(service='wps', request='execute', version='1.0.0', identifier='workflow',
                   datainputs=datainputs)
    assert_response_success(resp)
