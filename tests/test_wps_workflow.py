import tempfile
import yaml

import pytest
from pywps import Service
from pywps.tests import assert_response_success

from .common import TESTDATA, client_for

from malleefowl.processes.wps_workflow import DispelWorkflow


@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.online
def test_wps_thredds_workflow():
    doc = """
    workflow:
      name: test_thredds_workflow
    source:
      thredds:
        catalog_url: {0}
    worker:
      identifier: dummy
      url: http://localhost:5000/wps
      resource: dataset
      inputs: []
    """.format(TESTDATA['noaa_catalog_1'])
    fp = tempfile.NamedTemporaryFile(suffix=".txt")
    yaml.dump(yaml.load(doc), fp)

    client = client_for(Service(processes=[DispelWorkflow()]))
    datainputs = "workflow@xlink:href=file://{0}".format(fp.name)
    resp = client.get(
        service='wps', request='execute', version='1.0.0', identifier='workflow',
        datainputs=datainputs)
    assert_response_success(resp)
