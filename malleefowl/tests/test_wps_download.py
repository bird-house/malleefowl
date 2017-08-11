import pytest
from pywps import Service
from pywps.tests import assert_response_success

from .common import TESTDATA, client_for

from malleefowl.processes.wps_download import Download


@pytest.mark.online
def test_wps_download():
    client = client_for(Service(processes=[Download()]))
    datainputs = "resource={}".format(TESTDATA['noaa_nc_1'])
    resp = client.get(
        service='WPS', request='Execute', version='1.0.0',
        identifier='download',
        datainputs=datainputs)
    assert_response_success(resp)
