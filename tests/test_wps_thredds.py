import pytest
from pywps import Service
from pywps.tests import assert_response_success

from .common import TESTDATA, client_for

from malleefowl.processes.wps_thredds import ThreddsDownload


@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.online
def test_wps_thredds_download():
    client = client_for(Service(processes=[ThreddsDownload()]))
    datainputs = "url=@xlink:href={}".format(TESTDATA['noaa_catalog_1'])
    resp = client.get(
        service='WPS', request='Execute', version='1.0.0',
        identifier='thredds_download',
        datainputs=datainputs)
    assert_response_success(resp)
