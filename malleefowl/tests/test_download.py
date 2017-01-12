import pytest

import os
from malleefowl.download import download
from malleefowl.tests.common import TESTDATA


@pytest.mark.online
def test_download():
    result = download(TESTDATA['noaa_nc_1'])
    assert os.path.basename(result) == "slp.1955.nc"
    assert 'file:///' not in result


@pytest.mark.online
def test_download_with_file_url():
    result = download(TESTDATA['noaa_nc_1'], use_file_url=True)
    assert os.path.basename(result) == "slp.1955.nc"
    assert 'file:///' in result
