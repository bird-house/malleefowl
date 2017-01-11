import pytest
from unittest import TestCase

import os
from malleefowl.download import download, download_with_archive, download_files
from malleefowl.tests.common import TESTDATA


class DownloadTestCase(TestCase):

    @pytest.mark.online
    def test_download(self):
        result = download(TESTDATA['noaa_nc_1'])
        assert os.path.basename(result) == "slp.1955.nc"
        assert 'file:///' not in result

    @pytest.mark.online
    def test_download_with_file_url(self):
        result = download(TESTDATA['noaa_nc_1'], use_file_url=True)
        assert os.path.basename(result) == "slp.1955.nc"
        assert 'file:///' in result
