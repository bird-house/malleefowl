import nose.tools
from unittest import TestCase
from nose.plugins.attrib import attr

import os
from malleefowl.download import download, download_with_archive, download_files
from tests.common import TESTDATA

class DownloadTestCase(TestCase):

    @attr('online')
    def test_download(self):
        result = download(TESTDATA['noaa_nc_1'])
        nose.tools.ok_(os.path.basename(result) == "slp.1955.nc", result)
        nose.tools.ok_(not 'file:///' in result, result)

    @attr('online')
    def test_download_with_file_url(self):
        result = download(TESTDATA['noaa_nc_1'], use_file_url=True)
        nose.tools.ok_(os.path.basename(result) == "slp.1955.nc", result)
        nose.tools.ok_('file:///' in result, result)


        
        

