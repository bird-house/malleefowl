import nose.tools
from unittest import TestCase
from nose import SkipTest
from nose.plugins.attrib import attr

from os.path import basename

from malleefowl.download import download, download_with_archive, download_files

class DownloadTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.slp_1955_nc = "http://www.esrl.noaa.gov/psd/thredds/fileServer/Datasets/ncep.reanalysis.dailyavgs/surface/slp.1955.nc"
        
    @attr('online')
    def test_wget_download(self):
        result = download(self.slp_1955_nc)
        nose.tools.ok_(basename(result) == "slp.1955.nc", result)
        nose.tools.ok_(not 'file:///' in result, result)

    @attr('online')
    def test_wget_download_with_file_url(self):
        result = download(self.slp_1955_nc, use_file_url=True)
        nose.tools.ok_(basename(result) == "slp.1955.nc", result)
        nose.tools.ok_('file:///' in result, result)

    @attr('online')
    def test_download_files(self):
        results = download_files(urls=[self.slp_1955_nc])
        nose.tools.ok_(len(results) == 1, results)
        
        

