import nose.tools
from unittest import TestCase
from nose import SkipTest
from nose.plugins.attrib import attr

from os.path import basename

from __init__ import TESTDATA

from malleefowl.download import download, wget_download

class DownloadTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.slp_1955_nc = "http://www.esrl.noaa.gov/psd/thredds/fileServer/Datasets/ncep.reanalysis.dailyavgs/surface/slp.1955.nc"
        cls.tasmax_nc = TESTDATA['tasmax_WAS-44_MPI-M-MPI-ESM-LR_historical_r1i1p1_MPI-CSC-REMO2009_v1_day_20010101-20051231.nc']
        
    @attr('online')
    @attr('slow')
    def test_download(self):
        filename = download(
            url=self.slp_1955_nc,
            cache_enabled=False)
        # avoid caching
        filename = download(
            url=self.slp_1955_nc,
            cache_enabled=True)

    @attr('online')
    @attr('testdata')
    def test_wget_download(self):
        result = wget_download(self.slp_1955_nc)
        nose.tools.ok_(basename(result) == "slp.1955.nc", result)

