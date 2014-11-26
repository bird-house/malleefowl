import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl.download import download

@attr('online')
@attr('slow')
def test_download():
    filename = download(url="http://www.esrl.noaa.gov/psd/thredds/fileServer/Datasets/ncep.reanalysis.dailyavgs/surface/slp.1955.nc", cache_enabled=False)
    # avoid caching
    filename = download(url="http://www.esrl.noaa.gov/psd/thredds/fileServer/Datasets/ncep.reanalysis.dailyavgs/surface/slp.1955.nc", cache_enabled=True)

