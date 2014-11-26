import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl.download import download

def test_download():
    filename = download(url="http://www.esrl.noaa.gov/psd/thredds/fileServer/Datasets/ncep.reanalysis.dailyavgs/surface/slp.1955.nc")
    nose.tools.ok_(False, filename)
