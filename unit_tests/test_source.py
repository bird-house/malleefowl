from nose.tools import ok_, with_setup, raises
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import source, tokenmgr

def setup():
    tokenmgr.init()

def test_list_files():
    files = source.list_files(tokenmgr.test_token(), 'nc')
    ok_(len(files) > 0, files)

def test_get_files():
    file_path = source.get_files(tokenmgr.test_token(), 'test1.nc')
    ok_('test1.nc' in file_path, file_path)
