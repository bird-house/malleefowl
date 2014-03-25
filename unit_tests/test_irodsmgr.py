from nose.tools import ok_, with_setup, raises
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import irodsmgr, tokenmgr

def setup():
    tokenmgr.init()

def test_list_files():
    files = irodsmgr.list_files(token=tokenmgr.test_token(), filter='nc', folder='/DKRZ_CORDEX_Zone/home/public/wps/test1')
    ok_(len(files) > 0, files)
    ok_('pr_Amon_MPI-ESM-LR_amip_r1i1p1_197901-200812.nc' in files, files)
