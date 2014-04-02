import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

import tempfile

    
def test_icclim2():
    #/home/main/Software/icclim_fiona/bin/nosetests unit_tests/test_cscenv.py
    from malleefowl import cscenv
    files = ['/var/lib/pywps/files/nils.hempelmann_hzg.de/tasmax_day_MPI-ESM-LR_historical_r1i1p1_20000101-20051231.nc']
    result = cscenv.indices( files, icclim_SU=True )
    
    nose.tools.ok_('nc' in result, result)
    