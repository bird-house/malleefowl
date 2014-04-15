import nose.tools 
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import tokenmgr, wpsclient

import __init__ as base

TEST_TOKEN = tokenmgr.get_uuid()

def setup():
    tokenmgr.init()
    tokenmgr._add(TEST_TOKEN, 'amelie@montematre.org')

@attr('online')
def test_wget():
    result = wpsclient.execute(
        service = base.SERVICE,
        identifier = "org.malleefowl.esgf.wget.source",
        inputs = [
            ('file_identifier', 'http://bmbf-ipcc-ar5.dkrz.de/thredds/fileServer/cmip5/output1/MPI-M/MPI-ESM-LR/historical/day/atmos/day/r1i1p1/v20111006/tas/tas_day_MPI-ESM-LR_historical_r1i1p1_20000101-20051231.nc'),
            ('token', TEST_TOKEN),
            ('credentials', '')
            ],
        
        outputs = [('output', True), ('sidecar', True)]
        )
    nose.tools.ok_(len(result) == 2, result)
    nose.tools.ok_('.nc' in result[0]['reference'])
    nose.tools.ok_('.json' in result[1]['reference'])

    import urllib
    content = urllib.urlopen(result[1]['reference']).read()
    nose.tools.ok_('tas_day_MPI-ESM-LR_historical_r1i1p1_20000101-20051231.nc' in content, content)
    
