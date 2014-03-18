import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import wpsclient

import __init__ as base

import json
import tempfile

@attr('online')
def test_dummy():
    result = wpsclient.execute(
        service = base.SERVICE,
        identifier = "org.malleefowl.test.dummyprocess",
        inputs = [('input1', '2'), ('input2', '3')],
        outputs = [('output1', True), ('output2', True)]
        )
    nose.tools.ok_(len(result) == 2, result)
    nose.tools.ok_('http' in result[0]['reference'])
    nose.tools.ok_('http' in result[1]['reference'])

    
    
@attr('online')
def test_indices():
    result = wpsclient.execute(
        service = base.SERVICE,
        identifier = "de.csc.indices.worker",
        inputs = [('file_identifier', 'http://localhost:8080/thredds/fileServer/test/nils.hempelmann_hzg.de/tas_MNA-44_ICHEC-EC-EARTH_historical_r12i1p1_SMHI-RCA4_v1_day_20010101-20051231.nc'), 
        ('file_identifier', 'http://localhost:8080/thredds/fileServer/test/nils.hempelmann_hzg.de/pr_MNA-44_ICHEC-EC-EARTH_historical_r12i1p1_SMHI-RCA4_v1_day_20010101-20051231.nc'),
        ('tas_yearmean', 'True'), 
        ('pr_yearsum', 'False'), 
        ('tas_5to9mean', 'False'), 
        ('tas_6to8mean', 'True'),
        ('pr_5to9sum', 'False'), 
        ('pr_6to8sum', 'True'), 
        ('heavyprecip','True'), 
        ('prThreshold','20') ],
        outputs = [('output', True)],
        verbose=True
        )
        
    nose.tools.ok_('tar' in result[0]['reference'], result)
   
