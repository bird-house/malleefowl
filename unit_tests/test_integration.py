import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import wpsclient

import __init__ as base

import json
import tempfile

@attr('integration')
def test_inout():
    raise SkipTest
    result = wpsclient.execute(
        service = base.SERVICE,
        identifier = "org.malleefowl.test.inout",
        inputs = [('intRequired', '2')],
        )
    nose.tools.ok_(len(result) == 12, len(result))
    nose.tools.ok_('intRequired' in str(result), result)

@attr('integration')
def test_visualisation():
    raise SkipTest
    result = wpsclient.execute(
        service = base.SERVICE,
        identifier = "de.csc.visualisation.worker",
        inputs = [('variable', 'tas'),
                  ('file_identifier',
                   'https://mouflon.dkrz.de/wpsoutputs/tutorial/tas_EUR-11_MPI-M-MPI-ESM-LR_historical_r1i1p1_SMHI-RCA4_v1_day_19700101-19701231.nc')],
        outputs=[("output", True)]
        
        )
    nose.tools.ok_(len(result) == 1, len(result))
    nose.tools.ok_('html' in result[0]['reference'], result)

@attr('integration')
def test_cdo_sinfo():
    raise SkipTest
    result = wpsclient.execute(
        service = base.SERVICE,
        identifier = "de.dkrz.cdo.sinfo.worker",
        inputs=[('file_identifier',
                 'https://mouflon.dkrz.de/wpsoutputs/tutorial/tas_EUR-11_MPI-M-MPI-ESM-LR_historical_r1i1p1_SMHI-RCA4_v1_day_19700101-19701231.nc')],
        outputs=[("output", True)]
        
        )
    nose.tools.ok_(len(result) == 1, len(result))
    nose.tools.ok_('txt' in result[0]['reference'], result)


