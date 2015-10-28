import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from owslib.wps import WebProcessingService, monitorExecution

import __init__ as base

def setup():
    pass

@attr('online')
def test_inout():
    raise SkipTest
    wps = WebProcessingService(base.SERVICE, verbose=False)
    execution = wps.execute(
        'inout',
        inputs=[('intRequired', '0')],
        output=[('string', False)])
    monitorExecution(execution, sleepSecs=1)
    
    result = execution.processOutputs[0].data[0]
    nose.tools.ok_(False, result)
    
   
