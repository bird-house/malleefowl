import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from owslib.wps import WebProcessingService, monitorExecution

import __init__ as base

from malleefowl import tokenmgr, database

TEST_TOKEN = tokenmgr.get_uuid()
TEST_USERID = 'test@malleefowl.org'

def setup():
    database.add_token(token=TEST_TOKEN, userid=TEST_USERID)

@attr('online')
def test_inout():
    wps = WebProcessingService(base.SERVICE, verbose=False)
    execution = wps.execute(
        'org.malleefowl.test.inout',
        inputs=[('string', TEST_USERID), ('intRequired', '0')],
        output=[('string', False)])
    monitorExecution(execution, sleepSecs=1)
    
    result = execution.processOutputs[0].data[0]
    nose.tools.ok_(TEST_USERID in result, result)
    
   
