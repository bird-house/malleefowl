import nose.tools
from nose import SkipTest

from malleefowl import wpsclient

service = "http://localhost:8090/wps"

import json
import tempfile

def test_execute():
    result = wpsclient.execute(
        service = service,
        identifier = "org.malleefowl.test.dummyprocess",
        inputs = [('input1', '2'), ('input2', '3')],
        outputs = [('output1', True), ('output2', True)]
        )
    nose.tools.ok_(len(result) == 2)

    f = tempfile.NamedTemporaryFile(mode='w+')
    wpsclient.write_result(f.name, 'execute', result, format='json')
    obj = json.load(f)
    nose.tools.ok_(len(obj) == 2)
    nose.tools.ok_(obj[0]['reference'] != None)
    nose.tools.ok_(obj[1]['reference'] != None)
